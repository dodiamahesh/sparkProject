package com.tcs.dmj.dwh.service.impl;

import com.tcs.dmj.dwh.domain.JobRun;
import com.tcs.dmj.dwh.enums.JobStatus;
import com.tcs.dmj.dwh.pipeline.EtlPipeline;
import com.tcs.dmj.dwh.pipeline.JobMetrics;
import com.tcs.dmj.dwh.pipeline.PipelineContext;
import com.tcs.dmj.dwh.pipeline.PipelineRegistry;
import com.tcs.dmj.dwh.repository.JobRunRepository;
import com.tcs.dmj.dwh.repository.WatermarkRepository;
import com.tcs.dmj.dwh.service.DwhPipelineService;
import com.tcs.dmj.dwh.service.SourceCommitService;
import com.tcs.dmj.dwh.service.dto.PipelineInfoDTO;
import com.tcs.dmj.dwh.service.dto.SourceInfoDTO;
import com.tcs.dmj.dwh.service.JobRunNotFoundException;
import com.tcs.dmj.dwh.utils.ErrorMessages;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

/**
 * Runs a pipeline from start to end (in the calling thread: the REST call returns when the job is done)
 * and records everything in the meta tables. There is ONE way to run: INCREMENTAL by timestamp (watermark).
 *
 *   1. lock the pipeline (no double runs)
 *   2. window end = now; create run row in meta.meta_pipeline_run (status NULL = running)
 *   3. pipeline.run()          -> per source: read updatedAt > last watermark AND <= window end,
 *                                 validate, transform, load
 *   4. commit                  -> save rejects, advance watermarks
 *      (pipelines with directLoad(): 3 + 4 run in ONE transaction -> all-or-nothing)
 *   5. finish run row          -> SUCCESS / SUCCESS_WITH_FAILED / FAILED + counts + error
 *   6. always: free Spark cache, release lock
 */
@Service
public class DwhPipelineServiceImpl implements DwhPipelineService {

    private static final Logger log = LoggerFactory.getLogger(DwhPipelineServiceImpl.class);

    private final PipelineRegistry pipelineRegistry;
    private final JobRunRepository jobRunRepository;
    private final SourceCommitService commitService;
    private final WatermarkRepository watermarkRepository;
    private final JobLockManager lockManager;
    private final SparkSession spark;
    private final TransactionTemplate transaction;

    public DwhPipelineServiceImpl(PipelineRegistry pipelineRegistry, JobRunRepository jobRunRepository,
                                  SourceCommitService commitService, WatermarkRepository watermarkRepository,
                                  JobLockManager lockManager, SparkSession spark, TransactionTemplate transaction) {
        this.pipelineRegistry = pipelineRegistry;
        this.jobRunRepository = jobRunRepository;
        this.commitService = commitService;
        this.watermarkRepository = watermarkRepository;
        this.lockManager = lockManager;
        this.spark = spark;
        this.transaction = transaction;
    }

    @Override
    public List<PipelineInfoDTO> listPipelines() {
        return pipelineRegistry.getAllPipelines().stream()
                .map(pipeline -> new PipelineInfoDTO(
                        pipeline.name(),
                        pipeline.description(),
                        pipeline.sourceCollections(),
                        pipeline.sources().stream().map(SourceInfoDTO::from).toList(),
                        pipeline.targetTables(),
                        watermarkRepository.findByPipeline(pipeline.name()),
                        lockManager.isRunning(pipeline.name())))
                .toList();
    }

    @Override
    public JobRun runPipeline(String pipelineName, String triggeredBy) {
        EtlPipeline pipeline = pipelineRegistry.getPipeline(pipelineName);
        lockManager.acquire(pipeline.name());
        try {
            // Upper bound of the window for EVERY source of this run. Data changed after this moment is read next run.
            // MILLIS (not seconds): MongoDB dates have millisecond precision, so a document changed in the same second
            // as the run start is still inside the window and can never fall between two runs.
            Instant windowEnd = Instant.now().truncatedTo(ChronoUnit.MILLIS);
            long jobRunId = jobRunRepository.createRun(pipeline.name(), windowEnd, triggeredBy);
            execute(pipeline, jobRunId, windowEnd);
            return jobRunRepository.findById(jobRunId).orElseThrow(() -> new JobRunNotFoundException(jobRunId));
        } finally {
            lockManager.release(pipeline.name());
        }
    }

    private void execute(EtlPipeline pipeline, long jobRunId, Instant windowEnd) {
        PipelineContext context = new PipelineContext(jobRunId, pipeline.name(), windowEnd, pipeline.directLoad());
        long startMillis = System.currentTimeMillis();
        log.info("========== Job {} STARTED: pipeline={}, windowEnd={} ==========", jobRunId, pipeline.name(), windowEnd);

        // Groups all Spark jobs of this run under one name (visible in Spark logs / UI)
        spark.sparkContext().setJobGroup("etl-job-" + jobRunId, pipeline.name() + " until " + windowEnd, true);
        try {
            if (context.directLoad()) {
                // ALL-OR-NOTHING: every table + rejected records + watermarks in ONE transaction.
                // Any exception -> ROLLBACK of everything.
                transaction.executeWithoutResult(status -> {
                    pipeline.run(context);
                    commitService.commit(context);
                });
                log.info("[job {}] transaction committed", jobRunId);
            } else {
                pipeline.run(context);
                commitService.commit(context);
            }

            JobMetrics metrics = context.metrics();
            JobStatus status = JobStatus.of(metrics.validCount() + metrics.deletedCount(), metrics.rejectedCount());
            String message = status == JobStatus.FAILED
                    ? "All " + metrics.rejectedCount() + " records were rejected (see meta.meta_rejected_record)"
                    : null;
            jobRunRepository.finishRun(jobRunId, status, metrics, message, null);
            log.info("========== Job {} {} in {} ms: read={}, valid={}, rejected={}, deleted={}, loaded={} ==========",
                    jobRunId, status, System.currentTimeMillis() - startMillis,
                    metrics.sourceCount(), metrics.validCount(), metrics.rejectedCount(),
                    metrics.deletedCount(), metrics.loadedCount());

        } catch (Throwable error) {
            // Catch Throwable (not only Exception) so even an Error never leaves a run without a status
            log.error("========== Job {} FAILED: {} ==========", jobRunId, error.getMessage(), error);
            if (context.directLoad()) {
                context.metrics().resetLoaded();   // rolled back: nothing of this run is in the tables
            }
            saveFailure(jobRunId, context, error);
            if (error instanceof Error fatal) {
                throw fatal;
            }
        } finally {
            context.releaseResources();
            spark.sparkContext().clearJobGroup();
        }
    }

    private void saveFailure(long jobRunId, PipelineContext context, Throwable error) {
        try {
            jobRunRepository.finishRun(jobRunId, JobStatus.FAILED, context.metrics(),
                    ErrorMessages.shortMessage(error), ErrorMessages.stackTrace(error));
        } catch (RuntimeException auditError) {
            log.error("Could not save FAILED status for job {}", jobRunId, auditError);
        }
    }

    /**
     * On startup: runs left without a status by a crash are marked FAILED. This is also the first query on
     * the meta tables, so a database without tables stops the application here with a clear message
     * (the application does not create tables itself).
     */
    @EventListener(ApplicationReadyEvent.class)
    public void closeInterruptedJobs() {
        int closed;
        try {
            closed = jobRunRepository.failInterruptedRuns();
        } catch (BadSqlGrammarException e) {
            throw new IllegalStateException("ETL tables not found in PostgreSQL (" + e.getMostSpecificCause().getMessage()
                    + "). Run sample-data/<job>/03_postgres_tables.sql for every job once, then start the application again.", e);
        }
        if (closed > 0) {
            log.warn("Marked {} job run(s) as FAILED because the application stopped while they were running", closed);
        }
    }
}
