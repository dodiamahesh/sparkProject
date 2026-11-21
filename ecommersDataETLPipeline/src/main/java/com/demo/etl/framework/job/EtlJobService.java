package com.demo.etl.framework.job;

import com.demo.etl.framework.audit.JobRunRepository;
import com.demo.etl.framework.audit.JobStatus;
import com.demo.etl.framework.audit.JobStepRepository;
import com.demo.etl.framework.common.ErrorMessages;
import com.demo.etl.framework.exception.JobRunNotFoundException;
import com.demo.etl.framework.pipeline.EtlPipeline;
import com.demo.etl.framework.pipeline.JobMetrics;
import com.demo.etl.framework.pipeline.PipelineContext;
import com.demo.etl.framework.pipeline.PipelineRegistry;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * Runs a pipeline from start to end (in the calling thread: the REST call returns when the job is done)
 * and records everything in the meta tables. There is ONE way to run: INCREMENTAL by timestamp (watermark).
 *
 *   1. lock the pipeline (no double runs)
 *   2. window end = now; create run row in meta.meta_pipeline_run (status NULL = running)
 *   3. pipeline.run()          -> per source: read updatedAt > last watermark AND <= window end,
 *                                 validate, transform, load
 *   4. commit                  -> save rejects, remove fixed rejects, advance watermarks
 *   5. finish run row          -> SUCCESS / SUCCESS_WITH_FAILED / FAILED + counts + error
 *   6. always: free Spark cache, release lock
 */
@Service
public class EtlJobService {

    private static final Logger log = LoggerFactory.getLogger(EtlJobService.class);

    private final PipelineRegistry pipelineRegistry;
    private final JobRunRepository jobRunRepository;
    private final JobStepRepository jobStepRepository;
    private final SourceCommitService commitService;
    private final JobLockManager lockManager;
    private final SparkSession spark;

    public EtlJobService(PipelineRegistry pipelineRegistry, JobRunRepository jobRunRepository,
                         JobStepRepository jobStepRepository, SourceCommitService commitService,
                         JobLockManager lockManager, SparkSession spark) {
        this.pipelineRegistry = pipelineRegistry;
        this.jobRunRepository = jobRunRepository;
        this.jobStepRepository = jobStepRepository;
        this.commitService = commitService;
        this.lockManager = lockManager;
        this.spark = spark;
    }

    /** Runs the pipeline and returns the finished run with all its steps. */
    public JobRunDetails runPipeline(String pipelineName, String triggeredBy) {
        EtlPipeline pipeline = pipelineRegistry.getPipeline(pipelineName);
        lockManager.acquire(pipeline.name());
        try {
            // Upper bound of the window for EVERY source of this run. Data changed after this moment is read next run.
            // MILLIS (not seconds): MongoDB dates have millisecond precision, so a document changed in the same second
            // as the run start is still inside the window and can never fall between two runs.
            Instant windowEnd = Instant.now().truncatedTo(ChronoUnit.MILLIS);
            long jobRunId = jobRunRepository.createRun(pipeline.name(), windowEnd, triggeredBy);
            execute(pipeline, jobRunId, windowEnd);
            return getJobRun(jobRunId);
        } finally {
            lockManager.release(pipeline.name());
        }
    }

    public JobRunDetails getJobRun(long jobRunId) {
        return jobRunRepository.findById(jobRunId)
                .map(run -> new JobRunDetails(run, jobStepRepository.findByJobRunId(jobRunId)))
                .orElseThrow(() -> new JobRunNotFoundException(jobRunId));
    }

    private void execute(EtlPipeline pipeline, long jobRunId, Instant windowEnd) {
        PipelineContext context = new PipelineContext(jobRunId, pipeline.name(), windowEnd);
        long startMillis = System.currentTimeMillis();
        log.info("========== Job {} STARTED: pipeline={}, windowEnd={} ==========", jobRunId, pipeline.name(), windowEnd);

        // Groups all Spark jobs of this run under one name (visible in Spark logs / UI)
        spark.sparkContext().setJobGroup("etl-job-" + jobRunId, pipeline.name() + " until " + windowEnd, true);
        try {
            pipeline.run(context);
            commitService.commit(context);

            JobMetrics metrics = context.metrics();
            JobStatus status = JobStatus.of(metrics.validCount(), metrics.rejectedCount());
            String message = status == JobStatus.FAILED
                    ? "All " + metrics.rejectedCount() + " records were rejected (see meta.meta_rejected_record)"
                    : null;
            jobRunRepository.finishRun(jobRunId, status, metrics, message, null);
            log.info("========== Job {} {} in {} ms: read={}, valid={}, rejected={}, loaded={}, resolved={} ==========",
                    jobRunId, status, System.currentTimeMillis() - startMillis,
                    metrics.sourceCount(), metrics.validCount(), metrics.rejectedCount(),
                    metrics.loadedCount(), metrics.resolvedCount());

        } catch (Throwable error) {
            // Catch Throwable (not only Exception) so even an Error never leaves a run without a status
            log.error("========== Job {} FAILED: {} ==========", jobRunId, error.getMessage(), error);
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
