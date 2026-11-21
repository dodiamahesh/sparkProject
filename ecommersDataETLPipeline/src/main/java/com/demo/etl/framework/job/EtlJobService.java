package com.demo.etl.framework.job;

import com.demo.etl.config.AsyncConfig;
import com.demo.etl.framework.audit.JobRunRepository;
import com.demo.etl.framework.audit.JobStatus;
import com.demo.etl.framework.audit.JobStepRepository;
import com.demo.etl.framework.common.ErrorMessages;
import com.demo.etl.framework.exception.EtlException;
import com.demo.etl.framework.exception.JobRunNotFoundException;
import com.demo.etl.framework.pipeline.EtlPipeline;
import com.demo.etl.framework.pipeline.PipelineContext;
import com.demo.etl.framework.pipeline.PipelineRegistry;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.task.TaskRejectedException;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * Runs a pipeline from start to end and records everything in the audit tables.
 * There is ONE way to run: INCREMENTAL by timestamp (watermark).
 *
 *   1. lock the pipeline (no double runs)
 *   2. window end = now; create job run row (status RUNNING, window_end)
 *   3. pipeline.run()          -> per source: read updatedAt > last watermark AND <= window end,
 *                                 validate, transform, load
 *   4. commit                  -> save rejects, remove fixed rejects, save warnings, advance watermarks
 *   5. finish job run row      -> SUCCESS / SUCCESS_WITH_REJECTS / FAILED + counts + error
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
    private final ThreadPoolTaskExecutor executor;
    private final SparkSession spark;

    public EtlJobService(PipelineRegistry pipelineRegistry, JobRunRepository jobRunRepository,
                         JobStepRepository jobStepRepository, SourceCommitService commitService,
                         JobLockManager lockManager,
                         @Qualifier(AsyncConfig.ETL_JOB_EXECUTOR) ThreadPoolTaskExecutor executor,
                         SparkSession spark) {
        this.pipelineRegistry = pipelineRegistry;
        this.jobRunRepository = jobRunRepository;
        this.jobStepRepository = jobStepRepository;
        this.commitService = commitService;
        this.lockManager = lockManager;
        this.executor = executor;
        this.spark = spark;
    }

    /**
     * @param async true  = return immediately (status RUNNING), poll GET /api/etl/jobs/{id}
     *              false = wait until the job ends and return the final status
     */
    public JobRunDetails runPipeline(String pipelineName, boolean async, String triggeredBy) {
        EtlPipeline pipeline = pipelineRegistry.getPipeline(pipelineName);
        lockManager.acquire(pipeline.name());

        // Upper bound of the window for EVERY source of this run. Data changed after this moment is read next run.
        // MILLIS (not seconds): MongoDB dates have millisecond precision, so a document changed in the same second
        // as the run start is still inside the window and can never fall between two runs.
        Instant windowEnd = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        long jobRunId;
        try {
            jobRunId = jobRunRepository.createRun(pipeline.name(), windowEnd, triggeredBy);
        } catch (RuntimeException e) {
            lockManager.release(pipeline.name());
            throw e;
        }

        if (async) {
            try {
                executor.execute(() -> execute(pipeline, jobRunId, windowEnd));
            } catch (TaskRejectedException e) {
                lockManager.release(pipeline.name());
                PipelineContext context = new PipelineContext(jobRunId, pipeline.name(), windowEnd);
                jobRunRepository.finishRun(jobRunId, JobStatus.FAILED, context.metrics(), "Job queue is full", null);
                throw new EtlException("Too many jobs are waiting. Try again later.", e);
            }
        } else {
            execute(pipeline, jobRunId, windowEnd);
        }
        return getJobRun(jobRunId);
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

            JobStatus status = context.metrics().rejectedCount() > 0 ? JobStatus.SUCCESS_WITH_REJECTS : JobStatus.SUCCESS;
            jobRunRepository.finishRun(jobRunId, status, context.metrics(), null, null);
            log.info("========== Job {} {} in {} ms: read={}, valid={}, rejected={}, loaded={}, resolved={}, warnings={} ==========",
                    jobRunId, status, System.currentTimeMillis() - startMillis,
                    context.metrics().sourceCount(), context.metrics().validCount(), context.metrics().rejectedCount(),
                    context.metrics().loadedCount(), context.metrics().resolvedCount(), context.metrics().warningCount());

        } catch (Throwable error) {
            // Catch Throwable (not only Exception) so even an Error never leaves a job stuck in RUNNING
            log.error("========== Job {} FAILED: {} ==========", jobRunId, error.getMessage(), error);
            saveFailure(jobRunId, context, error);
            if (error instanceof Error fatal) {
                throw fatal;
            }
        } finally {
            context.releaseResources();
            spark.sparkContext().clearJobGroup();
            lockManager.release(pipeline.name());
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
     * On startup: runs left in RUNNING by a crash are marked FAILED. This is also the first query on the
     * audit tables, so a database without tables stops the application here with a clear message
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
