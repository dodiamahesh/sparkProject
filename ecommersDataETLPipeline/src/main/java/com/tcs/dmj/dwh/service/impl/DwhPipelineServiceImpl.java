package com.tcs.dmj.dwh.service.impl;

import com.tcs.dmj.dwh.config.ApplicationProperties;
import com.tcs.dmj.dwh.domain.MetaPipeline;
import com.tcs.dmj.dwh.domain.MetaPipelineRun;
import com.tcs.dmj.dwh.enums.PipelineStatus;
import com.tcs.dmj.dwh.pipeline.EtlPipeline;
import com.tcs.dmj.dwh.pipeline.JobMetrics;
import com.tcs.dmj.dwh.pipeline.PipelineContext;
import com.tcs.dmj.dwh.pipeline.PipelineRegistry;
import com.tcs.dmj.dwh.repository.MetaPipelineRepository;
import com.tcs.dmj.dwh.repository.MetaPipelineRunRepository;
import com.tcs.dmj.dwh.service.DwhPipelineService;
import com.tcs.dmj.dwh.service.LineageService;
import com.tcs.dmj.dwh.service.PipelineAlreadyRunningException;
import com.tcs.dmj.dwh.service.PipelineRunNotFoundException;
import com.tcs.dmj.dwh.service.SourceCommitService;
import com.tcs.dmj.dwh.service.dto.PipelineInfoDTO;
import com.tcs.dmj.dwh.service.dto.SourceInfoDTO;
import com.tcs.dmj.dwh.utils.ErrorMessages;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Runs a pipeline from start to end (in the calling thread: the REST call returns when the run is done)
 * and records everything in the meta tables. There is ONE way to run: INCREMENTAL by timestamp.
 *
 *   0. at start-up             -> every pipeline of the code is registered in meta.meta_pipeline
 *   1. lock the pipeline (no double runs)
 *   2. window end = now; create run row in meta.meta_pipeline_run (status RUNNING, last_run_time = window end)
 *   3. pipeline.run()          -> per source: read updatedAt > meta_pipeline.last_run_time AND <= window end,
 *                                 validate, transform, load (every load records its table lineage)
 *   4. commit                  -> save rejects, meta_pipeline.last_run_time = window end
 *      (pipelines with directLoad(): 3 + 4 run in ONE transaction -> all-or-nothing)
 *   5. finish run row          -> SUCCESS / SUCCESS_WITH_FAILED / FAILED + counts + error
 *   6. successful run          -> lineage: meta_asset, meta_lineage, meta_column_lineage
 *   7. always: free Spark cache, release lock
 */
@Service
public class DwhPipelineServiceImpl implements DwhPipelineService {

    private static final Logger log = LoggerFactory.getLogger(DwhPipelineServiceImpl.class);

    private final PipelineRegistry pipelineRegistry;
    private final MetaPipelineRepository metaPipelineRepository;
    private final MetaPipelineRunRepository pipelineRunRepository;
    private final SourceCommitService commitService;
    private final LineageService lineageService;
    private final PipelineRunLock runLock;
    private final SparkSession spark;
    private final TransactionTemplate transaction;
    private final String applicationJobName;

    public DwhPipelineServiceImpl(PipelineRegistry pipelineRegistry, MetaPipelineRepository metaPipelineRepository,
                                  MetaPipelineRunRepository pipelineRunRepository, SourceCommitService commitService,
                                  LineageService lineageService, PipelineRunLock runLock, SparkSession spark,
                                  TransactionTemplate transaction, ApplicationProperties properties) {
        this.pipelineRegistry = pipelineRegistry;
        this.metaPipelineRepository = metaPipelineRepository;
        this.pipelineRunRepository = pipelineRunRepository;
        this.commitService = commitService;
        this.lineageService = lineageService;
        this.runLock = runLock;
        this.spark = spark;
        this.transaction = transaction;
        this.applicationJobName = properties.spark().appName();
    }

    @Override
    public List<PipelineInfoDTO> listPipelines() {
        Map<String, MetaPipeline> registered = metaPipelineRepository.findAll().stream()
                .collect(Collectors.toMap(MetaPipeline::pipelineName, Function.identity()));
        return pipelineRegistry.getAllPipelines().stream()
                .map(pipeline -> {
                    MetaPipeline meta = registered.get(pipeline.name());
                    return new PipelineInfoDTO(
                            meta == null ? 0 : meta.pipelineId(),
                            pipeline.name(),
                            pipeline.pipelineType().name(),
                            pipeline.description(),
                            pipeline.sourceCollections(),
                            pipeline.sources().stream().map(SourceInfoDTO::from).toList(),
                            pipeline.targetTables(),
                            meta == null ? null : meta.lastRunTime(),
                            pipelineRunRepository.isRunning(pipeline.name()));
                })
                .toList();
    }

    @Override
    public MetaPipelineRun runPipeline(String pipelineName) {
        EtlPipeline pipeline = pipelineRegistry.getPipeline(pipelineName);
        try (PipelineRunLock.Held lock = runLock.tryAcquire(pipeline.name())) {
            if (lock == null) {
                throw new PipelineAlreadyRunningException(pipeline.name());
            }
            // we hold the lock: a RUNNING row of this pipeline can only be left behind by a process that died
            int interrupted = pipelineRunRepository.failInterruptedRuns(pipeline.name());
            if (interrupted > 0) {
                log.warn("{}: {} run(s) left RUNNING by a stopped process marked FAILED", pipeline.name(), interrupted);
            }
            // Upper bound of the window for EVERY source of this run. Data changed after this moment is read next run.
            // MILLIS (not seconds): MongoDB dates have millisecond precision, so a document changed in the same second
            // as the run start is still inside the window and can never fall between two runs.
            Instant windowEnd = Instant.now().truncatedTo(ChronoUnit.MILLIS);
            long pipelineRunId = pipelineRunRepository.createRun(pipeline.name(), windowEnd);
            execute(pipeline, pipelineRunId, windowEnd);
            return pipelineRunRepository.findById(pipelineRunId)
                    .orElseThrow(() -> new PipelineRunNotFoundException(pipelineRunId));
        }
    }

    private void execute(EtlPipeline pipeline, long pipelineRunId, Instant windowEnd) {
        PipelineContext context = new PipelineContext(pipelineRunId, pipeline.name(), windowEnd, pipeline.directLoad());
        long startMillis = System.currentTimeMillis();
        log.info("========== Job {} STARTED: pipeline={}, windowEnd={} ==========", pipelineRunId, pipeline.name(), windowEnd);

        // Groups all Spark jobs of this run under one name (visible in Spark logs / UI)
        spark.sparkContext().setJobGroup("etl-job-" + pipelineRunId, pipeline.name() + " until " + windowEnd, true);
        try {
            if (context.directLoad()) {
                // ALL-OR-NOTHING: every table + rejected records + last_run_time in ONE transaction.
                // Any exception -> ROLLBACK of everything.
                transaction.executeWithoutResult(status -> {
                    pipeline.run(context);
                    commitService.commit(context);
                });
                log.info("[job {}] transaction committed", pipelineRunId);
            } else {
                pipeline.run(context);
                commitService.commit(context);
            }

            JobMetrics metrics = context.metrics();
            long successRecords = metrics.validCount() + metrics.deletedCount();
            PipelineStatus status = PipelineStatus.of(successRecords, metrics.rejectedCount());
            boolean allRejected = status == PipelineStatus.FAILED;
            pipelineRunRepository.finishRun(pipelineRunId, status, metrics.sourceCount(), successRecords,
                    metrics.rejectedCount(),
                    allRejected ? "ALL_RECORDS_REJECTED" : null,
                    allRejected ? "All " + metrics.rejectedCount() + " records were rejected (see meta.meta_rejected_record)" : null);
            log.info("========== Job {} {} in {} ms: read={}, valid={}, rejected={}, deleted={}, loaded={} ==========",
                    pipelineRunId, status, System.currentTimeMillis() - startMillis,
                    metrics.sourceCount(), metrics.validCount(), metrics.rejectedCount(),
                    metrics.deletedCount(), metrics.loadedCount());
            if (!allRejected) {
                lineageService.record(pipeline, context);
            }

        } catch (Throwable error) {
            // Catch Throwable (not only Exception) so even an Error never leaves a run RUNNING
            log.error("========== Job {} FAILED: {} ==========", pipelineRunId, error.getMessage(), error);
            if (context.directLoad()) {
                context.metrics().resetLoaded();   // rolled back: nothing of this run is in the tables
            }
            saveFailure(pipelineRunId, context, error);
            if (error instanceof Error fatal) {
                throw fatal;
            }
        } finally {
            context.releaseResources();
            spark.sparkContext().clearJobGroup();
        }
    }

    private void saveFailure(long pipelineRunId, PipelineContext context, Throwable error) {
        try {
            JobMetrics metrics = context.metrics();
            pipelineRunRepository.finishRun(pipelineRunId, PipelineStatus.FAILED, metrics.sourceCount(),
                    metrics.validCount() + metrics.deletedCount(), metrics.rejectedCount(),
                    ErrorMessages.errorCode(error), ErrorMessages.shortMessage(error));
        } catch (RuntimeException auditError) {
            log.error("Could not save FAILED status for job {}", pipelineRunId, auditError);
        }
    }

    /**
     * On start-up (ApplicationStartedEvent: BEFORE the batch runner of an Airflow task starts its pipeline):
     *   1. every pipeline of the code is registered in meta.meta_pipeline (pipelines that are no longer in the
     *      code get active_flag 'N'; last_run_time is never touched here)
     *   2. runs left RUNNING by a process that died are marked FAILED. Only for pipelines whose run lock is free:
     *      a run that is busy in ANOTHER process (REST service, other Airflow task) is left alone.
     * These are also the first queries on the meta tables, so a database without tables stops the application
     * here with a clear message (the application does not create tables itself).
     */
    @EventListener(ApplicationStartedEvent.class)
    public void onStartup() {
        int closed = 0;
        try {
            for (EtlPipeline pipeline : pipelineRegistry.getAllPipelines()) {
                metaPipelineRepository.register(pipeline.name(), pipeline.pipelineType().name(),
                        pipeline.description(), applicationJobName, pipeline.airflowDagId());
            }
            int deactivated = metaPipelineRepository.deactivateAllExcept(
                    pipelineRegistry.getAllPipelines().stream().map(EtlPipeline::name).toList());
            if (deactivated > 0) {
                log.warn("{} pipeline(s) in meta.meta_pipeline are no longer in the code: active_flag = 'N'", deactivated);
            }
            for (String pipelineName : pipelineRunRepository.pipelinesWithRunningRuns()) {
                try (PipelineRunLock.Held lock = runLock.tryAcquire(pipelineName)) {
                    if (lock != null) {
                        closed += pipelineRunRepository.failInterruptedRuns(pipelineName);
                    }
                }
            }
        } catch (BadSqlGrammarException e) {
            throw new IllegalStateException("Meta tables not found in PostgreSQL (" + e.getMostSpecificCause().getMessage()
                    + "). Run sample-data/<job>/03_postgres_tables.sql for every job once, then start the application again.", e);
        }
        if (closed > 0) {
            log.warn("Marked {} run(s) as FAILED because their process stopped while they were running", closed);
        }
    }
}
