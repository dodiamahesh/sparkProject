package com.tcs.dmj.dwh.service.impl;

import com.tcs.dmj.dwh.config.ApplicationProperties;
import com.tcs.dmj.dwh.core.EtlPipeline;
import com.tcs.dmj.dwh.core.JobMetrics;
import com.tcs.dmj.dwh.core.PipelineContext;
import com.tcs.dmj.dwh.core.PipelineRegistry;
import com.tcs.dmj.dwh.domain.MetaPipeline;
import com.tcs.dmj.dwh.domain.MetaPipelineRun;
import com.tcs.dmj.dwh.enums.PipelineStatus;
import com.tcs.dmj.dwh.exception.PipelineAlreadyRunningException;
import com.tcs.dmj.dwh.exception.PipelineConfigurationException;
import com.tcs.dmj.dwh.exception.PipelineRunNotFoundException;
import com.tcs.dmj.dwh.repository.MetaPipelineRepository;
import com.tcs.dmj.dwh.repository.MetaPipelineRunRepository;
import com.tcs.dmj.dwh.service.DwhPipelineService;
import com.tcs.dmj.dwh.service.LineageService;
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
 * Runs a pipeline from start to end in the calling thread:
 * lock -> run row (RUNNING) -> pipeline.run() -> commit (rejects + last_run_time) -> status -> lineage.
 * directLoad pipelines: run + commit in ONE transaction.
 */
@Service
public class DwhPipelineServiceImpl implements DwhPipelineService {

    private static final Logger log = LoggerFactory.getLogger(DwhPipelineServiceImpl.class);
    private static final String ALL_REJECTED = "ALL_RECORDS_REJECTED";

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
                .map(pipeline -> toInfo(pipeline, registered.get(pipeline.name())))
                .toList();
    }

    private PipelineInfoDTO toInfo(EtlPipeline pipeline, MetaPipeline meta) {
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
    }

    @Override
    public MetaPipelineRun runPipeline(String pipelineName) {
        EtlPipeline pipeline = pipelineRegistry.getPipeline(pipelineName);
        try (PipelineRunLock.Held lock = runLock.tryAcquire(pipeline.name())) {
            if (lock == null) {
                throw new PipelineAlreadyRunningException(pipeline.name());
            }
            // we hold the lock: a RUNNING row can only be left behind by a process that died
            int interrupted = pipelineRunRepository.failInterruptedRuns(pipeline.name());
            if (interrupted > 0) {
                log.warn("{}: {} run(s) left RUNNING by a stopped process marked FAILED", pipeline.name(), interrupted);
            }
            // millis: MongoDB dates have millisecond precision, so no document falls between two runs
            Instant windowEnd = Instant.now().truncatedTo(ChronoUnit.MILLIS);
            long pipelineRunId = pipelineRunRepository.createRun(pipeline.name(), windowEnd);
            execute(pipeline, pipelineRunId, windowEnd);
            return pipelineRunRepository.findById(pipelineRunId).orElseThrow(() -> new PipelineRunNotFoundException(pipelineRunId));
        }
    }

    private void execute(EtlPipeline pipeline, long pipelineRunId, Instant windowEnd) {
        PipelineContext context = new PipelineContext(pipelineRunId, pipeline.name(), windowEnd, pipeline.directLoad());
        long startMillis = System.currentTimeMillis();
        log.info("========== Job {} STARTED: pipeline={}, windowEnd={} ==========", pipelineRunId, pipeline.name(), windowEnd);
        spark.sparkContext().setJobGroup("etl-job-" + pipelineRunId, pipeline.name() + " until " + windowEnd, true);
        try {
            runAndCommit(pipeline, context);
            finishSuccessfulRun(pipeline, context, startMillis);
        } catch (Throwable error) {   // Throwable: even an Error never leaves a run RUNNING
            log.error("========== Job {} FAILED: {} ==========", pipelineRunId, error.getMessage(), error);
            if (context.directLoad()) {
                context.metrics().resetLoaded();   // rolled back
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

    /** directLoad: ONE transaction, any exception rolls everything back. */
    private void runAndCommit(EtlPipeline pipeline, PipelineContext context) {
        if (context.directLoad()) {
            transaction.executeWithoutResult(status -> {
                pipeline.run(context);
                commitService.commit(context);
            });
            log.info("[job {}] transaction committed", context.pipelineRunId());
        } else {
            pipeline.run(context);
            commitService.commit(context);
        }
    }

    private void finishSuccessfulRun(EtlPipeline pipeline, PipelineContext context, long startMillis) {
        JobMetrics metrics = context.metrics();
        long successRecords = metrics.validCount() + metrics.deletedCount();
        PipelineStatus status = PipelineStatus.of(successRecords, metrics.rejectedCount());
        boolean allRejected = status == PipelineStatus.FAILED;

        pipelineRunRepository.finishRun(context.pipelineRunId(), status, metrics.sourceCount(), successRecords,
                metrics.rejectedCount(),
                allRejected ? ALL_REJECTED : null,
                allRejected ? "All " + metrics.rejectedCount() + " records were rejected (see meta.meta_rejected_record)" : null);
        log.info("========== Job {} {} in {} ms: read={}, valid={}, rejected={}, deleted={}, loaded={} ==========",
                context.pipelineRunId(), status, System.currentTimeMillis() - startMillis,
                metrics.sourceCount(), metrics.validCount(), metrics.rejectedCount(),
                metrics.deletedCount(), metrics.loadedCount());
        if (!allRejected) {
            lineageService.record(pipeline, context);
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

    // ---------------------------------------------------------------- start-up

    /**
     * Before any run: register the pipelines of the code in meta.meta_pipeline and close runs left RUNNING by a
     * process that died (only when the pipeline's lock is free, so a run busy in another process is left alone).
     */
    @EventListener(ApplicationStartedEvent.class)
    public void onStartup() {
        try {
            registerPipelines();
            closeInterruptedRuns();
        } catch (BadSqlGrammarException e) {
            throw new PipelineConfigurationException("Meta tables not found in PostgreSQL ("
                    + e.getMostSpecificCause().getMessage() + "). Run sample-data/<job>/03_postgres_tables.sql "
                    + "for every job once, then start the application again.", e);
        }
    }

    private void registerPipelines() {
        for (EtlPipeline pipeline : pipelineRegistry.getAllPipelines()) {
            metaPipelineRepository.register(pipeline.name(), pipeline.pipelineType().name(),
                    pipeline.description(), applicationJobName, pipeline.name());   // DAG id = pipeline name
        }
        List<String> names = pipelineRegistry.getAllPipelines().stream().map(EtlPipeline::name).toList();
        int deactivated = metaPipelineRepository.deactivateAllExcept(names);
        if (deactivated > 0) {
            log.warn("{} pipeline(s) in meta.meta_pipeline are no longer in the code: active_flag = 'N'", deactivated);
        }
    }

    private void closeInterruptedRuns() {
        int closed = 0;
        for (String pipelineName : pipelineRunRepository.pipelinesWithRunningRuns()) {
            try (PipelineRunLock.Held lock = runLock.tryAcquire(pipelineName)) {
                if (lock != null) {
                    closed += pipelineRunRepository.failInterruptedRuns(pipelineName);
                }
            }
        }
        if (closed > 0) {
            log.warn("Marked {} run(s) as FAILED because their process stopped while they were running", closed);
        }
    }
}
