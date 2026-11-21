package com.demo.etl.framework.job;

import com.demo.etl.framework.audit.DataWarningRepository;
import com.demo.etl.framework.audit.RejectedRecordRepository;
import com.demo.etl.framework.audit.StepTracker;
import com.demo.etl.framework.audit.WatermarkRepository;
import com.demo.etl.framework.pipeline.PipelineContext;
import com.demo.etl.framework.quality.QualityColumns;
import com.demo.etl.framework.source.SourceBatch;
import com.demo.etl.framework.target.PostgresWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Service;

import static org.apache.spark.sql.functions.col;

/**
 * The "commit" phase. Runs ONLY after the pipeline loaded all its tables successfully.
 * For every source collection processed in the job:
 *
 *   1. save rejected rows        -> etl_audit.etl_rejected_record (insert new / update existing)
 *   2. remove fixed rows         -> rows that were rejected before and are valid now are deleted
 *   3. save rule warnings        -> etl_audit.etl_data_warning (loaded rows that broke a warning rule)
 *   4. advance the watermark     -> etl_audit.etl_watermark = start time of this run
 *                                   (skipped for derived batches such as order lines: the parent collection has it)
 *
 * Because the watermark moves LAST, a job that fails half-way changes nothing: the next run reads the same
 * window again (the UPSERTs make that safe). MongoDB is never written.
 *
 * A rejected document is read again when the source changes it (its timestamp becomes newer than the
 * watermark). If it is valid then, it is loaded and step 2 removes it from the rejected table.
 */
@Service
public class SourceCommitService {

    private final PostgresWriter postgresWriter;
    private final RejectedRecordRepository rejectedRecordRepository;
    private final DataWarningRepository warningRepository;
    private final WatermarkRepository watermarkRepository;
    private final StepTracker stepTracker;

    public SourceCommitService(PostgresWriter postgresWriter, RejectedRecordRepository rejectedRecordRepository,
                               DataWarningRepository warningRepository, WatermarkRepository watermarkRepository,
                               StepTracker stepTracker) {
        this.postgresWriter = postgresWriter;
        this.rejectedRecordRepository = rejectedRecordRepository;
        this.warningRepository = warningRepository;
        this.watermarkRepository = watermarkRepository;
        this.stepTracker = stepTracker;
    }

    public void commit(PipelineContext context) {
        for (SourceBatch batch : context.sourceBatches()) {
            String collection = batch.collection();

            stepTracker.track(context, "Save rejected records [" + collection + "]",
                    () -> saveRejectedRecords(context, batch));

            long resolved = stepTracker.track(context, "Remove fixed records from rejected table [" + collection + "]",
                    () -> deleteResolvedRecords(context, batch));
            context.metrics().addResolved(resolved);

            if (batch.warningCount() > 0) {
                long saved = stepTracker.track(context, "Save data warnings [" + collection + "]",
                        () -> saveRuleWarnings(context, batch));
                context.metrics().addWarnings(saved);
            }
        }

        // Only after everything above succeeded: the next run starts where this one ended
        for (SourceBatch batch : context.sourceBatches()) {
            if (!batch.derived()) {
                stepTracker.track(context, "Advance watermark [" + batch.collection() + "] to " + context.windowEnd(), () -> {
                    watermarkRepository.advance(context.pipelineName(), batch.collection(),
                            batch.source().latestRecordColumn(), context.windowEnd(), context.jobRunId());
                    return batch.sourceCount();
                });
            }
        }
    }

    private long saveRejectedRecords(PipelineContext context, SourceBatch batch) {
        if (batch.rejectedCount() == 0) {
            return 0;
        }
        String staging = postgresWriter.writeToStaging(context, batch.rejected(), "rejected_" + batch.collection());
        try {
            return rejectedRecordRepository.upsertFromStaging(staging, context.pipelineName(),
                    batch.collection(), context.jobRunId());
        } finally {
            postgresWriter.dropStagingTable(staging);
        }
    }

    private long saveRuleWarnings(PipelineContext context, SourceBatch batch) {
        String staging = postgresWriter.writeToStaging(context, batch.warnings(), "warnings_" + batch.collection());
        try {
            return warningRepository.insertRuleWarningsFromStaging(staging, context.pipelineName(),
                    batch.collection(), context.jobRunId());
        } finally {
            postgresWriter.dropStagingTable(staging);
        }
    }

    private long deleteResolvedRecords(PipelineContext context, SourceBatch batch) {
        if (batch.validCount() == 0) {
            return 0;
        }
        Dataset<Row> validIds = batch.valid().select(col(QualityColumns.SOURCE_ID).as(QualityColumns.REJECTED_SOURCE_ID));
        String staging = postgresWriter.writeToStaging(context, validIds, "resolved_" + batch.collection());
        try {
            return rejectedRecordRepository.deleteResolvedFromStaging(staging, context.pipelineName(), batch.collection());
        } finally {
            postgresWriter.dropStagingTable(staging);
        }
    }
}
