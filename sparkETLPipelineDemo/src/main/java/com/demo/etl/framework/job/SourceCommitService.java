package com.demo.etl.framework.job;

import com.demo.etl.framework.audit.RejectedRecordRepository;
import com.demo.etl.framework.audit.StepTracker;
import com.demo.etl.framework.pipeline.PipelineContext;
import com.demo.etl.framework.quality.ErrorType;
import com.demo.etl.framework.quality.QualityColumns;
import com.demo.etl.framework.source.MongoFlagUpdater;
import com.demo.etl.framework.source.SourceBatch;
import com.demo.etl.framework.source.SourceFlag;
import com.demo.etl.framework.target.PostgresWriter;
import org.apache.spark.sql.Column;
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
 *   3. update MongoDB flags      -> valid = 'Y', rejected = 'E', duplicate = 'D'
 *
 * Because the flags are updated last, a job that fails half-way leaves MongoDB untouched and the
 * next run simply processes the same documents again (the UPSERT makes that safe).
 */
@Service
public class SourceCommitService {

    private final PostgresWriter postgresWriter;
    private final RejectedRecordRepository rejectedRecordRepository;
    private final MongoFlagUpdater flagUpdater;
    private final StepTracker stepTracker;

    public SourceCommitService(PostgresWriter postgresWriter, RejectedRecordRepository rejectedRecordRepository,
                               MongoFlagUpdater flagUpdater, StepTracker stepTracker) {
        this.postgresWriter = postgresWriter;
        this.rejectedRecordRepository = rejectedRecordRepository;
        this.flagUpdater = flagUpdater;
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

            stepTracker.track(context, "Update MongoDB flags [" + collection + "]",
                    () -> updateSourceFlags(context, batch));
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

    private long updateSourceFlags(PipelineContext context, SourceBatch batch) {
        String collection = batch.collection();
        Column errorType = col(QualityColumns.REJECTED_ERROR_TYPE);

        Dataset<Row> loadedIds = batch.valid().select(QualityColumns.SOURCE_ID);
        Dataset<Row> rejectedIds = batch.rejected()
                .filter(errorType.notEqual(ErrorType.DUPLICATE.name()))
                .select(QualityColumns.REJECTED_SOURCE_ID);
        Dataset<Row> duplicateIds = batch.rejected()
                .filter(errorType.equalTo(ErrorType.DUPLICATE.name()))
                .select(QualityColumns.REJECTED_SOURCE_ID);

        return flagUpdater.updateFlag(collection, loadedIds, SourceFlag.LOADED, context.jobRunId())
                + flagUpdater.updateFlag(collection, rejectedIds, SourceFlag.REJECTED, context.jobRunId())
                + flagUpdater.updateFlag(collection, duplicateIds, SourceFlag.DUPLICATE, context.jobRunId());
    }
}
