package com.demo.etl.framework.job;

import com.demo.etl.framework.audit.RejectedRecordRepository;
import com.demo.etl.framework.audit.StepTracker;
import com.demo.etl.framework.audit.WatermarkRepository;
import com.demo.etl.framework.pipeline.PipelineContext;
import com.demo.etl.framework.quality.QualityColumns;
import com.demo.etl.framework.source.SourceBatch;
import com.demo.etl.framework.target.PostgresWriter;
import org.springframework.stereotype.Service;

/**
 * The "commit" phase. Runs ONLY after the pipeline loaded all its tables successfully.
 * For every source collection processed in the job:
 *
 *   1. save rejected rows        -> meta.meta_rejected_record (one row per record + error, error_id ->
 *                                   meta.meta_error_master)
 *                                   (directLoad pipelines: without staging table, in the job's transaction)
 *   2. advance the watermark     -> meta.meta_watermark = start time of this run
 *   (first: every soft delete read from the source must have been applied by the pipeline, else the job fails)
 *                                   (skipped for derived batches such as order lines: the parent collection has it)
 *
 * Because the watermark moves LAST, a job that fails half-way changes nothing: the next run reads the same
 * window again (the UPSERTs make that safe). MongoDB is never written.
 *
 * A rejected document is read again when the source changes it (its timestamp becomes newer than the
 * watermark). If it is valid then, it is loaded; its rows in meta.meta_rejected_record stay as history.
 */
@Service
public class SourceCommitService {

    private final PostgresWriter postgresWriter;
    private final RejectedRecordRepository rejectedRecordRepository;
    private final WatermarkRepository watermarkRepository;
    private final StepTracker stepTracker;

    public SourceCommitService(PostgresWriter postgresWriter, RejectedRecordRepository rejectedRecordRepository,
                               WatermarkRepository watermarkRepository, StepTracker stepTracker) {
        this.postgresWriter = postgresWriter;
        this.rejectedRecordRepository = rejectedRecordRepository;
        this.watermarkRepository = watermarkRepository;
        this.stepTracker = stepTracker;
    }

    public void commit(PipelineContext context) {
        // Safety: soft deletes read from the source must have been applied, otherwise they would be lost for good
        // (the watermark moves on). Fails the job BEFORE anything of the commit phase is written.
        for (SourceBatch batch : context.sourceBatches()) {
            if (batch.deletedCount() > 0 && !context.deletesApplied(batch.collection())) {
                throw new IllegalStateException(batch.deletedCount() + " document(s) of " + batch.collection()
                        + " were soft-deleted at the source, but the pipeline did not apply them "
                        + "(AbstractEtlPipeline.applyDeletes). The watermark was not moved.");
            }
        }

        for (SourceBatch batch : context.sourceBatches()) {
            stepTracker.track(context, "Save rejected records [" + batch.collection() + "]",
                    () -> saveRejectedRecords(context, batch));
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

    /** @return number of rejected records saved */
    private long saveRejectedRecords(PipelineContext context, SourceBatch batch) {
        if (batch.rejectedCount() == 0) {
            return 0;
        }
        if (context.directLoad()) {
            // no staging table: rows go straight from Spark into the meta tables, inside the job's transaction
            rejectedRecordRepository.saveDirect(QualityColumns.oneRowPerError(batch.rejected()).toLocalIterator(),
                    context.pipelineName(), batch.collection(), context.jobRunId());
            return batch.rejectedCount();
        }
        String staging = postgresWriter.writeToStaging(context, QualityColumns.oneRowPerError(batch.rejected()),
                "rejected_" + batch.collection());
        try {
            rejectedRecordRepository.saveFromStaging(staging, context.pipelineName(), batch.collection(), context.jobRunId());
            return batch.rejectedCount();
        } finally {
            postgresWriter.dropStagingTable(staging);
        }
    }
}
