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
 *   2. advance the watermark     -> meta.meta_watermark = start time of this run
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
