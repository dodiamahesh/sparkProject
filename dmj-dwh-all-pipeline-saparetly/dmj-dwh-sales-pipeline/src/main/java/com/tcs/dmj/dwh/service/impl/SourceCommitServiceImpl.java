package com.tcs.dmj.dwh.service.impl;

import com.tcs.dmj.dwh.connector.source.SourceBatch;
import com.tcs.dmj.dwh.core.PipelineContext;
import com.tcs.dmj.dwh.repository.MetaPipelineRepository;
import com.tcs.dmj.dwh.repository.RejectedRecordRepository;
import com.tcs.dmj.dwh.service.SourceCommitService;
import com.tcs.dmj.dwh.validation.QualityColumns;
import org.springframework.stereotype.Service;

/**
 * The "commit" phase. Runs ONLY after the pipeline loaded all its tables successfully.
 * For every source collection processed in the job:
 *
 *   1. save rejected records     -> meta.meta_rejected_record (ONE row per record, error_ids[] ->
 *                                   meta.meta_error_master: every rule it broke), written directly (no staging table)
 *   2. advance last_run_time     -> meta.meta_pipeline.last_run_time = start time of this run (once per pipeline)
 *   (first: every soft delete read from the source must have been applied by the pipeline, else the job fails)
 *
 * Because last_run_time moves LAST, a job that fails half-way changes nothing: the next run reads the same
 * window again (the UPSERTs make that safe). MongoDB is never written.
 *
 * A rejected document is read again when the source changes it (its timestamp becomes newer than
 * last_run_time). If it is valid then, it is loaded; its rows in meta.meta_rejected_record stay as history.
 */
@Service
public class SourceCommitServiceImpl implements SourceCommitService {

    private final RejectedRecordRepository rejectedRecordRepository;
    private final MetaPipelineRepository metaPipelineRepository;

    public SourceCommitServiceImpl(RejectedRecordRepository rejectedRecordRepository,
                                   MetaPipelineRepository metaPipelineRepository) {
        this.rejectedRecordRepository = rejectedRecordRepository;
        this.metaPipelineRepository = metaPipelineRepository;
    }

    @Override
    public void commit(PipelineContext context) {
        // Safety: soft deletes read from the source must have been applied, otherwise they would be lost for good
        // (last_run_time moves on). Fails the job BEFORE anything of the commit phase is written.
        for (SourceBatch batch : context.sourceBatches()) {
            if (batch.deletedCount() > 0 && !context.deletesApplied(batch.collection())) {
                throw new IllegalStateException(batch.deletedCount() + " document(s) of " + batch.collection()
                        + " were soft-deleted at the source, but the pipeline did not apply them "
                        + "(AbstractEtlPipeline.applyDeletes). last_run_time was not moved.");
            }
        }

        for (SourceBatch batch : context.sourceBatches()) {
            if (batch.rejectedCount() > 0) {
                rejectedRecordRepository.save(QualityColumns.oneRowPerError(batch.rejected()).toLocalIterator(),
                        context.pipelineName(), batch.collection(), context.pipelineRunId());
            }
        }

        // Only after everything above succeeded: the next run starts where this one ended
        metaPipelineRepository.advanceLastRunTime(context.pipelineName(), context.windowEnd());
    }
}
