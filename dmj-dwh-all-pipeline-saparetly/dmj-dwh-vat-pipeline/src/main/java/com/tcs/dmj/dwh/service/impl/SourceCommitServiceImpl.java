package com.tcs.dmj.dwh.service.impl;

import com.tcs.dmj.dwh.connector.source.SourceBatch;
import com.tcs.dmj.dwh.connector.target.PostgresWriter;
import com.tcs.dmj.dwh.core.PipelineContext;
import com.tcs.dmj.dwh.exception.SoftDeletesNotAppliedException;
import com.tcs.dmj.dwh.repository.MetaPipelineRepository;
import com.tcs.dmj.dwh.repository.RejectedRecordRepository;
import com.tcs.dmj.dwh.service.SourceCommitService;
import com.tcs.dmj.dwh.validation.QualityColumns;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Service;

/**
 * Commit phase, after every table is loaded: check soft deletes were applied, save the rejected records,
 * then move last_run_time (last, so a failed run is read again next time).
 */
@Service
public class SourceCommitServiceImpl implements SourceCommitService {

    private final PostgresWriter postgresWriter;
    private final RejectedRecordRepository rejectedRecordRepository;
    private final MetaPipelineRepository metaPipelineRepository;

    public SourceCommitServiceImpl(PostgresWriter postgresWriter, RejectedRecordRepository rejectedRecordRepository,
                                   MetaPipelineRepository metaPipelineRepository) {
        this.postgresWriter = postgresWriter;
        this.rejectedRecordRepository = rejectedRecordRepository;
        this.metaPipelineRepository = metaPipelineRepository;
    }

    @Override
    public void commit(PipelineContext context) {
        requireDeletesApplied(context);
        for (SourceBatch batch : context.sourceBatches()) {
            saveRejectedRecords(context, batch);
        }
        metaPipelineRepository.advanceLastRunTime(context.pipelineName(), context.windowEnd());
    }

    /** Deletes read from a source but never applied would be lost for good once last_run_time moves. */
    private static void requireDeletesApplied(PipelineContext context) {
        for (SourceBatch batch : context.sourceBatches()) {
            if (batch.deletedCount() > 0 && !context.deletesApplied(batch.collection())) {
                throw new SoftDeletesNotAppliedException(batch.collection(), batch.deletedCount());
            }
        }
    }

    private void saveRejectedRecords(PipelineContext context, SourceBatch batch) {
        if (batch.rejectedCount() == 0) {
            return;
        }
        Dataset<Row> errorRows = QualityColumns.oneRowPerError(batch.rejected());
        if (context.directLoad()) {
            rejectedRecordRepository.saveDirect(errorRows.toLocalIterator(), context.pipelineName(),
                    batch.collection(), context.pipelineRunId());
            return;
        }
        String staging = postgresWriter.writeToStaging(context, errorRows, "rejected_" + batch.collection());
        try {
            rejectedRecordRepository.saveFromStaging(staging, context.pipelineName(), batch.collection(),
                    context.pipelineRunId());
        } finally {
            postgresWriter.dropStagingTable(staging);
        }
    }
}
