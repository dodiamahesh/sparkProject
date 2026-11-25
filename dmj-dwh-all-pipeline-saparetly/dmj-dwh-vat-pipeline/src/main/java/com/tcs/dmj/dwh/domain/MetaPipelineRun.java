package com.tcs.dmj.dwh.domain;

import java.time.OffsetDateTime;

/**
 * One row of meta.meta_pipeline_run. successRecords = valid + soft-deleted, failedRecords = rejected,
 * lastRunTime = this run read data up to here.
 */
public record MetaPipelineRun(
        long pipelineRunId,
        long pipelineId,
        String pipelineName,
        OffsetDateTime lastRunTime,
        OffsetDateTime startTime,
        OffsetDateTime endTime,
        String pipelineStatus,
        Long totalRecords,
        Long successRecords,
        Long failedRecords,
        String errorCode,
        String errorMessage,
        OffsetDateTime createdAt,
        OffsetDateTime updatedAt) {
}
