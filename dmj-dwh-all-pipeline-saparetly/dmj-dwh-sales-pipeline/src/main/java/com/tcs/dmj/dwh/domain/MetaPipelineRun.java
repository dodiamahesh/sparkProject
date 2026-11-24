package com.tcs.dmj.dwh.domain;

import java.time.OffsetDateTime;

/**
 * One row of meta.meta_pipeline_run (+ pipeline_name from meta.meta_pipeline).
 *
 * @param lastRunTime    the run reads data with updatedAt &lt;= this time (window end). When the run succeeds,
 *                       it becomes meta.meta_pipeline.last_run_time, so the next run starts from here.
 * @param pipelineStatus RUNNING / SUCCESS / SUCCESS_WITH_FAILED / FAILED
 * @param totalRecords   records read from the source
 * @param successRecords records handled successfully: valid (loaded) + soft-deleted at the source
 * @param failedRecords  records rejected (validation, duplicates)
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
