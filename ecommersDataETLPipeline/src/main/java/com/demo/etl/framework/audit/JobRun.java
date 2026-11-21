package com.demo.etl.framework.audit;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;

/**
 * One row of meta.meta_pipeline_run.
 * windowEnd = the run read data up to this time (updatedAt &lt;= windowEnd).
 * status    = SUCCESS / SUCCESS_WITH_FAILED / FAILED, null while the run is still busy.
 */
public record JobRun(
        long jobRunId,
        String pipelineName,
        OffsetDateTime windowEnd,
        String status,
        String triggeredBy,
        LocalDateTime startTime,
        LocalDateTime endTime,
        Long durationMs,
        long sourceCount,
        long validCount,
        long rejectedCount,
        long duplicateCount,
        long loadedCount,
        long resolvedCount,
        String errorMessage,
        String errorDetail) {
}
