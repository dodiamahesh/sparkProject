package com.demo.etl.framework.audit;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;

/** One row of etl_audit.etl_job_run. windowEnd = the run read data up to this time (updatedAt &lt;= windowEnd). */
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
        long warningCount,
        String errorMessage,
        String errorDetail) {
}
