package com.demo.etl.framework.audit;

import java.time.LocalDateTime;

/** One row of etl_audit.etl_job_run. */
public record JobRun(
        long jobRunId,
        String pipelineName,
        String loadType,
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
