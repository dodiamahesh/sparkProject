package com.demo.etl.framework.audit;

import java.time.LocalDateTime;

/** One row of etl_audit.etl_job_step. */
public record JobStep(
        long jobStepId,
        int stepOrder,
        String stepName,
        String status,
        Long recordCount,
        LocalDateTime startTime,
        LocalDateTime endTime,
        Long durationMs,
        String errorMessage) {
}
