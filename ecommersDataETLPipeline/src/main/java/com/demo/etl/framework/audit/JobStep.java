package com.demo.etl.framework.audit;

import java.time.LocalDateTime;

/** One row of meta.meta_pipeline_step. status = SUCCESS / FAILED, null while the step is still running. */
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
