package com.demo.etl.framework.audit;

import java.time.LocalDateTime;

/** One row of etl_audit.etl_rejected_record. */
public record RejectedRecord(
        long rejectedRecordId,
        String pipelineName,
        String sourceCollection,
        String sourceId,
        String businessKey,
        String errorType,
        String errorReason,
        String rawRecord,
        int attemptCount,
        long firstJobRunId,
        long lastJobRunId,
        LocalDateTime firstRejectedAt,
        LocalDateTime lastRejectedAt) {
}
