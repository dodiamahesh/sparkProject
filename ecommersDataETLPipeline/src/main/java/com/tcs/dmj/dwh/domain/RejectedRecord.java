package com.tcs.dmj.dwh.domain;

import java.time.LocalDateTime;

/**
 * One row of meta.meta_rejected_record (= one failed rule of one record), with its error from
 * meta.meta_error_master. A record that broke 2 rules has 2 rows (same sourceId, different errorId).
 *
 * @param fieldValue the bad value of {@code fieldName} in this record
 */
public record RejectedRecord(
        long rejectedRecordId,
        String pipelineName,
        String sourceCollection,
        String sourceId,
        String businessKey,
        int errorId,
        String errorType,
        String fieldName,
        String errorMessage,
        String fieldValue,
        String rawRecord,
        int attemptCount,
        long firstJobRunId,
        long lastJobRunId,
        LocalDateTime firstRejectedAt,
        LocalDateTime lastRejectedAt) {
}
