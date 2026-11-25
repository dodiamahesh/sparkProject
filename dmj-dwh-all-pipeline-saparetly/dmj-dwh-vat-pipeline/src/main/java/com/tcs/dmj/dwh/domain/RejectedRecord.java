package com.tcs.dmj.dwh.domain;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/** One row of meta.meta_rejected_record: one rejected record with every rule it broke. */
public record RejectedRecord(
        long rejectedRecordId,
        String pipelineName,
        String sourceCollection,
        String sourceId,
        String businessKey,
        List<Integer> errorIds,
        String errorMessages,
        Map<String, String> fieldValues,
        String rawRecord,
        int attemptCount,
        long firstJobRunId,
        long lastJobRunId,
        LocalDateTime firstRejectedAt,
        LocalDateTime lastRejectedAt) {
}
