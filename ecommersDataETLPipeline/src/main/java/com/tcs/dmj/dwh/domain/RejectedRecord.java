package com.tcs.dmj.dwh.domain;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * One row of meta.meta_rejected_record = ONE rejected record with ALL the rules it broke.
 *
 * @param errorIds      meta.meta_error_master.error_id of every failed rule, sorted
 * @param errorMessages the messages of errorIds, comma separated, in the same order
 * @param fieldValues   the bad value per checked field, e.g. {"amount": "-5", "tin": "X1"}
 *                      (rules on several fields have no entry here)
 * @param rawRecord     the original document (JSON, before cleaning)
 */
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
