package com.tcs.dmj.dwh.service;

import com.tcs.dmj.dwh.domain.RejectedRecord;
import com.tcs.dmj.dwh.service.dto.RejectedRecordSummaryDTO;

import java.util.List;

/**
 * Rejected records (meta.meta_rejected_record + meta.meta_error_master).
 * Implementation: {@link com.tcs.dmj.dwh.service.impl.RejectedRecordServiceImpl}.
 */
public interface RejectedRecordService {

    /**
     * One row per rejected record, with all the rules it broke (error_ids, error_messages, field_values).
     *
     * @param pipelineName optional filter
     * @param collection   optional filter
     * @param errorType    optional filter: records with at least one error of this type (CORRUPT, VALIDATION, ORPHAN, DUPLICATE)
     * @param limit        maximum number of rows (kept between 1 and 1000)
     */
    List<RejectedRecord> listRejectedRecords(String pipelineName, String collection, String errorType, int limit);

    /** Number of rejected records per pipeline / collection / error. */
    List<RejectedRecordSummaryDTO> summary();
}
