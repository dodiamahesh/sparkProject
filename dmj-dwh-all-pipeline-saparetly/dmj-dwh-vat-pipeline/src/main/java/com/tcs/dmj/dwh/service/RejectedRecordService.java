package com.tcs.dmj.dwh.service;

import com.tcs.dmj.dwh.domain.RejectedRecord;
import com.tcs.dmj.dwh.service.dto.RejectedRecordSummaryDTO;

import java.util.List;

/** Rejected records (meta.meta_rejected_record + meta.meta_error_master). */
public interface RejectedRecordService {

    /** Optional filters; errorType = records with at least one error of that type; limit 1..1000. */
    List<RejectedRecord> listRejectedRecords(String pipelineName, String collection, String errorType, int limit);

    List<RejectedRecordSummaryDTO> summary();
}
