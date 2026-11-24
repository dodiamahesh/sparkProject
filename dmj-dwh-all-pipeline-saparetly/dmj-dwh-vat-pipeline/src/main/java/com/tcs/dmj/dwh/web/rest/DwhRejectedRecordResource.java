package com.tcs.dmj.dwh.web.rest;

import com.tcs.dmj.dwh.domain.RejectedRecord;
import com.tcs.dmj.dwh.service.RejectedRecordService;
import com.tcs.dmj.dwh.service.dto.RejectedRecordSummaryDTO;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Bad records that are waiting to be fixed: ONE row per record with ALL the rules it broke: error_ids
 * (meta.meta_error_master), error_messages (comma separated) and field_values (the bad value per field).
 * errorType filter = records with at least one error of that type. /summary = records per error.
 *
 *   GET /api/admin/dwh/rejected-records?pipelineName=payment-pipeline&collection=payments&errorType=CORRUPT
 *   GET /api/admin/dwh/rejected-records/summary
 *
 * How to fix: correct the document in MongoDB (the source sets a newer updatedAt) and run the pipeline:
 * the corrected document is inside the next incremental window.
 * A corrected record is loaded; its rows here stay as history (first / last run, attempt_count).
 */
@RestController
@RequestMapping("/api/admin/dwh/rejected-records")
public class DwhRejectedRecordResource {

    private final RejectedRecordService rejectedRecordService;

    public DwhRejectedRecordResource(RejectedRecordService rejectedRecordService) {
        this.rejectedRecordService = rejectedRecordService;
    }

    @GetMapping
    public List<RejectedRecord> listRejectedRecords(
            @RequestParam(required = false) String pipelineName,
            @RequestParam(required = false) String collection,
            @RequestParam(required = false) String errorType,
            @RequestParam(defaultValue = "100") int limit) {
        return rejectedRecordService.listRejectedRecords(pipelineName, collection, errorType, limit);
    }

    @GetMapping("/summary")
    public List<RejectedRecordSummaryDTO> summary() {
        return rejectedRecordService.summary();
    }
}
