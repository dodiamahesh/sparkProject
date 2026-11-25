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
 * GET /api/admin/dwh/rejected-records?pipelineName=vat-return-pipeline&collection=vat_return_coll&errorType=CORRUPT
 * GET /api/admin/dwh/rejected-records/summary
 * To fix a record: correct it in MongoDB (new updatedAt) and run the pipeline again.
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
