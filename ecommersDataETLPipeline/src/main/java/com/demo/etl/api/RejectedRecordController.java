package com.demo.etl.api;

import com.demo.etl.framework.audit.RejectedRecord;
import com.demo.etl.framework.audit.RejectedRecordRepository;
import com.demo.etl.framework.audit.RejectedRecordSummary;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Bad records that are waiting to be fixed: one row per record + failed rule, with the error from
 * meta.meta_error_master (error_id, error_type, error_message) and the bad field_value.
 *
 *   GET /api/etl/rejected-records?pipelineName=payment-pipeline&collection=payments&errorType=CORRUPT
 *   GET /api/etl/rejected-records/summary
 *
 * How to fix: correct the document in MongoDB (the source sets a newer updatedAt) and run the pipeline:
 * the corrected document is inside the next incremental window.
 * Records that are valid now are loaded and disappear from this list.
 */
@RestController
@RequestMapping("/api/etl/rejected-records")
public class RejectedRecordController {

    private static final int MAX_LIMIT = 1000;

    private final RejectedRecordRepository rejectedRecordRepository;

    public RejectedRecordController(RejectedRecordRepository rejectedRecordRepository) {
        this.rejectedRecordRepository = rejectedRecordRepository;
    }

    @GetMapping
    public List<RejectedRecord> listRejectedRecords(
            @RequestParam(required = false) String pipelineName,
            @RequestParam(required = false) String collection,
            @RequestParam(required = false) String errorType,
            @RequestParam(defaultValue = "100") int limit) {
        return rejectedRecordRepository.find(pipelineName, collection, errorType, Math.min(Math.max(limit, 1), MAX_LIMIT));
    }

    @GetMapping("/summary")
    public List<RejectedRecordSummary> summary() {
        return rejectedRecordRepository.summary();
    }
}
