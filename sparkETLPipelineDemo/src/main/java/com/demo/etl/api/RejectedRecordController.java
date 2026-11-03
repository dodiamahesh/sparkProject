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
 * Bad records that are waiting to be fixed.
 *
 *   GET /api/etl/rejected-records?pipelineName=payment-pipeline&collection=payments&errorType=CORRUPT
 *   GET /api/etl/rejected-records/summary
 *
 * How to fix: correct the document in MongoDB, then run the pipeline with loadType=RETRY_REJECTED.
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
