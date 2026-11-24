package com.tcs.dmj.dwh.service.impl;

import com.tcs.dmj.dwh.domain.RejectedRecord;
import com.tcs.dmj.dwh.repository.RejectedRecordRepository;
import com.tcs.dmj.dwh.service.RejectedRecordService;
import com.tcs.dmj.dwh.service.dto.RejectedRecordSummaryDTO;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class RejectedRecordServiceImpl implements RejectedRecordService {

    private static final int MAX_LIMIT = 1000;

    private final RejectedRecordRepository rejectedRecordRepository;

    public RejectedRecordServiceImpl(RejectedRecordRepository rejectedRecordRepository) {
        this.rejectedRecordRepository = rejectedRecordRepository;
    }

    @Override
    public List<RejectedRecord> listRejectedRecords(String pipelineName, String collection, String errorType, int limit) {
        return rejectedRecordRepository.find(pipelineName, collection, errorType, Math.min(Math.max(limit, 1), MAX_LIMIT));
    }

    @Override
    public List<RejectedRecordSummaryDTO> summary() {
        return rejectedRecordRepository.summary();
    }
}
