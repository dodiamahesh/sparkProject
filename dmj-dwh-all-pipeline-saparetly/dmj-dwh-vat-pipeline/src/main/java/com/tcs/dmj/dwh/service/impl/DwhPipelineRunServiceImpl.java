package com.tcs.dmj.dwh.service.impl;

import com.tcs.dmj.dwh.domain.MetaPipelineRun;
import com.tcs.dmj.dwh.exception.PipelineRunNotFoundException;
import com.tcs.dmj.dwh.repository.MetaPipelineRunRepository;
import com.tcs.dmj.dwh.service.DwhPipelineRunService;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class DwhPipelineRunServiceImpl implements DwhPipelineRunService {

    private static final int MAX_LIMIT = 500;

    private final MetaPipelineRunRepository pipelineRunRepository;

    public DwhPipelineRunServiceImpl(MetaPipelineRunRepository pipelineRunRepository) {
        this.pipelineRunRepository = pipelineRunRepository;
    }

    @Override
    public List<MetaPipelineRun> listPipelineRuns(String pipelineName, String status, int limit) {
        return pipelineRunRepository.findRecent(pipelineName, status, Math.min(Math.max(limit, 1), MAX_LIMIT));
    }

    @Override
    public MetaPipelineRun getPipelineRun(long pipelineRunId) {
        return pipelineRunRepository.findById(pipelineRunId)
                .orElseThrow(() -> new PipelineRunNotFoundException(pipelineRunId));
    }
}
