package com.tcs.dmj.dwh.service.impl;

import com.tcs.dmj.dwh.domain.JobRun;
import com.tcs.dmj.dwh.repository.JobRunRepository;
import com.tcs.dmj.dwh.service.DwhJobService;
import com.tcs.dmj.dwh.service.JobRunNotFoundException;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class DwhJobServiceImpl implements DwhJobService {

    private static final int MAX_LIMIT = 500;

    private final JobRunRepository jobRunRepository;

    public DwhJobServiceImpl(JobRunRepository jobRunRepository) {
        this.jobRunRepository = jobRunRepository;
    }

    @Override
    public List<JobRun> listJobRuns(String pipelineName, String status, int limit) {
        return jobRunRepository.findRecent(pipelineName, status, Math.min(Math.max(limit, 1), MAX_LIMIT));
    }

    @Override
    public JobRun getJobRun(long jobRunId) {
        return jobRunRepository.findById(jobRunId).orElseThrow(() -> new JobRunNotFoundException(jobRunId));
    }
}
