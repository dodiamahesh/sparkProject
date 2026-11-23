package com.tcs.dmj.dwh.service;

import com.tcs.dmj.dwh.domain.JobRun;

import java.util.List;

/**
 * Run history (meta.meta_pipeline_run).
 * Implementation: {@link com.tcs.dmj.dwh.service.impl.DwhJobServiceImpl}.
 */
public interface DwhJobService {

    /**
     * Latest runs first.
     *
     * @param pipelineName optional filter
     * @param status       optional filter: SUCCESS, SUCCESS_WITH_FAILED or FAILED
     * @param limit        maximum number of runs (kept between 1 and 500)
     */
    List<JobRun> listJobRuns(String pipelineName, String status, int limit);

    /** @throws JobRunNotFoundException no run with this id */
    JobRun getJobRun(long jobRunId);
}
