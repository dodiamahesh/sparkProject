package com.tcs.dmj.dwh.service;

import com.tcs.dmj.dwh.domain.MetaPipelineRun;

import java.util.List;

/**
 * Run history (meta.meta_pipeline_run).
 * Implementation: {@link com.tcs.dmj.dwh.service.impl.DwhPipelineRunServiceImpl}.
 */
public interface DwhPipelineRunService {

    /**
     * Latest runs first.
     *
     * @param pipelineName optional filter
     * @param status       optional filter: RUNNING, SUCCESS, SUCCESS_WITH_FAILED or FAILED
     * @param limit        maximum number of runs (kept between 1 and 500)
     */
    List<MetaPipelineRun> listPipelineRuns(String pipelineName, String status, int limit);

    /** @throws PipelineRunNotFoundException no run with this id */
    MetaPipelineRun getPipelineRun(long pipelineRunId);
}
