package com.tcs.dmj.dwh.service;

import com.tcs.dmj.dwh.domain.MetaPipelineRun;

import java.util.List;

/** Run history (meta.meta_pipeline_run). */
public interface DwhPipelineRunService {

    /** Latest runs first; pipelineName and status are optional filters, limit 1..500. */
    List<MetaPipelineRun> listPipelineRuns(String pipelineName, String status, int limit);

    MetaPipelineRun getPipelineRun(long pipelineRunId);
}
