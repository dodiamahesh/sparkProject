package com.tcs.dmj.dwh.service;

import com.tcs.dmj.dwh.domain.MetaPipelineRun;
import com.tcs.dmj.dwh.service.dto.PipelineInfoDTO;

import java.util.List;

/** Lists the pipelines and runs them (always incremental). */
public interface DwhPipelineService {

    List<PipelineInfoDTO> listPipelines();

    /**
     * Runs the pipeline in the calling thread and returns the finished run.
     * Throws PipelineNotFoundException / PipelineAlreadyRunningException.
     */
    MetaPipelineRun runPipeline(String pipelineName);
}
