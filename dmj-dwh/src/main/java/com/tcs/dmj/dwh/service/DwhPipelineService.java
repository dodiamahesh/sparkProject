package com.tcs.dmj.dwh.service;

import com.tcs.dmj.dwh.domain.MetaPipelineRun;
import com.tcs.dmj.dwh.service.dto.PipelineInfoDTO;

import java.util.List;

/**
 * Lists the pipelines and runs them. Every run is INCREMENTAL by timestamp (meta.meta_pipeline.last_run_time).
 * Implementation: {@link com.tcs.dmj.dwh.service.impl.DwhPipelineServiceImpl}.
 */
public interface DwhPipelineService {

    /** Every registered pipeline with its sources, target tables, last_run_time and whether it is running. */
    List<PipelineInfoDTO> listPipelines();

    /**
     * Runs the pipeline in the calling thread and returns the finished run
     * (status SUCCESS / SUCCESS_WITH_FAILED / FAILED, counts, error code + message).
     *
     * @throws PipelineNotFoundException       no pipeline with this name
     * @throws PipelineAlreadyRunningException the same pipeline is already running
     */
    MetaPipelineRun runPipeline(String pipelineName);
}
