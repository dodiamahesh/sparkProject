package com.tcs.dmj.dwh.web.rest;

import com.tcs.dmj.dwh.domain.MetaPipelineRun;
import com.tcs.dmj.dwh.service.DwhPipelineService;
import com.tcs.dmj.dwh.service.dto.PipelineInfoDTO;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * List pipelines and trigger them. Every run is INCREMENTAL: per source it reads the documents whose
 * timestamp (e.g. updatedAt) is after meta.meta_pipeline.last_run_time, up to the moment the run starts.
 *
 *   GET  /api/admin/dwh/pipelines                        -> pipelines, their sources and last_run_time
 *   POST /api/admin/dwh/pipelines/{pipelineName}/run     -> runs the pipeline and answers when it has finished
 */
@RestController
@RequestMapping("/api/admin/dwh/pipelines")
public class DwhPipelineResource {

    private final DwhPipelineService pipelineService;

    public DwhPipelineResource(DwhPipelineService pipelineService) {
        this.pipelineService = pipelineService;
    }

    @GetMapping
    public List<PipelineInfoDTO> listPipelines() {
        return pipelineService.listPipelines();
    }

    /**
     * Synchronous: the response is the finished run (status SUCCESS / SUCCESS_WITH_FAILED / FAILED, counts, error).
     * A FAILED job is still HTTP 200: the run row tells why it failed.
     */
    @PostMapping("/{pipelineName}/run")
    public MetaPipelineRun runPipeline(@PathVariable String pipelineName) {
        return pipelineService.runPipeline(pipelineName);
    }
}
