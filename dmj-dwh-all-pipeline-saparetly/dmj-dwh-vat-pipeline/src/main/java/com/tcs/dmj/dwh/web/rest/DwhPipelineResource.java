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
 * GET  /api/admin/dwh/pipelines                    -> pipelines, sources, last_run_time
 * POST /api/admin/dwh/pipelines/{pipelineName}/run -> runs the pipeline, answers when it has finished
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

    /** A FAILED run is still HTTP 200: the run row says why it failed. */
    @PostMapping("/{pipelineName}/run")
    public MetaPipelineRun runPipeline(@PathVariable String pipelineName) {
        return pipelineService.runPipeline(pipelineName);
    }
}
