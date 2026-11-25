package com.tcs.dmj.dwh.web.rest;

import com.tcs.dmj.dwh.domain.MetaPipelineRun;
import com.tcs.dmj.dwh.service.DwhPipelineRunService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * GET /api/admin/dwh/pipeline-runs?pipelineName=vat-return-pipeline&status=FAILED&limit=20
 * GET /api/admin/dwh/pipeline-runs/{pipelineRunId}
 */
@RestController
@RequestMapping("/api/admin/dwh/pipeline-runs")
public class DwhPipelineRunResource {

    private final DwhPipelineRunService pipelineRunService;

    public DwhPipelineRunResource(DwhPipelineRunService pipelineRunService) {
        this.pipelineRunService = pipelineRunService;
    }

    @GetMapping
    public List<MetaPipelineRun> listPipelineRuns(
            @RequestParam(required = false) String pipelineName,
            @RequestParam(required = false) String status,
            @RequestParam(defaultValue = "20") int limit) {
        return pipelineRunService.listPipelineRuns(pipelineName, status, limit);
    }

    @GetMapping("/{pipelineRunId}")
    public MetaPipelineRun getPipelineRun(@PathVariable long pipelineRunId) {
        return pipelineRunService.getPipelineRun(pipelineRunId);
    }
}
