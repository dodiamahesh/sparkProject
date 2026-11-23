package com.tcs.dmj.dwh.web.rest;

import com.tcs.dmj.dwh.domain.JobRun;
import com.tcs.dmj.dwh.service.DwhJobService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Run history (meta.meta_pipeline_run).
 *
 *   GET /api/admin/dwh/jobs?pipelineName=payment-pipeline&status=FAILED&limit=20
 *   GET /api/admin/dwh/jobs/{jobRunId}          -> one run (status, counts, error)
 *
 * status: SUCCESS, SUCCESS_WITH_FAILED or FAILED (null while the run is still busy).
 */
@RestController
@RequestMapping("/api/admin/dwh/jobs")
public class DwhJobResource {

    private final DwhJobService jobService;

    public DwhJobResource(DwhJobService jobService) {
        this.jobService = jobService;
    }

    @GetMapping
    public List<JobRun> listJobRuns(
            @RequestParam(required = false) String pipelineName,
            @RequestParam(required = false) String status,
            @RequestParam(defaultValue = "20") int limit) {
        return jobService.listJobRuns(pipelineName, status, limit);
    }

    @GetMapping("/{jobRunId}")
    public JobRun getJobRun(@PathVariable long jobRunId) {
        return jobService.getJobRun(jobRunId);
    }
}
