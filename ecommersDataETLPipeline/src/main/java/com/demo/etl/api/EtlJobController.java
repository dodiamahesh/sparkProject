package com.demo.etl.api;

import com.demo.etl.framework.audit.JobRun;
import com.demo.etl.framework.audit.JobRunRepository;
import com.demo.etl.framework.job.EtlJobService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Run history (meta.meta_pipeline_run).
 *
 *   GET /api/etl/jobs?pipelineName=payment-pipeline&status=FAILED&limit=20
 *   GET /api/etl/jobs/{jobRunId}          -> one run (status, counts, error)
 *
 * status: SUCCESS, SUCCESS_WITH_FAILED or FAILED (null while the run is still busy).
 */
@RestController
@RequestMapping("/api/etl/jobs")
public class EtlJobController {

    private static final int MAX_LIMIT = 500;

    private final JobRunRepository jobRunRepository;
    private final EtlJobService jobService;

    public EtlJobController(JobRunRepository jobRunRepository, EtlJobService jobService) {
        this.jobRunRepository = jobRunRepository;
        this.jobService = jobService;
    }

    @GetMapping
    public List<JobRun> listJobRuns(
            @RequestParam(required = false) String pipelineName,
            @RequestParam(required = false) String status,
            @RequestParam(defaultValue = "20") int limit) {
        return jobRunRepository.findRecent(pipelineName, status, Math.min(Math.max(limit, 1), MAX_LIMIT));
    }

    @GetMapping("/{jobRunId}")
    public JobRun getJobRun(@PathVariable long jobRunId) {
        return jobService.getJobRun(jobRunId);
    }
}
