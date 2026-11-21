package com.demo.etl.api;

import com.demo.etl.framework.audit.DataWarning;
import com.demo.etl.framework.audit.DataWarningRepository;
import com.demo.etl.framework.audit.JobRun;
import com.demo.etl.framework.audit.JobRunRepository;
import com.demo.etl.framework.job.EtlJobService;
import com.demo.etl.framework.job.JobRunDetails;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Job history and status.
 *
 *   GET /api/etl/jobs?pipelineName=payment-pipeline&status=FAILED&limit=20
 *   GET /api/etl/jobs/{jobRunId}          -> job + every step (where did it fail, how long, how many rows)
 *   GET /api/etl/jobs/{jobRunId}/warnings -> data-quality warnings of that run (RULE / SCHEMA_DRIFT / VOLUME)
 */
@RestController
@RequestMapping("/api/etl/jobs")
public class EtlJobController {

    private static final int MAX_LIMIT = 500;

    private final JobRunRepository jobRunRepository;
    private final EtlJobService jobService;
    private final DataWarningRepository warningRepository;

    public EtlJobController(JobRunRepository jobRunRepository, EtlJobService jobService,
                            DataWarningRepository warningRepository) {
        this.jobRunRepository = jobRunRepository;
        this.jobService = jobService;
        this.warningRepository = warningRepository;
    }

    @GetMapping
    public List<JobRun> listJobRuns(
            @RequestParam(required = false) String pipelineName,
            @RequestParam(required = false) String status,
            @RequestParam(defaultValue = "20") int limit) {
        return jobRunRepository.findRecent(pipelineName, status, Math.min(Math.max(limit, 1), MAX_LIMIT));
    }

    @GetMapping("/{jobRunId}")
    public JobRunDetails getJobRun(@PathVariable long jobRunId) {
        return jobService.getJobRun(jobRunId);
    }

    @GetMapping("/{jobRunId}/warnings")
    public List<DataWarning> getWarnings(@PathVariable long jobRunId) {
        jobService.getJobRun(jobRunId);   // 404 when the job does not exist
        return warningRepository.findByJobRunId(jobRunId);
    }
}
