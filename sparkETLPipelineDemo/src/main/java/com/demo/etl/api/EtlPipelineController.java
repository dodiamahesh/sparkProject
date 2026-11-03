package com.demo.etl.api;

import com.demo.etl.api.dto.PipelineInfo;
import com.demo.etl.framework.job.EtlJobService;
import com.demo.etl.framework.job.JobLockManager;
import com.demo.etl.framework.job.JobRunDetails;
import com.demo.etl.framework.pipeline.LoadType;
import com.demo.etl.framework.pipeline.PipelineRegistry;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.List;

/**
 * List pipelines and trigger them.
 *
 *   GET  /api/etl/pipelines
 *   POST /api/etl/pipelines/{pipelineName}/run?loadType=INCREMENTAL&async=false
 */
@RestController
@RequestMapping("/api/etl/pipelines")
public class EtlPipelineController {

    private final PipelineRegistry pipelineRegistry;
    private final EtlJobService jobService;
    private final JobLockManager lockManager;

    public EtlPipelineController(PipelineRegistry pipelineRegistry, EtlJobService jobService, JobLockManager lockManager) {
        this.pipelineRegistry = pipelineRegistry;
        this.jobService = jobService;
        this.lockManager = lockManager;
    }

    @GetMapping
    public List<PipelineInfo> listPipelines() {
        List<String> loadTypes = Arrays.stream(LoadType.values()).map(Enum::name).toList();
        return pipelineRegistry.getAllPipelines().stream()
                .map(pipeline -> new PipelineInfo(
                        pipeline.name(),
                        pipeline.description(),
                        pipeline.sourceCollections(),
                        pipeline.targetTables(),
                        pipeline.fullLoadTruncateTables(),
                        loadTypes,
                        lockManager.isRunning(pipeline.name())))
                .toList();
    }

    /**
     * @param loadType FULL | INCREMENTAL | RETRY_REJECTED (case-insensitive)
     * @param async    false (default) = wait for the result; true = return at once with HTTP 202
     */
    @PostMapping("/{pipelineName}/run")
    public ResponseEntity<JobRunDetails> runPipeline(
            @PathVariable String pipelineName,
            @RequestParam(defaultValue = "INCREMENTAL") String loadType,
            @RequestParam(defaultValue = "false") boolean async,
            @RequestHeader(value = "X-Triggered-By", defaultValue = "api") String triggeredBy) {

        JobRunDetails result = jobService.runPipeline(pipelineName, LoadType.from(loadType), async, triggeredBy);
        HttpStatus status = async ? HttpStatus.ACCEPTED : HttpStatus.OK;
        return ResponseEntity.status(status).body(result);
    }
}
