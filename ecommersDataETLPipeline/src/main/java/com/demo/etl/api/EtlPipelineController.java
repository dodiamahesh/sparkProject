package com.demo.etl.api;

import com.demo.etl.api.dto.PipelineInfo;
import com.demo.etl.api.dto.SourceInfo;
import com.demo.etl.framework.audit.WatermarkRepository;
import com.demo.etl.framework.job.EtlJobService;
import com.demo.etl.framework.job.JobLockManager;
import com.demo.etl.framework.job.JobRunDetails;
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

import java.util.List;

/**
 * List pipelines and trigger them. Every run is INCREMENTAL: per source it reads the documents whose
 * timestamp (e.g. updatedAt) is after the last watermark, up to the moment the run starts.
 *
 *   GET  /api/etl/pipelines                          -> pipelines, their sources and current watermarks
 *   POST /api/etl/pipelines/{pipelineName}/run?async=false
 */
@RestController
@RequestMapping("/api/etl/pipelines")
public class EtlPipelineController {

    private final PipelineRegistry pipelineRegistry;
    private final EtlJobService jobService;
    private final JobLockManager lockManager;
    private final WatermarkRepository watermarkRepository;

    public EtlPipelineController(PipelineRegistry pipelineRegistry, EtlJobService jobService,
                                 JobLockManager lockManager, WatermarkRepository watermarkRepository) {
        this.pipelineRegistry = pipelineRegistry;
        this.jobService = jobService;
        this.lockManager = lockManager;
        this.watermarkRepository = watermarkRepository;
    }

    @GetMapping
    public List<PipelineInfo> listPipelines() {
        return pipelineRegistry.getAllPipelines().stream()
                .map(pipeline -> new PipelineInfo(
                        pipeline.name(),
                        pipeline.description(),
                        pipeline.sourceCollections(),
                        pipeline.sources().stream().map(SourceInfo::from).toList(),
                        pipeline.targetTables(),
                        watermarkRepository.findByPipeline(pipeline.name()),
                        lockManager.isRunning(pipeline.name())))
                .toList();
    }

    /**
     * @param async false (default) = wait for the result; true = return at once with HTTP 202
     */
    @PostMapping("/{pipelineName}/run")
    public ResponseEntity<JobRunDetails> runPipeline(
            @PathVariable String pipelineName,
            @RequestParam(defaultValue = "false") boolean async,
            @RequestHeader(value = "X-Triggered-By", defaultValue = "api") String triggeredBy) {

        JobRunDetails result = jobService.runPipeline(pipelineName, async, triggeredBy);
        HttpStatus status = async ? HttpStatus.ACCEPTED : HttpStatus.OK;
        return ResponseEntity.status(status).body(result);
    }
}
