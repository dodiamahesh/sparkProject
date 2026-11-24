package com.tcs.dmj.dwh.service.dto;

import java.time.OffsetDateTime;
import java.util.List;

/**
 * One pipeline as shown in GET /api/admin/dwh/pipelines.
 *
 * @param pipelineId  meta.meta_pipeline.pipeline_id
 * @param lastRunTime data was read up to this time by the last successful run (next run reads newer
 *                    documents); null = never succeeded, the next run reads everything
 */
public record PipelineInfoDTO(
        long pipelineId,
        String name,
        String pipelineType,
        String description,
        List<String> sourceCollections,
        List<SourceInfoDTO> sources,
        List<String> targetTables,
        OffsetDateTime lastRunTime,
        boolean running) {
}
