package com.tcs.dmj.dwh.service.dto;

import java.time.OffsetDateTime;
import java.util.List;

/** One pipeline in GET /api/admin/dwh/pipelines. */
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
