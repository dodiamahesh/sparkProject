package com.tcs.dmj.dwh.service.dto;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

/**
 * One pipeline as shown in GET /api/admin/dwh/pipelines.
 *
 * @param watermarks collection -> data read up to this time (next run reads newer documents);
 *                   a collection that was never loaded is not in the map (its first run reads everything)
 */
public record PipelineInfoDTO(
        String name,
        String description,
        List<String> sourceCollections,
        List<SourceInfoDTO> sources,
        List<String> targetTables,
        Map<String, OffsetDateTime> watermarks,
        boolean running) {
}
