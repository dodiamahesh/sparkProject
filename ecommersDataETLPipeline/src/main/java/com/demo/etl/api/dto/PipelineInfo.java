package com.demo.etl.api.dto;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

/**
 * One pipeline as shown in GET /api/etl/pipelines.
 *
 * @param watermarks collection -> data read up to this time (next run reads newer documents);
 *                   a collection that was never loaded is not in the map (its first run reads everything)
 */
public record PipelineInfo(
        String name,
        String description,
        List<String> sourceCollections,
        List<SourceInfo> sources,
        List<String> targetTables,
        Map<String, OffsetDateTime> watermarks,
        boolean running) {
}
