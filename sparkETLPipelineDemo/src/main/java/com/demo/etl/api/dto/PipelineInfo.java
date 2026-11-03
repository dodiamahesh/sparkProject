package com.demo.etl.api.dto;

import java.util.List;

public record PipelineInfo(
        String name,
        String description,
        List<String> sourceCollections,
        List<String> targetTables,
        List<String> fullLoadTruncateTables,
        List<String> loadTypes,
        boolean running) {
}
