package com.demo.etl.api.dto;

import com.demo.etl.framework.source.SourceDefinition;

import java.util.List;

/** The data contract of one source, as shown in GET /api/etl/pipelines. */
public record SourceInfo(
        String collection,
        String description,
        String owner,
        String sla,
        String businessKey,
        List<String> fields,
        Double maxRejectPercent,
        String sourceFilter) {

    public static SourceInfo from(SourceDefinition source) {
        return new SourceInfo(
                source.collection(),
                source.metadata().description(),
                source.metadata().owner(),
                source.metadata().sla(),
                source.businessKey(),
                source.fields(),
                source.maxRejectPercent(),
                source.sourceFilter());
    }
}
