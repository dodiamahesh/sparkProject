package com.demo.etl.api.dto;

import com.demo.etl.framework.source.SourceDefinition;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** The data contract of one source, as shown in GET /api/etl/pipelines. */
public record SourceInfo(
        String collection,
        String description,
        String owner,
        String sla,
        String businessKey,
        List<String> fields,
        Double maxRejectPercent,
        Long expectedMinRows,
        Long expectedMaxRows,
        boolean failOnSchemaDrift,
        Map<String, String> piiFields,
        String sourceFilter) {

    public static SourceInfo from(SourceDefinition source) {
        Map<String, String> pii = new TreeMap<>();
        source.pii().forEach((field, type) -> pii.put(field, type.name()));
        return new SourceInfo(
                source.collection(),
                source.metadata().description(),
                source.metadata().owner(),
                source.metadata().sla(),
                source.businessKey(),
                source.fields(),
                source.checks().maxRejectPercent(),
                source.checks().minRows(),
                source.checks().maxRows(),
                source.checks().failOnSchemaDrift(),
                pii,
                source.sourceFilter());
    }
}
