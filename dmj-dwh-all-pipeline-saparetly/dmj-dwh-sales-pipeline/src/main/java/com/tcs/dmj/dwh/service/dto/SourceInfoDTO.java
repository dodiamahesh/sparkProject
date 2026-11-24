package com.tcs.dmj.dwh.service.dto;

import com.tcs.dmj.dwh.connector.source.SourceDefinition;

import java.util.List;

/** The data contract of one source, as shown in GET /api/admin/dwh/pipelines. */
public record SourceInfoDTO(
        String collection,
        String description,
        String owner,
        String sla,
        String businessKey,
        List<String> fields,
        Double maxRejectPercent,
        String sourceFilter,
        String softDeleteField) {

    public static SourceInfoDTO from(SourceDefinition source) {
        return new SourceInfoDTO(
                source.collection(),
                source.metadata().description(),
                source.metadata().owner(),
                source.metadata().sla(),
                source.businessKey(),
                source.fields(),
                source.maxRejectPercent(),
                source.sourceFilter(),
                source.softDeleteField());
    }
}
