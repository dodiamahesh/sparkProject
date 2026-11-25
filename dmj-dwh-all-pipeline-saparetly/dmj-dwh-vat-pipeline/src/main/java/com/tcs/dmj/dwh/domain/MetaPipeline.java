package com.tcs.dmj.dwh.domain;

import java.time.OffsetDateTime;

/** One row of meta.meta_pipeline. lastRunTime = incremental watermark (null = never succeeded). */
public record MetaPipeline(
        long pipelineId,
        String pipelineName,
        String pipelineType,
        String airflowDagId,
        String applicationJobName,
        String pipelineDescription,
        OffsetDateTime lastRunTime,
        String activeFlag,
        OffsetDateTime createdAt,
        OffsetDateTime updatedAt) {
}
