package com.tcs.dmj.dwh.domain;

import java.time.OffsetDateTime;

/**
 * One row of meta.meta_pipeline: the registry of the pipelines (registered automatically at start-up).
 *
 * @param lastRunTime incremental watermark of the pipeline: the next run reads updatedAt &gt; lastRunTime.
 *                    NULL = the pipeline never succeeded yet, the next run reads everything.
 */
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
