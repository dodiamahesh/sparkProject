package com.tcs.dmj.dwh.enums;

/** Category of a pipeline (meta.meta_pipeline.pipeline_type). */
public enum PipelineType {
    SOURCE_TO_RAW,
    RAW_TO_CURATED,
    CURATED_TO_AGGREGATED
}
