package com.tcs.dmj.dwh.enums;

/** Functional category of a pipeline (meta.meta_pipeline.pipeline_type). */
public enum PipelineType {

    /** Consumer: source system -> raw layer. */
    SOURCE_TO_RAW,

    /** Raw layer (MongoDB) -> curated layer (PostgreSQL silver / curated). Every pipeline of this project. */
    RAW_TO_CURATED,

    /** Curated layer -> aggregated (reporting) tables. */
    CURATED_TO_AGGREGATED
}
