package com.demo.etl.framework.pipeline;

import java.util.List;

/**
 * Contract for every ETL pipeline.
 *
 * To add a new pipeline:
 *   1. create a class that extends {@link AbstractEtlPipeline}
 *   2. annotate it with @Component
 *   3. implement run(...)
 * It is picked up automatically by {@link PipelineRegistry} and can be triggered from the REST API.
 */
public interface EtlPipeline {

    /** Unique name used in the REST URL, e.g. "payment-pipeline". */
    String name();

    String description();

    /** MongoDB collections this pipeline reads (shown in the API). */
    List<String> sourceCollections();

    /** PostgreSQL tables this pipeline writes (shown in the API). */
    List<String> targetTables();

    /**
     * Tables emptied before a FULL load. Usually only the FACT tables:
     * dimensions are shared with other pipelines, so they are upserted instead of truncated.
     */
    default List<String> fullLoadTruncateTables() {
        return List.of();
    }

    /** The pipeline logic: extract -> validate -> transform -> load. */
    void run(PipelineContext context);
}
