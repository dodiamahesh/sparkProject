package com.tcs.dmj.dwh.core;

import com.tcs.dmj.dwh.connector.source.SourceDefinition;
import com.tcs.dmj.dwh.enums.PipelineType;

import java.util.List;

/**
 * Contract for every ETL pipeline.
 *
 * To add a new pipeline:
 *   1. create a class that extends {@link AbstractEtlPipeline}
 *   2. annotate it with @Component
 *   3. implement sources() and run(...)
 * It is picked up automatically by {@link PipelineRegistry} and can be triggered from the REST API.
 */
public interface EtlPipeline {

    /**
     * Unique name, e.g. "payment-pipeline". The SAME name is used everywhere: REST URL, --run-pipeline=&lt;name&gt;,
     * meta.meta_pipeline.pipeline_name and the Airflow DAG id (meta.meta_pipeline.airflow_dag_id). At most 30
     * characters; letters, digits, "-", "_" and "." only (Airflow DAG id rule).
     */
    String name();

    /** Short description, stored in meta.meta_pipeline.pipeline_description (at most 100 characters). */
    String description();

    /**
     * The source definitions (data contracts) this pipeline reads: collection, checks, owner, SLA ...
     * Shown in GET /api/admin/dwh/pipelines.
     */
    List<SourceDefinition> sources();

    /** MongoDB collections this pipeline reads (derived from {@link #sources()}). */
    default List<String> sourceCollections() {
        return sources().stream().map(SourceDefinition::collection).toList();
    }

    /** PostgreSQL tables this pipeline writes (shown in the API). */
    List<String> targetTables();

    /** Functional category, stored in meta.meta_pipeline.pipeline_type. */
    default PipelineType pipelineType() {
        return PipelineType.RAW_TO_CURATED;
    }

    /**
     * Selected field-to-field lineage for meta.meta_column_lineage (critical fields only; table lineage is found
     * automatically). Written when a run succeeds. Default: none.
     */
    default List<ColumnLineage> columnLineage() {
        return List.of();
    }

    /** The pipeline logic: extract -> validate -> transform -> load. */
    void run(PipelineContext context);

    /**
     * false (default) = tables are loaded through etl_staging tables, every table commits on its own.
     * true = the pipeline writes DIRECTLY into its tables (AbstractEtlPipeline.loadDirect, no staging) and the whole
     *        run is ONE database transaction: all tables + rejected records + last_run_time commit together, or a
     *        failure rolls everything back (all-or-nothing).
     */
    default boolean directLoad() {
        return false;
    }
}
