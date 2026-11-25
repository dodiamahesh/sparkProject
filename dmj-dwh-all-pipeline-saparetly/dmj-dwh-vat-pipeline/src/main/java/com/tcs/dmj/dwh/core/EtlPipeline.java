package com.tcs.dmj.dwh.core;

import com.tcs.dmj.dwh.connector.source.SourceDefinition;
import com.tcs.dmj.dwh.enums.PipelineType;

import java.util.List;

/**
 * Contract of every pipeline. A new pipeline = a @Component that extends AbstractEtlPipeline;
 * PipelineRegistry finds it automatically.
 */
public interface EtlPipeline {

    /**
     * Unique name, also the REST URL part, --run-pipeline value and Airflow DAG id.
     * At most 30 characters: letters, digits, "-", "_" and ".".
     */
    String name();

    /** At most 100 characters (meta.meta_pipeline.pipeline_description). */
    String description();

    /** The collections this pipeline reads (data contracts). */
    List<SourceDefinition> sources();

    default List<String> sourceCollections() {
        return sources().stream().map(SourceDefinition::collection).toList();
    }

    /** The PostgreSQL tables this pipeline writes (shown in the API). */
    List<String> targetTables();

    default PipelineType pipelineType() {
        return PipelineType.RAW_TO_CURATED;
    }

    /** Field-to-field lineage of critical columns (table lineage is found automatically). */
    default List<ColumnLineage> columnLineage() {
        return List.of();
    }

    /** extract -> validate -> transform -> load */
    void run(PipelineContext context);

    /**
     * false = load through staging tables, every table commits on its own.
     * true  = load directly; the whole run (tables + rejects + last_run_time) is ONE transaction.
     */
    default boolean directLoad() {
        return false;
    }
}
