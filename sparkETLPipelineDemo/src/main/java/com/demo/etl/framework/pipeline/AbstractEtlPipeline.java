package com.demo.etl.framework.pipeline;

import com.demo.etl.framework.quality.DataQualityRules;
import com.demo.etl.framework.quality.DataSanitizer;
import com.demo.etl.framework.source.SourceBatch;
import com.demo.etl.framework.source.SourceDefinition;
import com.demo.etl.framework.target.DateDimension;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Base class for pipelines. It gives you a few simple building blocks, so a pipeline reads like a recipe:
 *
 * <pre>
 *   SourceBatch payments = extractAndValidate(ctx, SOURCE, sanitizer, rules);   // E + data quality
 *   Dataset&lt;Row&gt; typed = mapper.toTyped(payments.valid());                    // T
 *   load(ctx, mapper.toDimPayer(typed), "silver.dim_payer", "payer_id");         // L
 * </pre>
 *
 * Every building block is recorded as a step in etl_audit.etl_job_step.
 * Saving rejected records and updating MongoDB flags happens automatically after run() succeeds.
 */
public abstract class AbstractEtlPipeline implements EtlPipeline {

    protected final PipelineToolkit toolkit;

    protected AbstractEtlPipeline(PipelineToolkit toolkit) {
        this.toolkit = toolkit;
    }

    /**
     * EXTRACT one MongoDB collection, then SANITIZE, VALIDATE and DE-DUPLICATE it.
     * Use {@code batch.valid()} for the next steps. Bad rows are saved for you at the end of the job.
     */
    protected SourceBatch extractAndValidate(PipelineContext context, SourceDefinition source,
                                             DataSanitizer sanitizer, DataQualityRules rules) {
        return toolkit.sourceProcessor().process(context, source, sanitizer, rules);
    }

    /** UPSERT rows into a PostgreSQL table, matching on the business key column(s). */
    protected long load(PipelineContext context, Dataset<Row> rows, String targetTable, String... businessKeyColumns) {
        long count = toolkit.stepTracker().track(context, "Load " + targetTable,
                () -> toolkit.postgresWriter().upsert(context, rows, targetTable, businessKeyColumns));
        context.metrics().addLoaded(count);
        return count;
    }

    /** Adds every date found in {@code dateColumn} to the shared silver.dim_date table. */
    protected long loadDateDimension(PipelineContext context, Dataset<Row> rows, String dateColumn) {
        return load(context, DateDimension.fromColumn(rows, dateColumn), DateDimension.TABLE, DateDimension.KEY_COLUMN);
    }

    /** Replaces a business key with the dimension's surrogate key (adds the key column). */
    protected Dataset<Row> addSurrogateKey(Dataset<Row> facts, String dimensionTable,
                                           String surrogateKeyColumn, String... businessKeyColumns) {
        return toolkit.postgresReader().addSurrogateKey(facts, dimensionTable, surrogateKeyColumn, businessKeyColumns);
    }

    /** Reads a PostgreSQL table (for lookups / ORPHAN checks). */
    protected Dataset<Row> readTable(String table, String... columns) {
        return toolkit.postgresReader().readTable(table, columns);
    }
}
