package com.demo.etl.framework.pipeline;

import com.demo.etl.framework.quality.DataQualityRules;
import com.demo.etl.framework.quality.DataSanitizer;
import com.demo.etl.framework.source.SourceBatch;
import com.demo.etl.framework.source.SourceDefinition;
import com.demo.etl.framework.target.DateDimension;
import com.demo.etl.framework.target.Scd2Writer;
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
 * Every building block is recorded as a step in meta.meta_pipeline_step.
 * Saving rejected records and advancing the watermarks happens automatically after run() succeeds.
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
    protected SourceBatch extractAndValidate(PipelineContext context, SourceDefinition source,DataSanitizer sanitizer, DataQualityRules rules) {
        return toolkit.sourceProcessor().process(context, source, sanitizer, rules);
    }

    /**
     * VALIDATE rows built from another batch (e.g. one row per line of an order's items[] array).
     * A bad line is rejected on its own; its parent document is still loaded. See
     * {@link com.demo.etl.framework.source.SourceProcessor#processDerived}.
     */
    protected SourceBatch validateDerived(PipelineContext context, SourceDefinition definition,
                                          Dataset<Row> rows, DataQualityRules rules) {
        return toolkit.sourceProcessor().processDerived(context, definition, rows, rules);
    }

    /** Loads a slowly changing dimension TYPE 2 (history kept). See {@link Scd2Writer}. */
    protected long loadScd2(PipelineContext context, Dataset<Row> rows, Scd2Writer.Scd2Spec spec) {
        long count = toolkit.stepTracker().track(context, "Load " + spec.table() + " (SCD2)",
                () -> toolkit.scd2Writer().load(context, rows, spec));
        context.metrics().addLoaded(count);
        return count;
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
