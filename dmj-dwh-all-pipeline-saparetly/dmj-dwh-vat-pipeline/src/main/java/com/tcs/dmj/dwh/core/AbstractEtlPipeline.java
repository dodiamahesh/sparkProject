package com.tcs.dmj.dwh.core;

import com.tcs.dmj.dwh.connector.source.SourceBatch;
import com.tcs.dmj.dwh.connector.source.SourceDefinition;
import com.tcs.dmj.dwh.connector.target.DateDimension;
import com.tcs.dmj.dwh.connector.target.DirectPostgresWriter;
import com.tcs.dmj.dwh.connector.target.Scd2Writer;
import com.tcs.dmj.dwh.validation.DataQualityRules;
import com.tcs.dmj.dwh.validation.DataSanitizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * Base class for pipelines. It gives you a few simple building blocks, so a pipeline reads like a recipe:
 *
 * <pre>
 *   SourceBatch returns = extractAndValidate(ctx, SOURCE, sanitizer, rules);    // E + data quality
 *   Dataset&lt;Row&gt; typed = mapper.toTyped(returns.valid());                     // T
 *   loadDirect(ctx, mapper.toDimTaxpayer(typed), "silver.dim_taxpayer", List.of("tin"));   // L
 * </pre>
 *
 * Saving rejected records and advancing last_run_time happens automatically after run() succeeds.
 * Every load also records its TABLE lineage (which collections / tables it was built from, see
 * {@link LineageCollector}); it is written to meta.meta_lineage when the run succeeds.
 */
public abstract class AbstractEtlPipeline implements EtlPipeline {

    private static final Logger log = LoggerFactory.getLogger(AbstractEtlPipeline.class);

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
     * VALIDATE rows built from another batch (e.g. one row per element of an array field).
     * A bad line is rejected on its own; its parent document is still loaded. See
     * {@link com.tcs.dmj.dwh.connector.source.SourceProcessor#processDerived}.
     */
    protected SourceBatch validateDerived(PipelineContext context, SourceDefinition definition,
                                          Dataset<Row> rows, DataQualityRules rules) {
        return toolkit.sourceProcessor().processDerived(context, definition, rows, rules);
    }

    /** Loads a slowly changing dimension TYPE 2 (history kept). See {@link Scd2Writer}. */
    protected long loadScd2(PipelineContext context, Dataset<Row> rows, Scd2Writer.Scd2Spec spec) {
        toolkit.lineageCollector().record(context, rows, spec.table(), List.of());
        return loaded(context, spec.table(), toolkit.scd2Writer().load(context, rows, spec));
    }

    /** UPSERT rows into a PostgreSQL table, matching on the business key column(s). */
    protected long load(PipelineContext context, Dataset<Row> rows, String targetTable, String... businessKeyColumns) {
        toolkit.lineageCollector().record(context, rows, targetTable, List.of());
        return loaded(context, targetTable, toolkit.postgresWriter().upsert(context, rows, targetTable, businessKeyColumns));
    }

    /** Adds every date found in {@code dateColumn} to the shared silver.dim_date table. */
    protected long loadDateDimension(PipelineContext context, Dataset<Row> rows, String dateColumn) {
        return load(context, DateDimension.fromColumn(rows, dateColumn), DateDimension.TABLE, DateDimension.KEY_COLUMN);
    }

    // ------------------------------------------------------------------ direct load (no staging)

    /**
     * UPSERT rows DIRECTLY into a PostgreSQL table, without a staging table, inside the job's transaction.
     * Only for pipelines with {@link #directLoad()} = true: nothing is visible until the whole job commits.
     *
     * @param lookups surrogate keys looked up in SQL (a dimension written in this same transaction is
     *                invisible to Spark), see {@link DirectPostgresWriter.KeyLookup}
     */
    protected long loadDirect(PipelineContext context, Dataset<Row> rows, String targetTable, List<String> keyColumns,
                              DirectPostgresWriter.KeyLookup... lookups) {
        toolkit.lineageCollector().record(context, rows, targetTable,
                Arrays.stream(lookups).map(DirectPostgresWriter.KeyLookup::dimensionTable).toList());
        return loaded(context, targetTable, toolkit.directWriter().upsert(context, rows, targetTable, keyColumns, List.of(lookups)));
    }

    /** Adds every date of the given columns to silver.dim_date, directly (see {@link #loadDirect}). */
    protected long loadDateDimensionDirect(PipelineContext context, Dataset<Row> rows, String... dateColumns) {
        Dataset<Row> dates = null;
        for (String dateColumn : dateColumns) {
            Dataset<Row> datesOfColumn = DateDimension.fromColumn(rows, dateColumn);
            dates = dates == null ? datesOfColumn : dates.union(datesOfColumn);
        }
        return loadDirect(context, dates.distinct(), DateDimension.TABLE, List.of(DateDimension.KEY_COLUMN));
    }

    // ------------------------------------------------------------------ soft delete

    /**
     * SOFT DELETE: the documents of {@code batch} that were deleted at the source (isDeleted = true) are marked
     * is_deleted = true / deleted_at in {@code targetTable}, on the rows whose {@code targetKeyColumn} holds their
     * business key. Rows are never removed, so facts and history stay. Call it once per target table that is
     * built from this source; the job FAILS if a source with deletes is never applied (nothing is lost silently).
     *
     * <pre>
     *   applyDeletes(ctx, returns, "silver.fact_vat_return", "ack_no");
     *   applyDeletes(ctx, returns, "silver.fact_x", "ack_no");          // same source in a 2nd table: call it again
     * </pre>
     */
    protected long applyDeletes(PipelineContext context, SourceBatch batch, String targetTable, String targetKeyColumn) {
        return softDelete(context, batch, targetTable, targetKeyColumn, false);
    }

    /** SOFT DELETE for an SCD2 dimension: the CURRENT version is closed (valid_to = deleted_at) and marked deleted. */
    protected long applyDeletesScd2(PipelineContext context, SourceBatch batch, Scd2Writer.Scd2Spec spec) {
        return softDelete(context, batch, spec.table(), spec.businessKey(), true);
    }

    private long softDelete(PipelineContext context, SourceBatch batch, String table, String keyColumn, boolean scd2) {
        context.markDeletesApplied(batch.collection());
        if (batch.deletedCount() == 0) {
            return 0;
        }
        return toolkit.softDeleteWriter().apply(context, batch, table, keyColumn, scd2);
    }

    private static long loaded(PipelineContext context, String table, long count) {
        context.metrics().addLoaded(count);
        log.info("[job {}] loaded {}: {} rows", context.pipelineRunId(), table, count);
        return count;
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
