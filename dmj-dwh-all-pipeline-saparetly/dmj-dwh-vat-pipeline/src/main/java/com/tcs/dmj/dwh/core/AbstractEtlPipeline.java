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
 * Base class of every pipeline: building blocks for extract + validate, load and soft delete.
 * Every load also records its table lineage. Rejects and last_run_time are saved after run() succeeds.
 */
public abstract class AbstractEtlPipeline implements EtlPipeline {

    private static final Logger log = LoggerFactory.getLogger(AbstractEtlPipeline.class);

    protected final PipelineToolkit toolkit;

    protected AbstractEtlPipeline(PipelineToolkit toolkit) {
        this.toolkit = toolkit;
    }

    // ---------------------------------------------------------------- extract + validate

    /** Reads one collection (incremental window), cleans, validates and de-duplicates it. Use batch.valid(). */
    protected SourceBatch extractAndValidate(PipelineContext context, SourceDefinition source,DataSanitizer sanitizer, DataQualityRules rules) {
        return toolkit.sourceProcessor().process(context, source, sanitizer, rules);
    }

    /** Validates rows built from another batch (e.g. array lines); a bad line is rejected on its own. */
    protected SourceBatch validateDerived(PipelineContext context, SourceDefinition definition,Dataset<Row> rows, DataQualityRules rules) {
        return toolkit.sourceProcessor().processDerived(context, definition, rows, rules);
    }

    // ---------------------------------------------------------------- load with staging (directLoad = false)

    /** UPSERT on the business key column(s), through a staging table. */
    protected long load(PipelineContext context, Dataset<Row> rows, String targetTable, String... businessKeyColumns) {
        toolkit.lineageCollector().record(context, rows, targetTable, List.of());
        return loaded(context, targetTable, toolkit.postgresWriter().upsert(context, rows, targetTable, businessKeyColumns));
    }

    /** SCD type 2 dimension (history kept). */
    protected long loadScd2(PipelineContext context, Dataset<Row> rows, Scd2Writer.Scd2Spec spec) {
        toolkit.lineageCollector().record(context, rows, spec.table(), List.of());
        return loaded(context, spec.table(), toolkit.scd2Writer().load(context, rows, spec));
    }

    /** Adds every date of {@code dateColumn} to silver.dim_date. */
    protected long loadDateDimension(PipelineContext context, Dataset<Row> rows, String dateColumn) {
        return load(context, DateDimension.fromColumn(rows, dateColumn), DateDimension.TABLE, DateDimension.KEY_COLUMN);
    }

    // ---------------------------------------------------------------- direct load (directLoad = true)

    /**
     * UPSERT straight into the table, inside the run's transaction.
     *
     * @param lookups surrogate keys looked up inside the SQL (for dimensions written in this same run)
     */
    protected long loadDirect(PipelineContext context, Dataset<Row> rows, String targetTable, List<String> keyColumns,DirectPostgresWriter.KeyLookup... lookups) {
        List<String> lookupTables = Arrays.stream(lookups).map(DirectPostgresWriter.KeyLookup::dimensionTable).toList();
        toolkit.lineageCollector().record(context, rows, targetTable, lookupTables);
        long written = toolkit.directWriter().upsert(context, rows, targetTable, keyColumns, List.of(lookups));
        return loaded(context, targetTable, written);
    }

    /** Adds every date of the given columns to silver.dim_date (direct load). */
    protected long loadDateDimensionDirect(PipelineContext context, Dataset<Row> rows, String... dateColumns) {
        Dataset<Row> dates = null;
        for (String dateColumn : dateColumns) {
            Dataset<Row> datesOfColumn = DateDimension.fromColumn(rows, dateColumn);
            dates = dates == null ? datesOfColumn : dates.union(datesOfColumn);
        }
        return loadDirect(context, dates.distinct(), DateDimension.TABLE, List.of(DateDimension.KEY_COLUMN));
    }

    // ---------------------------------------------------------------- soft delete

    /**
     * Marks the rows of {@code targetTable} whose {@code targetKeyColumn} holds a key deleted at the source.
     * Call it once per table built from the source; a source whose deletes are never applied fails the run.
     */
    protected long applyDeletes(PipelineContext context, SourceBatch batch, String targetTable, String targetKeyColumn) {
        return softDelete(context, batch, targetTable, targetKeyColumn, false);
    }

    /** Same for an SCD2 dimension: the current version is closed and marked deleted. */
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

    // ---------------------------------------------------------------- lookups

    /** Joins the dimension and adds its surrogate key column (committed data only). */
    protected Dataset<Row> addSurrogateKey(Dataset<Row> facts, String dimensionTable,String surrogateKeyColumn, String... businessKeyColumns) {
        return toolkit.postgresReader().addSurrogateKey(facts, dimensionTable, surrogateKeyColumn, businessKeyColumns);
    }

    /** Reads a PostgreSQL table (committed data only), e.g. for ORPHAN checks. */
    protected Dataset<Row> readTable(String table, String... columns) {
        return toolkit.postgresReader().readTable(table, columns);
    }

    private static long loaded(PipelineContext context, String table, long count) {
        context.metrics().addLoaded(count);
        log.info("[job {}] loaded {}: {} rows", context.pipelineRunId(), table, count);
        return count;
    }
}
