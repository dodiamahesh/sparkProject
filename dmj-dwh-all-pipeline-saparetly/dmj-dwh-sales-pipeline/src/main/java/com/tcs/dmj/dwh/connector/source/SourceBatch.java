package com.tcs.dmj.dwh.connector.source;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Result of reading + cleaning + validating ONE source collection in ONE job run.
 *
 * @param valid       clean rows, ready to transform (same column names as MongoDB, all String)
 * @param rejected    bad rows in the standard rejected format, one row per record (saved at the end of the job)
 * @param deleted     business keys soft-deleted at the source in this window: the business key column +
 *                    {@link #DELETED_AT}. null when the source has no soft delete (and for derived batches).
 *                    The pipeline applies them to its tables with AbstractEtlPipeline.applyDeletes(...)
 * @param sourceCount documents read (active + deleted)
 * @param derived     true = the rows do not come straight from a MongoDB collection but were built from
 *                    another batch (e.g. the items[] array of an order, one row per line). Rejected rows are
 *                    saved as usual, but they have NO window of their own: the parent collection
 *                    window covers them.
 */
public record SourceBatch(
        SourceDefinition source,
        Dataset<Row> valid,
        Dataset<Row> rejected,
        Dataset<Row> deleted,
        long sourceCount,
        long validCount,
        long rejectedCount,
        long duplicateCount,
        long deletedCount,
        boolean derived) {

    /** Column of {@link #deleted}: when the document was deleted at the source (its timestamp field). */
    public static final String DELETED_AT = "_deleted_at";

    public String collection() {
        return source.collection();
    }

    /** Rejected rows that are really bad (duplicates are a normal part of change data). */
    public long badRowCount() {
        return rejectedCount - duplicateCount;
    }

    /** Documents that went through validation (soft-deleted documents are not validated). */
    public long activeCount() {
        return sourceCount - deletedCount;
    }
}
