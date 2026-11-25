package com.tcs.dmj.dwh.connector.source;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Result of one source in one run.
 *
 * @param valid   clean rows (MongoDB column names, all String)
 * @param deleted business keys soft-deleted at the source (+ DELETED_AT); null when the source has no soft delete
 * @param derived true = rows built from another batch (e.g. array lines), not read from MongoDB
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

    /** Column of {@link #deleted}: when the document was deleted at the source. */
    public static final String DELETED_AT = "_deleted_at";

    public String collection() {
        return source.collection();
    }

    /** Rejected rows without the duplicates. */
    public long badRowCount() {
        return rejectedCount - duplicateCount;
    }

    /** Rows that were validated (soft-deleted documents are not). */
    public long activeCount() {
        return sourceCount - deletedCount;
    }
}
