package com.demo.etl.framework.source;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Result of reading + cleaning + validating ONE source collection in ONE job run.
 *
 * @param valid    clean rows, ready to transform (same column names as MongoDB, all String, PII masked)
 * @param rejected bad rows in the standard rejected format, one row per record (saved at the end of the job)
 * @param derived  true = the rows do not come straight from a MongoDB collection but were built from
 *                 another batch (e.g. the items[] array of an order, one row per line). Rejected rows are
 *                 saved as usual, but they have NO watermark of their own: the parent collection's
 *                 watermark covers them.
 */
public record SourceBatch(
        SourceDefinition source,
        Dataset<Row> valid,
        Dataset<Row> rejected,
        long sourceCount,
        long validCount,
        long rejectedCount,
        long duplicateCount,
        boolean derived) {

    public String collection() {
        return source.collection();
    }

    /** Rejected rows that are really bad (duplicates are a normal part of change data). */
    public long badRowCount() {
        return rejectedCount - duplicateCount;
    }
}
