package com.demo.etl.framework.source;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Result of reading + cleaning + validating ONE source collection in ONE job run.
 *
 * @param valid    clean rows, ready to transform (same column names as MongoDB, all String, PII masked)
 * @param rejected bad rows in the standard rejected format (saved at the end of the job)
 * @param warnings valid rows that broke a warning rule: source_id, business_key, warning_message
 *                 (saved at the end of the job)
 */
public record SourceBatch(
        SourceDefinition source,
        Dataset<Row> valid,
        Dataset<Row> rejected,
        Dataset<Row> warnings,
        long sourceCount,
        long validCount,
        long rejectedCount,
        long duplicateCount,
        long warningCount) {

    public String collection() {
        return source.collection();
    }

    /** Rejected rows that are really bad (duplicates are a normal part of change data). */
    public long badRowCount() {
        return rejectedCount - duplicateCount;
    }
}
