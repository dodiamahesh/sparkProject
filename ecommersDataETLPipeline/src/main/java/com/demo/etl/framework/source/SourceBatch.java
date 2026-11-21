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
 * @param derived  true = the rows do not come straight from a MongoDB collection but were built from
 *                 another batch (e.g. the items[] array of an order, one row per line). Rejected rows and
 *                 warnings are saved as usual, but NO MongoDB flag is written for them: the parent
 *                 document already gets its own flag.
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
        long warningCount,
        boolean derived) {

    /** A normal batch, read directly from a MongoDB collection. */
    public SourceBatch(SourceDefinition source, Dataset<Row> valid, Dataset<Row> rejected, Dataset<Row> warnings,
                       long sourceCount, long validCount, long rejectedCount, long duplicateCount, long warningCount) {
        this(source, valid, rejected, warnings, sourceCount, validCount, rejectedCount, duplicateCount, warningCount, false);
    }

    public String collection() {
        return source.collection();
    }

    /** Rejected rows that are really bad (duplicates are a normal part of change data). */
    public long badRowCount() {
        return rejectedCount - duplicateCount;
    }
}
