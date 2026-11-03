package com.demo.etl.framework.source;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Result of reading + cleaning + validating ONE source collection in ONE job run.
 *
 * @param valid    clean rows, ready to transform (same column names as MongoDB, all String)
 * @param rejected bad rows in the standard rejected format (saved at the end of the job)
 */
public record SourceBatch(
        SourceDefinition source,
        Dataset<Row> valid,
        Dataset<Row> rejected,
        long sourceCount,
        long validCount,
        long rejectedCount,
        long duplicateCount) {

    public String collection() {
        return source.collection();
    }
}
