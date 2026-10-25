package com.de.spark.pipeline.bronze2silver;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

/**
 * Builds rows for de_silver.etl_rejected_record: which record, why it was rejected, and its original data.
 * Every rejected or dropped record ends up there, so nothing disappears silently.
 */
public final class Rejects {

    // Validation reasons
    public static final String MISSING_ID = "MISSING_ID";
    public static final String MISSING_TIMESTAMP = "MISSING_TIMESTAMP";
    public static final String MISSING_STATION = "MISSING_STATION";
    public static final String INVALID_COORDINATES = "INVALID_COORDINATES";
    public static final String MISSING_STATION_NAME = "MISSING_STATION_NAME";
    // Duplicate reasons
    public static final String DUPLICATE_IN_BATCH = "DUPLICATE_IN_BATCH";
    public static final String DUPLICATE_OF_EXISTING = "DUPLICATE_OF_EXISTING";

    /** Same column layout as stg_rejected_record. */
    public static Dataset<Row> of(Dataset<Row> df, String sourceCollection, Column recordKey,
                                  Column reason, Column details, Column rawPayload) {
        return df.select(
                lit(sourceCollection).as("source_collection"),
                recordKey.as("record_key"),
                reason.as("reason"),
                substring(details, 1, 500).as("details"),
                rawPayload.as("raw_payload"));
    }

    private Rejects() {
    }
}
