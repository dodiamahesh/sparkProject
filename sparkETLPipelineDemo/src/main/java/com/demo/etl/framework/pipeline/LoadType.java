package com.demo.etl.framework.pipeline;

import java.util.Arrays;

/**
 * How much source data a job reads. Driven by the flag field in MongoDB (not by timestamps).
 */
public enum LoadType {

    /** Read ALL documents. Fact tables are truncated and rebuilt, dimensions are upserted. */
    FULL,

    /** Read only documents with processedFlag = 'N' (or no flag yet). */
    INCREMENTAL,

    /** Read only documents with processedFlag = 'E' (rejected earlier, maybe fixed now). */
    RETRY_REJECTED;

    /** Case-insensitive parse with a helpful error message. */
    public static LoadType from(String value) {
        return Arrays.stream(values())
                .filter(type -> type.name().equalsIgnoreCase(value))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "Invalid loadType '" + value + "'. Allowed values: " + Arrays.toString(values())));
    }
}
