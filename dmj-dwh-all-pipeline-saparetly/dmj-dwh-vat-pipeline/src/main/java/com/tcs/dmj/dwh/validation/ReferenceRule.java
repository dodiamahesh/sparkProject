package com.tcs.dmj.dwh.validation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/** ORPHAN rule: the value of {@code column} must exist in {@code lookup.lookupColumn}. */
public record ReferenceRule(String column, Dataset<Row> lookup, String lookupColumn, String lookupName) {
}
