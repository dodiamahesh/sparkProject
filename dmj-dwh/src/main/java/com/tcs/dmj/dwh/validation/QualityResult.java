package com.tcs.dmj.dwh.validation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Output of the data-quality check.
 *
 * @param valid    records that passed every rule (same columns as the input) - they are loaded
 * @param rejected records that failed, one row per record in the standard format:
 *                 source_id, business_key, errors (array of every failed rule), raw_record
 */
public record QualityResult(Dataset<Row> valid, Dataset<Row> rejected) {
}
