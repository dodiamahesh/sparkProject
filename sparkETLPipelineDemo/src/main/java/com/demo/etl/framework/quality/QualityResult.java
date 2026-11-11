package com.demo.etl.framework.quality;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Output of the data-quality check.
 *
 * @param valid    records that passed every error rule (same columns as the input) - they are loaded
 * @param rejected records that failed, in the standard format:
 *                 source_id, business_key, error_type, error_reason, raw_record
 * @param warnings VALID records that broke a warning rule: source_id, business_key, warning_message
 */
public record QualityResult(Dataset<Row> valid, Dataset<Row> rejected, Dataset<Row> warnings) {
}
