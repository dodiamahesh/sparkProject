package com.demo.etl.framework.quality;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Output of the data-quality check.
 *
 * @param valid    records that passed every rule (same columns as the input)
 * @param rejected records that failed, in the standard format:
 *                 source_id, business_key, error_type, error_reason, raw_record
 */
public record QualityResult(Dataset<Row> valid, Dataset<Row> rejected) {
}
