package com.tcs.dmj.dwh.validation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/** valid = rows that passed every rule; rejected = source_id, business_key, errors[], raw_record. */
public record QualityResult(Dataset<Row> valid, Dataset<Row> rejected) {
}
