package com.demo.etl.framework.quality;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Referential-integrity rule: the value in {@code column} must exist in {@code lookup.lookupColumn}.
 * Example: transactions.customerId must exist in silver.dim_customer.customer_id.
 */
public record ReferenceRule(String column, Dataset<Row> lookup, String lookupColumn, String lookupName) {
}
