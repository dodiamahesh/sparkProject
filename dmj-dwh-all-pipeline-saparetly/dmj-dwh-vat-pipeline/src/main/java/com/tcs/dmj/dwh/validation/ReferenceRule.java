package com.tcs.dmj.dwh.validation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Referential-integrity rule: the value in {@code column} must exist in {@code lookup.lookupColumn}.
 * Example: vat_return_coll.formId must exist in silver.dim_tax_form.form_id.
 */
public record ReferenceRule(String column, Dataset<Row> lookup, String lookupColumn, String lookupName) {
}
