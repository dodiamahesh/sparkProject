package com.tcs.dmj.dwh.validation;

import com.tcs.dmj.dwh.enums.ErrorType;
import org.apache.spark.sql.Column;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

/**
 * One data-quality rule.
 *
 * @param errorType what kind of problem this rule detects
 * @param failWhen  Spark condition that is TRUE when the record is BAD
 * @param fieldName the checked field; its value is stored with the error (null = rule on several fields)
 * @param message   readable description, e.g. "amount must be greater than 0". Together with errorType it is
 *                  the key of the error in meta.meta_error_master, so it must NOT contain record values.
 */
public record ValidationRule(ErrorType errorType, Column failWhen, String fieldName, String message) {

    /** struct(error_type, field_name, error_message, field_value) when the rule fails, NULL when it passes. */
    public Column errorWhenFailed() {
        Column value = fieldName == null ? lit(null) : col(fieldName);
        return when(failWhen, QualityColumns.errorStruct(errorType, fieldName, message, value));
    }
}
