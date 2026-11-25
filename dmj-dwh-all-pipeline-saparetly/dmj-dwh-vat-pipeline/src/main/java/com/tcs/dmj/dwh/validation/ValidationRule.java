package com.tcs.dmj.dwh.validation;

import com.tcs.dmj.dwh.enums.ErrorType;
import org.apache.spark.sql.Column;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

/**
 * One rule. failWhen = true for a BAD record. fieldName = the checked field (null = rule on several fields).
 * The message must be fixed text: it is the key of the error in meta.meta_error_master.
 */
public record ValidationRule(ErrorType errorType, Column failWhen, String fieldName, String message) {

    /** The error struct when the rule fails, NULL when it passes. */
    public Column errorWhenFailed() {
        Column value = fieldName == null ? lit(null) : col(fieldName);
        return when(failWhen, QualityColumns.errorStruct(errorType, fieldName, message, value));
    }
}
