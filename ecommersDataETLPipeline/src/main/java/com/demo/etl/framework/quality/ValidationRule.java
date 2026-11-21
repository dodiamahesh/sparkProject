package com.demo.etl.framework.quality;

import org.apache.spark.sql.Column;

import java.util.function.Function;

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

    /**
     * struct(error_type, field_name, error_message, field_value) when the rule fails, NULL when it passes.
     *
     * @param valueOf column name -> column to store as field_value; masks PII fields, so a real email
     *                never ends up in the rejected table
     */
    public Column errorWhenFailed(Function<String, Column> valueOf) {
        Column value = fieldName == null ? lit(null) : valueOf.apply(fieldName);
        return when(failWhen, QualityColumns.errorStruct(errorType, fieldName, message, value));
    }
}
