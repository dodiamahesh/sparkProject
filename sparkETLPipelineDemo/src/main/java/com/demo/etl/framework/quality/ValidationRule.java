package com.demo.etl.framework.quality;

import org.apache.spark.sql.Column;

import java.util.function.Function;

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

/**
 * One data-quality rule.
 *
 * @param errorType   what kind of problem this rule detects
 * @param failWhen    Spark condition that is TRUE when the record is BAD
 * @param text        readable description, e.g. "amount must be greater than 0"
 * @param valueColumn column whose value is shown in the message (null = no value)
 */
public record ValidationRule(ErrorType errorType, Column failWhen, String text, String valueColumn) {

    /**
     * Builds "[LABEL] text (value: x)" so the rejected table shows exactly what was wrong.
     *
     * @param label   "CORRUPT", "VALIDATION", "ORPHAN" ... or "WARNING"
     * @param valueOf column name -> column to show; masks PII fields, so a real email never ends up in a message
     */
    public Column message(String label, Function<String, Column> valueOf) {
        String prefix = "[" + label + "] " + text;
        if (valueColumn == null) {
            return lit(prefix);
        }
        return concat(lit(prefix + " (value: "), coalesce(valueOf.apply(valueColumn), lit("null")), lit(")"));
    }
}
