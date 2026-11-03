package com.demo.etl.framework.quality;

import com.demo.etl.framework.common.SparkTypes;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.current_timestamp;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.not;

/**
 * DATA VALIDATION rules for one source, written as a readable list:
 * <pre>
 *   DataQualityRules.create()
 *       .mustBeDecimal("amount")                       // CORRUPT check
 *       .required("paymentId", "amount")               // VALIDATION check
 *       .mustBePositive("amount")
 *       .mustBeOneOf("currency", "INR", "USD")
 *       .mustExistIn("customerId", customers, "customer_id", "dim_customer");   // ORPHAN check
 * </pre>
 * Every rule describes when a record is BAD. A record with zero failed rules is valid.
 * All failed rules of a record are collected, so the rejected table shows every problem at once.
 */
public final class DataQualityRules {

    private static final String EMAIL_REGEX = "^[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{2,}$";
    private static final String INTEGER_REGEX = "^-?[0-9]+$";

    private final List<ValidationRule> rules = new ArrayList<>();
    private final List<ReferenceRule> referenceRules = new ArrayList<>();

    private DataQualityRules() {
    }

    public static DataQualityRules create() {
        return new DataQualityRules();
    }

    // ------------------------------------------------------------------
    // CORRUPT checks: the value is there, but it cannot be converted
    // ------------------------------------------------------------------

    public DataQualityRules mustBeDecimal(String column) {
        Column value = col(column);
        return add(ErrorType.CORRUPT, value.isNotNull().and(value.cast(SparkTypes.MONEY).isNull()),
                column + " is not a valid number", column);
    }

    public DataQualityRules mustBeInteger(String column) {
        Column value = col(column);
        return add(ErrorType.CORRUPT, value.isNotNull().and(not(value.rlike(INTEGER_REGEX))),
                column + " is not a whole number", column);
    }

    /** Expects yyyy-MM-dd. */
    public DataQualityRules mustBeDate(String column) {
        Column value = col(column);
        return add(ErrorType.CORRUPT, value.isNotNull().and(value.cast(SparkTypes.DATE).isNull()),
                column + " is not a valid date (expected yyyy-MM-dd)", column);
    }

    /** Expects yyyy-MM-ddTHH:mm:ss. */
    public DataQualityRules mustBeTimestamp(String column) {
        Column value = col(column);
        return add(ErrorType.CORRUPT, value.isNotNull().and(value.cast(SparkTypes.TIMESTAMP).isNull()),
                column + " is not a valid timestamp (expected yyyy-MM-ddTHH:mm:ss)", column);
    }

    // ------------------------------------------------------------------
    // VALIDATION checks: business rules
    // ------------------------------------------------------------------

    public DataQualityRules required(String... columns) {
        for (String column : columns) {
            add(ErrorType.VALIDATION, col(column).isNull(), column + " is required", null);
        }
        return this;
    }

    public DataQualityRules mustBePositive(String column) {
        return add(ErrorType.VALIDATION, col(column).cast(SparkTypes.MONEY).leq(0),
                column + " must be greater than 0", column);
    }

    public DataQualityRules mustNotBeNegative(String column) {
        return add(ErrorType.VALIDATION, col(column).cast(SparkTypes.MONEY).lt(0),
                column + " cannot be negative", column);
    }

    public DataQualityRules mustBeBetween(String column, double min, double max) {
        Column number = col(column).cast(SparkTypes.RATE);
        return add(ErrorType.VALIDATION, number.lt(min).or(number.gt(max)),
                column + " must be between " + min + " and " + max, column);
    }

    public DataQualityRules mustNotBeInFuture(String column) {
        return add(ErrorType.VALIDATION, col(column).cast(SparkTypes.TIMESTAMP).gt(current_timestamp()),
                column + " cannot be in the future", column);
    }

    public DataQualityRules mustBeOneOf(String column, String... allowedValues) {
        Column value = col(column);
        return add(ErrorType.VALIDATION, value.isNotNull().and(not(value.isin((Object[]) allowedValues))),
                column + " must be one of " + Arrays.toString(allowedValues), column);
    }

    public DataQualityRules mustMatch(String column, String regex, String description) {
        Column value = col(column);
        return add(ErrorType.VALIDATION, value.isNotNull().and(not(value.rlike(regex))),
                column + " " + description, column);
    }

    public DataQualityRules mustBeValidEmail(String column) {
        return mustMatch(column, EMAIL_REGEX, "is not a valid email address");
    }

    /** Anything else, e.g. {@code custom(VALIDATION, "from and to account must differ", col("from").equalTo(col("to")))}. */
    public DataQualityRules custom(ErrorType errorType, String description, Column failWhen) {
        return add(errorType, failWhen, description, null);
    }

    // ------------------------------------------------------------------
    // ORPHAN checks: referential integrity
    // ------------------------------------------------------------------

    /**
     * The value must exist in another dataset (usually a dimension table already in PostgreSQL).
     *
     * @param lookup       dataset that contains the allowed keys
     * @param lookupColumn key column inside {@code lookup}
     * @param lookupName   friendly name for the error message, e.g. "dim_customer"
     */
    public DataQualityRules mustExistIn(String column, Dataset<Row> lookup, String lookupColumn, String lookupName) {
        referenceRules.add(new ReferenceRule(column, lookup, lookupColumn, lookupName));
        return this;
    }

    // ------------------------------------------------------------------

    public List<ValidationRule> rules() {
        return Collections.unmodifiableList(rules);
    }

    public List<ReferenceRule> referenceRules() {
        return Collections.unmodifiableList(referenceRules);
    }

    /** Builds "[TYPE] text (value: x)" so the rejected table shows exactly what was wrong. */
    static Column message(ErrorType errorType, String text, String valueColumn) {
        String prefix = "[" + errorType.name() + "] " + text;
        if (valueColumn == null) {
            return lit(prefix);
        }
        return concat(lit(prefix + " (value: "), coalesce(col(valueColumn), lit("null")), lit(")"));
    }

    private DataQualityRules add(ErrorType errorType, Column failWhen, String text, String valueColumn) {
        rules.add(new ValidationRule(errorType, failWhen, message(errorType, text, valueColumn)));
        return this;
    }
}
