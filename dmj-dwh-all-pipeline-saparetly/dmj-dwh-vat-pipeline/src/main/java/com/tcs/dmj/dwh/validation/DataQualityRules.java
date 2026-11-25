package com.tcs.dmj.dwh.validation;

import com.tcs.dmj.dwh.enums.ErrorType;
import com.tcs.dmj.dwh.service.constant.SparkTypes;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.current_timestamp;
import static org.apache.spark.sql.functions.not;

/**
 * The data-quality rules of one source. Every rule says when a record is BAD.
 * A rule message is fixed text (no record values): error type + message = one row in meta.meta_error_master.
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

    // ---------------------------------------------------------------- CORRUPT: value cannot be converted

    public DataQualityRules mustBeDecimal(String column) {
        return add(ErrorType.CORRUPT, cannotBeCastTo(column, SparkTypes.MONEY), column,
                column + " is not a valid number");
    }

    public DataQualityRules mustBeInteger(String column) {
        Column value = col(column);
        return add(ErrorType.CORRUPT, value.isNotNull().and(not(value.rlike(INTEGER_REGEX))), column,
                column + " is not a whole number");
    }

    /** yyyy-MM-dd */
    public DataQualityRules mustBeDate(String column) {
        return add(ErrorType.CORRUPT, cannotBeCastTo(column, SparkTypes.DATE), column,
                column + " is not a valid date (expected yyyy-MM-dd)");
    }

    /** yyyy-MM-ddTHH:mm:ss */
    public DataQualityRules mustBeTimestamp(String column) {
        return add(ErrorType.CORRUPT, cannotBeCastTo(column, SparkTypes.TIMESTAMP), column,
                column + " is not a valid timestamp (expected yyyy-MM-ddTHH:mm:ss)");
    }

    // ---------------------------------------------------------------- VALIDATION: business rules

    public DataQualityRules required(String... columns) {
        for (String column : columns) {
            add(ErrorType.VALIDATION, col(column).isNull(), column, column + " is required");
        }
        return this;
    }

    public DataQualityRules mustBePositive(String column) {
        return add(ErrorType.VALIDATION, money(column).leq(0), column, column + " must be greater than 0");
    }

    public DataQualityRules mustNotBeNegative(String column) {
        return add(ErrorType.VALIDATION, money(column).lt(0), column, column + " cannot be negative");
    }

    public DataQualityRules mustNotBeMoreThan(String column, double max) {
        return add(ErrorType.VALIDATION, money(column).gt(max), column, column + " is more than " + max);
    }

    public DataQualityRules mustBeBetween(String column, double min, double max) {
        Column number = col(column).cast(SparkTypes.RATE);
        return add(ErrorType.VALIDATION, number.lt(min).or(number.gt(max)), column,
                column + " must be between " + min + " and " + max);
    }

    public DataQualityRules mustNotBeInFuture(String column) {
        return add(ErrorType.VALIDATION, col(column).cast(SparkTypes.TIMESTAMP).gt(current_timestamp()), column,
                column + " cannot be in the future");
    }

    public DataQualityRules mustBeOneOf(String column, String... allowedValues) {
        Column value = col(column);
        return add(ErrorType.VALIDATION, value.isNotNull().and(not(value.isin((Object[]) allowedValues))), column,
                column + " must be one of " + Arrays.toString(allowedValues));
    }

    public DataQualityRules mustMatch(String column, String regex, String description) {
        Column value = col(column);
        return add(ErrorType.VALIDATION, value.isNotNull().and(not(value.rlike(regex))), column,
                column + " " + description);
    }

    public DataQualityRules mustBeValidEmail(String column) {
        return mustMatch(column, EMAIL_REGEX, "is not a valid email address");
    }

    /** Any other rule: {@code failWhen} is true for a BAD record. */
    public DataQualityRules custom(ErrorType errorType, String message, Column failWhen) {
        return add(errorType, failWhen, null, message);
    }

    // ---------------------------------------------------------------- ORPHAN: referenced key must exist

    /** The value of {@code column} must exist in {@code lookup.lookupColumn}. lookupName is used in the message. */
    public DataQualityRules mustExistIn(String column, Dataset<Row> lookup, String lookupColumn, String lookupName) {
        referenceRules.add(new ReferenceRule(column, lookup, lookupColumn, lookupName));
        return this;
    }

    // ----------------------------------------------------------------

    public List<ValidationRule> rules() {
        return Collections.unmodifiableList(rules);
    }

    public List<ReferenceRule> referenceRules() {
        return Collections.unmodifiableList(referenceRules);
    }

    private static Column money(String column) {
        return col(column).cast(SparkTypes.MONEY);
    }

    /** Value present, but the cast gives NULL (non-ANSI mode). */
    private static Column cannotBeCastTo(String column, DataType type) {
        Column value = col(column);
        return value.isNotNull().and(value.cast(type).isNull());
    }

    private DataQualityRules add(ErrorType errorType, Column failWhen, String fieldName, String message) {
        rules.add(new ValidationRule(errorType, failWhen, fieldName, message));
        return this;
    }
}
