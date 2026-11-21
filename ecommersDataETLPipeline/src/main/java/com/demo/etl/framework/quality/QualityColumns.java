package com.demo.etl.framework.quality;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.struct;

/**
 * Technical column names added by the framework. They all start with "_" so they never
 * clash with business columns.
 */
public final class QualityColumns {

    /** MongoDB document id. */
    public static final String SOURCE_ID = "_id";

    /** Original document as JSON (taken BEFORE cleaning) - stored in the rejected table. */
    public static final String RAW_RECORD = "_raw_record";

    static final String ERRORS = "_dq_errors";
    static final String ROW_NUMBER = "_dq_row_number";
    static final String REFERENCE_PREFIX = "_dq_ref_";

    /**
     * Columns of the standard "rejected records" dataset: ONE row per rejected record.
     * {@link #REJECTED_ERRORS} is an array with every failed rule of that record (most serious first).
     */
    public static final String REJECTED_SOURCE_ID = "source_id";
    public static final String REJECTED_BUSINESS_KEY = "business_key";
    public static final String REJECTED_ERRORS = "errors";
    public static final String REJECTED_RAW_RECORD = "raw_record";

    /**
     * Fields of ONE error (struct inside errors[]). error_type + error_message identify the row in
     * meta.meta_error_master; field_value is the bad value of this record (PII masked).
     */
    public static final String ERROR_TYPE = "error_type";
    public static final String FIELD_NAME = "field_name";
    public static final String ERROR_MESSAGE = "error_message";
    public static final String FIELD_VALUE = "field_value";

    static final StructType ERROR_SCHEMA = new StructType()
            .add(ERROR_TYPE, DataTypes.StringType)
            .add(FIELD_NAME, DataTypes.StringType)
            .add(ERROR_MESSAGE, DataTypes.StringType)
            .add(FIELD_VALUE, DataTypes.StringType);

    private QualityColumns() {
    }

    public static boolean isTechnicalColumn(String column) {
        return column.startsWith("_");
    }

    /** One error as a struct. Always built here, so every error struct has the same field order. */
    static Column errorStruct(ErrorType type, String fieldName, String message, Column value) {
        return struct(
                lit(type.name()).as(ERROR_TYPE),
                lit(fieldName).cast(DataTypes.StringType).as(FIELD_NAME),
                lit(message).as(ERROR_MESSAGE),
                value.cast(DataTypes.StringType).as(FIELD_VALUE));
    }

    /** The most serious error type of a rejected record (errors[] is sorted, see {@link ErrorType}). */
    public static Column mainErrorType() {
        return col(REJECTED_ERRORS).getItem(0).getField(ERROR_TYPE);
    }

    /**
     * Rejected records -> ONE row per (record, error):
     * source_id, business_key, error_type, field_name, error_message, field_value, raw_record.
     * This is the shape of meta.meta_rejected_record (error_type + error_message become an error_id).
     */
    public static Dataset<Row> oneRowPerError(Dataset<Row> rejected) {
        return rejected
                .select(col(REJECTED_SOURCE_ID), col(REJECTED_BUSINESS_KEY),
                        explode(col(REJECTED_ERRORS)).as("e"), col(REJECTED_RAW_RECORD))
                .select(col(REJECTED_SOURCE_ID), col(REJECTED_BUSINESS_KEY),
                        col("e." + ERROR_TYPE).as(ERROR_TYPE), col("e." + FIELD_NAME).as(FIELD_NAME),
                        col("e." + ERROR_MESSAGE).as(ERROR_MESSAGE), col("e." + FIELD_VALUE).as(FIELD_VALUE),
                        col(REJECTED_RAW_RECORD));
    }
}
