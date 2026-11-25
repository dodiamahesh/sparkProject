package com.tcs.dmj.dwh.validation;

import com.tcs.dmj.dwh.enums.ErrorType;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.struct;

/** Technical column names of the framework. They start with "_" so they never clash with business columns. */
public final class QualityColumns {

    /** MongoDB document id. */
    public static final String SOURCE_ID = "_id";
    /** The original document as JSON (before cleaning), stored for rejected records. */
    public static final String RAW_RECORD = "_raw_record";

    static final String ERRORS = "_dq_errors";
    static final String ROW_NUMBER = "_dq_row_number";
    static final String REFERENCE_PREFIX = "_dq_ref_";

    // rejected dataset: one row per rejected record, errors = array of every failed rule
    public static final String REJECTED_SOURCE_ID = "source_id";
    public static final String REJECTED_BUSINESS_KEY = "business_key";
    public static final String REJECTED_ERRORS = "errors";
    public static final String REJECTED_RAW_RECORD = "raw_record";

    // fields of one error struct
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

    static Column errorStruct(ErrorType type, String fieldName, String message, Column value) {
        return struct(
                lit(type.name()).as(ERROR_TYPE),
                lit(fieldName).cast(DataTypes.StringType).as(FIELD_NAME),
                lit(message).as(ERROR_MESSAGE),
                value.cast(DataTypes.StringType).as(FIELD_VALUE));
    }

    /** The most serious error type of a rejected record. */
    public static Column mainErrorType() {
        return col(REJECTED_ERRORS).getItem(0).getField(ERROR_TYPE);
    }

    /** Rejected records -> one row per (record, error), the shape saved in meta.meta_rejected_record. */
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
