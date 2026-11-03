package com.demo.etl.framework.quality;

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
    static final String ERROR_TYPE = "_dq_error_type";
    static final String ROW_NUMBER = "_dq_row_number";
    static final String REFERENCE_PREFIX = "_dq_ref_";

    /** Columns of the standard "rejected records" dataset. */
    public static final String REJECTED_SOURCE_ID = "source_id";
    public static final String REJECTED_BUSINESS_KEY = "business_key";
    public static final String REJECTED_ERROR_TYPE = "error_type";
    public static final String REJECTED_ERROR_REASON = "error_reason";
    public static final String REJECTED_RAW_RECORD = "raw_record";

    private QualityColumns() {
    }

    public static boolean isTechnicalColumn(String column) {
        return column.startsWith("_");
    }
}
