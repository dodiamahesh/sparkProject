package com.demo.etl.framework.audit;

public enum JobStatus {
    /** Job is running right now. */
    RUNNING,
    /** All records loaded, nothing rejected. */
    SUCCESS,
    /** Job finished, valid records loaded, some records rejected (see etl_rejected_record). */
    SUCCESS_WITH_REJECTS,
    /** Job stopped because of an error (see error_message). Nothing was committed to MongoDB flags. */
    FAILED
}
