package com.demo.etl.framework.audit;

/** Kind of row in etl_audit.etl_data_warning. */
public enum WarningType {

    /** A warning rule failed on a record that WAS loaded (e.g. phone missing, amount unusually high). */
    RULE,

    /** A MongoDB collection has a field the source definition does not know, or misses an expected field. */
    SCHEMA_DRIFT,

    /** An INCREMENTAL run read fewer / more rows than expected. */
    VOLUME
}
