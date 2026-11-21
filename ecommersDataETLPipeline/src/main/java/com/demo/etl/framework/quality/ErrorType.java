package com.demo.etl.framework.quality;

/**
 * Why a record was rejected. The order matters: when a record breaks several rules,
 * the FIRST type in this list is stored as its main error_type.
 */
public enum ErrorType {

    /** Value exists but has the wrong data type: "12abc" for an amount, "2026-13-45" for a date. */
    CORRUPT,

    /** Value has the right type but breaks a business rule: missing field, negative amount, bad email. */
    VALIDATION,

    /** Referential integrity: the record points to a customer/account/merchant that does not exist. */
    ORPHAN,

    /** Same business key seen more than once in this batch. One copy is kept, the others are rejected. */
    DUPLICATE
}
