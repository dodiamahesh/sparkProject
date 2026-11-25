package com.tcs.dmj.dwh.enums;

/** Why a record was rejected. The order matters: the first error of a record is its most serious one. */
public enum ErrorType {

    /** Wrong data type, e.g. "12abc" for an amount. */
    CORRUPT,
    /** Right type, but a business rule is broken. */
    VALIDATION,
    /** The referenced code / key does not exist. */
    ORPHAN,
    /** Same business key more than once in one run (the latest copy is kept). */
    DUPLICATE
}
