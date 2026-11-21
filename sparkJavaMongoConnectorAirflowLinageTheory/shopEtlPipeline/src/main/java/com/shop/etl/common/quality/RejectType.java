package com.shop.etl.common.quality;

/**
 * Reject na prakar (meta.meta_rejected.reject_type). Design doc Section 3.2.
 *
 * Pipeline ma aa j kram ma check thay che: CORRUPT → DUPLICATE → VALIDATION → FK_MISSING.
 * Pehlo fail thayelo rule j lakhay che (ek row nu ek j reject reason).
 */
public enum RejectType {

    /** Type convert na thay: price "abc", date "2026-13-45". */
    CORRUPT,

    /** Same business key 2 vaar: latest rakhyo, juno reject. */
    DUPLICATE,

    /** Business rule fail: email format, qty 0, status allowed list ma nathi. */
    VALIDATION,

    /** Reference data nathi: order no customer C099 dim_customer ma nathi. */
    FK_MISSING,

    /** Ek thi vadhare entity ne jodto business niyam: cancelled order no shipment (Job-4). */
    BUSINESS_RULE
}
