package com.shop.etl.dataset.silver;

/**
 * DATASET CLASS: {@code curated.dim_payment_method} (SILVER). Nani reference dimension (4 rows).
 * UPI, CARD, NETBANKING, COD. Values SalesReferenceData mathi seed thay che.
 */
public final class DimPaymentMethod {

    public static final String TABLE = "dim_payment_method";

    public static final String PAYMENT_METHOD_KEY = "payment_method_key";
    public static final String METHOD_CODE = "method_code";
    public static final String METHOD_NAME = "method_name";
    public static final String IS_ONLINE = "is_online";

    private DimPaymentMethod() {
    }
}
