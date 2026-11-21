package com.shop.etl.dataset.silver;

/**
 * DATASET CLASS: {@code curated.fact_payment} (SILVER). Transaction fact.
 *
 * GRAIN: 1 row = 1 payment.  UPSERT KEY: payment_id
 * customer_key payment mathi nahi, ORDER mathi aave che (fact_sales lookup).
 */
public final class FactPayment {

    public static final String TABLE = "fact_payment";

    public static final String PAYMENT_ID = "payment_id";
    public static final String ORDER_NO = "order_no";
    public static final String PAYMENT_DATE_KEY = "payment_date_key";
    public static final String CUSTOMER_KEY = "customer_key";
    public static final String PAYMENT_METHOD_KEY = "payment_method_key";
    public static final String PAYMENT_STATUS = "payment_status";
    public static final String AMOUNT = "amount";

    private FactPayment() {
    }
}
