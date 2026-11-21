package com.shop.etl.dataset.silver;

/**
 * DATASET CLASS: {@code curated.fact_sales} (SILVER). Transaction fact.
 *
 * GRAIN: 1 row = 1 order ni 1 product line.  UPSERT KEY: (order_no, line_no)
 * 20 orders → 32 lines → 2 line reject → 30 rows (design doc Section 6.5).
 */
public final class FactSales {

    public static final String TABLE = "fact_sales";

    public static final String ORDER_NO = "order_no";
    public static final String LINE_NO = "line_no";
    public static final String ORDER_DATE_KEY = "order_date_key";
    public static final String CUSTOMER_KEY = "customer_key";
    public static final String PRODUCT_KEY = "product_key";
    public static final String PAYMENT_METHOD_KEY = "payment_method_key";
    public static final String ORDER_STATUS_KEY = "order_status_key";
    public static final String QUANTITY = "quantity";
    public static final String UNIT_PRICE = "unit_price";
    public static final String LINE_AMOUNT = "line_amount";

    private FactSales() {
    }
}
