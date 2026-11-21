package com.shop.etl.dataset.gold;

/**
 * DATASET CLASS: {@code gold.customer_360_summary} (GOLD layer). Design doc Section 9 (Job-5).
 *
 * Job-2 aa table NATHI lakhto. Aa class Job-5 (J5_CUSTOMER_360) mate taiyar rakhi che, jethi
 * dataset classes na 3 layer (raw / silver / gold) ek j pattern ma dekhay.
 *
 * GRAIN: 1 row = 1 customer_code (customer_key nahi: SCD2 ni badhi versions ek row ma).
 * Source: silver tables (Mongo nahi). Load: EPAS ma SQL upsert (staging nahi).
 */
public final class Customer360Summary {

    public static final String TABLE = "customer_360_summary";

    public static final String CUSTOMER_CODE = "customer_code";
    public static final String CUSTOMER_NAME = "customer_name";
    public static final String CITY = "city";
    public static final String TIER = "tier";
    public static final String TOTAL_ORDERS = "total_orders";
    public static final String NET_SPEND = "net_spend";
    public static final String LAST_ORDER_DATE = "last_order_date";
    public static final String PAID_AMOUNT = "paid_amount";
    public static final String REFUND_AMOUNT = "refund_amount";
    public static final String TOTAL_TICKETS = "total_tickets";
    public static final String OPEN_TICKETS = "open_tickets";
    public static final String AVG_RATING = "avg_rating";
    public static final String LATE_DELIVERIES = "late_deliveries";
    public static final String CUSTOMER_SEGMENT = "customer_segment";
    public static final String SPEND_RANK = "spend_rank";
    public static final String LAST_REFRESHED_RUN = "last_refreshed_run";

    private Customer360Summary() {
    }
}
