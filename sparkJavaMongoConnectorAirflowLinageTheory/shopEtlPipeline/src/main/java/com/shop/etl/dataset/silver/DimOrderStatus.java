package com.shop.etl.dataset.silver;

/**
 * DATASET CLASS: {@code curated.dim_order_status} (SILVER). Nani reference dimension (5 rows).
 * Values SalesReferenceData mathi seed thay che (insert if not exists).
 */
public final class DimOrderStatus {

    public static final String TABLE = "dim_order_status";

    public static final String ORDER_STATUS_KEY = "order_status_key";
    public static final String STATUS_CODE = "status_code";
    public static final String DESCRIPTION = "description";
    public static final String IS_FINAL = "is_final";

    private DimOrderStatus() {
    }
}
