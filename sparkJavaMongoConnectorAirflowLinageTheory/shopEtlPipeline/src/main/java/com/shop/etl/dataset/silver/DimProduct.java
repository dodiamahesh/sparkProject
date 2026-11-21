package com.shop.etl.dataset.silver;

/**
 * DATASET CLASS: {@code curated.dim_product} (SILVER). Job-1 banave che, Job-2 fakt LOOKUP kare.
 *
 * Job-2 ma upyog: order line na product_code mathi product_key. Product na male to line
 * FK_MISSING reject (J2-F02). Etle Job-1 Job-2 pehla chalvo joie (DAG dependency).
 */
public final class DimProduct {

    public static final String TABLE = "dim_product";

    public static final String PRODUCT_KEY = "product_key";
    public static final String PRODUCT_CODE = "product_code";

    private DimProduct() {
    }
}
