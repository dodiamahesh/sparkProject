package com.shop.etl.dataset.silver;

/**
 * DATASET CLASS: {@code curated.dim_customer} (SILVER). SCD Type 2. Design doc Section 6.5.
 *
 * Ek customer ni GHANI rows (versions) hoi shake. Fakt ek {@code is_current = 'Y'}.
 *   SCD2 columns (badlay to NAVI version): city, state, tier
 *   SCD1 columns (badlay to OVERWRITE):    customer_name, email, phone
 *
 * Fact tables (fact_sales, fact_payment) {@code customer_key} rakhe che, je order vakhte ni
 * version batave. Amit Mumbai thi Pune gayo to juna order Mumbai version (key 3) sathe j rahe.
 */
public final class DimCustomer {

    public static final String TABLE = "dim_customer";

    public static final String CUSTOMER_KEY = "customer_key";
    public static final String CUSTOMER_CODE = "customer_code";
    public static final String CUSTOMER_NAME = "customer_name";
    public static final String EMAIL = "email";
    public static final String PHONE = "phone";
    public static final String CITY = "city";
    public static final String STATE = "state";
    public static final String TIER = "tier";
    public static final String SIGNUP_DATE = "signup_date";
    public static final String VALID_FROM = "valid_from";
    public static final String VALID_TO = "valid_to";
    public static final String IS_CURRENT = "is_current";

    private DimCustomer() {
    }
}
