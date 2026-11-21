package com.shop.etl.dataset.silver;

/**
 * Silver (curated) tables na COMMON columns ane helper.
 *
 * Badhi silver tables ma aa 3 ETL columns che (03_postgres_create_tables.sql):
 *   etl_run_id      → kaya run e chhelli vaar row lakhi/badli
 *   etl_inserted_at → row pehli vaar kyare aavi
 *   etl_updated_at  → chhelli vaar kyare badlai (DB trigger pote set kare)
 *
 * Aa columns thi: audit ma inserted/updated count, ane Job-5 ma "kaya customer no data badlayo".
 */
public final class SilverTable {

    public static final String ETL_RUN_ID = "etl_run_id";
    public static final String ETL_INSERTED_AT = "etl_inserted_at";
    public static final String ETL_UPDATED_AT = "etl_updated_at";

    private SilverTable() {
    }

    /** "curated" + "fact_sales" → "curated.fact_sales". Schema naam yml mathi aave. */
    public static String qualified(String schema, String table) {
        return schema + "." + table;
    }
}
