package com.shop.etl.common.io;

import java.io.Serializable;

/**
 * Ek SQL statement + eno binder. JdbcBatchWriter darek row mate aava ek ke vadhare steps chalave che.
 *
 * Example:
 *   - fact_sales mate 1 step:  INSERT ... ON CONFLICT DO UPDATE
 *   - dim_customer (SCD2) mate 3 steps: SCD1 update → juni version close → navi version insert
 *
 * @param name   logs mate naam ("scd2-close")
 * @param sql    parameterized SQL ({@code ?} placeholders)
 * @param binder row na values {@code ?} ma set kare
 */
public record JdbcStep(String name, String sql, RowBinder binder) implements Serializable {
}
