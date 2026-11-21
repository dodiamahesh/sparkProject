package com.shop.etl.jobs.sales.customer;

import com.shop.etl.common.config.EtlProperties;
import com.shop.etl.common.io.JdbcBatchWriter;
import com.shop.etl.common.io.JdbcStep;
import com.shop.etl.common.io.RowBinder;
import com.shop.etl.common.job.JobContext;
import com.shop.etl.dataset.silver.DimCustomer;
import com.shop.etl.dataset.silver.SilverTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

import java.util.List;

import static com.shop.etl.dataset.raw.RawCustomer.CITY;
import static com.shop.etl.dataset.raw.RawCustomer.CUSTOMER_CODE;
import static com.shop.etl.dataset.raw.RawCustomer.EMAIL;
import static com.shop.etl.dataset.raw.RawCustomer.NAME;
import static com.shop.etl.dataset.raw.RawCustomer.PHONE;
import static com.shop.etl.dataset.raw.RawCustomer.SIGNUP_DATE;
import static com.shop.etl.dataset.raw.RawCustomer.STATE;
import static com.shop.etl.dataset.raw.RawCustomer.TIER;
import static com.shop.etl.dataset.raw.RawCustomer.UPDATED_AT;

/**
 * CUSTOMERS LOAD SERVICE: sari rows ne {@code dim_customer} ma SCD TYPE 2 thi lakhe che.
 * Staging table vagar, DIRECT. Design doc Section 6.5 ane 16.1.
 *
 * Darek customer mate 3 SQL steps (ek j transaction tukda ma, aa j order ma):
 * <pre>
 *   STEP 1  SCD1  : name / email / phone badlaya? → badhi versions ma OVERWRITE (navi row nahi)
 *   STEP 2  CLOSE : city / state / tier badlaya?  → current row bandh
 *                   (valid_to = updated_at, is_current = 'N')
 *   STEP 3  INSERT: current row nathi? (navo customer, ke step-2 e bandh kari)
 *                   → navi version. valid_from:
 *                       pehli vaar   = signup_date (juna orders ne pan sachi version male)
 *                       navi version = updated_at  (badlav no samay)
 * </pre>
 *
 * Example (04:00 run):
 *   C003 Mumbai/Gold → Pune/Platinum : step-2 key 3 bandh, step-3 key 21 navi
 *   C002 phone badlayo               : step-1 j (SCD1)
 *   C023 navo                        : step-3 j
 *   Same data farithi aave (re-run)  : 3 ma thi ek pan step kai nathi kartu (idempotent)
 *
 * Kem darek step ma "IS DISTINCT FROM"? (1) Value same hoy to UPDATE j na thay: DB par ochhu
 * lakhan (dead tuples nahi), ane audit ma "updated" count sachu. (2) NULL ne pan sachi rite
 * compare kare (NULL = NULL), {@code <>} nathi kartu.
 */
@Component
public class CustomerLoader {

    private final JdbcBatchWriter writer;
    private final String table;

    public CustomerLoader(JdbcBatchWriter writer, EtlProperties properties) {
        this.writer = writer;
        this.table = SilverTable.qualified(properties.postgres().schema().silver(), DimCustomer.TABLE);
    }

    /**
     * @param valid sanitized + validated customers (ek customer_code ek j vaar, dedupe pachhi)
     */
    public void load(JobContext ctx, Dataset<Row> valid) {
        String runId = ctx.runId();
        writer.write(table, valid, List.of(
                new JdbcStep("scd1-overwrite", scd1Sql(), scd1Binder(runId)),
                new JdbcStep("scd2-close", closeSql(), closeBinder(runId)),
                new JdbcStep("scd2-insert", insertSql(), insertBinder(runId))));
    }

    private String scd1Sql() {
        return "UPDATE " + table + " SET customer_name = ?, email = ?, phone = ?, etl_run_id = ? "
                + "WHERE customer_code = ? AND (customer_name, email, phone) IS DISTINCT FROM (?, ?, ?)";
    }

    private String closeSql() {
        return "UPDATE " + table + " SET valid_to = ?, is_current = 'N', etl_run_id = ? "
                + "WHERE customer_code = ? AND is_current = 'Y' AND valid_from < ? "
                + "AND (city, state, tier) IS DISTINCT FROM (?, ?, ?)";
    }

    private String insertSql() {
        return "INSERT INTO " + table + " (customer_code, customer_name, email, phone, city, state, tier, "
                + "signup_date, valid_from, etl_run_id) "
                + "SELECT ?, ?, ?, ?, ?, ?, ?, ?, "
                + "       CASE WHEN EXISTS (SELECT 1 FROM " + table + " d WHERE d.customer_code = ?) "
                + "            THEN ? ELSE COALESCE(?, ?) END, ? "
                + "WHERE NOT EXISTS (SELECT 1 FROM " + table + " d WHERE d.customer_code = ? AND d.is_current = 'Y')";
    }

    // Binders STATIC che: fakt runId (String) capture kare, bean nahi → Spark serialize kari shake.

    private static RowBinder scd1Binder(String runId) {
        return (ps, row, v) -> {
            v.string(ps, 1, row, NAME);
            v.string(ps, 2, row, EMAIL);
            v.string(ps, 3, row, PHONE);
            v.constant(ps, 4, runId);
            v.string(ps, 5, row, CUSTOMER_CODE);
            v.string(ps, 6, row, NAME);
            v.string(ps, 7, row, EMAIL);
            v.string(ps, 8, row, PHONE);
        };
    }

    private static RowBinder closeBinder(String runId) {
        return (ps, row, v) -> {
            v.timestamp(ps, 1, row, UPDATED_AT);
            v.constant(ps, 2, runId);
            v.string(ps, 3, row, CUSTOMER_CODE);
            v.timestamp(ps, 4, row, UPDATED_AT);
            v.string(ps, 5, row, CITY);
            v.string(ps, 6, row, STATE);
            v.string(ps, 7, row, TIER);
        };
    }

    private static RowBinder insertBinder(String runId) {
        return (ps, row, v) -> {
            v.string(ps, 1, row, CUSTOMER_CODE);
            v.string(ps, 2, row, NAME);
            v.string(ps, 3, row, EMAIL);
            v.string(ps, 4, row, PHONE);
            v.string(ps, 5, row, CITY);
            v.string(ps, 6, row, STATE);
            v.string(ps, 7, row, TIER);
            v.date(ps, 8, row, SIGNUP_DATE);
            v.string(ps, 9, row, CUSTOMER_CODE);
            v.timestamp(ps, 10, row, UPDATED_AT);       // navi version: badlav no samay
            v.dateAsTimestamp(ps, 11, row, SIGNUP_DATE); // pehli version: signup_date
            v.timestamp(ps, 12, row, UPDATED_AT);       // signup_date na hoy to updated_at
            v.constant(ps, 13, runId);
            v.string(ps, 14, row, CUSTOMER_CODE);
        };
    }
}
