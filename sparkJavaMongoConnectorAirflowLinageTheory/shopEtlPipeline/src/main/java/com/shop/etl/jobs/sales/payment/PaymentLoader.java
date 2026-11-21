package com.shop.etl.jobs.sales.payment;

import com.shop.etl.common.config.EtlProperties;
import com.shop.etl.common.io.JdbcBatchWriter;
import com.shop.etl.common.io.JdbcStep;
import com.shop.etl.common.io.RowBinder;
import com.shop.etl.common.job.JobContext;
import com.shop.etl.dataset.silver.DimPaymentMethod;
import com.shop.etl.dataset.silver.FactPayment;
import com.shop.etl.dataset.silver.SilverTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

import java.util.List;

import static com.shop.etl.dataset.raw.RawPayment.AMOUNT;
import static com.shop.etl.dataset.raw.RawPayment.METHOD;
import static com.shop.etl.dataset.raw.RawPayment.ORDER_NO;
import static com.shop.etl.dataset.raw.RawPayment.PAID_AT;
import static com.shop.etl.dataset.raw.RawPayment.PAYMENT_ID;
import static com.shop.etl.dataset.raw.RawPayment.STATUS;
import static com.shop.etl.dataset.raw.RawPayment.UPDATED_AT;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

/**
 * PAYMENTS LOAD SERVICE (Job-2, Part C): fact_payment ma upsert.
 *
 * GRAIN: 1 row = 1 payment. UPSERT KEY: payment_id.
 * payment_date_key = paid_at no IST divas (paid_at na hoy to updated_at).
 *
 * Example 04:00 run: PAY10 PENDING → SUCCESS, paid 10-21
 *   → e j row UPDATE: status SUCCESS, payment_date_key 20261007 → 20261021
 */
@Component
public class PaymentLoader {

    private final JdbcBatchWriter writer;
    private final String table;

    public PaymentLoader(JdbcBatchWriter writer, EtlProperties properties) {
        this.writer = writer;
        this.table = SilverTable.qualified(properties.postgres().schema().silver(), FactPayment.TABLE);
    }

    /**
     * @param valid   sari payments (customer_key validator ma aavi gai che)
     * @param methods SalesLookups.paymentMethods() (broadcast)
     */
    public void load(JobContext ctx, Dataset<Row> valid, Dataset<Row> methods) {
        Dataset<Row> facts = valid
                .join(methods, valid.col(METHOD).equalTo(methods.col(DimPaymentMethod.METHOD_CODE)), "left")
                .select(
                        col(PAYMENT_ID),
                        col(ORDER_NO),
                        date_format(coalesce(col(PAID_AT), col(UPDATED_AT)), "yyyyMMdd").cast("int")
                                .as(FactPayment.PAYMENT_DATE_KEY),
                        col(FactPayment.CUSTOMER_KEY),
                        col(DimPaymentMethod.PAYMENT_METHOD_KEY),
                        col(STATUS).as(FactPayment.PAYMENT_STATUS),
                        col(AMOUNT));

        writer.write(table, facts, List.of(new JdbcStep("fact-payment-upsert", upsertSql(), binder(ctx.runId()))));
    }

    /** Upsert: value badlay to j update (IS DISTINCT FROM). */
    private String upsertSql() {
        return "INSERT INTO " + table + " AS t (payment_id, order_no, payment_date_key, customer_key, "
                + "payment_method_key, payment_status, amount, etl_run_id) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
                + "ON CONFLICT (payment_id) DO UPDATE SET "
                + "  order_no = EXCLUDED.order_no, payment_date_key = EXCLUDED.payment_date_key, "
                + "  customer_key = EXCLUDED.customer_key, payment_method_key = EXCLUDED.payment_method_key, "
                + "  payment_status = EXCLUDED.payment_status, amount = EXCLUDED.amount, "
                + "  etl_run_id = EXCLUDED.etl_run_id "
                + "WHERE (t.order_no, t.payment_date_key, t.customer_key, t.payment_method_key, t.payment_status, t.amount) "
                + "  IS DISTINCT FROM (EXCLUDED.order_no, EXCLUDED.payment_date_key, EXCLUDED.customer_key, "
                + "       EXCLUDED.payment_method_key, EXCLUDED.payment_status, EXCLUDED.amount)";
    }

    /** Static binder (fakt runId capture). */
    private static RowBinder binder(String runId) {
        return (ps, row, v) -> {
            v.string(ps, 1, row, PAYMENT_ID);
            v.string(ps, 2, row, ORDER_NO);
            v.integer(ps, 3, row, FactPayment.PAYMENT_DATE_KEY);
            v.integer(ps, 4, row, FactPayment.CUSTOMER_KEY);
            v.integer(ps, 5, row, DimPaymentMethod.PAYMENT_METHOD_KEY);
            v.string(ps, 6, row, FactPayment.PAYMENT_STATUS);
            v.decimal(ps, 7, row, AMOUNT);
            v.constant(ps, 8, runId);
        };
    }
}
