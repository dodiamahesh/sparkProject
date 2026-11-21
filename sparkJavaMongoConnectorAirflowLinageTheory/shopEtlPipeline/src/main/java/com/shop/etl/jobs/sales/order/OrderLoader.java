package com.shop.etl.jobs.sales.order;

import com.shop.etl.common.config.EtlProperties;
import com.shop.etl.common.io.JdbcBatchWriter;
import com.shop.etl.common.io.JdbcStep;
import com.shop.etl.common.io.RowBinder;
import com.shop.etl.common.job.JobContext;
import com.shop.etl.common.quality.Clean;
import com.shop.etl.dataset.silver.DimOrderStatus;
import com.shop.etl.dataset.silver.DimPaymentMethod;
import com.shop.etl.dataset.silver.DimProduct;
import com.shop.etl.dataset.silver.FactSales;
import com.shop.etl.dataset.silver.SilverTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

import java.util.List;

import static com.shop.etl.dataset.raw.RawOrder.ID;
import static com.shop.etl.dataset.raw.RawOrder.ITEMS;
import static com.shop.etl.dataset.raw.RawOrder.ITEM_PRODUCT_CODE;
import static com.shop.etl.dataset.raw.RawOrder.ITEM_QTY;
import static com.shop.etl.dataset.raw.RawOrder.ITEM_UNIT_PRICE;
import static com.shop.etl.dataset.raw.RawOrder.ORDER_NO;
import static com.shop.etl.dataset.raw.RawOrder.ORDER_TS;
import static com.shop.etl.dataset.raw.RawOrder.PAYMENT_METHOD;
import static com.shop.etl.dataset.raw.RawOrder.STATUS;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;
import static org.apache.spark.sql.functions.posexplode;

/**
 * ORDERS LOAD SERVICE (Job-2, Part B): order ne LINES ma todvu, ane fact_sales ma upsert.
 *
 * <pre>
 *   O1001 { items: [ {P001, 1, 25000}, {P003, 2, 2000} ] }      ← 1 document
 *                       │ posexplode (position + item)
 *                       ▼
 *   O1001 | line 1 | P001 | 1 | 25000                           ← 2 rows
 *   O1001 | line 2 | P003 | 2 |  2000
 * </pre>
 * Design doc Section 16.2. {@code posexplode} array ni POSITION pan aape, je line_no bane:
 * (order_no, line_no) e fact_sales ni unique upsert key.
 *
 * fact_sales GRAIN: 1 row = 1 order line. Upsert: {@code ON CONFLICT (order_no, line_no)}.
 * Status badlay (SHIPPED → DELIVERED) to e j rows UPDATE thay, navi nahi.
 */
@Component
public class OrderLoader {

    private final JdbcBatchWriter writer;
    private final String table;

    public OrderLoader(JdbcBatchWriter writer, EtlProperties properties) {
        this.writer = writer;
        this.table = SilverTable.qualified(properties.postgres().schema().silver(), FactSales.TABLE);
    }

    /**
     * Sara orders ne line rows ma explode kare che. Line na raw values (qty_raw, unit_price_raw)
     * pan rakhe che, jethi line reject thay to sachi kharab value dekhay.
     *
     * Output columns: _id, order_no, line_no, product_code, quantity, unit_price, qty_raw,
     * unit_price_raw, order_ts, status, payment_method, customer_key
     */
    public Dataset<Row> explodeLines(Dataset<Row> validOrders) {
        return validOrders
                .select(col(ID), col(ORDER_NO), col(ORDER_TS), col(STATUS), col(PAYMENT_METHOD),
                        col(FactSales.CUSTOMER_KEY),
                        posexplode(col(ITEMS)).as(new String[]{"_pos", "_item"}))
                .withColumn(FactSales.LINE_NO, col("_pos").plus(1))
                .withColumn(DimProduct.PRODUCT_CODE, Clean.upperTrim(col("_item." + ITEM_PRODUCT_CODE)))
                .withColumn(OrderValidator.QTY_RAW, col("_item." + ITEM_QTY))
                .withColumn(OrderValidator.PRICE_RAW, col("_item." + ITEM_UNIT_PRICE))
                .withColumn(FactSales.QUANTITY, Clean.trimToNull(col("_item." + ITEM_QTY)).cast("int"))
                .withColumn(FactSales.UNIT_PRICE, Clean.amount(col("_item." + ITEM_UNIT_PRICE)).cast("decimal(12,2)"))
                .drop("_pos", "_item");
    }

    /**
     * Sari lines ne fact_sales ma upsert kare che.
     *
     * Lookups (banne nani, broadcast): status_code → order_status_key, method_code → payment_method_key.
     * Date key: order_ts no IST divas (spark.sql.session.timeZone = Asia/Kolkata) → 20261001.
     *
     * @param validLines   line rules pass thayeli lines (product_key sathe)
     * @param statuses     SalesLookups.orderStatuses()
     * @param methods      SalesLookups.paymentMethods()
     */
    public void write(JobContext ctx, Dataset<Row> validLines, Dataset<Row> statuses, Dataset<Row> methods) {
        Dataset<Row> facts = validLines
                .join(statuses, validLines.col(STATUS).equalTo(statuses.col(DimOrderStatus.STATUS_CODE)), "left")
                .join(methods, validLines.col(PAYMENT_METHOD).equalTo(methods.col(DimPaymentMethod.METHOD_CODE)), "left")
                .select(
                        col(ORDER_NO),
                        col(FactSales.LINE_NO),
                        date_format(col(ORDER_TS), "yyyyMMdd").cast("int").as(FactSales.ORDER_DATE_KEY),
                        col(FactSales.CUSTOMER_KEY),
                        col(FactSales.PRODUCT_KEY),
                        col(DimPaymentMethod.PAYMENT_METHOD_KEY),
                        col(DimOrderStatus.ORDER_STATUS_KEY),
                        col(FactSales.QUANTITY),
                        col(FactSales.UNIT_PRICE),
                        col(FactSales.QUANTITY).multiply(col(FactSales.UNIT_PRICE)).cast("decimal(14,2)")
                                .as(FactSales.LINE_AMOUNT));

        writer.write(table, facts, List.of(new JdbcStep("fact-sales-upsert", upsertSql(), binder(ctx.runId()))));
    }

    /**
     * Upsert SQL. {@code WHERE ... IS DISTINCT FROM}: kai badlayu j na hoy to row ne adakvanu nahi
     * (re-run ma DB par lakhan nahi, ane audit ma "updated" sachu).
     */
    private String upsertSql() {
        return "INSERT INTO " + table + " AS t (order_no, line_no, order_date_key, customer_key, product_key, "
                + "payment_method_key, order_status_key, quantity, unit_price, line_amount, etl_run_id) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                + "ON CONFLICT (order_no, line_no) DO UPDATE SET "
                + "  order_date_key = EXCLUDED.order_date_key, customer_key = EXCLUDED.customer_key, "
                + "  product_key = EXCLUDED.product_key, payment_method_key = EXCLUDED.payment_method_key, "
                + "  order_status_key = EXCLUDED.order_status_key, quantity = EXCLUDED.quantity, "
                + "  unit_price = EXCLUDED.unit_price, line_amount = EXCLUDED.line_amount, "
                + "  etl_run_id = EXCLUDED.etl_run_id "
                + "WHERE (t.order_date_key, t.customer_key, t.product_key, t.payment_method_key, t.order_status_key, "
                + "       t.quantity, t.unit_price, t.line_amount) "
                + "  IS DISTINCT FROM (EXCLUDED.order_date_key, EXCLUDED.customer_key, EXCLUDED.product_key, "
                + "       EXCLUDED.payment_method_key, EXCLUDED.order_status_key, EXCLUDED.quantity, "
                + "       EXCLUDED.unit_price, EXCLUDED.line_amount)";
    }

    /** Static binder (fakt runId capture). */
    private static RowBinder binder(String runId) {
        return (ps, row, v) -> {
            v.string(ps, 1, row, ORDER_NO);
            v.integer(ps, 2, row, FactSales.LINE_NO);
            v.integer(ps, 3, row, FactSales.ORDER_DATE_KEY);
            v.integer(ps, 4, row, FactSales.CUSTOMER_KEY);
            v.integer(ps, 5, row, FactSales.PRODUCT_KEY);
            v.integer(ps, 6, row, DimPaymentMethod.PAYMENT_METHOD_KEY);
            v.integer(ps, 7, row, DimOrderStatus.ORDER_STATUS_KEY);
            v.integer(ps, 8, row, FactSales.QUANTITY);
            v.decimal(ps, 9, row, FactSales.UNIT_PRICE);
            v.decimal(ps, 10, row, FactSales.LINE_AMOUNT);
            v.constant(ps, 11, runId);
        };
    }
}
