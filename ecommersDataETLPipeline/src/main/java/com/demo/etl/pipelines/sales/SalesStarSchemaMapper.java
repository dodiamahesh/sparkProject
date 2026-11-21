package com.demo.etl.pipelines.sales;

import com.demo.etl.framework.common.SparkTypes;
import com.demo.etl.framework.quality.QualityColumns;
import com.demo.etl.framework.target.DateDimension;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.nullif;
import static org.apache.spark.sql.functions.posexplode;
import static org.apache.spark.sql.functions.regexp_replace;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.to_json;
import static org.apache.spark.sql.functions.trim;
import static org.apache.spark.sql.functions.upper;

/**
 * TRANSFORM step of the sales pipeline: MongoDB documents (Strings) -> star-schema rows (real types).
 */
final class SalesStarSchemaMapper {

    /** One element of orders.items[]. Read as Strings, so "qty": 0 or "qty": "abc" can both be validated. */
    private static final ArrayType ITEMS_TYPE = DataTypes.createArrayType(new StructType()
            .add("product_code", DataTypes.StringType)
            .add("qty", DataTypes.StringType)
            .add("unit_price", DataTypes.StringType));

    private SalesStarSchemaMapper() {
    }

    /** items JSON text '[{"product_code": "P001", "qty": 1, ...}]' -> array of structs (NULL when not valid JSON). */
    static Column parseItems(Column itemsJson) {
        return from_json(itemsJson, ITEMS_TYPE);
    }

    // ------------------------------------------------------------------ dimensions

    static Dataset<Row> toDimProduct(Dataset<Row> validProducts) {
        return validProducts.select(
                col("product_code"),
                col("name").as("product_name"),
                col("brand_name"),
                col("category_name"),
                col("parent_category_name"),
                col("price").cast(SparkTypes.MONEY).as("current_price"),
                col("is_active").cast(DataTypes.BooleanType).as("is_active"));
    }

    /**
     * Rows for the SCD2 writer. changed_at = when this version became true (updated_at);
     * first_valid_from = signup date, so the FIRST version also covers orders placed before the first change.
     */
    static Dataset<Row> toDimCustomer(Dataset<Row> validCustomers) {
        return validCustomers.select(
                col("customer_code"),
                col("name").as("customer_name"),
                col("email"),
                col("phone"),
                col("city"),
                col("state"),
                col("pincode"),
                col("tier"),
                col("signup_date").cast(SparkTypes.DATE).as("signup_date"),
                col("updated_at").cast(SparkTypes.TIMESTAMP).as("changed_at"),
                col("signup_date").cast(SparkTypes.DATE).cast(SparkTypes.TIMESTAMP).as("first_valid_from"));
    }

    // ------------------------------------------------------------------ order lines (items[] explode)

    /**
     * One VALID order document -> one row per product line:
     * <pre>
     *   O1008 { items: [ {P017, 2, 1800}, {P018, 0, 900} ] }
     *     -> O1008 | line 1 | P017 | 2 | 1800
     *        O1008 | line 2 | P018 | 0 |  900     (rejected later by the line rules: qty 0)
     * </pre>
     * Adds the technical columns the quality engine needs: _id = "&lt;document _id&gt;#&lt;line_no&gt;" and
     * _raw_record = the line as JSON (shown in the rejected table).
     */
    static Dataset<Row> toOrderLines(Dataset<Row> validOrders) {
        return validOrders
                .select(col(QualityColumns.SOURCE_ID).as("_doc_id"), col("order_no"), col("customer_code"),
                        col("order_ts"), col("status"), col("payment_method"), col("updated_at"),
                        posexplode(parseItems(col("items"))).as(new String[]{"_pos", "_item"}))
                .withColumn("line_no", col("_pos").plus(1))
                .withColumn("product_code", upper(blankToNull(col("_item.product_code"))))
                .withColumn("qty", blankToNull(col("_item.qty")))
                .withColumn("unit_price", regexp_replace(blankToNull(col("_item.unit_price")), "[₹,\\s]", ""))
                .withColumn("line_id", concat(col("order_no"), lit("/line-"), col("line_no")))
                .withColumn(QualityColumns.SOURCE_ID, concat(col("_doc_id"), lit("#"), col("line_no")))
                .withColumn(QualityColumns.RAW_RECORD, to_json(struct(
                        col("order_no"), col("line_no"), col("_item.product_code").as("product_code"),
                        col("_item.qty").as("qty"), col("_item.unit_price").as("unit_price"))))
                .drop("_doc_id", "_pos", "_item");
    }

    /** Valid lines with real types; status / method renamed to the code columns of their dimensions. */
    static Dataset<Row> toTypedOrderLines(Dataset<Row> validLines) {
        return validLines.select(
                col("order_no"),
                col("line_no").cast(SparkTypes.INT).as("line_no"),
                col("customer_code"),
                col("product_code"),
                col("status").as("status_code"),
                col("payment_method").as("method_code"),
                col("order_ts").cast(SparkTypes.TIMESTAMP).as("order_ts"),
                col("qty").cast(SparkTypes.INT).as("quantity"),
                col("unit_price").cast(SparkTypes.MONEY).as("unit_price"));
    }

    /**
     * SCD2 POINT-IN-TIME lookup: the customer_key of the version that was valid when the event happened.
     * <pre>
     *   customer_code matches AND valid_from &lt;= event_ts &lt; valid_to
     * </pre>
     * Falls back to the CURRENT version (e.g. an order placed before the customer's signup date).
     *
     * @param customerVersions curated.dim_customer: customer_key, customer_code, valid_from, valid_to, is_current
     */
    static Dataset<Row> withCustomerKeyAt(Dataset<Row> rows, Dataset<Row> customerVersions, String eventTsColumn) {
        Dataset<Row> versions = customerVersions.select(
                col("customer_code").as("_v_code"), col("customer_key").as("_v_key"),
                col("valid_from").as("_v_from"), col("valid_to").as("_v_to"));
        Dataset<Row> current = customerVersions.filter(col("is_current").equalTo("Y"))
                .select(col("customer_code").as("_c_code"), col("customer_key").as("_c_key"));

        return rows
                .join(versions, col("customer_code").equalTo(col("_v_code"))
                        .and(col(eventTsColumn).geq(col("_v_from")))
                        .and(col(eventTsColumn).lt(col("_v_to"))), "left")
                .join(current, col("customer_code").equalTo(col("_c_code")), "left")
                .withColumn("customer_key", coalesce(col("_v_key"), col("_c_key")))
                .drop("_v_code", "_v_key", "_v_from", "_v_to", "_c_code", "_c_key");
    }

    /** Expects customer_key, product_key, order_status_key and payment_method_key to be added already. */
    static Dataset<Row> toFactSales(Dataset<Row> linesWithKeys) {
        return linesWithKeys.select(
                col("order_no"),
                col("line_no"),
                DateDimension.dateKey(col("order_ts")).as("order_date_key"),
                col("customer_key"),
                col("product_key"),
                col("payment_method_key"),
                col("order_status_key"),
                col("quantity"),
                col("unit_price"),
                col("quantity").multiply(col("unit_price")).cast(SparkTypes.MONEY).as("line_amount"));
    }

    // ------------------------------------------------------------------ payments

    /** paid_at is the business date of the payment; when it is missing, updated_at is used. */
    static Dataset<Row> toTypedPayments(Dataset<Row> validPayments) {
        return validPayments.select(
                col("payment_id"),
                col("order_no"),
                col("method").as("method_code"),
                col("status").as("payment_status"),
                col("amount").cast(SparkTypes.MONEY).as("amount"),
                coalesce(col("paid_at"), col("updated_at")).cast(SparkTypes.TIMESTAMP).as("paid_at"));
    }

    /**
     * A payment has no customer of its own: it takes the customer_key of its ORDER.
     * {@code orderCustomers} = curated.fact_sales (order_no, customer_key), so orders of earlier runs count too.
     */
    static Dataset<Row> withOrderCustomerKey(Dataset<Row> payments, Dataset<Row> orderCustomers) {
        Dataset<Row> orders = orderCustomers.dropDuplicates("order_no")
                .select(col("order_no").as("_o_no"), col("customer_key"));
        return payments.join(orders, col("order_no").equalTo(col("_o_no")), "inner").drop("_o_no");
    }

    /** Expects customer_key and payment_method_key to be added already. */
    static Dataset<Row> toFactPayment(Dataset<Row> paymentsWithKeys) {
        return paymentsWithKeys.select(
                col("payment_id"),
                col("order_no"),
                DateDimension.dateKey(col("paid_at")).as("payment_date_key"),
                col("customer_key"),
                col("payment_method_key"),
                col("payment_status"),
                col("amount"));
    }

    private static Column blankToNull(Column value) {
        return nullif(trim(value), lit(""));
    }
}
