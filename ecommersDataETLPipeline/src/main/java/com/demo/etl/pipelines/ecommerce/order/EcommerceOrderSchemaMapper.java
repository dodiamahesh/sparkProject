package com.demo.etl.pipelines.ecommerce.order;

import com.demo.etl.framework.common.SparkTypes;
import com.demo.etl.framework.target.DateDimension;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat_ws;

/**
 * TRANSFORM step: MongoDB documents (camelCase strings) -> snowflake-schema rows (snake_case, real types).
 */
final class EcommerceOrderSchemaMapper {

    private EcommerceOrderSchemaMapper() {
    }

    // ------------------------------------------------------------------ dimensions

    static Dataset<Row> toDimCategory(Dataset<Row> validCategories) {
        return validCategories.select(
                col("categoryId").as("category_id"),
                col("categoryName").as("category_name"),
                col("department"));
    }

    /** Typed products, still with category_id (needed to look up category_key). */
    static Dataset<Row> toTypedProducts(Dataset<Row> validProducts) {
        return validProducts.select(
                col("productId").as("product_id"),
                col("sku"),
                col("productName").as("product_name"),
                col("brand"),
                col("categoryId").as("category_id"),
                col("listPrice").cast(SparkTypes.MONEY).as("list_price"),
                col("costPrice").cast(SparkTypes.MONEY).as("cost_price"),
                col("status").as("product_status"),
                col("launchDate").cast(SparkTypes.DATE).as("launch_date"));
    }

    /**
     * SNOWFLAKE: dim_product stores only category_key (a link to dim_category),
     * not the category name / department. Expects category_key to be added already.
     */
    static Dataset<Row> toDimProduct(Dataset<Row> productsWithCategoryKey) {
        return productsWithCategoryKey.select(
                col("product_id"),
                col("sku"),
                col("product_name"),
                col("brand"),
                col("category_key"),
                col("list_price"),
                col("cost_price"),
                col("product_status"),
                col("launch_date"));
    }

    static Dataset<Row> toDimCustomer(Dataset<Row> validCustomers) {
        return validCustomers.select(
                col("customerId").as("customer_id"),
                col("firstName").as("first_name"),
                col("lastName").as("last_name"),
                concat_ws(" ", col("firstName"), col("lastName")).as("full_name"),
                col("email"),
                col("phone"),
                col("city"),
                col("state"),
                col("pincode"),
                col("segment").as("customer_segment"),
                col("signupDate").cast(SparkTypes.DATE).as("signup_date"));
    }

    // ------------------------------------------------------------------ fact 1: order header

    static Dataset<Row> toTypedOrders(Dataset<Row> validOrders) {
        return validOrders.select(
                col("orderId").as("order_id"),
                col("customerId").as("customer_id"),
                col("orderStatus").as("order_status"),
                col("paymentMode").as("payment_mode"),
                col("channel"),
                col("couponCode").as("coupon_code"),
                col("shippingFee").cast(SparkTypes.MONEY).as("shipping_fee"),
                col("shippingCity").as("shipping_city"),
                col("shippingState").as("shipping_state"),
                col("orderDate").cast(SparkTypes.TIMESTAMP).as("order_ts"));
    }

    /** Expects customer_key to be added already. */
    static Dataset<Row> toFactOrder(Dataset<Row> ordersWithKeys) {
        return ordersWithKeys.select(
                col("order_id"),
                col("customer_key"),
                DateDimension.dateKey(col("order_ts")).as("date_key"),
                col("order_status"),
                col("payment_mode"),
                col("channel"),
                col("coupon_code"),
                col("shipping_fee"),
                col("shipping_city"),
                col("shipping_state"),
                col("order_ts"));
    }

    // ------------------------------------------------------------------ fact 2: order lines

    static Dataset<Row> toTypedOrderItems(Dataset<Row> validItems) {
        return validItems.select(
                col("orderItemId").as("order_item_id"),
                col("orderId").as("order_id"),
                col("productId").as("product_id"),
                col("quantity").cast(SparkTypes.INT).as("quantity"),
                col("unitPrice").cast(SparkTypes.MONEY).as("unit_price"),
                col("discountAmount").cast(SparkTypes.MONEY).as("discount_amount"));
    }

    /**
     * An order line has no customer or date of its own: it inherits them from its order header.
     * {@code orderHeaders} = silver.fact_order (order_id, customer_key, date_key), which also contains
     * orders loaded in EARLIER runs, so a line that arrives later than its order still finds it.
     */
    static Dataset<Row> withOrderHeaderKeys(Dataset<Row> items, Dataset<Row> orderHeaders) {
        return items
                .join(orderHeaders, items.col("order_id").equalTo(orderHeaders.col("order_id")), "inner")
                .drop(orderHeaders.col("order_id"));
    }

    /** Expects product_key, customer_key and date_key to be added already. */
    static Dataset<Row> toFactOrderItem(Dataset<Row> itemsWithKeys) {
        return itemsWithKeys.select(
                col("order_item_id"),
                col("order_id"),
                col("product_key"),
                col("customer_key"),
                col("date_key"),
                col("quantity"),
                col("unit_price"),
                col("discount_amount"),
                col("quantity").multiply(col("unit_price")).cast(SparkTypes.MONEY).as("gross_amount"),
                col("quantity").multiply(col("unit_price")).minus(col("discount_amount")).cast(SparkTypes.MONEY).as("net_amount"));
    }
}
