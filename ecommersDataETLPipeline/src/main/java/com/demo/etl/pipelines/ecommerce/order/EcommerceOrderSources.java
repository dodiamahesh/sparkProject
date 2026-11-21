package com.demo.etl.pipelines.ecommerce.order;

import com.demo.etl.framework.common.SparkTypes;
import com.demo.etl.framework.quality.DataQualityRules;
import com.demo.etl.framework.quality.DataSanitizer;
import com.demo.etl.framework.quality.ErrorType;
import com.demo.etl.framework.source.SourceDefinition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

/**
 * WHAT we read from MongoDB and HOW we clean + validate it, for the 5 collections of the order pipeline.
 */
final class EcommerceOrderSources {

    private EcommerceOrderSources() {
    }

    // =================================================================== ecom_categories
    static final SourceDefinition CATEGORIES = SourceDefinition.builder("ecom_categories")
            .fields("categoryId", "categoryName", "department", "updatedAt")
            .businessKey("categoryId")
            .latestRecordColumn("updatedAt")
            .excludeTestRecords()
            .maxRejectPercent(30)
            .expectedRows(0, 1_000)
            .description("Product categories of the web shop catalogue")
            .owner("catalog-team@demo.com")
            .sla("on change (a few times a month)")
            .build();

    static DataSanitizer categorySanitizer() {
        return DataSanitizer.create()
                .trimAllAndBlankToNull()
                .toUpperCase("categoryId", "department")
                .toProperCase("categoryName");
    }

    static DataQualityRules categoryRules() {
        return DataQualityRules.create()
                .required("categoryId", "categoryName", "department")
                .mustMatch("categoryId", "^CAT-[0-9]{2}$", "must look like CAT-01")
                .mustBeOneOf("department", "ELECTRONICS", "FASHION", "HOME", "BEAUTY", "GROCERY", "SPORTS", "BOOKS");
    }

    // =================================================================== ecom_products
    static final SourceDefinition PRODUCTS = SourceDefinition.builder("ecom_products")
            .fields("productId", "sku", "productName", "categoryId", "brand", "listPrice", "costPrice",
                    "status", "launchDate", "updatedAt")
            .businessKey("productId")
            .latestRecordColumn("updatedAt")
            .excludeTestRecords()
            .maxRejectPercent(30)
            .expectedRows(0, 100_000)
            .description("Products (SKU master data) of the web shop catalogue")
            .owner("catalog-team@demo.com")
            .sla("daily by 05:00")
            .build();

    static DataSanitizer productSanitizer() {
        return DataSanitizer.create()
                .trimAllAndBlankToNull()
                .toUpperCase("productId", "sku", "categoryId", "status")
                .removeThousandSeparators("listPrice", "costPrice");
    }

    /** @param categoryIds every category_id already in silver.dim_category (SNOWFLAKE parent) */
    static DataQualityRules productRules(Dataset<Row> categoryIds) {
        return DataQualityRules.create()
                .mustBeDecimal("listPrice")
                .mustBeDecimal("costPrice")
                .mustBeDate("launchDate")
                .required("productId", "productName", "categoryId", "listPrice", "status")
                .mustMatch("productId", "^PRD-[0-9]{4}$", "must look like PRD-0001")
                .mustBePositive("listPrice")
                .mustNotBeNegative("costPrice")
                .custom(ErrorType.VALIDATION, "costPrice cannot be higher than listPrice",
                        col("costPrice").cast(SparkTypes.MONEY).gt(col("listPrice").cast(SparkTypes.MONEY)))
                .mustBeOneOf("status", "ACTIVE", "OUT_OF_STOCK", "DISCONTINUED")
                .mustNotBeInFuture("launchDate")
                .mustExistIn("categoryId", categoryIds, "category_id", "dim_category")
                .warnings(w -> w
                        .required("brand", "sku"));
    }

    // =================================================================== ecom_customers
    static final SourceDefinition CUSTOMERS = SourceDefinition.builder("ecom_customers")
            .fields("customerId", "firstName", "lastName", "email", "phone", "city", "state", "pincode",
                    "segment", "signupDate", "updatedAt")
            .businessKey("customerId")
            .latestRecordColumn("updatedAt")
            .excludeTestRecords()
            .maxRejectPercent(30)
            .expectedRows(0, 1_000_000)
            .maskEmail("email")
            .maskPhone("phone")
            .description("Registered shop customers (sign-ups and profile changes)")
            .owner("customer-accounts-team@demo.com")
            .sla("every hour")
            .build();

    static DataSanitizer customerSanitizer() {
        return DataSanitizer.create()
                .trimAllAndBlankToNull()
                .toUpperCase("customerId", "segment")
                .toProperCase("firstName", "lastName", "city", "state")
                .toLowerCase("email")
                .normalizeIndianPhone("phone");
    }

    static DataQualityRules customerRules() {
        return DataQualityRules.create()
                .mustBeDate("signupDate")
                .required("customerId", "firstName", "lastName", "email", "segment")
                .mustMatch("customerId", "^ECU-[0-9]{4}$", "must look like ECU-0001")
                .mustBeValidEmail("email")
                .mustMatch("phone", "^[6-9][0-9]{9}$", "must be a 10 digit Indian mobile number")
                .mustMatch("pincode", "^[1-9][0-9]{5}$", "must be a 6 digit Indian pincode")
                .mustBeOneOf("segment", "REGULAR", "PRIME", "VIP")
                .mustNotBeInFuture("signupDate")
                .warnings(w -> w
                        .required("phone", "pincode"));
    }

    // =================================================================== ecom_orders (order HEADER)
    static final SourceDefinition ORDERS = SourceDefinition.builder("ecom_orders")
            .fields("orderId", "customerId", "orderDate", "orderStatus", "paymentMode", "channel",
                    "couponCode", "shippingFee", "shippingCity", "shippingState", "updatedAt")
            .businessKey("orderId")
            .latestRecordColumn("updatedAt")
            .excludeTestRecords()
            .maxRejectPercent(30)
            .expectedRows(1, 500_000)
            .description("Order headers from web, Android and iOS checkout")
            .owner("checkout-team@demo.com")
            .sla("every 15 minutes")
            .build();

    static DataSanitizer orderSanitizer() {
        return DataSanitizer.create()
                .trimAllAndBlankToNull()
                .toUpperCase("orderId", "customerId", "orderStatus", "paymentMode", "channel", "couponCode")
                .toProperCase("shippingCity", "shippingState")
                .removeThousandSeparators("shippingFee");
    }

    /** @param customerIds every customer_id already in silver.dim_ecom_customer */
    static DataQualityRules orderRules(Dataset<Row> customerIds) {
        return DataQualityRules.create()
                .mustBeTimestamp("orderDate")
                .mustBeDecimal("shippingFee")
                .required("orderId", "customerId", "orderDate", "orderStatus", "paymentMode", "channel", "shippingFee")
                .mustMatch("orderId", "^ORD-[0-9]{5}$", "must look like ORD-00001")
                .mustNotBeInFuture("orderDate")
                .mustNotBeNegative("shippingFee")
                .mustBeOneOf("orderStatus", "PLACED", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED")
                .mustBeOneOf("paymentMode", "UPI", "CARD", "COD", "NETBANKING", "WALLET")
                .mustBeOneOf("channel", "WEB", "ANDROID", "IOS")
                .mustExistIn("customerId", customerIds, "customer_id", "dim_ecom_customer")
                .warnings(w -> w
                        .mustBeOneOf("couponCode", "DIWALI10", "FIRST50", "FREESHIP"));
    }

    // =================================================================== ecom_order_items (order LINES)
    static final SourceDefinition ORDER_ITEMS = SourceDefinition.builder("ecom_order_items")
            .fields("orderItemId", "orderId", "productId", "quantity", "unitPrice", "discountAmount", "updatedAt")
            .businessKey("orderItemId")
            .latestRecordColumn("updatedAt")
            .excludeTestRecords()
            .maxRejectPercent(30)
            .expectedRows(1, 2_000_000)
            .description("Order lines: one product (with quantity, price, discount) inside an order")
            .owner("checkout-team@demo.com")
            .sla("every 15 minutes")
            .build();

    static DataSanitizer orderItemSanitizer() {
        return DataSanitizer.create()
                .trimAllAndBlankToNull()
                .toUpperCase("orderItemId", "orderId", "productId")
                .removeThousandSeparators("unitPrice", "discountAmount")
                // a missing discount means "no discount"
                .custom("discountAmount", discount -> coalesce(discount, lit("0")));
    }

    /**
     * @param orderIds   every order_id already in silver.fact_order (this run AND earlier runs)
     * @param productIds every product_id already in silver.dim_product
     */
    static DataQualityRules orderItemRules(Dataset<Row> orderIds, Dataset<Row> productIds) {
        return DataQualityRules.create()
                .mustBeInteger("quantity")
                .mustBeDecimal("unitPrice")
                .mustBeDecimal("discountAmount")
                .required("orderItemId", "orderId", "productId", "quantity", "unitPrice")
                .mustMatch("orderItemId", "^OIT-[0-9]{6}$", "must look like OIT-000001")
                .mustBePositive("quantity")
                .mustBePositive("unitPrice")
                .mustNotBeNegative("discountAmount")
                .custom(ErrorType.VALIDATION, "discountAmount cannot be higher than quantity * unitPrice",
                        col("discountAmount").cast(SparkTypes.MONEY)
                                .gt(col("quantity").cast(SparkTypes.MONEY).multiply(col("unitPrice").cast(SparkTypes.MONEY))))
                .mustExistIn("orderId", orderIds, "order_id", "fact_order")
                .mustExistIn("productId", productIds, "product_id", "dim_product")
                .warnings(w -> w
                        .custom(ErrorType.VALIDATION, "discountAmount is more than 30% of quantity * unitPrice, please review",
                                col("discountAmount").cast(SparkTypes.MONEY).gt(col("quantity").cast(SparkTypes.MONEY)
                                        .multiply(col("unitPrice").cast(SparkTypes.MONEY)).multiply(0.3))));
    }
}
