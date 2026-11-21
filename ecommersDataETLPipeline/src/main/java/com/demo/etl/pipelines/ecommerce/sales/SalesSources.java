package com.demo.etl.pipelines.ecommerce.sales;

import com.demo.etl.framework.quality.DataQualityRules;
import com.demo.etl.framework.quality.DataSanitizer;
import com.demo.etl.framework.quality.ErrorType;
import com.demo.etl.framework.source.SourceDefinition;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.regexp_replace;
import static org.apache.spark.sql.functions.size;

/**
 * WHAT we read from MongoDB and HOW we clean + validate it, for Job-2 (J2_SALES).
 * Rule codes (J2-S01, J2-V02 ...) are the codes of the design document "Job-2: Multiple Collections".
 *
 * Every field is read from MongoDB as String. Nested documents (address, brand, category) arrive as
 * JSON text and are split into columns with {@code fromJson}; the items[] array is parsed later
 * (one row per line) by {@link SalesStarSchemaMapper#toOrderLines}.
 */
final class SalesSources {

    private SalesSources() {
    }

    /** Allowed values (also seeded in curated.dim_order_status / dim_payment_method by sample-data/sales-pipeline/03_postgres_tables.sql). */
    static final String[] ORDER_STATUSES = {"PLACED", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED"};
    static final String[] PAYMENT_METHODS = {"UPI", "CARD", "NETBANKING", "COD"};
    static final String[] PAYMENT_STATUSES = {"SUCCESS", "FAILED", "REFUNDED", "PENDING"};
    static final String[] TIERS = {"Bronze", "Silver", "Gold", "Platinum"};

    /** J2-S04: state code -> full name. */
    private static final Map<String, String> STATE_CODES = Map.ofEntries(
            Map.entry("GJ", "Gujarat"), Map.entry("MH", "Maharashtra"), Map.entry("RJ", "Rajasthan"),
            Map.entry("DL", "Delhi"), Map.entry("KA", "Karnataka"), Map.entry("TN", "Tamil Nadu"),
            Map.entry("TS", "Telangana"), Map.entry("UP", "Uttar Pradesh"), Map.entry("WB", "West Bengal"),
            Map.entry("MP", "Madhya Pradesh"), Map.entry("KL", "Kerala"));

    /** J2-S07 / J2-S09: payment method text (after trim + UPPER + single spaces) -> standard code. */
    private static final Map<String, String> PAYMENT_METHOD_ALIASES = Map.of(
            "NET BANKING", "NETBANKING",
            "INTERNET BANKING", "NETBANKING",
            "CASH ON DELIVERY", "COD",
            "CREDIT CARD", "CARD",
            "DEBIT CARD", "CARD");

    /** "₹29,000" -> "29000" (rupee sign, thousand separators and spaces removed). "abc" stays "abc". */
    private static Column amountText(Column value) {
        return regexp_replace(value, "[₹,\\s]", "");
    }

    // =================================================================== products  (-> dim_product)
    // In the design document dim_product came from Job-1. Here it is built by THIS job, so the
    // sales pipeline does not depend on any other job.
    static final SourceDefinition PRODUCTS = SourceDefinition.builder("products")
            .fields("product_code", "name", "brand", "category", "price", "is_active", "updated_at")
            .businessKey("product_code")
            .latestRecordColumn("updated_at")
            .ignoreFields("supplier", "stock")          // inventory data, not needed for sales
            .maxRejectPercent(30)
            .description("Product catalogue (product master) of the shop")
            .owner("catalog-team@demo.com")
            .sla("on change")
            .build();

    static DataSanitizer productSanitizer() {
        return DataSanitizer.create()
                .fromJson("brand_name", "brand", "$.name")
                .fromJson("category_name", "category", "$.name")
                .fromJson("parent_category_name", "category", "$.parent.name")
                .trimAllAndBlankToNull()
                .toUpperCase("product_code")
                .toLowerCase("is_active")
                .custom("price", SalesSources::amountText);                     // "₹1,200" -> "1200"
    }

    static DataQualityRules productRules() {
        return DataQualityRules.create()
                .mustBeDecimal("price")                                          // P022 "abc" -> CORRUPT
                .required("product_code", "name", "category_name", "price")      // P021 no category
                .mustMatch("product_code", "^P[0-9]{3}$", "must look like P001")
                .mustBePositive("price");                                        // P023 -150
        // duplicate product_code (P003 twice) -> latest updated_at kept automatically
    }

    // =================================================================== customers  (-> dim_customer SCD2)
    static final SourceDefinition CUSTOMERS = SourceDefinition.builder("customers")
            .fields("customer_code", "name", "email", "phone", "address", "tier", "signup_date", "updated_at")
            .businessKey("customer_code")
            .latestRecordColumn("updated_at")                                    // J2-D01 latest version kept
            .maxRejectPercent(30)
            .description("Registered customers (sign-ups and profile changes)")
            .owner("customer-accounts-team@demo.com")
            .sla("every hour")
            .build();

    static DataSanitizer customerSanitizer() {
        return DataSanitizer.create()
                .fromJson("city", "address", "$.city")
                .fromJson("state", "address", "$.state")
                .fromJson("pincode", "address", "$.pincode")
                .trimAllAndBlankToNull()
                .toUpperCase("customer_code", "state")
                .toLowerCase("email")                                            // J2-S01
                .normalizeIndianPhone("phone")                                   // J2-S02 "+91-98765 00004" -> 9876500004
                .toProperCase("city")                                            // J2-S03 "lucknow " -> Lucknow
                .mapValues("state", STATE_CODES)                                 // J2-S04 RJ -> Rajasthan
                .toProperCase("state", "tier")
                .defaultIfNull("tier", "Bronze");                                // J2-S05
    }

    static DataQualityRules customerRules() {
        return DataQualityRules.create()
                .mustBeDate("signup_date")
                .required("customer_code", "name", "email")                      // J2-V01 name (C022)
                .mustMatch("customer_code", "^C[0-9]{3}$", "must look like C001")
                .mustBeValidEmail("email")                                       // J2-V02 (C021 test@@mail)
                .mustMatch("phone", "^[0-9]{10}$", "must have 10 digits")        // J2-V03
                .mustBeOneOf("tier", TIERS)                                      // J2-V04
                .warnings(w -> w.required("phone", "city"));
    }

    // =================================================================== orders  (document level)
    static final SourceDefinition ORDERS = SourceDefinition.builder("orders")
            .fields("order_no", "customer_code", "order_ts", "status", "payment_method", "items", "updated_at")
            .businessKey("order_no")
            .latestRecordColumn("updated_at")                                    // J2-D02 latest version kept
            .ignoreFields("shipping", "order_total")                             // not part of the sales star
            .maxRejectPercent(30)
            .description("Orders with their product lines (items[] array)")
            .owner("checkout-team@demo.com")
            .sla("every 15 minutes")
            .build();

    static DataSanitizer orderSanitizer() {
        return DataSanitizer.create()
                .trimAllAndBlankToNull()
                .toUpperCase("order_no", "customer_code", "status", "payment_method")   // J2-S06 delivered -> DELIVERED
                .custom("payment_method", method -> regexp_replace(method, "\\s+", " "))
                .mapValues("payment_method", PAYMENT_METHOD_ALIASES);                   // J2-S07 Net Banking -> NETBANKING
    }

    /** @param customerCodes customer_code of every version in curated.dim_customer */
    static DataQualityRules orderRules(Dataset<Row> customerCodes) {
        return DataQualityRules.create()
                .mustBeTimestamp("order_ts")                                     // J2-C01 O1022 "2026-13-45"
                .required("order_no", "customer_code", "order_ts", "status", "payment_method")
                .custom(ErrorType.VALIDATION, "items must contain at least one line",  // J2-V05 O1023
                        size(SalesStarSchemaMapper.parseItems(col("items"))).leq(0))
                .mustBeOneOf("status", ORDER_STATUSES)                           // J2-V08
                .mustBeOneOf("payment_method", PAYMENT_METHODS)
                .mustExistIn("customer_code", customerCodes, "customer_code", "dim_customer");   // J2-F01 O1021 (C099)
    }

    // =================================================================== orders.items  (line level, derived)
    /**
     * One row per element of orders.items[]. Not a MongoDB collection: built by
     * {@link SalesStarSchemaMapper#toOrderLines} and validated with validateDerived(...).
     * business key "O1008/line-2" is what you see in the rejected table.
     */
    static final SourceDefinition ORDER_LINES = SourceDefinition.builder("orders.items")
            .fields("line_id", "order_no", "line_no", "product_code", "qty", "unit_price", "updated_at")
            .businessKey("line_id")
            .latestRecordColumn("updated_at")
            .description("Product lines of an order (items[] array, one row per line)")
            .owner("checkout-team@demo.com")
            .build();

    /** @param productCodes every product_code in curated.dim_product */
    static DataQualityRules orderLineRules(Dataset<Row> productCodes) {
        return DataQualityRules.create()
                .mustBeInteger("qty")
                .mustBeDecimal("unit_price")
                .required("product_code", "qty", "unit_price")
                .mustBePositive("qty")                                           // J2-V06 O1008 line-2 (qty 0)
                .mustBePositive("unit_price")                                    // J2-V07
                .mustExistIn("product_code", productCodes, "product_code", "dim_product");   // J2-F02 O1012 line-2 (P022)
    }

    // =================================================================== payments  (-> fact_payment)
    static final SourceDefinition PAYMENTS = SourceDefinition.builder("payments")
            .fields("payment_id", "order_no", "method", "amount", "status", "paid_at", "updated_at")
            .businessKey("payment_id")
            .latestRecordColumn("updated_at")                                    // J2-D03 PAY05 callback twice
            .maxRejectPercent(30)
            .description("Payments from the payment gateway (UPI, card, net banking, COD)")
            .owner("payments-team@demo.com")
            .sla("every 15 minutes")
            .build();

    static DataSanitizer paymentSanitizer() {
        return DataSanitizer.create()
                .trimAllAndBlankToNull()
                .toUpperCase("payment_id", "order_no", "status", "method")       // J2-S09 success -> SUCCESS
                .custom("method", method -> regexp_replace(method, "\\s+", " "))
                .mapValues("method", PAYMENT_METHOD_ALIASES)
                .custom("amount", SalesSources::amountText);                     // J2-S08 "₹29,000" -> 29000
    }

    /**
     * @param loadedOrders order_no of EVERY order in curated.fact_sales (this run and earlier runs):
     *                     an incremental payment can belong to an order loaded days ago.
     */
    static DataQualityRules paymentRules(Dataset<Row> loadedOrders) {
        return DataQualityRules.create()
                .mustBeDecimal("amount")                                         // J2-C02 PAY22 "abc"
                .mustBeTimestamp("paid_at")
                .required("payment_id", "order_no", "amount", "status", "method")
                .mustBePositive("amount")                                        // J2-V09
                .mustBeOneOf("status", PAYMENT_STATUSES)                         // J2-V10
                .mustBeOneOf("method", PAYMENT_METHODS)
                .mustExistIn("order_no", loadedOrders, "order_no", "fact_sales");   // J2-F03 PAY21 (O1021 was rejected)
    }
}
