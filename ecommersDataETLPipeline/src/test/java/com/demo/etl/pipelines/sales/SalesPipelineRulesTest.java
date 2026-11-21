package com.demo.etl.pipelines.sales;

import com.demo.etl.framework.quality.DataQualityEngine;
import com.demo.etl.framework.quality.QualityColumns;
import com.demo.etl.framework.quality.QualityResult;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.lit;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Plain unit test (no Spring, no databases) for Job-2 (sales-pipeline), using the examples of the
 * design document: sanitize rules J2-S01..S09, line level rules J2-V06 / J2-F02 and the SCD2
 * point-in-time customer lookup.
 */
class SalesPipelineRulesTest {

    private static SparkSession spark;
    private final DataQualityEngine engine = new DataQualityEngine();

    @BeforeAll
    static void startSpark() {
        spark = SparkSession.builder()
                .master("local[1]")
                .appName("sales-pipeline-rules-test")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.ansi.enabled", "false")
                .config("spark.sql.shuffle.partitions", "1")
                .config("spark.sql.session.timeZone", "Asia/Kolkata")
                .getOrCreate();
    }

    @AfterAll
    static void stopSpark() {
        spark.stop();
    }

    @Test
    void customersAreSanitizedDeduplicatedAndValidated() {
        Dataset<Row> raw = withRawRecord(spark.createDataFrame(List.of(
                customer("1", "C002", "Priya Shah", "  PRIYA.SHAH@MAIL.COM ", "9876500002", "Surat", "Gujarat", "Silver", "2026-10-01T04:30:00Z"),
                customer("2", "C004", "Neha Joshi", "neha.joshi@mail.com", "+91-98765 00004", "Pune", "Maharashtra", "Bronze", "2026-10-01T04:30:00Z"),
                customer("3", "C007", "Vikram Singh", "vikram.singh@mail.com", "9876500007", "Jaipur", "RJ", "Bronze", "2026-10-02T04:30:00Z"),
                customer("4", "C011", "Mehul Shah", "mehul.shah@mail.com", "9876500011", "Ahmedabad", "Gujarat", null, "2026-10-04T04:30:00Z"),
                customer("5", "C013", "Rohan Gupta", "rohan.gupta@mail.com", "9876500013", "lucknow ", "Uttar Pradesh", "Bronze", "2026-10-04T04:30:00Z"),
                customer("6", "C005", "Karan Desai", "karan.desai@mail.com", "9876500005", "Vadodara", "Gujarat", "Silver", "2026-10-02T04:30:00Z"),
                customer("7", "C005", "Karan Desai", "karan.desai@mail.com", "9876500005", "Vadodara", "Gujarat", "Bronze", "2026-09-01T04:30:00Z"),
                customer("8", "C021", "Test User", "test@@mail", "9876500021", "Surat", "Gujarat", "Bronze", "2026-10-08T04:30:00Z"),
                customer("9", "C022", null, "no.name@mail.com", "9876500022", "Pune", "Maharashtra", "Bronze", "2026-10-08T04:30:00Z")),
                SalesSources.CUSTOMERS.schema()));

        QualityResult result = engine.check(SalesSources.customerSanitizer().apply(raw), SalesSources.customerRules(),
                "customer_code", "updated_at");
        Map<String, Row> valid = byKey(result.valid(), "customer_code");

        assertEquals("priya.shah@mail.com", valid.get("C002").getAs("email"));     // J2-S01
        assertEquals("9876500004", valid.get("C004").getAs("phone"));              // J2-S02
        assertEquals("Lucknow", valid.get("C013").getAs("city"));                  // J2-S03
        assertEquals("Rajasthan", valid.get("C007").getAs("state"));               // J2-S04
        assertEquals("Bronze", valid.get("C011").getAs("tier"));                   // J2-S05
        assertEquals("Silver", valid.get("C005").getAs("tier"));                   // J2-D01 latest kept
        assertEquals(6, valid.size());

        Map<String, String> rejected = result.rejected().collectAsList().stream().collect(Collectors.toMap(
                r -> r.getAs(QualityColumns.REJECTED_SOURCE_ID), SalesPipelineRulesTest::mainErrorType));
        assertEquals(Map.of("7", "DUPLICATE", "8", "VALIDATION", "9", "VALIDATION"), rejected);
    }

    @Test
    void orderLinesAreExplodedAndABadLineIsRejectedAlone() {
        Dataset<Row> orders = withRawRecord(spark.createDataFrame(List.of(
                order("a1", "O1008", "RETURNED", "COD",
                        "[{\"product_code\": \"P017\", \"qty\": 2, \"unit_price\": 1800}, {\"product_code\": \"P018\", \"qty\": 0, \"unit_price\": 900}]"),
                order("a2", "O1012", "DELIVERED", "Net Banking",
                        "[{\"product_code\": \"P002\", \"qty\": 1, \"unit_price\": 12000}, {\"product_code\": \"P022\", \"qty\": 1, \"unit_price\": 999}]")),
                SalesSources.ORDERS.schema()));
        Dataset<Row> sanitized = SalesSources.orderSanitizer().apply(orders);
        assertEquals("NETBANKING", byKey(sanitized, "order_no").get("O1012").getAs("payment_method"));   // J2-S07

        Dataset<Row> knownProducts = spark.createDataFrame(List.of(
                RowFactory.create("P017"), RowFactory.create("P018"), RowFactory.create("P002")),
                new StructType().add("product_code", DataTypes.StringType));
        QualityResult lines = engine.check(SalesStarSchemaMapper.toOrderLines(sanitized),
                SalesSources.orderLineRules(knownProducts), "line_id", "updated_at");

        assertEquals(List.of("O1008/line-1", "O1012/line-1"), keys(lines.valid(), "line_id"));
        Map<String, String> rejected = lines.rejected().collectAsList().stream().collect(Collectors.toMap(
                r -> r.getAs(QualityColumns.REJECTED_BUSINESS_KEY), SalesPipelineRulesTest::mainErrorType));
        assertEquals(Map.of("O1008/line-2", "VALIDATION", "O1012/line-2", "ORPHAN"), rejected);   // J2-V06, J2-F02
    }

    @Test
    void paymentAmountIsCleanedAndCorruptAmountIsRejected() {
        Dataset<Row> payments = withRawRecord(spark.createDataFrame(List.of(
                RowFactory.create("p1", "PAY01", "O1001", "UPI", "₹29,000", "success", "2026-10-01T04:46:00Z", "2026-10-01T04:46:00Z"),
                RowFactory.create("p2", "PAY22", "O1020", "UPI", "abc", "FAILED", "2026-10-16T11:05:00Z", "2026-10-16T11:05:00Z")),
                SalesSources.PAYMENTS.schema()));
        Dataset<Row> loadedOrders = spark.createDataFrame(List.of(RowFactory.create("O1001"), RowFactory.create("O1020")),
                new StructType().add("order_no", DataTypes.StringType));

        QualityResult result = engine.check(SalesSources.paymentSanitizer().apply(payments),
                SalesSources.paymentRules(loadedOrders), "payment_id", "updated_at");

        Row pay01 = byKey(result.valid(), "payment_id").get("PAY01");
        assertEquals("29000", pay01.getAs("amount"));                              // J2-S08
        assertEquals("SUCCESS", pay01.getAs("status"));                            // J2-S09
        Row pay22 = result.rejected().first();
        assertEquals("PAY22", pay22.getAs(QualityColumns.REJECTED_BUSINESS_KEY));
        assertEquals("CORRUPT", mainErrorType(pay22)); // J2-C02
    }

    @Test
    void orderGetsTheCustomerVersionThatWasValidAtOrderTime() {
        StructType versionSchema = new StructType()
                .add("customer_key", DataTypes.LongType).add("customer_code", DataTypes.StringType)
                .add("valid_from", DataTypes.TimestampType).add("valid_to", DataTypes.TimestampType)
                .add("is_current", DataTypes.StringType);
        Dataset<Row> versions = spark.createDataFrame(List.of(
                RowFactory.create(3L, "C003", ts("2025-03-05 00:00:00"), ts("2026-10-21 01:20:00"), "N"),   // Mumbai
                RowFactory.create(21L, "C003", ts("2026-10-21 01:20:00"), ts("9999-12-31 00:00:00"), "Y")), // Pune
                versionSchema);
        Dataset<Row> orders = spark.createDataFrame(List.of(
                RowFactory.create("O1003", "C003", ts("2026-10-02 09:05:00")),
                RowFactory.create("O9999", "C003", ts("2026-10-22 10:00:00"))),
                new StructType().add("order_no", DataTypes.StringType).add("customer_code", DataTypes.StringType)
                        .add("order_ts", DataTypes.TimestampType));

        Map<String, Row> withKeys = byKey(SalesStarSchemaMapper.withCustomerKeyAt(orders, versions, "order_ts"), "order_no");
        assertEquals(3L, (long) withKeys.get("O1003").<Long>getAs("customer_key"));   // old order stays on old version
        assertEquals(21L, (long) withKeys.get("O9999").<Long>getAs("customer_key"));
    }

    // ------------------------------------------------------------------ helpers

    private static Row customer(String id, String code, String name, String email, String phone,
                                String city, String state, String tier, String updatedAt) {
        String address = "{\"city\": \"" + city + "\", \"state\": \"" + state + "\", \"pincode\": \"380001\"}";
        return RowFactory.create(id, code, name, email, phone, address, tier, "2025-01-10", updatedAt);
    }

    private static Row order(String id, String orderNo, String status, String method, String items) {
        return RowFactory.create(id, orderNo, "C007", "2026-10-05T11:15:00Z", status, method, items, "2026-10-09T16:30:00Z");
    }

    /** What SourceProcessor adds on extract: the raw document as JSON. */
    private static Dataset<Row> withRawRecord(Dataset<Row> rows) {
        return rows.withColumn(QualityColumns.RAW_RECORD, lit("{}"));
    }

    private static Timestamp ts(String value) {
        return Timestamp.valueOf(value);
    }

    private static Map<String, Row> byKey(Dataset<Row> rows, String key) {
        return rows.collectAsList().stream().collect(Collectors.toMap(r -> r.getAs(key), r -> r));
    }

    private static List<String> keys(Dataset<Row> rows, String key) {
        List<String> keys = rows.collectAsList().stream().map(r -> r.<String>getAs(key)).sorted().toList();
        assertTrue(!keys.isEmpty());
        return keys;
    }

    /** errors[0].error_type of a rejected row: the most serious problem of the record. */
    private static String mainErrorType(Row rejected) {
        List<Row> errors = rejected.getList(rejected.fieldIndex(QualityColumns.REJECTED_ERRORS));
        return errors.get(0).getAs(QualityColumns.ERROR_TYPE);
    }
}
