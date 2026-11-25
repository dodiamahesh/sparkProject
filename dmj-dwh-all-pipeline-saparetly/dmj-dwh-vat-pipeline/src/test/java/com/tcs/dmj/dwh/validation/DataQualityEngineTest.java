package com.tcs.dmj.dwh.validation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Unit test without Spring or databases: sanitizer + rules + engine on an in-memory dataset. */
class DataQualityEngineTest {

    private static SparkSession spark;

    @BeforeAll
    static void startSpark() {
        spark = SparkSession.builder()
                .master("local[1]")
                .appName("dq-test")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.ansi.enabled", "false")
                .config("spark.sql.shuffle.partitions", "1")
                .getOrCreate();
    }

    @AfterAll
    static void stopSpark() {
        spark.stop();
    }

    @Test
    void splitsValidCorruptInvalidOrphanAndDuplicateRows() {
        StructType schema = new StructType()
                .add("_id", DataTypes.StringType)
                .add("_raw_record", DataTypes.StringType)
                .add("orderId", DataTypes.StringType)
                .add("customerId", DataTypes.StringType)
                .add("amount", DataTypes.StringType)
                .add("currency", DataTypes.StringType)
                .add("updatedAt", DataTypes.StringType);

        Dataset<Row> rows = spark.createDataFrame(List.of(
                RowFactory.create("1", "{}", "ORD-1", "C1", " 1,250.50 ", "inr", "2026-01-01T10:00:00"), // valid after cleaning
                RowFactory.create("2", "{}", "ORD-2", "C1", "12abc", "INR", "2026-01-01T10:00:00"),      // CORRUPT
                RowFactory.create("3", "{}", "ORD-3", "C1", "-5", "XYZ", "2026-01-01T10:00:00"),         // VALIDATION (2 errors)
                RowFactory.create("4", "{}", "ORD-4", "C9", "10", "INR", "2026-01-01T10:00:00"),         // ORPHAN
                RowFactory.create("5", "{}", "ORD-5", "C1", "10", "INR", "2026-01-01T10:00:00"),         // older copy -> DUPLICATE
                RowFactory.create("6", "{}", "ORD-5", "C1", "20", "INR", "2026-01-05T10:00:00")          // newer copy -> kept
        ), schema);

        Dataset<Row> customers = spark.createDataFrame(List.of(RowFactory.create("C1")),
                new StructType().add("customer_id", DataTypes.StringType));

        Dataset<Row> cleaned = DataSanitizer.create()
                .trimAllAndBlankToNull()
                .toUpperCase("currency")
                .removeThousandSeparators("amount")
                .apply(rows);

        DataQualityRules rules = DataQualityRules.create()
                .mustBeDecimal("amount")
                .required("orderId", "amount")
                .mustBePositive("amount")
                .mustBeOneOf("currency", "INR", "USD")
                .mustExistIn("customerId", customers, "customer_id", "customers");

        QualityResult result = new DataQualityEngine().check(cleaned, rules, "orderId", "updatedAt");

        List<String> validIds = result.valid().select("_id").as(Encoders.STRING()).collectAsList();
        assertEquals(List.of("1", "6"), validIds.stream().sorted().toList());
        assertEquals("1250.50", result.valid().filter(col("_id").equalTo("1")).first().getAs("amount"));

        Map<String, String> errorTypeById = result.rejected().withColumn("main_error", QualityColumns.mainErrorType())
                .collectAsList().stream()
                .collect(Collectors.toMap(row -> row.getAs("source_id"), row -> row.getAs("main_error")));
        assertEquals(Map.of("2", "CORRUPT", "3", "VALIDATION", "4", "ORPHAN", "5", "DUPLICATE"), errorTypeById);

        // row 3 broke TWO rules -> two errors, each with its own message (key of meta_error_master) and value
        List<String> errorsOfRow3 = QualityColumns.oneRowPerError(result.rejected().filter(col("source_id").equalTo("3")))
                .collectAsList().stream()
                .map(row -> row.getAs("error_message") + " = " + row.getAs("field_value"))
                .sorted().toList();
        assertEquals(List.of("amount must be greater than 0 = -5", "currency must be one of [INR, USD] = XYZ"), errorsOfRow3);

        Row duplicate = QualityColumns.oneRowPerError(result.rejected().filter(col("source_id").equalTo("5"))).first();
        assertEquals("another record with the same orderId was kept (latest updatedAt wins)", duplicate.getAs("error_message"));
        assertEquals("ORD-5", duplicate.getAs("field_value"));
    }
}
