package com.demo.etl.framework.quality;

import org.apache.spark.sql.Dataset;
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

import static org.apache.spark.sql.functions.col;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Plain unit test: WARNING rules (record stays valid, warning reported) and PII masking in messages.
 */
class DataQualityWarningAndPiiTest {

    private static SparkSession spark;

    @BeforeAll
    static void startSpark() {
        spark = SparkSession.builder()
                .master("local[1]")
                .appName("dq-warning-pii-test")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.ansi.enabled", "false")
                .config("spark.sql.shuffle.partitions", "1")
                .getOrCreate();
    }

    @AfterAll
    static void stopSpark() {
        spark.stop();
    }

    private Dataset<Row> rows() {
        StructType schema = new StructType()
                .add("_id", DataTypes.StringType)
                .add("_raw_record", DataTypes.StringType)
                .add("customerId", DataTypes.StringType)
                .add("email", DataTypes.StringType)
                .add("phone", DataTypes.StringType)
                .add("amount", DataTypes.StringType)
                .add("updatedAt", DataTypes.StringType);
        return spark.createDataFrame(List.of(
                RowFactory.create("1", "{}", "C1", "rahul.shah@gmail.com", "9876543210", "100", "2026-01-01T10:00:00"),  // clean
                RowFactory.create("2", "{}", "C2", "neha@yahoo.in", null, "250000", "2026-01-01T10:00:00"),           // 2 warnings
                RowFactory.create("3", "{}", "C3", "bad@@mail", "9876500000", "100", "2026-01-01T10:00:00")           // rejected
        ), schema);
    }

    private DataQualityRules rules() {
        return DataQualityRules.create()
                .required("customerId", "email")
                .mustBeValidEmail("email")
                .warnings(w -> w
                        .required("phone")
                        .mustNotBeMoreThan("amount", 100_000));
    }

    @Test
    void warningRulesKeepTheRecordValidAndReportEveryWarning() {
        QualityResult result = new DataQualityEngine().check(rows(), rules(), "customerId", "updatedAt");

        assertEquals(2, result.valid().count());                       // C1 and C2 are loaded
        assertFalse(List.of(result.valid().columns()).contains(QualityColumns.WARNINGS));

        List<Row> warnings = result.warnings().collectAsList();
        assertEquals(1, warnings.size());
        assertEquals("C2", warnings.get(0).getAs("business_key"));
        String message = warnings.get(0).getAs("warning_message");
        assertTrue(message.contains("[WARNING] phone is required"));
        assertTrue(message.contains("[WARNING] amount is more than 100000.0 (value: 250000)"));
    }

    @Test
    void piiValuesAreMaskedInErrorMessages() {
        QualityResult result = new DataQualityEngine().check(rows(), rules(), "customerId", "updatedAt",
                Map.of("email", PiiType.EMAIL, "phone", PiiType.PHONE));

        String reason = result.rejected().filter(col("business_key").equalTo("C3")).first().getAs("error_reason");
        assertTrue(reason.contains("(value: b****@@mail)"), reason);
        assertFalse(reason.contains("bad@@mail"));
    }

    @Test
    void maskFunctionsKeepOnlyWhatIsNeeded() {
        Row row = spark.createDataFrame(List.of(RowFactory.create("rahul.shah@gmail.com", "9876543210", "no-at-sign")),
                        new StructType().add("e", DataTypes.StringType).add("p", DataTypes.StringType).add("x", DataTypes.StringType))
                .select(PiiType.EMAIL.mask(col("e")), PiiType.PHONE.mask(col("p")), PiiType.EMAIL.mask(col("x")))
                .first();
        assertEquals("r****@gmail.com", row.getString(0));
        assertEquals("XXXXXX3210", row.getString(1));
        assertEquals("****", row.getString(2));
    }

    @Test
    void orphanChecksCannotBeWarnings() {
        Dataset<Row> lookup = rows().select("customerId");
        assertThrows(IllegalArgumentException.class, () -> DataQualityRules.create()
                .warnings(w -> w.mustExistIn("customerId", lookup, "customerId", "x")));
    }
}
