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

/**
 * Plain unit test: PII values are masked before they are stored with a rejected record.
 */
class DataQualityPiiTest {

    private static SparkSession spark;

    @BeforeAll
    static void startSpark() {
        spark = SparkSession.builder()
                .master("local[1]")
                .appName("dq-pii-test")
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
    void piiValuesAreMaskedInTheStoredErrorValue() {
        StructType schema = new StructType()
                .add("_id", DataTypes.StringType)
                .add("_raw_record", DataTypes.StringType)
                .add("customerId", DataTypes.StringType)
                .add("email", DataTypes.StringType)
                .add("updatedAt", DataTypes.StringType);
        Dataset<Row> rows = spark.createDataFrame(List.of(
                RowFactory.create("1", "{}", "C1", "rahul.shah@gmail.com", "2026-01-01T10:00:00"),   // valid
                RowFactory.create("3", "{}", "C3", "bad@@mail", "2026-01-01T10:00:00")              // rejected
        ), schema);
        DataQualityRules rules = DataQualityRules.create()
                .required("customerId", "email")
                .mustBeValidEmail("email");

        QualityResult result = new DataQualityEngine().check(rows, rules, "customerId", "updatedAt",
                Map.of("email", PiiType.EMAIL));

        Row error = QualityColumns.oneRowPerError(result.rejected()).filter(col("business_key").equalTo("C3")).first();
        assertEquals("email", error.getAs(QualityColumns.FIELD_NAME));
        assertEquals("email is not a valid email address", error.getAs(QualityColumns.ERROR_MESSAGE));
        assertEquals("b****@@mail", error.getAs(QualityColumns.FIELD_VALUE));
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
}
