package com.tcs.dmj.dwh.pipeline.vatreturn;

import com.tcs.dmj.dwh.validation.DataQualityEngine;
import com.tcs.dmj.dwh.validation.QualityColumns;
import com.tcs.dmj.dwh.validation.QualityResult;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.lit;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** VAT sanitizing, rules and mapping, without Spring or databases (documents like 01_mongo_full_load.js). */
class VatReturnPipelineRulesTest {

    private static SparkSession spark;
    private final DataQualityEngine engine = new DataQualityEngine();

    @BeforeAll
    static void startSpark() {
        spark = SparkSession.builder()
                .master("local[1]")
                .appName("vat-return-rules-test")
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
    void messyValuesAreSanitizedAndTheReturnIsValid() {
        QualityResult result = check(document("1", "120000000610", d -> {
            d.put("rtnType", "A");
            d.put("taxpayerCategory", " individual ");
            d.put("fileFrom", "mobl");
            d.put("taxableOutputDataDto", outputs("2,80,000", "56000"));
        }));

        Row valid = result.valid().first();
        assertEquals(0, result.rejected().count());
        assertEquals("Amended", valid.getAs("rtnType"));
        assertEquals("Individual", valid.getAs("taxpayerCategory"));
        assertEquals("MOBL", valid.getAs("fileFrom"));
        assertEquals("280000", valid.getAs("taxableOutputsValue"));
        assertEquals("APRV", valid.getAs("rtnStatus"));                 // flattened from rtnRegisterVatDTO
        assertEquals("2026-10-14T10:43:38Z", valid.getAs("filedAt"));   // nested crtDt
    }

    @Test
    void everyBadReturnIsRejectedWithItsOwnError() {
        QualityResult result = check(
                document("1", "120000000617", d -> d.put("tin", "X12345")),
                document("2", "120000000618", d -> {
                    d.put("isNilReturn", "true");
                    d.put("taxableOutputDataDto", outputs("50000", "0"));
                    d.put("vatComputationData", computation("0", "0", "0"));
                }),
                document("3", "120000000619", d -> d.put("vatComputationData", computation("40000", "60000.00", "99999.00"))),
                document("4", "120000000620", d -> d.put("formId", "99999")),
                document("5", "120000000615", d -> d.put("taxableOutputDataDto", outputs("20O000", "40000"))),
                document("6", "120000000623", d -> {
                    d.put("rtnFromDate", "2027-01-01");
                    d.put("rtnToDate", "2027-01-31");
                    d.put("month", "1");
                    d.put("year", "2027");
                }));

        assertEquals(0, result.valid().count());
        Map<String, String> errorByAck = QualityColumns.oneRowPerError(result.rejected()).collectAsList().stream()
                .collect(Collectors.toMap(r -> r.getAs("business_key"), r -> r.getAs("error_message")));
        assertEquals(Map.of(
                "120000000617", "tin must look like P0000000001",
                "120000000618", "a nil return cannot have taxable outputs",
                "120000000619", "netOutputTax must equal totalOutputTax - deductibleInputTax",
                "120000000620", "formId not found in dim_tax_form",
                "120000000615", "taxableOutputsValue is not a valid number",
                "120000000623", "rtnToDate cannot be in the future"), errorByAck);
    }

    @Test
    void validReturnIsMappedToTypedFactColumns() {
        Dataset<Row> typed = VatReturnSchemaMapper.toTyped(check(document("1", "120000000601", d -> { })).valid());
        Row fact = VatReturnSchemaMapper.toFactVatReturn(typed).first();

        assertEquals(202609, (int) fact.getAs("period_key"));
        assertEquals(20261014, (int) fact.getAs("filed_date_key"));
        assertEquals(20261101, (int) fact.getAs("due_date_key"));
        assertEquals(LocalDateTime.of(2026, 10, 14, 10, 43, 38), fact.getAs("filed_at"));   // source wall-clock time
        assertEquals(new BigDecimal("40000.00"), fact.getAs("vat_levies"));
        assertEquals(new BigDecimal("-20000.00"), fact.getAs("net_payable"));
        assertEquals(new BigDecimal("0.00"), fact.getAs("interest_amount"));
        assertEquals(-18, (int) fact.getAs("delay_days"));
        assertEquals(false, fact.getAs("is_late_return"));
    }

    // ------------------------------------------------------------------ helpers

    private QualityResult check(Row... documents) {
        Dataset<Row> raw = spark.createDataFrame(List.of(documents), VatReturnSources.VAT_RETURNS.schema())
                .withColumn(QualityColumns.RAW_RECORD, lit("{}"));
        Dataset<Row> formIds = spark.createDataFrame(List.of(RowFactory.create("12027")),
                new org.apache.spark.sql.types.StructType().add("form_id", "string"));
        return engine.check(VatReturnSources.sanitizer().apply(raw), VatReturnSources.rules(formIds),
                "ackNo", "updatedAt");
    }

    /** A good return (BNSJ, Sep 2026, like ackNo 120000000601); {@code change} makes it messy or bad. */
    private static Row document(String id, String ackNo, Consumer<Map<String, String>> change) {
        Map<String, String> d = new LinkedHashMap<>();
        d.put("tin", "P0000000620");
        d.put("taxpayerName", "BNSJ");
        d.put("taxpayerCategory", "Non-Individual");
        d.put("taxpayerJurisdiction", "Q");
        d.put("taxpayerId", "110330002299");
        d.put("rtnFromDate", "2026-09-01");
        d.put("rtnToDate", "2026-09-30");
        d.put("rtnDueDt", "2026-11-01");
        d.put("month", "9");
        d.put("year", "2026");
        d.put("rtnType", "Original");
        d.put("isNilReturn", "false");
        d.put("fileFrom", "INET");
        d.put("ackNo", ackNo);
        d.put("formId", "12027");
        d.put("taxableOutputDataDto", outputs("200000", "40000"));
        d.put("purchExpnsData", "{\"taxableValueOfLocalPurchases\": \"300000\", \"taxableValueOfImportedGoods\": \"0\", "
                + "\"taxableValueOfImportedServices\": \"0\", \"taxableValueOfTotalInput\": \"300000\", "
                + "\"taxableValueOfTaxOnPurchasesExp\": \"60000\"}");
        d.put("vatComputationData", computation("40000", "60000.00", "-20000.00"));
        d.put("rtnRegisterVatDTO", "{\"rtnStatus\": \"APRV\", \"rtnCrdtBroughtFrwd\": 0, \"rtnCrdtCarriedFrwd\": -20000, "
                + "\"crtDt\": \"2026-10-14T10:43:38Z\", \"isLateRtn\": \"N\", \"isActive\": \"Y\", "
                + "\"genAcknowledgementVatHeader\": {\"ackDate\": \"2026-10-14T00:00:00Z\"}}");
        d.put("additionalInputData", "{\"valueOfStockBoughtStandard\": \"0\", \"leviesOfStockBoughtStandard\": \"0\", "
                + "\"valueStockBoughtFlateRate\": \"0\", \"leviesStockBoughtFlateRate\": \"0\"}");
        d.put("penaltyAmount", "0.00");
        d.put("interest", "0.00");
        d.put("netTax", "-20000.00");
        d.put("delayDays", "-18");
        d.put("updatedAt", "2026-10-14T10:43:38Z");
        change.accept(d);

        String[] schemaFields = VatReturnSources.VAT_RETURNS.schema().fieldNames();   // _id + fields + isDeleted
        Object[] values = new Object[schemaFields.length];
        values[0] = id;
        for (int i = 1; i < schemaFields.length; i++) {
            values[i] = d.get(schemaFields[i]);                                       // isDeleted: not set = null
        }
        return RowFactory.create(values);
    }

    private static String outputs(String outputs, String levies) {
        return "{\"taxableOutputsValue\": \"" + outputs + "\", \"vatLeveies\": \"" + levies + "\", "
                + "\"zeroRatedTaxableValue\": \"0\", \"reliefTaxableValue\": \"0\", \"exemptSupTaxableValue\": \"0\"}";
    }

    private static String computation(String outputTax, String deductibleInputTax, String netOutputTax) {
        return "{\"totalOutputTaxValue\": \"" + outputTax + "\", \"totalInputTaxValue\": \"" + deductibleInputTax + "\", "
                + "\"dedInputTaxValue\": \"" + deductibleInputTax + "\", \"netOutputTaxValue\": \"" + netOutputTax + "\", "
                + "\"withHoldingVatCrValue\": \"0\", \"creditLastMonth\": \"0\", \"netPayableValue\": \"" + netOutputTax + "\"}";
    }
}
