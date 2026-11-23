package com.tcs.dmj.dwh.pipeline.vatreturn;

import com.tcs.dmj.dwh.connector.source.SourceDefinition;
import com.tcs.dmj.dwh.enums.ErrorType;
import com.tcs.dmj.dwh.service.constant.SparkTypes;
import com.tcs.dmj.dwh.validation.DataQualityRules;
import com.tcs.dmj.dwh.validation.DataSanitizer;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.abs;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.month;
import static org.apache.spark.sql.functions.year;

/**
 * WHAT we read from MongoDB vat_return_coll and HOW we clean + validate it.
 * Design, sample data and expected results: sample-data/vat-return-pipeline/vat-return-pipeline.md.
 *
 * Every field is read as String. The nested objects (taxableOutputDataDto, purchExpnsData, vatComputationData,
 * rtnRegisterVatDTO, additionalInputData) arrive as JSON text and are flattened into columns with fromJson.
 */
final class VatReturnSources {

    private VatReturnSources() {
    }

    /** Allowed values: the same codes are seeded in the reference dimensions (03_postgres_tables.sql). */
    static final String[] CATEGORIES = {"Individual", "Non-Individual"};
    static final String[] RETURN_TYPES = {"Original", "Amended"};
    static final String[] CHANNELS = {"INET", "OFFC", "MOBL"};
    static final String[] STATUSES = {"APRV", "PEND", "REJT"};

    /**
     * Every amount: column name in Spark -> {nested object, JSON path in the source}.
     * The column names are also the field names in the error messages (meta_error_master).
     */
    static final Map<String, String[]> AMOUNTS = new LinkedHashMap<>();

    static {
        amount("taxableOutputsValue", "taxableOutputDataDto", "$.taxableOutputsValue");
        amount("vatLevies", "taxableOutputDataDto", "$.vatLeveies");                 // source spelling "Leveies"
        amount("zeroRatedValue", "taxableOutputDataDto", "$.zeroRatedTaxableValue");
        amount("reliefValue", "taxableOutputDataDto", "$.reliefTaxableValue");
        amount("exemptSuppliesValue", "taxableOutputDataDto", "$.exemptSupTaxableValue");
        amount("localPurchasesValue", "purchExpnsData", "$.taxableValueOfLocalPurchases");
        amount("importedGoodsValue", "purchExpnsData", "$.taxableValueOfImportedGoods");
        amount("importedServicesValue", "purchExpnsData", "$.taxableValueOfImportedServices");
        amount("totalInputValue", "purchExpnsData", "$.taxableValueOfTotalInput");
        amount("inputTaxOnPurchases", "purchExpnsData", "$.taxableValueOfTaxOnPurchasesExp");
        amount("totalOutputTax", "vatComputationData", "$.totalOutputTaxValue");
        amount("totalInputTax", "vatComputationData", "$.totalInputTaxValue");
        amount("deductibleInputTax", "vatComputationData", "$.dedInputTaxValue");
        amount("netOutputTax", "vatComputationData", "$.netOutputTaxValue");
        amount("withholdingVatCredit", "vatComputationData", "$.withHoldingVatCrValue");
        amount("creditLastMonth", "vatComputationData", "$.creditLastMonth");
        amount("netPayable", "vatComputationData", "$.netPayableValue");
        amount("creditBroughtForward", "rtnRegisterVatDTO", "$.rtnCrdtBroughtFrwd");
        amount("creditCarriedForward", "rtnRegisterVatDTO", "$.rtnCrdtCarriedFrwd");
        amount("penaltyAmount", null, null);                                          // top level
        amount("interest", null, null);
        amount("netTax", null, null);
        amount("stockStandardValue", "additionalInputData", "$.valueOfStockBoughtStandard");
        amount("stockStandardLevies", "additionalInputData", "$.leviesOfStockBoughtStandard");
        amount("stockFlatRateValue", "additionalInputData", "$.valueStockBoughtFlateRate");
        amount("stockFlatRateLevies", "additionalInputData", "$.leviesStockBoughtFlateRate");
    }

    private static void amount(String column, String jsonColumn, String jsonPath) {
        AMOUNTS.put(column, new String[]{jsonColumn, jsonPath});
    }

    // =================================================================== vat_return_coll
    static final SourceDefinition VAT_RETURNS = SourceDefinition.builder("vat_return_coll")
            .fields("tin", "taxpayerName", "taxpayerCategory", "taxpayerJurisdiction", "taxpayerId",
                    "rtnFromDate", "rtnToDate", "rtnDueDt", "month", "year", "rtnType", "isNilReturn", "fileFrom",
                    "ackNo", "formId", "taxableOutputDataDto", "purchExpnsData", "vatComputationData",
                    "rtnRegisterVatDTO", "additionalInputData", "penaltyAmount", "interest", "netTax", "delayDays",
                    "updatedAt")
            .businessKey("ackNo")                 // one filing = one acknowledgement number
            .latestRecordColumn("updatedAt")      // watermark: must be set by the source on every insert / update
            .softDelete()                         // isDeleted = true at the source -> soft delete in silver
            .excludeTestRecords()
            .maxRejectPercent(30)
            .description("Monthly VAT returns filed by taxpayers (one document per filing)")
            .owner("vat-returns-team@demo.com")
            .sla("monthly, peak around the due date")
            .build();

    static DataSanitizer sanitizer() {
        DataSanitizer sanitizer = DataSanitizer.create();
        // 1. flatten the nested objects (BEFORE trimAllAndBlankToNull, so the new columns are cleaned too)
        AMOUNTS.forEach((column, source) -> {
            if (source[0] != null) {
                sanitizer.fromJson(column, source[0], source[1]);
            }
        });
        return sanitizer
                .fromJson("rtnStatus", "rtnRegisterVatDTO", "$.rtnStatus")
                .fromJson("filedAt", "rtnRegisterVatDTO", "$.crtDt")
                .fromJson("isLateRtn", "rtnRegisterVatDTO", "$.isLateRtn")
                .fromJson("isActive", "rtnRegisterVatDTO", "$.isActive")
                .fromJson("ackDate", "rtnRegisterVatDTO", "$.genAcknowledgementVatHeader.ackDate")
                // 2. standard cleaning
                .trimAllAndBlankToNull()
                .toUpperCase("tin", "fileFrom", "rtnType", "taxpayerCategory", "rtnStatus", "isLateRtn", "isActive")
                .toLowerCase("isNilReturn")
                .mapValues("rtnType", Map.of("O", "Original", "ORIGINAL", "Original", "A", "Amended", "AMENDED", "Amended"))
                .mapValues("taxpayerCategory", Map.of("INDIVIDUAL", "Individual",
                        "NON-INDIVIDUAL", "Non-Individual", "NON INDIVIDUAL", "Non-Individual"))
                .removeThousandSeparators(AMOUNTS.keySet().toArray(String[]::new));   // "2,80,000" -> "280000"
    }

    /** @param formIds every form_id in silver.dim_tax_form (seeded reference data) */
    static DataQualityRules rules(Dataset<Row> formIds) {
        DataQualityRules rules = DataQualityRules.create();
        // CORRUPT: wrong data type
        AMOUNTS.keySet().forEach(rules::mustBeDecimal);
        rules.mustBeDate("rtnFromDate")
                .mustBeDate("rtnToDate")
                .mustBeDate("rtnDueDt")
                .mustBeInteger("month")
                .mustBeInteger("year")
                .mustBeInteger("delayDays")
                .mustBeTimestamp("filedAt")
                .mustBeTimestamp("ackDate");

        // VALIDATION: business rules
        Column from = col("rtnFromDate").cast(SparkTypes.DATE);
        Column to = col("rtnToDate").cast(SparkTypes.DATE);
        rules.required("tin", "taxpayerName", "taxpayerCategory", "ackNo", "rtnFromDate", "rtnToDate", "month", "year",
                        "rtnType", "formId", "fileFrom", "rtnStatus", "filedAt", "netTax")
                .mustMatch("tin", "^P[0-9]{10}$", "must look like P0000000001")
                .mustMatch("ackNo", "^[0-9]{12}$", "must be a 12 digit acknowledgement number")
                .mustBeOneOf("taxpayerCategory", CATEGORIES)
                .mustBeOneOf("rtnType", RETURN_TYPES)
                .mustBeOneOf("fileFrom", CHANNELS)
                .mustBeOneOf("rtnStatus", STATUSES)
                .mustBeOneOf("isNilReturn", "true", "false")
                .mustNotBeNegative("taxableOutputsValue")
                .mustNotBeNegative("vatLevies")
                .mustNotBeNegative("totalInputValue")
                .mustNotBeNegative("inputTaxOnPurchases")
                .mustNotBeNegative("penaltyAmount")
                .mustNotBeNegative("interest")
                .mustNotBeInFuture("rtnToDate")      // a month can only be filed after it ended
                .custom(ErrorType.VALIDATION, "month must be between 1 and 12",
                        col("month").cast(SparkTypes.INT).lt(1).or(col("month").cast(SparkTypes.INT).gt(12)))
                .custom(ErrorType.VALIDATION, "rtnToDate cannot be before rtnFromDate", to.lt(from))
                .custom(ErrorType.VALIDATION, "month and year must match rtnFromDate",
                        month(from).notEqual(col("month").cast(SparkTypes.INT))
                                .or(year(from).notEqual(col("year").cast(SparkTypes.INT))))
                .custom(ErrorType.VALIDATION, "a nil return cannot have taxable outputs",
                        col("isNilReturn").equalTo("true").and(col("taxableOutputsValue").cast(SparkTypes.MONEY).gt(0)))
                .custom(ErrorType.VALIDATION, "netOutputTax must equal totalOutputTax - deductibleInputTax",
                        abs(col("netOutputTax").cast(SparkTypes.MONEY)
                                .minus(col("totalOutputTax").cast(SparkTypes.MONEY)
                                        .minus(col("deductibleInputTax").cast(SparkTypes.MONEY)))).gt(0.01))
                // ORPHAN: the form must exist in the reference dimension
                .mustExistIn("formId", formIds, "form_id", "dim_tax_form");
        return rules;
    }
}
