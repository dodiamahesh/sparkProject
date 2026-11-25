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
 * WHAT is read from vat_return_coll, HOW it is cleaned and WHICH rules it must pass.
 * Nested objects arrive as JSON text and are flattened into columns with fromJson.
 */
final class VatReturnSources {

    private VatReturnSources() {
    }

    // allowed codes: the same values are seeded in the reference dimensions (03_postgres_tables.sql)
    static final String[] CATEGORIES = {"Individual", "Non-Individual"};
    static final String[] RETURN_TYPES = {"Original", "Amended"};
    static final String[] CHANNELS = {"INET", "OFFC", "MOBL"};
    static final String[] STATUSES = {"APRV", "PEND", "REJT"};

    /** Every amount: Spark column -> {nested object, JSON path}; null object = top-level field. */
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
        amount("penaltyAmount", null, null);
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

    static final SourceDefinition VAT_RETURNS = SourceDefinition.builder("vat_return_coll")
            .fields("tin", "taxpayerName", "taxpayerCategory", "taxpayerJurisdiction", "taxpayerId",
                    "rtnFromDate", "rtnToDate", "rtnDueDt", "month", "year", "rtnType", "isNilReturn", "fileFrom",
                    "ackNo", "formId", "taxableOutputDataDto", "purchExpnsData", "vatComputationData",
                    "rtnRegisterVatDTO", "additionalInputData", "penaltyAmount", "interest", "netTax", "delayDays",
                    "updatedAt")
            .businessKey("ackNo")
            .latestRecordColumn("updatedAt")
            .softDelete()
            .excludeTestRecords()
            .maxRejectPercent(30)
            .description("Monthly VAT returns filed by taxpayers (one document per filing)")
            .owner("vat-returns-team@demo.com")
            .sla("monthly, peak around the due date")
            .build();

    // ---------------------------------------------------------------- cleaning

    static DataSanitizer sanitizer() {
        DataSanitizer sanitizer = DataSanitizer.create();
        flattenNestedFields(sanitizer);   // before trimming, so the new columns are cleaned too
        return sanitizer
                .trimAllAndBlankToNull()
                .toUpperCase("tin", "fileFrom", "rtnType", "taxpayerCategory", "rtnStatus", "isLateRtn", "isActive")
                .toLowerCase("isNilReturn")
                .mapValues("rtnType", Map.of("O", "Original", "ORIGINAL", "Original", "A", "Amended", "AMENDED", "Amended"))
                .mapValues("taxpayerCategory", Map.of("INDIVIDUAL", "Individual",
                        "NON-INDIVIDUAL", "Non-Individual", "NON INDIVIDUAL", "Non-Individual"))
                .removeThousandSeparators(AMOUNTS.keySet().toArray(String[]::new));   // "2,80,000" -> "280000"
    }

    private static void flattenNestedFields(DataSanitizer sanitizer) {
        AMOUNTS.forEach((column, source) -> {
            if (source[0] != null) {
                sanitizer.fromJson(column, source[0], source[1]);
            }
        });
        sanitizer.fromJson("rtnStatus", "rtnRegisterVatDTO", "$.rtnStatus")
                .fromJson("filedAt", "rtnRegisterVatDTO", "$.crtDt")
                .fromJson("isLateRtn", "rtnRegisterVatDTO", "$.isLateRtn")
                .fromJson("isActive", "rtnRegisterVatDTO", "$.isActive")
                .fromJson("ackDate", "rtnRegisterVatDTO", "$.genAcknowledgementVatHeader.ackDate");
    }

    // ---------------------------------------------------------------- rules

    /** @param formIds every form_id of silver.dim_tax_form */
    static DataQualityRules rules(Dataset<Row> formIds) {
        DataQualityRules rules = DataQualityRules.create();
        addTypeRules(rules);
        addBusinessRules(rules);
        rules.mustExistIn("formId", formIds, "form_id", "dim_tax_form");
        return rules;
    }

    /** CORRUPT: wrong data type. */
    private static void addTypeRules(DataQualityRules rules) {
        AMOUNTS.keySet().forEach(rules::mustBeDecimal);
        rules.mustBeDate("rtnFromDate")
                .mustBeDate("rtnToDate")
                .mustBeDate("rtnDueDt")
                .mustBeInteger("month")
                .mustBeInteger("year")
                .mustBeInteger("delayDays")
                .mustBeTimestamp("filedAt")
                .mustBeTimestamp("ackDate");
    }

    /** VALIDATION: business rules. */
    private static void addBusinessRules(DataQualityRules rules) {
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
                .custom(ErrorType.VALIDATION, "month must be between 1 and 12", monthOutOfRange())
                .custom(ErrorType.VALIDATION, "rtnToDate cannot be before rtnFromDate", toBeforeFrom())
                .custom(ErrorType.VALIDATION, "month and year must match rtnFromDate", periodDoesNotMatchFromDate())
                .custom(ErrorType.VALIDATION, "a nil return cannot have taxable outputs", nilReturnWithOutputs())
                .custom(ErrorType.VALIDATION, "netOutputTax must equal totalOutputTax - deductibleInputTax",
                        netOutputTaxDoesNotAddUp());
    }

    private static Column monthOutOfRange() {
        Column month = col("month").cast(SparkTypes.INT);
        return month.lt(1).or(month.gt(12));
    }

    private static Column toBeforeFrom() {
        return col("rtnToDate").cast(SparkTypes.DATE).lt(col("rtnFromDate").cast(SparkTypes.DATE));
    }

    private static Column periodDoesNotMatchFromDate() {
        Column from = col("rtnFromDate").cast(SparkTypes.DATE);
        return month(from).notEqual(col("month").cast(SparkTypes.INT))
                .or(year(from).notEqual(col("year").cast(SparkTypes.INT)));
    }

    private static Column nilReturnWithOutputs() {
        return col("isNilReturn").equalTo("true").and(money("taxableOutputsValue").gt(0));
    }

    private static Column netOutputTaxDoesNotAddUp() {
        Column expected = money("totalOutputTax").minus(money("deductibleInputTax"));
        return abs(money("netOutputTax").minus(expected)).gt(0.01);
    }

    private static Column money(String column) {
        return col(column).cast(SparkTypes.MONEY);
    }
}
