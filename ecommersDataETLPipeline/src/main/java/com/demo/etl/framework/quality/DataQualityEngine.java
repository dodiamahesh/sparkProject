package com.demo.etl.framework.quality;

import com.demo.etl.framework.common.SparkTypes;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.demo.etl.framework.quality.QualityColumns.ERRORS;
import static com.demo.etl.framework.quality.QualityColumns.ERROR_TYPE;
import static com.demo.etl.framework.quality.QualityColumns.RAW_RECORD;
import static com.demo.etl.framework.quality.QualityColumns.REFERENCE_PREFIX;
import static com.demo.etl.framework.quality.QualityColumns.REJECTED_BUSINESS_KEY;
import static com.demo.etl.framework.quality.QualityColumns.REJECTED_ERROR_REASON;
import static com.demo.etl.framework.quality.QualityColumns.REJECTED_ERROR_TYPE;
import static com.demo.etl.framework.quality.QualityColumns.REJECTED_RAW_RECORD;
import static com.demo.etl.framework.quality.QualityColumns.REJECTED_SOURCE_ID;
import static com.demo.etl.framework.quality.QualityColumns.ROW_NUMBER;
import static com.demo.etl.framework.quality.QualityColumns.SOURCE_ID;
import static com.demo.etl.framework.quality.QualityColumns.WARNINGS;
import static com.demo.etl.framework.quality.QualityColumns.WARNING_MESSAGE;
import static org.apache.spark.sql.functions.broadcast;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.when;

/**
 * Applies {@link DataQualityRules} to a dataset and splits it into VALID, REJECTED and WARNING rows.
 *
 * Order of checks:
 *   1. ORPHAN lookups are joined in (left join with the lookup keys)
 *   2. every error rule is evaluated -> list of error messages per row
 *   3. rows with errors = rejected (CORRUPT / VALIDATION / ORPHAN)
 *   4. rows without errors are de-duplicated on the business key -> extra copies = rejected (DUPLICATE)
 *   5. warning rules are evaluated on the valid rows -> they stay valid, the warnings are reported
 */
@Component
public class DataQualityEngine {

    private static final String SEPARATOR = " | ";
    private static final String WARNING_LABEL = "WARNING";

    /** Same as {@link #check(Dataset, DataQualityRules, String, String, Map)} without PII fields. */
    public QualityResult check(Dataset<Row> rows, DataQualityRules qualityRules,
                               String businessKey, String latestRecordColumn) {
        return check(rows, qualityRules, businessKey, latestRecordColumn, Map.of());
    }

    /**
     * @param rows               cleaned rows (must contain _id and _raw_record)
     * @param businessKey        column that identifies a record, e.g. "paymentId"
     * @param latestRecordColumn when duplicates exist, the row with the highest value here wins (e.g. "updatedAt")
     * @param piiFields          fields whose value is MASKED inside error / warning messages
     */
    public QualityResult check(Dataset<Row> rows, DataQualityRules qualityRules,
                               String businessKey, String latestRecordColumn, Map<String, PiiType> piiFields) {

        Function<String, Column> valueOf = column -> piiFields.containsKey(column)
                ? piiFields.get(column).mask(col(column))
                : col(column);

        List<ValidationRule> rules = new ArrayList<>(qualityRules.rules());
        Dataset<Row> checked = rows;

        // 1. ORPHAN checks: join every lookup and add a rule "value present but not found"
        int index = 0;
        for (ReferenceRule reference : qualityRules.referenceRules()) {
            String foundColumn = REFERENCE_PREFIX + index++;
            Dataset<Row> lookupKeys = reference.lookup()
                    .select(col(reference.lookupColumn()).as(foundColumn))
                    .distinct();
            checked = checked.join(broadcast(lookupKeys),
                    checked.col(reference.column()).equalTo(lookupKeys.col(foundColumn)), "left");

            Column failWhen = col(reference.column()).isNotNull().and(col(foundColumn).isNull());
            rules.add(new ValidationRule(ErrorType.ORPHAN, failWhen,
                    reference.column() + " not found in " + reference.lookupName(), reference.column()));
        }

        // 2. Evaluate all error rules. concat_ws skips NULLs, so a row that passes everything gets "".
        checked = checked
                .withColumn(ERRORS, messages(rules, null, valueOf))
                .withColumn(ERROR_TYPE, mainErrorType(rules));

        for (int i = 0; i < index; i++) {
            checked = checked.drop(REFERENCE_PREFIX + i);
        }

        // 3. Rows that broke at least one rule
        Dataset<Row> failedRows = checked.filter(col(ERRORS).notEqual(""));

        // 4. De-duplicate the remaining rows: keep the latest version of each business key
        Dataset<Row> passedRows = checked.filter(col(ERRORS).equalTo(""))
                .withColumn(ROW_NUMBER, row_number().over(Window
                        .partitionBy(col(businessKey))
                        .orderBy(col(latestRecordColumn).cast(SparkTypes.TIMESTAMP).desc_nulls_last(), col(SOURCE_ID).asc())));

        Dataset<Row> duplicateRows = passedRows.filter(col(ROW_NUMBER).gt(1))
                .withColumn(ERROR_TYPE, lit(ErrorType.DUPLICATE.name()))
                .withColumn(ERRORS, concat(
                        lit("[DUPLICATE] another record with " + businessKey + " = "),
                        col(businessKey),
                        lit(" was kept (latest " + latestRecordColumn + " wins)")));

        // 5. Warnings: only on the rows that will be loaded
        Dataset<Row> validWithWarnings = passedRows.filter(col(ROW_NUMBER).equalTo(1))
                .drop(ROW_NUMBER, ERRORS, ERROR_TYPE)
                .withColumn(WARNINGS, messages(qualityRules.warningRules(), WARNING_LABEL, valueOf));

        Dataset<Row> validRows = validWithWarnings.drop(WARNINGS);
        Dataset<Row> warningRows = validWithWarnings
                .filter(col(WARNINGS).notEqual(""))
                .select(
                        col(SOURCE_ID).as(REJECTED_SOURCE_ID),
                        col(businessKey).cast(DataTypes.StringType).as(REJECTED_BUSINESS_KEY),
                        col(WARNINGS).as(WARNING_MESSAGE));

        Dataset<Row> rejectedRows = toRejectedFormat(failedRows, businessKey)
                .unionByName(toRejectedFormat(duplicateRows, businessKey));

        return new QualityResult(validRows, rejectedRows, warningRows);
    }

    /**
     * All failed rules of a row in one text, "" when none failed.
     *
     * @param label null = use each rule's error type ("[CORRUPT] ..."), otherwise this label ("[WARNING] ...")
     */
    private Column messages(List<ValidationRule> rules, String label, Function<String, Column> valueOf) {
        if (rules.isEmpty()) {
            return lit("");
        }
        Column[] messages = rules.stream()
                .map(rule -> when(rule.failWhen(), rule.message(label != null ? label : rule.errorType().name(), valueOf)))
                .toArray(Column[]::new);
        return concat_ws(SEPARATOR, messages);
    }

    /** The most serious error type of a row, using the order of {@link ErrorType}. */
    private Column mainErrorType(List<ValidationRule> rules) {
        List<Column> typeChecks = new ArrayList<>();
        for (ErrorType type : ErrorType.values()) {
            Column anyRuleOfThisTypeFailed = null;
            for (ValidationRule rule : rules) {
                if (rule.errorType() == type) {
                    anyRuleOfThisTypeFailed = anyRuleOfThisTypeFailed == null
                            ? rule.failWhen()
                            : anyRuleOfThisTypeFailed.or(rule.failWhen());
                }
            }
            if (anyRuleOfThisTypeFailed != null) {
                typeChecks.add(when(anyRuleOfThisTypeFailed, lit(type.name())));
            }
        }
        typeChecks.add(lit(null).cast(DataTypes.StringType));
        return coalesce(typeChecks.toArray(Column[]::new));
    }

    private Dataset<Row> toRejectedFormat(Dataset<Row> rows, String businessKey) {
        return rows.select(
                col(SOURCE_ID).as(REJECTED_SOURCE_ID),
                col(businessKey).cast(DataTypes.StringType).as(REJECTED_BUSINESS_KEY),
                col(ERROR_TYPE).as(REJECTED_ERROR_TYPE),
                col(ERRORS).as(REJECTED_ERROR_REASON),
                col(RAW_RECORD).as(REJECTED_RAW_RECORD));
    }
}
