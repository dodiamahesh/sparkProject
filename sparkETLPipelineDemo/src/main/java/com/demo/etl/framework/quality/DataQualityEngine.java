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
import static org.apache.spark.sql.functions.broadcast;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.when;

/**
 * Applies {@link DataQualityRules} to a dataset and splits it into VALID and REJECTED rows.
 *
 * Order of checks:
 *   1. ORPHAN lookups are joined in (left join with the lookup keys)
 *   2. every rule is evaluated -> list of error messages per row
 *   3. rows with errors = rejected (CORRUPT / VALIDATION / ORPHAN)
 *   4. rows without errors are de-duplicated on the business key -> extra copies = rejected (DUPLICATE)
 */
@Component
public class DataQualityEngine {

    private static final String ERROR_SEPARATOR = " | ";

    /**
     * @param rows               cleaned rows (must contain _id and _raw_record)
     * @param businessKey        column that identifies a record, e.g. "paymentId"
     * @param latestRecordColumn when duplicates exist, the row with the highest value here wins (e.g. "updatedAt")
     */
    public QualityResult check(Dataset<Row> rows, DataQualityRules qualityRules,
                               String businessKey, String latestRecordColumn) {

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
            String text = reference.column() + " not found in " + reference.lookupName();
            rules.add(new ValidationRule(ErrorType.ORPHAN, failWhen,
                    DataQualityRules.message(ErrorType.ORPHAN, text, reference.column())));
        }

        // 2. Evaluate all rules. concat_ws skips NULLs, so a row that passes everything gets "".
        Column[] messages = rules.stream()
                .map(rule -> when(rule.failWhen(), rule.message()))
                .toArray(Column[]::new);
        checked = checked
                .withColumn(ERRORS, concat_ws(ERROR_SEPARATOR, messages))
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

        Dataset<Row> validRows = passedRows.filter(col(ROW_NUMBER).equalTo(1))
                .drop(ROW_NUMBER, ERRORS, ERROR_TYPE);

        Dataset<Row> rejectedRows = toRejectedFormat(failedRows, businessKey)
                .unionByName(toRejectedFormat(duplicateRows, businessKey));

        return new QualityResult(validRows, rejectedRows);
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
