package com.demo.etl.framework.quality;

import com.demo.etl.framework.common.SparkTypes;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.demo.etl.framework.quality.QualityColumns.ERRORS;
import static com.demo.etl.framework.quality.QualityColumns.RAW_RECORD;
import static com.demo.etl.framework.quality.QualityColumns.REFERENCE_PREFIX;
import static com.demo.etl.framework.quality.QualityColumns.REJECTED_BUSINESS_KEY;
import static com.demo.etl.framework.quality.QualityColumns.REJECTED_ERRORS;
import static com.demo.etl.framework.quality.QualityColumns.REJECTED_RAW_RECORD;
import static com.demo.etl.framework.quality.QualityColumns.REJECTED_SOURCE_ID;
import static com.demo.etl.framework.quality.QualityColumns.ROW_NUMBER;
import static com.demo.etl.framework.quality.QualityColumns.SOURCE_ID;
import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.array_compact;
import static org.apache.spark.sql.functions.broadcast;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.size;

/**
 * Applies {@link DataQualityRules} to a dataset and splits it into VALID and REJECTED rows.
 *
 * Order of checks:
 *   1. ORPHAN lookups are joined in (left join with the lookup keys)
 *   2. every rule is evaluated -> array of the failed rules per row (most serious type first)
 *   3. rows with at least one error = rejected (CORRUPT / VALIDATION / ORPHAN)
 *   4. rows without errors are de-duplicated on the business key -> extra copies = rejected (DUPLICATE)
 */
@Component
public class DataQualityEngine {

    /** Same as {@link #check(Dataset, DataQualityRules, String, String, Map)} without PII fields. */
    public QualityResult check(Dataset<Row> rows, DataQualityRules qualityRules,
                               String businessKey, String latestRecordColumn) {
        return check(rows, qualityRules, businessKey, latestRecordColumn, Map.of());
    }

    /**
     * @param rows               cleaned rows (must contain _id and _raw_record)
     * @param businessKey        column that identifies a record, e.g. "paymentId"
     * @param latestRecordColumn when duplicates exist, the row with the highest value here wins (e.g. "updatedAt")
     * @param piiFields          fields whose value is MASKED in the stored error value
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
            rules.add(new ValidationRule(ErrorType.ORPHAN, failWhen, reference.column(),
                    reference.column() + " not found in " + reference.lookupName()));
        }

        // 2. Evaluate all rules. Sorted by type (stable), so errors[0] is always the most serious error.
        rules.sort(Comparator.comparing(ValidationRule::errorType));
        checked = checked.withColumn(ERRORS, failedRules(rules, valueOf));
        for (int i = 0; i < index; i++) {
            checked = checked.drop(REFERENCE_PREFIX + i);
        }

        // 3. Rows that broke at least one rule
        Dataset<Row> failedRows = checked.filter(size(col(ERRORS)).gt(0));

        // 4. De-duplicate the remaining rows: keep the latest version of each business key
        Dataset<Row> passedRows = checked.filter(size(col(ERRORS)).equalTo(0))
                .withColumn(ROW_NUMBER, row_number().over(Window
                        .partitionBy(col(businessKey))
                        .orderBy(col(latestRecordColumn).cast(SparkTypes.TIMESTAMP).desc_nulls_last(), col(SOURCE_ID).asc())));

        Dataset<Row> duplicateRows = passedRows.filter(col(ROW_NUMBER).gt(1))
                .withColumn(ERRORS, array(QualityColumns.errorStruct(ErrorType.DUPLICATE, businessKey,
                        "another record with the same " + businessKey + " was kept (latest " + latestRecordColumn + " wins)",
                        col(businessKey))));

        Dataset<Row> validRows = passedRows.filter(col(ROW_NUMBER).equalTo(1)).drop(ROW_NUMBER, ERRORS);
        Dataset<Row> rejectedRows = toRejectedFormat(failedRows, businessKey)
                .unionByName(toRejectedFormat(duplicateRows, businessKey));

        return new QualityResult(validRows, rejectedRows);
    }

    /** Every failed rule of a row as an array of error structs; empty array when the row is fine. */
    private Column failedRules(List<ValidationRule> rules, Function<String, Column> valueOf) {
        if (rules.isEmpty()) {
            return array().cast(DataTypes.createArrayType(QualityColumns.ERROR_SCHEMA));
        }
        Column[] errors = rules.stream().map(rule -> rule.errorWhenFailed(valueOf)).toArray(Column[]::new);
        return array_compact(array(errors));   // a passed rule gives NULL -> removed
    }

    private Dataset<Row> toRejectedFormat(Dataset<Row> rows, String businessKey) {
        return rows.select(
                col(SOURCE_ID).as(REJECTED_SOURCE_ID),
                col(businessKey).cast(DataTypes.StringType).as(REJECTED_BUSINESS_KEY),
                col(ERRORS).as(REJECTED_ERRORS),
                col(RAW_RECORD).as(REJECTED_RAW_RECORD));
    }
}
