package com.tcs.dmj.dwh.validation;

import com.tcs.dmj.dwh.enums.ErrorType;
import com.tcs.dmj.dwh.service.constant.SparkTypes;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static com.tcs.dmj.dwh.validation.QualityColumns.ERRORS;
import static com.tcs.dmj.dwh.validation.QualityColumns.RAW_RECORD;
import static com.tcs.dmj.dwh.validation.QualityColumns.REFERENCE_PREFIX;
import static com.tcs.dmj.dwh.validation.QualityColumns.REJECTED_BUSINESS_KEY;
import static com.tcs.dmj.dwh.validation.QualityColumns.REJECTED_ERRORS;
import static com.tcs.dmj.dwh.validation.QualityColumns.REJECTED_RAW_RECORD;
import static com.tcs.dmj.dwh.validation.QualityColumns.REJECTED_SOURCE_ID;
import static com.tcs.dmj.dwh.validation.QualityColumns.ROW_NUMBER;
import static com.tcs.dmj.dwh.validation.QualityColumns.SOURCE_ID;
import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.array_compact;
import static org.apache.spark.sql.functions.broadcast;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.size;

/**
 * Splits rows into VALID and REJECTED:
 * 1. join the ORPHAN lookups, 2. evaluate every rule, 3. rows with errors = rejected,
 * 4. remaining rows: one per business key (latest wins), the other copies = DUPLICATE.
 */
@Component
public class DataQualityEngine {

    /**
     * @param rows               cleaned rows (with _id and _raw_record)
     * @param latestRecordColumn on duplicates, the row with the highest value here wins
     */
    public QualityResult check(Dataset<Row> rows, DataQualityRules qualityRules,
                               String businessKey, String latestRecordColumn) {
        List<ValidationRule> rules = new ArrayList<>(qualityRules.rules());
        List<ReferenceRule> references = qualityRules.referenceRules();

        Dataset<Row> withLookups = joinLookups(rows, references, rules);
        Dataset<Row> checked = withErrors(withLookups, rules, references.size());

        Dataset<Row> failedRows = checked.filter(size(col(ERRORS)).gt(0));
        Dataset<Row> passedRows = numberCopiesPerKey(checked.filter(size(col(ERRORS)).equalTo(0)),
                businessKey, latestRecordColumn);

        Dataset<Row> validRows = passedRows.filter(col(ROW_NUMBER).equalTo(1)).drop(ROW_NUMBER, ERRORS);
        Dataset<Row> duplicateRows = markAsDuplicate(passedRows.filter(col(ROW_NUMBER).gt(1)),
                businessKey, latestRecordColumn);

        Dataset<Row> rejectedRows = toRejectedFormat(failedRows, businessKey)
                .unionByName(toRejectedFormat(duplicateRows, businessKey));
        return new QualityResult(validRows, rejectedRows);
    }

    /** Left-joins every lookup and adds an ORPHAN rule "value present but not found" for it. */
    private Dataset<Row> joinLookups(Dataset<Row> rows, List<ReferenceRule> references, List<ValidationRule> rules) {
        Dataset<Row> joined = rows;
        for (int i = 0; i < references.size(); i++) {
            ReferenceRule reference = references.get(i);
            String foundColumn = REFERENCE_PREFIX + i;
            Dataset<Row> lookupKeys = reference.lookup()
                    .select(col(reference.lookupColumn()).as(foundColumn))
                    .distinct();
            joined = joined.join(broadcast(lookupKeys),
                    joined.col(reference.column()).equalTo(lookupKeys.col(foundColumn)), "left");

            Column notFound = col(reference.column()).isNotNull().and(col(foundColumn).isNull());
            rules.add(new ValidationRule(ErrorType.ORPHAN, notFound, reference.column(),
                    reference.column() + " not found in " + reference.lookupName()));
        }
        return joined;
    }

    /** Adds the errors column (most serious error first) and drops the lookup columns again. */
    private Dataset<Row> withErrors(Dataset<Row> rows, List<ValidationRule> rules, int lookupCount) {
        rules.sort(Comparator.comparing(ValidationRule::errorType));
        Dataset<Row> checked = rows.withColumn(ERRORS, failedRules(rules));
        for (int i = 0; i < lookupCount; i++) {
            checked = checked.drop(REFERENCE_PREFIX + i);
        }
        return checked;
    }

    /** Every failed rule of a row as an array of error structs (empty when the row is fine). */
    private Column failedRules(List<ValidationRule> rules) {
        if (rules.isEmpty()) {
            return array().cast(DataTypes.createArrayType(QualityColumns.ERROR_SCHEMA));
        }
        Column[] errors = rules.stream().map(ValidationRule::errorWhenFailed).toArray(Column[]::new);
        return array_compact(array(errors));   // a passed rule gives NULL
    }

    private Dataset<Row> numberCopiesPerKey(Dataset<Row> rows, String businessKey, String latestRecordColumn) {
        Column latestFirst = col(latestRecordColumn).cast(SparkTypes.TIMESTAMP).desc_nulls_last();
        return rows.withColumn(ROW_NUMBER, row_number().over(Window
                .partitionBy(col(businessKey))
                .orderBy(latestFirst, col(SOURCE_ID).asc())));
    }

    private Dataset<Row> markAsDuplicate(Dataset<Row> rows, String businessKey, String latestRecordColumn) {
        String message = "another record with the same " + businessKey + " was kept (latest "
                + latestRecordColumn + " wins)";
        return rows.withColumn(ERRORS, array(
                QualityColumns.errorStruct(ErrorType.DUPLICATE, businessKey, message, col(businessKey))));
    }

    private Dataset<Row> toRejectedFormat(Dataset<Row> rows, String businessKey) {
        return rows.select(
                col(SOURCE_ID).as(REJECTED_SOURCE_ID),
                col(businessKey).cast(DataTypes.StringType).as(REJECTED_BUSINESS_KEY),
                col(ERRORS).as(REJECTED_ERRORS),
                col(RAW_RECORD).as(REJECTED_RAW_RECORD));
    }
}
