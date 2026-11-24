package com.tcs.dmj.dwh.pipeline.vatreturn;

import com.tcs.dmj.dwh.connector.target.DateDimension;
import com.tcs.dmj.dwh.service.constant.SparkTypes;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.quarter;
import static org.apache.spark.sql.functions.regexp_replace;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.substring;
import static org.apache.spark.sql.functions.to_date;
import static org.apache.spark.sql.functions.when;

/**
 * Transformations of vat-return-pipeline: validated (String) rows -> typed rows -> dimension / fact rows.
 * Pure Spark functions, no I/O.
 *
 * Dates: Ghana = UTC. Dates are taken from the TEXT of the source value ("2026-10-14T10:43:38Z" -> 2026-10-14),
 * not converted with the Spark session time zone (Asia/Kolkata), so a filing late in the evening keeps its own day.
 */
final class VatReturnSchemaMapper {

    private VatReturnSchemaMapper() {
    }

    /** camelCase amount column -> snake_case fact column: "vatLevies" -> "vat_levies", "interest" -> "interest_amount". */
    static String factColumn(String amountColumn) {
        return "interest".equals(amountColumn)
                ? "interest_amount"
                : amountColumn.replaceAll("([a-z0-9])([A-Z])", "$1_$2").toLowerCase();
    }

    /** "2026-10-14T10:43:38.639Z" -> the date of the text, 2026-10-14 */
    private static Column dateOfText(String column) {
        return to_date(substring(col(column), 1, 10));
    }

    /** All columns typed and renamed to the warehouse names. */
    static Dataset<Row> toTyped(Dataset<Row> valid) {
        List<Column> columns = new ArrayList<>(List.of(
                col("ackNo").as("ack_no"),
                col("tin"),
                col("taxpayerId").as("taxpayer_id"),
                col("taxpayerName").as("taxpayer_name"),
                col("taxpayerCategory").as("taxpayer_category"),
                col("taxpayerJurisdiction").as("jurisdiction_code"),
                // period
                col("year").cast(SparkTypes.INT).multiply(100).plus(col("month").cast(SparkTypes.INT)).as("period_key"),
                to_date(col("rtnFromDate")).as("period_from"),
                to_date(col("rtnToDate")).as("period_to"),
                col("month").cast(SparkTypes.INT).as("period_month"),
                col("year").cast(SparkTypes.INT).as("period_year"),
                quarter(to_date(col("rtnFromDate"))).as("period_quarter"),
                to_date(col("rtnDueDt")).as("due_date"),
                // codes of the reference dimensions (looked up in SQL at load time)
                col("formId").as("form_id"),
                col("rtnType").as("return_type_code"),
                col("rtnStatus").as("status_code"),
                col("fileFrom").as("channel_code"),
                // filing time: wall-clock time of the source text, no time-zone conversion (TIMESTAMP_NTZ)
                regexp_replace(col("filedAt"), "Z$", "").cast(DataTypes.TimestampNTZType).as("filed_at"),
                dateOfText("filedAt").as("filed_date"),
                dateOfText("ackDate").as("ack_date"),
                // flags
                col("isNilReturn").equalTo("true").as("is_nil_return"),
                when(col("isLateRtn").isNotNull(), col("isLateRtn").equalTo("Y"))
                        .otherwise(col("delayDays").cast(SparkTypes.INT).gt(0)).as("is_late_return"),
                coalesce(col("isActive").equalTo("Y"), lit(true)).as("is_active"),
                col("delayDays").cast(SparkTypes.INT).as("delay_days"),
                col("updatedAt").cast(SparkTypes.TIMESTAMP).as("updated_at")));
        for (Map.Entry<String, String[]> amount : VatReturnSources.AMOUNTS.entrySet()) {
            // an amount that is missing in the source counts as 0
            columns.add(coalesce(col(amount.getKey()).cast(SparkTypes.MONEY), lit(0).cast(SparkTypes.MONEY))
                    .as(factColumn(amount.getKey())));
        }
        return valid.select(columns.toArray(Column[]::new));
    }

    /** dim_taxpayer (SCD1): one row per tin, the values of its latest return win. */
    static Dataset<Row> toDimTaxpayer(Dataset<Row> typed) {
        return latestPer(typed, "tin")
                .select("tin", "taxpayer_id", "taxpayer_name", "taxpayer_category", "jurisdiction_code");
    }

    /** dim_tax_period: one row per return month. */
    static Dataset<Row> toDimTaxPeriod(Dataset<Row> typed) {
        return latestPer(typed, "period_key")
                .select("period_key", "period_from", "period_to", "period_month", "period_year", "period_quarter",
                        "due_date");
    }

    /**
     * fact_vat_return: one row per filed return. tin / form_id / *_code stay as business keys: the writer replaces
     * them with the surrogate keys inside the SQL (KeyLookup), because new taxpayers are not committed yet.
     */
    static Dataset<Row> toFactVatReturn(Dataset<Row> typed) {
        List<Column> columns = new ArrayList<>(List.of(
                col("ack_no"), col("tin"), col("period_key"), col("form_id"), col("return_type_code"),
                col("status_code"), col("channel_code"),
                DateDimension.dateKey(col("filed_date")).as("filed_date_key"),
                DateDimension.dateKey(col("due_date")).as("due_date_key"),
                DateDimension.dateKey(col("ack_date")).as("ack_date_key"),
                col("filed_at"), col("is_nil_return"), col("is_late_return"), col("is_active"), col("delay_days")));
        VatReturnSources.AMOUNTS.keySet().forEach(amount -> columns.add(col(factColumn(amount))));
        return typed.select(columns.toArray(Column[]::new));
    }

    /** The row with the latest updated_at per key (ack_no breaks ties), so the newest values win. */
    private static Dataset<Row> latestPer(Dataset<Row> typed, String key) {
        return typed
                .withColumn("_rn", row_number().over(Window.partitionBy(col(key))
                        .orderBy(col("updated_at").desc_nulls_last(), col("ack_no").desc())))
                .filter(col("_rn").equalTo(1))
                .drop("_rn");
    }
}
