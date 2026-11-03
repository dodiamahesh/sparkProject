package com.demo.etl.framework.common;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.row_number;

/**
 * Small, reusable DataFrame helpers.
 */
public final class SparkUtils {

    private static final String ROW_NUMBER_COLUMN = "_row_number";

    private SparkUtils() {
    }

    /**
     * Keeps ONE row per key: the row with the highest value in {@code orderByColumn} (e.g. the latest updated_at).
     * Typical use: build a dimension from a fact source where the same customer appears many times.
     */
    public static Dataset<Row> latestRecordPerKey(Dataset<Row> rows, String keyColumn, String orderByColumn) {
        return rows
                .withColumn(ROW_NUMBER_COLUMN, row_number().over(
                        Window.partitionBy(col(keyColumn)).orderBy(col(orderByColumn).desc_nulls_last())))
                .filter(col(ROW_NUMBER_COLUMN).equalTo(1))
                .drop(ROW_NUMBER_COLUMN);
    }
}
