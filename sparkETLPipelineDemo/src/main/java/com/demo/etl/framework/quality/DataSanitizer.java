package com.demo.etl.framework.quality;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.initcap;
import static org.apache.spark.sql.functions.length;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.lower;
import static org.apache.spark.sql.functions.regexp_replace;
import static org.apache.spark.sql.functions.substring;
import static org.apache.spark.sql.functions.trim;
import static org.apache.spark.sql.functions.upper;
import static org.apache.spark.sql.functions.when;

/**
 * DATA SANITIZATION = fix values that are "messy but correct" BEFORE validation.
 * Example: "  rahul.SHAH@Gmail.com " -> "rahul.shah@gmail.com", "inr" -> "INR", "1,250.50" -> "1250.50".
 *
 * Build it like a recipe, steps run in the order you add them:
 * <pre>
 *   DataSanitizer.create()
 *       .trimAllAndBlankToNull()
 *       .toUpperCase("currency", "status")
 *       .toLowerCase("email");
 * </pre>
 */
public final class DataSanitizer {

    private final List<UnaryOperator<Dataset<Row>>> steps = new ArrayList<>();

    private DataSanitizer() {
    }

    public static DataSanitizer create() {
        return new DataSanitizer();
    }

    /** Trims every business column and turns empty strings ("", "   ") into NULL. */
    public DataSanitizer trimAllAndBlankToNull() {
        steps.add(rows -> {
            Dataset<Row> result = rows;
            for (String column : rows.columns()) {
                if (!QualityColumns.isTechnicalColumn(column)) {
                    Column trimmed = trim(col(column));
                    result = result.withColumn(column, when(trimmed.equalTo(""), lit(null)).otherwise(trimmed));
                }
            }
            return result;
        });
        return this;
    }

    public DataSanitizer toUpperCase(String... columns) {
        return addStep(columns, column -> upper(col(column)));
    }

    public DataSanitizer toLowerCase(String... columns) {
        return addStep(columns, column -> lower(col(column)));
    }

    /** "rahul SHAH" -> "Rahul Shah". */
    public DataSanitizer toProperCase(String... columns) {
        return addStep(columns, column -> initcap(col(column)));
    }

    /** "1,25,000.50" -> "125000.50". */
    public DataSanitizer removeThousandSeparators(String... columns) {
        return addStep(columns, column -> regexp_replace(col(column), ",", ""));
    }

    /** "+91 98765-43210" / "098765 43210" -> "9876543210". */
    public DataSanitizer normalizeIndianPhone(String... columns) {
        return addStep(columns, column -> {
            Column digits = regexp_replace(col(column), "[^0-9]", "");
            return when(length(digits).equalTo(12).and(digits.startsWith("91")), substring(digits, 3, 10))
                    .when(length(digits).equalTo(11).and(digits.startsWith("0")), substring(digits, 2, 10))
                    .otherwise(digits);
        });
    }

    /** Any other cleaning you need: {@code custom("pincode", c -> regexp_replace(c, " ", ""))}. */
    public DataSanitizer custom(String column, UnaryOperator<Column> cleaner) {
        steps.add(rows -> rows.withColumn(column, cleaner.apply(col(column))));
        return this;
    }

    public Dataset<Row> apply(Dataset<Row> rows) {
        Dataset<Row> result = rows;
        for (UnaryOperator<Dataset<Row>> step : steps) {
            result = step.apply(result);
        }
        return result;
    }

    private DataSanitizer addStep(String[] columns, Function<String, Column> cleaner) {
        steps.add(rows -> {
            Dataset<Row> result = rows;
            for (String column : columns) {
                result = result.withColumn(column, cleaner.apply(column));
            }
            return result;
        });
        return this;
    }
}
