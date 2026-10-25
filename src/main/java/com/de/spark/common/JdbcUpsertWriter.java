package com.de.spark.common;

import com.de.spark.config.PipelineProperties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Two-step load into MySQL:
 *   1. stage()     Spark writes a DataFrame into a staging table in parallel (old staging rows are removed)
 *   2. *Sql()      SQL statements copy staging -> real table, all inside ONE transaction (see the jobs)
 * Because step 2 is a single transaction, a failure leaves the real tables unchanged.
 */
@Component
public class JdbcUpsertWriter {

    private final PipelineProperties props;

    public JdbcUpsertWriter(PipelineProperties props) {
        this.props = props;
    }

    /** Replaces the content of a staging table with the DataFrame (TRUNCATE keeps the Flyway DDL). */
    public void stage(Dataset<Row> df, String jdbcUrl, String stagingTable) {
        df.write()
                .mode(SaveMode.Overwrite)
                .option("truncate", "true")
                .jdbc(props.sparkUrl(jdbcUrl), stagingTable, props.sparkJdbcProps());
    }

    /**
     * INSERT ... ON DUPLICATE KEY UPDATE from a staging table (MySQL 8 alias syntax).
     * Rows with a new primary key are inserted, rows with an existing key are updated -> no duplicates,
     * and running the same batch twice gives the same result (idempotent).
     *
     * @param columns   data columns present in the staging table
     * @param keys      primary key columns (not updated)
     * @param extraCols columns filled from JDBC parameters (?), e.g. silver_loaded_at, batch_id
     */
    public static String upsertSql(String target, String staging, String[] columns, List<String> keys, String... extraCols) {
        List<String> all = Arrays.asList(columns);
        String insertCols = String.join(", ", all) + extraColumnList(extraCols, "");
        String selectCols = String.join(", ", all) + extraColumnList(extraCols, "? AS ");
        String updates = all.stream()
                .filter(c -> !keys.contains(c))
                .map(c -> c + " = s." + c)
                .collect(Collectors.joining(", "))
                + Arrays.stream(extraCols).map(c -> ", " + c + " = s." + c).collect(Collectors.joining());
        return "INSERT INTO " + target + " (" + insertCols + ")"
                + " SELECT * FROM (SELECT " + selectCols + " FROM " + staging + ") AS s"
                + " ON DUPLICATE KEY UPDATE " + updates;
    }

    /** Plain INSERT ... SELECT from a staging table. */
    public static String insertSql(String target, String staging, String[] columns, String... extraCols) {
        String cols = String.join(", ", columns);
        return "INSERT INTO " + target + " (" + cols + extraColumnList(extraCols, "") + ")"
                + " SELECT " + cols + extraColumnList(extraCols, "? AS ") + " FROM " + staging;
    }

    private static String extraColumnList(String[] extraCols, String prefix) {
        return Arrays.stream(extraCols).map(c -> ", " + prefix + c).collect(Collectors.joining());
    }
}
