package com.shop.etl.common.io;

import com.shop.etl.common.config.EtlProperties;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Postgres (silver) tables ne Spark Dataset tarike vanche che. Mostly LOOKUP mate
 * (dim_customer, dim_product, fact_sales mathi FK check ane surrogate key).
 *
 * Performance techniques:
 *   1. FAKT JARURI COLUMNS: SQL ma {@code SELECT customer_key, customer_code ...} lakho,
 *      {@code SELECT *} nahi. Network par ochho data.
 *   2. FILTER DB MA: {@code WHERE} SQL ma j lakho (jem ke lookback date). Postgres index
 *      vapari ne filter kare, Spark ne fakt jaruri rows male.
 *   3. PARALLEL READ (moti table mate): {@link #readPartitioned} table ne numeric column na
 *      range ma todi ne N connections thi sathe vanche che.
 *   4. FETCH SIZE: ek round-trip ma 10,000 rows (default JDBC 0/10 hoy to bahu dheemu).
 *
 * Nani dims (status, payment method, product) mate {@link #read} puratu che. Join vakhte
 * Spark ene BROADCAST kare che (darek executor ne copy, shuffle nahi).
 */
@Component
public class PostgresReader {

    private static final Logger log = LoggerFactory.getLogger(PostgresReader.class);

    private final SparkSession spark;
    private final JdbcTemplate jdbc;
    private final EtlProperties.Postgres pg;

    public PostgresReader(SparkSession spark, JdbcTemplate jdbc, EtlProperties properties) {
        this.spark = spark;
        this.jdbc = jdbc;
        this.pg = properties.postgres();
    }

    /**
     * SQL query nu result Dataset tarike (ek connection). Nani tables mate.
     *
     * @param sql SELECT query (schema naam sathe)
     */
    public Dataset<Row> read(String sql) {
        log.debug("Postgres read: {}", sql);
        return baseReader().option("query", sql).load();
    }

    /**
     * Moti table ne parallel vanche che. Pehla driver par {@code min/max(partitionColumn)} kadhi
     * ne range nakki kare, pachhi Spark N queries sathe chalave:
     * <pre>
     *   ... WHERE customer_key >= 1      AND customer_key < 5000
     *   ... WHERE customer_key >= 5000   AND customer_key < 10000   (etc.)
     * </pre>
     * Table khali hoy to normal read.
     *
     * @param sql             SELECT query
     * @param partitionColumn numeric column (mostly surrogate key, index vali)
     */
    public Dataset<Row> readPartitioned(String sql, String partitionColumn) {
        Map<String, Object> bounds = jdbc.queryForMap(
                "SELECT min(" + partitionColumn + ") AS lo, max(" + partitionColumn + ") AS hi FROM (" + sql + ") q");
        Object lo = bounds.get("lo");
        Object hi = bounds.get("hi");
        if (lo == null || hi == null || pg.readPartitions() <= 1) {
            return read(sql);
        }
        log.debug("Postgres partitioned read: column={}, range=[{}, {}], partitions={}",
                partitionColumn, lo, hi, pg.readPartitions());
        return baseReader()
                .option("dbtable", "(" + sql + ") q")
                .option("partitionColumn", partitionColumn)
                .option("lowerBound", lo.toString())
                .option("upperBound", hi.toString())
                .option("numPartitions", String.valueOf(pg.readPartitions()))
                .load();
    }

    /** Common JDBC options: url, user, password, driver, fetch size. */
    private DataFrameReader baseReader() {
        return spark.read()
                .format("jdbc")
                .option("url", pg.url())
                .option("user", pg.username())
                .option("password", pg.password())
                .option("driver", "org.postgresql.Driver")
                .option("fetchsize", String.valueOf(pg.readFetchSize()));
    }
}
