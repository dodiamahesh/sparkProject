package com.de.spark.pipeline.silver2gold;

import com.de.spark.common.JdbcUpsertWriter;
import com.de.spark.common.JobRunAuditService;
import com.de.spark.common.WatermarkService;
import com.de.spark.config.PipelineProperties;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import static com.de.spark.common.JdbcUpsertWriter.insertSql;
import static org.apache.spark.sql.functions.*;

/**
 * Job #2: Silver -> Gold, incremental with the "changed-keys recompute" pattern.
 *
 * An average cannot be updated by just adding new rows, so instead:
 *   1. find the groups touched since the watermark:  (station, day) of changed observations / stations
 *   2. read ALL silver rows of only those groups (MySQL filters them = pushdown)
 *   3. recompute the aggregates of those groups in Spark
 *   4. ONE gold transaction: delete the old rows of those groups, insert the new ones
 *      (a group whose rows were all deleted simply disappears)
 * Monthly region totals are rebuilt for every touched month (all regions), so a station moving
 * to another region is handled correctly.
 */
@Component
public class SilverToGoldJob {

    public static final String JOB = "silver_to_gold";
    private static final Logger log = LoggerFactory.getLogger(SilverToGoldJob.class);
    private static final DateTimeFormatter SQL_TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private final SparkSession spark;
    private final JdbcUpsertWriter writer;
    private final WatermarkService watermarks;
    private final JobRunAuditService audit;
    private final JdbcTemplate goldJdbc;
    private final TransactionTemplate goldTx;
    private final PipelineProperties props;

    public SilverToGoldJob(SparkSession spark, JdbcUpsertWriter writer, WatermarkService watermarks,
                           JobRunAuditService audit, @Qualifier("goldJdbc") JdbcTemplate goldJdbc,
                           @Qualifier("goldTx") TransactionTemplate goldTx, PipelineProperties props) {
        this.spark = spark;
        this.writer = writer;
        this.watermarks = watermarks;
        this.audit = audit;
        this.goldJdbc = goldJdbc;
        this.goldTx = goldTx;
        this.props = props;
    }

    public void run() {
        long from = watermarks.get(JOB);
        long to = System.currentTimeMillis();   // Job #1 already committed, so nothing newer is half-written
        String runId = audit.start(JOB, from, to);
        log.info("[{}] started, window silver_loaded_at in ({}, {}]", JOB, sqlTs(from), sqlTs(to));

        try {
            // 1. touched (station, day) groups: changed observations + observations of changed stations
            String changedKeys = "SELECT w.call_letters AS station_code, DATE(w.obs_ts) AS obs_date"
                    + " FROM weather_observation w"
                    + " WHERE w.silver_loaded_at > '" + sqlTs(from) + "' AND w.silver_loaded_at <= '" + sqlTs(to) + "'"
                    + " UNION"
                    + " SELECT w.call_letters, DATE(w.obs_ts) FROM weather_observation w"
                    + " JOIN dim_station d ON d.station_code = w.call_letters"
                    + " WHERE d.silver_loaded_at > '" + sqlTs(from) + "' AND d.silver_loaded_at <= '" + sqlTs(to) + "'";
            Dataset<Row> dailyKeys = readSilver("(" + changedKeys + ") k").cache();
            Dataset<Row> monthKeys = dailyKeys.select(date_format(col("obs_date"), "yyyy-MM").as("obs_month")).distinct();

            // 2. all live silver rows of the touched groups
            Dataset<Row> dailyRows = readSilver("(SELECT w.call_letters AS station_code, DATE(w.obs_ts) AS obs_date,"
                    + " w.air_temperature, w.dew_point, w.pressure, w.wind_speed_rate"
                    + " FROM weather_observation w JOIN (" + changedKeys + ") k"
                    + " ON k.station_code = w.call_letters"
                    + " AND w.obs_ts >= k.obs_date AND w.obs_ts < k.obs_date + INTERVAL 1 DAY"
                    + " WHERE w.is_deleted = 0) r").cache();
            Dataset<Row> monthlyRows = readSilver("(SELECT CASE WHEN d.is_deleted = 0 THEN d.region END AS region,"
                    + " DATE_FORMAT(w.obs_ts, '%Y-%m') AS obs_month, w.call_letters, w.air_temperature, w.pressure"
                    + " FROM weather_observation w LEFT JOIN dim_station d ON d.station_code = w.call_letters"
                    + " WHERE w.is_deleted = 0 AND DATE_FORMAT(w.obs_ts, '%Y-%m') IN"
                    + " (SELECT DATE_FORMAT(k.obs_date, '%Y-%m') FROM (" + changedKeys + ") k)) r").cache();
            long rowsRead = dailyRows.count() + monthlyRows.count();

            // 3. aggregates (sentinel values = "not measured" -> ignored)
            Column temp = measured(col("air_temperature"), 999.9);
            Column dew = measured(col("dew_point"), 999.9);
            Column pres = measured(col("pressure"), 9999.9);
            Column wind = measured(col("wind_speed_rate"), 999.9);

            Dataset<Row> daily = dailyRows.groupBy("station_code", "obs_date").agg(
                    count(lit(1)).cast("int").as("obs_count"),
                    round(avg(temp), 2).as("avg_air_temp"),
                    min(temp).as("min_air_temp"),
                    max(temp).as("max_air_temp"),
                    round(avg(dew), 2).as("avg_dew_point"),
                    round(avg(pres), 2).as("avg_pressure"),
                    max(wind).as("max_wind_speed"));

            Dataset<Row> quality = dailyRows.groupBy("station_code", "obs_date").agg(
                    count(lit(1)).cast("int").as("total_obs"),
                    missing(temp).as("missing_air_temp"),
                    missing(dew).as("missing_dew_point"),
                    missing(pres).as("missing_pressure"),
                    missing(wind).as("missing_wind_speed"),
                    round(sum(when(temp.isNotNull().and(dew.isNotNull()).and(pres.isNotNull()).and(wind.isNotNull()), 1)
                            .otherwise(0)).multiply(100.0).divide(count(lit(1))), 2).as("complete_obs_pct"));

            Dataset<Row> monthly = monthlyRows
                    .withColumn("region", coalesce(col("region"), lit("UNKNOWN")))
                    .groupBy("region", "obs_month").agg(
                            countDistinct("call_letters").cast("int").as("station_count"),
                            count(lit(1)).cast("int").as("obs_count"),
                            round(avg(temp), 2).as("avg_air_temp"),
                            min(temp).as("min_air_temp"),
                            max(temp).as("max_air_temp"),
                            round(avg(pres), 2).as("avg_pressure"));

            // STAGE
            String url = props.getGoldJdbcUrl();
            writer.stage(dailyKeys, url, "stg_keys_daily");
            writer.stage(monthKeys, url, "stg_keys_monthly");
            writer.stage(daily, url, "stg_daily_station_weather");
            writer.stage(quality, url, "stg_station_data_quality");
            writer.stage(monthly, url, "stg_monthly_region_weather");

            // 4. MERGE (one gold transaction): replace the touched groups
            Timestamp loadedAt = new Timestamp(System.currentTimeMillis());
            goldTx.executeWithoutResult(status -> {
                replace("daily_station_weather", "stg_daily_station_weather", daily.columns(),
                        "stg_keys_daily k ON k.station_code = g.station_code AND k.obs_date = g.obs_date", loadedAt, runId);
                replace("station_data_quality", "stg_station_data_quality", quality.columns(),
                        "stg_keys_daily k ON k.station_code = g.station_code AND k.obs_date = g.obs_date", loadedAt, runId);
                replace("monthly_region_weather", "stg_monthly_region_weather", monthly.columns(),
                        "stg_keys_monthly k ON k.obs_month = g.obs_month", loadedAt, runId);
            });

            // the watermark lives in de_silver; if the app stops right here, the next run recomputes
            // the same groups again, which gives the same result (idempotent)
            watermarks.set(JOB, to);
            long rowsWritten = tableCount("stg_daily_station_weather") + tableCount("stg_station_data_quality")
                    + tableCount("stg_monthly_region_weather");
            audit.success(runId, rowsRead, rowsWritten, 0);
            log.info("[{}] finished: groups={}, silver rows read={}, gold rows written={}",
                    JOB, dailyKeys.count(), rowsRead, rowsWritten);

            dailyKeys.unpersist();
            dailyRows.unpersist();
            monthlyRows.unpersist();
        } catch (RuntimeException e) {
            audit.failed(runId, e);
            throw e;
        }
    }

    /** DELETE the old gold rows of the touched groups, INSERT the recomputed ones. */
    private void replace(String target, String staging, String[] columns, String keyJoin, Timestamp loadedAt, String runId) {
        goldJdbc.update("DELETE g FROM " + target + " g JOIN " + keyJoin);
        goldJdbc.update(insertSql(target, staging, columns, "gold_loaded_at", "batch_id"), loadedAt, runId);
    }

    private Dataset<Row> readSilver(String query) {
        return spark.read().jdbc(props.getSilverJdbcUrl(), query, props.sparkJdbcProps());
    }

    private long tableCount(String table) {
        return goldJdbc.queryForObject("SELECT COUNT(*) FROM " + table, Long.class);
    }

    /** NULL when the value is missing or a "not measured" sentinel (999.9, 9999.9). */
    private static Column measured(Column value, double sentinel) {
        return when(value.isNull().or(value.geq(sentinel)), lit(null)).otherwise(value);
    }

    private static Column missing(Column value) {
        return sum(when(value.isNull(), 1).otherwise(0)).cast("int");
    }

    /** Watermark (epoch ms) as a MySQL DATETIME literal in the JVM time zone (same zone JDBC used to write it). */
    private static String sqlTs(long epochMs) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMs), ZoneId.systemDefault()).format(SQL_TS);
    }
}
