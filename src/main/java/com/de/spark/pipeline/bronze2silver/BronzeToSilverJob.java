package com.de.spark.pipeline.bronze2silver;

import com.de.spark.common.BronzeReader;
import com.de.spark.common.JdbcUpsertWriter;
import com.de.spark.common.JobRunAuditService;
import com.de.spark.common.WatermarkService;
import com.de.spark.config.PipelineProperties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.de.spark.common.JdbcUpsertWriter.insertSql;
import static com.de.spark.common.JdbcUpsertWriter.upsertSql;

/**
 * Job #1: Bronze (MongoDB) -> Silver (MySQL), incremental.
 *
 *   1. window   = (watermark, now - safety lag]  on bronze _ingested_at (set by mongo-cdc-subscriber)
 *   2. EXTRACT  only the bronze documents in the window (MongoDB filters them = pushdown)
 *   3. TRANSFORM validate, de-duplicate, flatten  (ObservationTransformer, StationTransformer)
 *   4. STAGE    Spark writes all results into stg_* tables
 *   5. MERGE    ONE transaction: upserts, child replace, soft deletes, rejects, new watermark
 * If anything fails, the transaction rolls back and the watermark stays -> the next run retries the same window.
 */
@Component
public class BronzeToSilverJob {

    public static final String JOB = "bronze_to_silver";
    private static final Logger log = LoggerFactory.getLogger(BronzeToSilverJob.class);

    private final BronzeReader reader;
    private final ObservationTransformer observationTransformer;
    private final StationTransformer stationTransformer;
    private final JdbcUpsertWriter writer;
    private final WatermarkService watermarks;
    private final JobRunAuditService audit;
    private final JdbcTemplate silverJdbc;
    private final TransactionTemplate silverTx;
    private final PipelineProperties props;

    public BronzeToSilverJob(BronzeReader reader, ObservationTransformer observationTransformer,
                             StationTransformer stationTransformer, JdbcUpsertWriter writer,
                             WatermarkService watermarks, JobRunAuditService audit,
                             JdbcTemplate silverJdbc, TransactionTemplate silverTx, PipelineProperties props) {
        this.reader = reader;
        this.observationTransformer = observationTransformer;
        this.stationTransformer = stationTransformer;
        this.writer = writer;
        this.watermarks = watermarks;
        this.audit = audit;
        this.silverJdbc = silverJdbc;
        this.silverTx = silverTx;
        this.props = props;
    }

    public void run() {
        long from = watermarks.get(JOB);
        long to = System.currentTimeMillis() - props.getSafetyLagMinutes() * 60_000L;
        String runId = audit.start(JOB, from, to);
        log.info("[{}] started, window _ingested_at in ({}, {}]", JOB, from, to);

        Dataset<Row> obsEvents = null, stationEvents = null, ownerEvents = null;
        try {
            // EXTRACT
            obsEvents = reader.readWindow(ObservationTransformer.SOURCE, BronzeSchemas.OBSERVATION, from, to)
                    .repartition(props.getPartitions()).cache();
            stationEvents = reader.readWindow(StationTransformer.STATION_SOURCE, BronzeSchemas.MYSQL_STATION, from, to).cache();
            ownerEvents = reader.readWindow(StationTransformer.OWNER_SOURCE, BronzeSchemas.MYSQL_STATION_OWNER, from, to).cache();
            long rowsRead = obsEvents.count() + stationEvents.count() + ownerEvents.count();

            // TRANSFORM
            ObservationTransformer.Result obs = observationTransformer.transform(obsEvents);
            StationTransformer.Result st = stationTransformer.transform(stationEvents, ownerEvents);
            Dataset<Row> rejects = obs.rejects.unionByName(st.rejects);

            // STAGE
            String url = props.getSilverJdbcUrl();
            writer.stage(st.dim, url, "stg_dim_station");
            writer.stage(obs.observations, url, "stg_weather_observation");
            for (Map.Entry<String, Dataset<Row>> child : obs.children.entrySet()) {
                writer.stage(child.getValue(), url, "stg_" + child.getKey());
            }
            writer.stage(obs.deleted, url, "stg_deleted_observation");
            writer.stage(rejects, url, "stg_rejected_record");

            long rowsWritten = count("stg_weather_observation") + count("stg_deleted_observation") + count("stg_dim_station");
            long rowsRejected = count("stg_rejected_record");

            // MERGE (one transaction)
            Timestamp loadedAt = new Timestamp(System.currentTimeMillis());
            silverTx.executeWithoutResult(status -> merge(obs, st, runId, loadedAt, to));

            audit.success(runId, rowsRead, rowsWritten, rowsRejected);
            log.info("[{}] finished: read={}, written={}, rejected={}", JOB, rowsRead, rowsWritten, rowsRejected);
        } catch (RuntimeException e) {
            audit.failed(runId, e);
            throw e;
        } finally {
            for (Dataset<Row> d : Arrays.asList(obsEvents, stationEvents, ownerEvents)) {
                if (d != null) d.unpersist();
            }
        }
    }

    private void merge(ObservationTransformer.Result obs, StationTransformer.Result st,
                       String runId, Timestamp loadedAt, long newWatermark) {
        // dimension + parent: insert new / update changed rows
        silverJdbc.update(upsertSql("dim_station", "stg_dim_station", st.dim.columns(),
                List.of("station_code"), "silver_loaded_at", "batch_id"), loadedAt, runId);
        silverJdbc.update(upsertSql("weather_observation", "stg_weather_observation", obs.observations.columns(),
                List.of("id"), "silver_loaded_at", "batch_id"), loadedAt, runId);

        // children: replace all child rows of every changed observation
        for (Map.Entry<String, Dataset<Row>> child : obs.children.entrySet()) {
            silverJdbc.update("DELETE FROM " + child.getKey()
                    + " WHERE observation_id IN (SELECT id FROM stg_weather_observation)");
            silverJdbc.update(insertSql(child.getKey(), "stg_" + child.getKey(), child.getValue().columns()));
        }

        // deleted in the source -> soft delete (row stays, is_deleted = 1)
        silverJdbc.update("UPDATE weather_observation w JOIN stg_deleted_observation d ON d.id = w.id"
                + " SET w.is_deleted = 1, w.source_ts = d.source_ts, w.bronze_ingested_at = d.bronze_ingested_at,"
                + " w.silver_loaded_at = ?, w.batch_id = ?", loadedAt, runId);

        // rejected / duplicate records, kept with reason and original data
        silverJdbc.update("INSERT INTO etl_rejected_record"
                + " (run_id, job_name, source_collection, record_key, reason, details, raw_payload, rejected_at)"
                + " SELECT ?, ?, source_collection, record_key, reason, details, raw_payload, ? FROM stg_rejected_record",
                runId, JOB, loadedAt);

        watermarks.set(silverJdbc, JOB, newWatermark);
    }

    private long count(String table) {
        return silverJdbc.queryForObject("SELECT COUNT(*) FROM " + table, Long.class);
    }
}
