package com.demo.etl.framework.audit;

import com.demo.etl.framework.source.LoadWindow;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * SQL for etl_audit.etl_watermark: "up to which timestamp did pipeline X read collection Y".
 *
 * Rules:
 *   1. no row / NULL       -> first run: read everything up to the run's start time
 *   2. next runs           -> read watermarkField > last_watermark AND <= run start time
 *   3. moved forward ONLY after the whole job succeeded (commit phase), so a failed run changes nothing
 *      and the next run reads the same window again (the UPSERTs make that safe)
 *   4. one row per pipeline + collection: two pipelines reading the same collection have their own watermark
 */
@Repository
public class WatermarkRepository {

    private final JdbcTemplate jdbc;

    public WatermarkRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** The window this run reads for one collection. */
    public LoadWindow window(String pipelineName, String collection, Instant runEnd) {
        List<OffsetDateTime> last = jdbc.queryForList(
                "SELECT last_watermark FROM etl_audit.etl_watermark WHERE pipeline_name = ? AND source_collection = ?",
                OffsetDateTime.class, pipelineName, collection);
        Instant start = last.isEmpty() || last.get(0) == null ? null : last.get(0).toInstant();
        return new LoadWindow(start, runEnd);
    }

    /** After a successful job: the collection has been read up to {@code runEnd}. */
    public void advance(String pipelineName, String collection, String watermarkField, Instant runEnd, long jobRunId) {
        jdbc.update("""
                INSERT INTO etl_audit.etl_watermark
                       (pipeline_name, source_collection, watermark_field, last_watermark, last_job_run_id, updated_at)
                VALUES (?, ?, ?, ?, ?, now())
                ON CONFLICT (pipeline_name, source_collection) DO UPDATE
                   SET watermark_field = EXCLUDED.watermark_field,
                       last_watermark  = EXCLUDED.last_watermark,
                       last_job_run_id = EXCLUDED.last_job_run_id,
                       updated_at      = now()
                """, pipelineName, collection, watermarkField, OffsetDateTime.ofInstant(runEnd, ZoneOffset.UTC), jobRunId);
    }

    /** collection -> last watermark (for GET /api/etl/pipelines). Collections never loaded are missing. */
    public Map<String, OffsetDateTime> findByPipeline(String pipelineName) {
        Map<String, OffsetDateTime> result = new LinkedHashMap<>();
        jdbc.query("SELECT source_collection, last_watermark FROM etl_audit.etl_watermark "
                        + "WHERE pipeline_name = ? ORDER BY source_collection",
                rs -> {
                    result.put(rs.getString(1), rs.getObject(2, OffsetDateTime.class));
                }, pipelineName);
        return result;
    }
}
