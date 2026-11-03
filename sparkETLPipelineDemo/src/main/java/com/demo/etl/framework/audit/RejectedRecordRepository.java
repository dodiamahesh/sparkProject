package com.demo.etl.framework.audit;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * SQL for etl_audit.etl_rejected_record.
 * Spark first writes the rows into a staging table, then these statements move them in one go.
 */
@Repository
public class RejectedRecordRepository {

    private static final RowMapper<RejectedRecord> ROW_MAPPER = (rs, rowNum) -> new RejectedRecord(
            rs.getLong("rejected_record_id"),
            rs.getString("pipeline_name"),
            rs.getString("source_collection"),
            rs.getString("source_id"),
            rs.getString("business_key"),
            rs.getString("error_type"),
            rs.getString("error_reason"),
            rs.getString("raw_record"),
            rs.getInt("attempt_count"),
            rs.getLong("first_job_run_id"),
            rs.getLong("last_job_run_id"),
            rs.getObject("first_rejected_at", LocalDateTime.class),
            rs.getObject("last_rejected_at", LocalDateTime.class));

    private final JdbcTemplate jdbc;

    public RejectedRecordRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * New rejected record -> INSERT.
     * Already rejected before -> UPDATE the reason and add 1 to attempt_count.
     */
    public int upsertFromStaging(String stagingTable, String pipelineName, String collection, long jobRunId) {
        return jdbc.update("""
                INSERT INTO etl_audit.etl_rejected_record
                       (pipeline_name, source_collection, source_id, business_key, error_type, error_reason,
                        raw_record, attempt_count, first_job_run_id, last_job_run_id, first_rejected_at, last_rejected_at)
                SELECT ?, ?, s.source_id, s.business_key, s.error_type, s.error_reason,
                       CAST(s.raw_record AS JSONB), 1, ?, ?, now(), now()
                  FROM %s s
                ON CONFLICT (pipeline_name, source_collection, source_id) DO UPDATE
                   SET business_key     = EXCLUDED.business_key,
                       error_type       = EXCLUDED.error_type,
                       error_reason     = EXCLUDED.error_reason,
                       raw_record       = EXCLUDED.raw_record,
                       attempt_count    = etl_audit.etl_rejected_record.attempt_count + 1,
                       last_job_run_id  = EXCLUDED.last_job_run_id,
                       last_rejected_at = now()
                """.formatted(stagingTable), pipelineName, collection, jobRunId, jobRunId);
    }

    /**
     * Records that were rejected before but are VALID in this run -> remove them from the rejected table.
     * The staging table has one column: source_id.
     */
    public int deleteResolvedFromStaging(String stagingTable, String pipelineName, String collection) {
        return jdbc.update("""
                DELETE FROM etl_audit.etl_rejected_record r
                 USING %s s
                 WHERE r.pipeline_name = ?
                   AND r.source_collection = ?
                   AND r.source_id = s.source_id
                """.formatted(stagingTable), pipelineName, collection);
    }

    /** All filters are optional (null = no filter). */
    public List<RejectedRecord> find(String pipelineName, String collection, String errorType, int limit) {
        StringBuilder sql = new StringBuilder("SELECT * FROM etl_audit.etl_rejected_record WHERE 1 = 1");
        List<Object> params = new ArrayList<>();
        if (pipelineName != null) {
            sql.append(" AND pipeline_name = ?");
            params.add(pipelineName);
        }
        if (collection != null) {
            sql.append(" AND source_collection = ?");
            params.add(collection);
        }
        if (errorType != null) {
            sql.append(" AND error_type = ?");
            params.add(errorType.toUpperCase());
        }
        sql.append(" ORDER BY pipeline_name, source_collection, business_key LIMIT ?");
        params.add(limit);
        return jdbc.query(sql.toString(), ROW_MAPPER, params.toArray());
    }

    public List<RejectedRecordSummary> summary() {
        return jdbc.query("""
                SELECT pipeline_name, source_collection, error_type, COUNT(*) AS record_count
                  FROM etl_audit.etl_rejected_record
                 GROUP BY pipeline_name, source_collection, error_type
                 ORDER BY pipeline_name, source_collection, error_type
                """, (rs, rowNum) -> new RejectedRecordSummary(
                rs.getString("pipeline_name"),
                rs.getString("source_collection"),
                rs.getString("error_type"),
                rs.getLong("record_count")));
    }
}
