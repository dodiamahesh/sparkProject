package com.demo.etl.framework.audit;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * SQL for meta.meta_rejected_record and meta.meta_error_master.
 *
 *   meta_error_master    one row per possible error (error_type + error_message), e.g.
 *                        12 | VALIDATION | amount | amount must be greater than 0
 *   meta_rejected_record one row per failed rule of a rejected record -> error_id + the bad field_value
 *
 * Spark first writes the rejected rows (one per record + error) into a staging table, then these
 * statements move them in one go.
 */
@Repository
public class RejectedRecordRepository {

    private static final RowMapper<RejectedRecord> ROW_MAPPER = (rs, rowNum) -> new RejectedRecord(
            rs.getLong("rejected_record_id"),
            rs.getString("pipeline_name"),
            rs.getString("source_collection"),
            rs.getString("source_id"),
            rs.getString("business_key"),
            rs.getInt("error_id"),
            rs.getString("error_type"),
            rs.getString("field_name"),
            rs.getString("error_message"),
            rs.getString("field_value"),
            rs.getString("raw_record"),
            rs.getInt("attempt_count"),
            rs.getLong("first_job_run_id"),
            rs.getLong("last_job_run_id"),
            rs.getObject("first_rejected_at", LocalDateTime.class),
            rs.getObject("last_rejected_at", LocalDateTime.class));

    private static final String SELECT_WITH_ERROR = """
            SELECT r.*, e.error_type, e.field_name, e.error_message
              FROM meta.meta_rejected_record r
              JOIN meta.meta_error_master e ON e.error_id = r.error_id
            """;

    private final JdbcTemplate jdbc;
    private final TransactionTemplate transaction;

    public RejectedRecordRepository(JdbcTemplate jdbc, TransactionTemplate transaction) {
        this.jdbc = jdbc;
        this.transaction = transaction;
    }

    /**
     * Saves the rejected rows of one collection. The staging table has one row per record + error:
     * source_id, business_key, error_type, field_name, error_message, field_value, raw_record.
     *
     *   1. an error message that is not in meta_error_master yet (a rule added later) gets its row there
     *   2. errors a record does NOT have any more (it was partly fixed) are removed
     *   3. new record + error -> INSERT; same record + error again -> UPDATE value / raw record, attempt_count + 1
     *
     * All three in ONE transaction.
     */
    public void saveFromStaging(String stagingTable, String pipelineName, String collection, long jobRunId) {
        transaction.executeWithoutResult(status -> {
            // NOT EXISTS first: ON CONFLICT alone would use up an error_id for every known error on every run
            jdbc.update("""
                    INSERT INTO meta.meta_error_master (error_type, field_name, error_message)
                    SELECT DISTINCT ON (s.error_type, s.error_message) s.error_type, s.field_name, s.error_message
                      FROM %s s
                     WHERE NOT EXISTS (SELECT 1 FROM meta.meta_error_master e
                                        WHERE e.error_type = s.error_type AND e.error_message = s.error_message)
                    ON CONFLICT (error_type, error_message) DO NOTHING
                    """.formatted(stagingTable));

            jdbc.update("""
                    DELETE FROM meta.meta_rejected_record r
                     WHERE r.pipeline_name = ?
                       AND r.source_collection = ?
                       AND r.source_id IN (SELECT source_id FROM %1$s)
                       AND NOT EXISTS (SELECT 1
                                         FROM %1$s s
                                         JOIN meta.meta_error_master e
                                           ON e.error_type = s.error_type AND e.error_message = s.error_message
                                        WHERE s.source_id = r.source_id
                                          AND e.error_id = r.error_id)
                    """.formatted(stagingTable), pipelineName, collection);

            jdbc.update("""
                    INSERT INTO meta.meta_rejected_record
                           (pipeline_name, source_collection, source_id, business_key, error_id, field_value,
                            raw_record, attempt_count, first_job_run_id, last_job_run_id, first_rejected_at, last_rejected_at)
                    SELECT DISTINCT ON (s.source_id, e.error_id)
                           ?, ?, s.source_id, s.business_key, e.error_id, s.field_value,
                           CAST(s.raw_record AS JSONB), 1, ?, ?, now(), now()
                      FROM %s s
                      JOIN meta.meta_error_master e
                        ON e.error_type = s.error_type AND e.error_message = s.error_message
                    ON CONFLICT (pipeline_name, source_collection, source_id, error_id) DO UPDATE
                       SET business_key     = EXCLUDED.business_key,
                           field_value      = EXCLUDED.field_value,
                           raw_record       = EXCLUDED.raw_record,
                           attempt_count    = meta.meta_rejected_record.attempt_count + 1,
                           last_job_run_id  = EXCLUDED.last_job_run_id,
                           last_rejected_at = now()
                    """.formatted(stagingTable), pipelineName, collection, jobRunId, jobRunId);
        });
    }

    /**
     * Records that were rejected before but are VALID in this run -> remove them (all their errors).
     * The staging table has one column: source_id.
     *
     * @return number of records (not error rows) removed
     */
    public long deleteResolvedFromStaging(String stagingTable, String pipelineName, String collection) {
        return jdbc.queryForObject("""
                WITH removed AS (
                    DELETE FROM meta.meta_rejected_record r
                     USING %s s
                     WHERE r.pipeline_name = ?
                       AND r.source_collection = ?
                       AND r.source_id = s.source_id
                    RETURNING r.source_id
                )
                SELECT COUNT(DISTINCT source_id) FROM removed
                """.formatted(stagingTable), Long.class, pipelineName, collection);
    }

    /** All filters are optional (null = no filter). */
    public List<RejectedRecord> find(String pipelineName, String collection, String errorType, int limit) {
        StringBuilder sql = new StringBuilder(SELECT_WITH_ERROR).append(" WHERE 1 = 1");
        List<Object> params = new ArrayList<>();
        if (pipelineName != null) {
            sql.append(" AND r.pipeline_name = ?");
            params.add(pipelineName);
        }
        if (collection != null) {
            sql.append(" AND r.source_collection = ?");
            params.add(collection);
        }
        if (errorType != null) {
            sql.append(" AND e.error_type = ?");
            params.add(errorType.toUpperCase());
        }
        sql.append(" ORDER BY r.pipeline_name, r.source_collection, r.business_key, e.error_type, r.error_id LIMIT ?");
        params.add(limit);
        return jdbc.query(sql.toString(), ROW_MAPPER, params.toArray());
    }

    public List<RejectedRecordSummary> summary() {
        return jdbc.query("""
                SELECT r.pipeline_name, r.source_collection, e.error_id, e.error_type, e.error_message,
                       COUNT(DISTINCT r.source_id) AS record_count
                  FROM meta.meta_rejected_record r
                  JOIN meta.meta_error_master e ON e.error_id = r.error_id
                 GROUP BY r.pipeline_name, r.source_collection, e.error_id, e.error_type, e.error_message
                 ORDER BY r.pipeline_name, r.source_collection, e.error_type, e.error_id
                """, (rs, rowNum) -> new RejectedRecordSummary(
                rs.getString("pipeline_name"),
                rs.getString("source_collection"),
                rs.getInt("error_id"),
                rs.getString("error_type"),
                rs.getString("error_message"),
                rs.getLong("record_count")));
    }
}
