package com.tcs.dmj.dwh.repository;

import com.tcs.dmj.dwh.domain.RejectedRecord;
import com.tcs.dmj.dwh.service.dto.RejectedRecordSummaryDTO;
import com.tcs.dmj.dwh.validation.QualityColumns;
import org.apache.spark.sql.Row;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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

    private static final int BATCH = 1000;

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
     * Same three steps as {@link #saveFromStaging}, WITHOUT a staging table (used by pipelines with directLoad()).
     * The rows come straight from Spark, one row per record + error (QualityColumns.oneRowPerError):
     * source_id, business_key, error_type, field_name, error_message, field_value, raw_record.
     *
     *   1. every error message not in meta_error_master yet is inserted there (a rule added later in the code)
     *   2. errors a record does NOT have any more (it was partly fixed) are removed
     *   3. new record + error -> INSERT; same record + error again -> UPDATE value / raw record, attempt_count + 1
     *
     * Runs in the CALLER's transaction (the job's all-or-nothing transaction); alone it gets its own.
     * Rejected rows are few, so they are grouped per record in driver memory.
     */
    public void saveDirect(Iterator<Row> errorRows, String pipelineName, String collection, long jobRunId) {
        // source_id -> its errors (type|message -> row)
        Map<String, Map<String, Row>> errorsBySource = new LinkedHashMap<>();
        Map<String, Row> distinctErrors = new LinkedHashMap<>();
        errorRows.forEachRemaining(row -> {
            String errorKey = errorKey(row.getAs(QualityColumns.ERROR_TYPE), row.getAs(QualityColumns.ERROR_MESSAGE));
            errorsBySource.computeIfAbsent(row.getAs(QualityColumns.REJECTED_SOURCE_ID), id -> new LinkedHashMap<>())
                    .putIfAbsent(errorKey, row);
            distinctErrors.putIfAbsent(errorKey, row);
        });
        if (errorsBySource.isEmpty()) {
            return;
        }
        List<Row> errorRowsToSave = errorsBySource.values().stream().flatMap(errors -> errors.values().stream()).toList();
        List<Map.Entry<String, Map<String, Row>>> records = new ArrayList<>(errorsBySource.entrySet());

        transaction.executeWithoutResult(status -> {
            // 1. new error messages -> master (NOT EXISTS first, so no error_id is used up for known errors)
            jdbc.batchUpdate("""
                    INSERT INTO meta.meta_error_master (error_type, field_name, error_message)
                    SELECT ?, ?, ?
                     WHERE NOT EXISTS (SELECT 1 FROM meta.meta_error_master WHERE error_type = ? AND error_message = ?)
                    ON CONFLICT (error_type, error_message) DO NOTHING
                    """, new ArrayList<>(distinctErrors.values()), BATCH, (ps, row) -> {
                ps.setString(1, row.getAs(QualityColumns.ERROR_TYPE));
                ps.setString(2, row.getAs(QualityColumns.FIELD_NAME));
                ps.setString(3, row.getAs(QualityColumns.ERROR_MESSAGE));
                ps.setString(4, row.getAs(QualityColumns.ERROR_TYPE));
                ps.setString(5, row.getAs(QualityColumns.ERROR_MESSAGE));
            });

            // 2. errors a record does not have any more
            jdbc.batchUpdate("""
                    DELETE FROM meta.meta_rejected_record r
                     WHERE r.pipeline_name = ? AND r.source_collection = ? AND r.source_id = ?
                       AND r.error_id NOT IN (SELECT e.error_id FROM meta.meta_error_master e
                                               WHERE e.error_type || '|' || e.error_message = ANY (?))
                    """, records, BATCH, (ps, record) -> {
                ps.setString(1, pipelineName);
                ps.setString(2, collection);
                ps.setString(3, record.getKey());
                ps.setArray(4, ps.getConnection().createArrayOf("text", record.getValue().keySet().toArray()));
            });

            // 3. upsert one row per record + error
            jdbc.batchUpdate("""
                    INSERT INTO meta.meta_rejected_record
                           (pipeline_name, source_collection, source_id, business_key, error_id, field_value,
                            raw_record, attempt_count, first_job_run_id, last_job_run_id, first_rejected_at, last_rejected_at)
                    SELECT ?, ?, ?, ?, e.error_id, ?, CAST(? AS JSONB), 1, ?, ?, now(), now()
                      FROM meta.meta_error_master e
                     WHERE e.error_type = ? AND e.error_message = ?
                    ON CONFLICT (pipeline_name, source_collection, source_id, error_id) DO UPDATE
                       SET business_key     = EXCLUDED.business_key,
                           field_value      = EXCLUDED.field_value,
                           raw_record       = EXCLUDED.raw_record,
                           attempt_count    = meta.meta_rejected_record.attempt_count + 1,
                           last_job_run_id  = EXCLUDED.last_job_run_id,
                           last_rejected_at = now()
                    """, errorRowsToSave, BATCH, (ps, row) -> {
                ps.setString(1, pipelineName);
                ps.setString(2, collection);
                ps.setString(3, row.getAs(QualityColumns.REJECTED_SOURCE_ID));
                ps.setString(4, row.getAs(QualityColumns.REJECTED_BUSINESS_KEY));
                ps.setString(5, row.getAs(QualityColumns.FIELD_VALUE));
                ps.setString(6, row.getAs(QualityColumns.REJECTED_RAW_RECORD));
                ps.setLong(7, jobRunId);
                ps.setLong(8, jobRunId);
                ps.setString(9, row.getAs(QualityColumns.ERROR_TYPE));
                ps.setString(10, row.getAs(QualityColumns.ERROR_MESSAGE));
            });
        });
    }

    private static String errorKey(String errorType, String errorMessage) {
        return errorType + "|" + errorMessage;
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

    public List<RejectedRecordSummaryDTO> summary() {
        return jdbc.query("""
                SELECT r.pipeline_name, r.source_collection, e.error_id, e.error_type, e.error_message,
                       COUNT(DISTINCT r.source_id) AS record_count
                  FROM meta.meta_rejected_record r
                  JOIN meta.meta_error_master e ON e.error_id = r.error_id
                 GROUP BY r.pipeline_name, r.source_collection, e.error_id, e.error_type, e.error_message
                 ORDER BY r.pipeline_name, r.source_collection, e.error_type, e.error_id
                """, (rs, rowNum) -> new RejectedRecordSummaryDTO(
                rs.getString("pipeline_name"),
                rs.getString("source_collection"),
                rs.getInt("error_id"),
                rs.getString("error_type"),
                rs.getString("error_message"),
                rs.getLong("record_count")));
    }
}
