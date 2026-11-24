package com.tcs.dmj.dwh.repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tcs.dmj.dwh.domain.RejectedRecord;
import com.tcs.dmj.dwh.service.dto.RejectedRecordSummaryDTO;
import com.tcs.dmj.dwh.validation.QualityColumns;
import org.apache.spark.sql.Row;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * SQL for meta.meta_rejected_record and meta.meta_error_master.
 *
 *   meta_error_master    one row per possible error (error_type + error_message), e.g.
 *                        12 | VALIDATION | amount | amount must be greater than 0
 *   meta_rejected_record ONE row per rejected record, with ALL its failed rules:
 *                        error_ids      {12,19}                       -&gt; meta_error_master.error_id (sorted)
 *                        error_messages "amount must be ..., tin ..." (same order, comma separated)
 *                        field_values   {"amount": "-5", "tin": "X1"} (the bad value per checked field)
 *
 * The same record rejected again: its row is overwritten with the CURRENT errors (a partly fixed record keeps
 * only the rules it still breaks) and attempt_count + 1. A record that becomes valid keeps its row as history.
 */
@Repository
public class RejectedRecordRepository {

    private static final TypeReference<Map<String, String>> FIELD_VALUES_TYPE = new TypeReference<>() {
    };
    private static final String MESSAGE_SEPARATOR = ", ";
    private static final int BATCH = 1000;

    private final JdbcTemplate jdbc;
    private final TransactionTemplate transaction;
    private final ObjectMapper objectMapper;
    private final RowMapper<RejectedRecord> rowMapper;

    public RejectedRecordRepository(JdbcTemplate jdbc, TransactionTemplate transaction, ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.transaction = transaction;
        this.objectMapper = objectMapper;
        this.rowMapper = (rs, rowNum) -> new RejectedRecord(
                rs.getLong("rejected_record_id"),
                rs.getString("pipeline_name"),
                rs.getString("source_collection"),
                rs.getString("source_id"),
                rs.getString("business_key"),
                errorIds(rs),
                rs.getString("error_messages"),
                fieldValues(rs.getString("field_values")),
                rs.getString("raw_record"),
                rs.getInt("attempt_count"),
                rs.getLong("first_job_run_id"),
                rs.getLong("last_job_run_id"),
                rs.getObject("first_rejected_at", LocalDateTime.class),
                rs.getObject("last_rejected_at", LocalDateTime.class));
    }

    /**
     * Saves the rejected records of one collection, directly (no staging table). The rows come straight from Spark,
     * one row per record + error (QualityColumns.oneRowPerError): source_id, business_key, error_type, field_name,
     * error_message, field_value, raw_record. Rejected rows are few, so they are grouped per record in driver
     * memory. In ONE transaction:
     *
     *   1. an error message that is not in meta_error_master yet (a rule added later) gets its row there
     *   2. one row per record -&gt; INSERT, or UPDATE of the existing row of that record
     */
    public void save(Iterator<Row> errorRows, String pipelineName, String collection, long pipelineRunId) {
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
        List<Map<String, Row>> records = new ArrayList<>(errorsBySource.values());

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

            // 2. one row per record: its error ids / messages are looked up in the master, sorted by error_id
            jdbc.batchUpdate("""
                    WITH errors AS (
                        SELECT e.error_id, e.error_message
                          FROM meta.meta_error_master e
                         WHERE e.error_type || '|' || e.error_message = ANY (?)
                    )
                    INSERT INTO meta.meta_rejected_record
                           (pipeline_name, source_collection, source_id, business_key, error_ids, error_messages,
                            field_values, raw_record, attempt_count, first_job_run_id, last_job_run_id,
                            first_rejected_at, last_rejected_at)
                    SELECT ?, ?, ?, ?,
                           (SELECT ARRAY_AGG(error_id ORDER BY error_id) FROM errors),
                           (SELECT STRING_AGG(error_message, '%s' ORDER BY error_id) FROM errors),
                           CAST(? AS JSONB), CAST(? AS JSONB), 1, ?, ?, now(), now()
                    %s
                    """.formatted(MESSAGE_SEPARATOR, ON_CONFLICT_UPDATE), records, BATCH, (ps, errors) -> {
                Row first = errors.values().iterator().next();
                ps.setArray(1, ps.getConnection().createArrayOf("text", errors.keySet().toArray()));
                ps.setString(2, pipelineName);
                ps.setString(3, collection);
                ps.setString(4, first.getAs(QualityColumns.REJECTED_SOURCE_ID));
                ps.setString(5, first.getAs(QualityColumns.REJECTED_BUSINESS_KEY));
                ps.setString(6, fieldValuesJson(errors.values()));
                ps.setString(7, first.getAs(QualityColumns.REJECTED_RAW_RECORD));
                ps.setLong(8, pipelineRunId);
                ps.setLong(9, pipelineRunId);
            });
        });
    }

    /** The same record again: overwrite its errors with the current ones, count the attempt. */
    private static final String ON_CONFLICT_UPDATE = """
            ON CONFLICT (pipeline_name, source_collection, source_id) DO UPDATE
               SET business_key     = EXCLUDED.business_key,
                   error_ids        = EXCLUDED.error_ids,
                   error_messages   = EXCLUDED.error_messages,
                   field_values     = EXCLUDED.field_values,
                   raw_record       = EXCLUDED.raw_record,
                   attempt_count    = meta.meta_rejected_record.attempt_count + 1,
                   last_job_run_id  = EXCLUDED.last_job_run_id,
                   last_rejected_at = now()""";

    private static String errorKey(String errorType, String errorMessage) {
        return errorType + "|" + errorMessage;
    }

    /** {"field": "bad value", ...} of the errors that name a field; null when none does. */
    private String fieldValuesJson(Iterable<Row> errors) {
        Map<String, String> values = new LinkedHashMap<>();
        for (Row error : errors) {
            String field = error.getAs(QualityColumns.FIELD_NAME);
            if (field != null) {
                values.putIfAbsent(field, error.getAs(QualityColumns.FIELD_VALUE));
            }
        }
        if (values.isEmpty()) {
            return null;
        }
        try {
            return objectMapper.writeValueAsString(values);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Could not write field_values as JSON", e);
        }
    }

    private Map<String, String> fieldValues(String json) {
        if (json == null) {
            return Map.of();
        }
        try {
            return objectMapper.readValue(json, FIELD_VALUES_TYPE);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("field_values is not a JSON object: " + json, e);
        }
    }

    private static List<Integer> errorIds(ResultSet rs) throws SQLException {
        Array array = rs.getArray("error_ids");
        return array == null ? List.of() : Arrays.asList((Integer[]) array.getArray());
    }

    /**
     * All filters are optional (null = no filter).
     *
     * @param errorType records with at least ONE error of this type
     */
    public List<RejectedRecord> find(String pipelineName, String collection, String errorType, int limit) {
        StringBuilder sql = new StringBuilder("SELECT r.* FROM meta.meta_rejected_record r WHERE 1 = 1");
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
            sql.append(" AND EXISTS (SELECT 1 FROM meta.meta_error_master e")
                    .append(" WHERE e.error_id = ANY (r.error_ids) AND e.error_type = ?)");
            params.add(errorType.toUpperCase());
        }
        sql.append(" ORDER BY r.pipeline_name, r.source_collection, r.business_key, r.source_id LIMIT ?");
        params.add(limit);
        return jdbc.query(sql.toString(), rowMapper, params.toArray());
    }

    /** Per pipeline / collection / error: how many rejected records have that error. */
    public List<RejectedRecordSummaryDTO> summary() {
        return jdbc.query("""
                SELECT r.pipeline_name, r.source_collection, e.error_id, e.error_type, e.error_message,
                       COUNT(*) AS record_count
                  FROM meta.meta_rejected_record r
                 CROSS JOIN LATERAL UNNEST(r.error_ids) AS u(error_id)
                  JOIN meta.meta_error_master e ON e.error_id = u.error_id
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
