package com.tcs.dmj.dwh.repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tcs.dmj.dwh.domain.RejectedRecord;
import com.tcs.dmj.dwh.exception.EtlException;
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
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * meta.meta_rejected_record (one row per rejected record: error_ids[], error_messages, field_values)
 * and meta.meta_error_master (one row per possible error). A record rejected again is overwritten with
 * its current errors and attempt_count + 1.
 */
@Repository
public class RejectedRecordRepository {

    private static final TypeReference<Map<String, String>> FIELD_VALUES_TYPE = new TypeReference<>() {
    };
    private static final String MESSAGE_SEPARATOR = ", ";
    private static final int BATCH = 1000;

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

    // NOT EXISTS first: ON CONFLICT alone would use up an error_id for every known error on every run
    private static final String NEW_ERRORS_FROM_STAGING = """
            INSERT INTO meta.meta_error_master (error_type, field_name, error_message)
            SELECT DISTINCT ON (s.error_type, s.error_message) s.error_type, s.field_name, s.error_message
              FROM %s s
             WHERE NOT EXISTS (SELECT 1 FROM meta.meta_error_master e
                                WHERE e.error_type = s.error_type AND e.error_message = s.error_message)
            ON CONFLICT (error_type, error_message) DO NOTHING
            """;

    private static final String RECORDS_FROM_STAGING = """
            WITH errors AS (
                SELECT DISTINCT ON (s.source_id, e.error_id)
                       s.source_id, s.business_key, e.error_id, e.error_message, s.field_name, s.field_value,
                       s.raw_record
                  FROM %s s
                  JOIN meta.meta_error_master e
                    ON e.error_type = s.error_type AND e.error_message = s.error_message
                 ORDER BY s.source_id, e.error_id
            ), records AS (
                SELECT source_id,
                       MAX(business_key)                                   AS business_key,
                       ARRAY_AGG(error_id ORDER BY error_id)               AS error_ids,
                       STRING_AGG(error_message, '%s' ORDER BY error_id)  AS error_messages,
                       JSONB_OBJECT_AGG(field_name, field_value)
                           FILTER (WHERE field_name IS NOT NULL)          AS field_values,
                       MAX(raw_record)                                     AS raw_record
                  FROM errors
                 GROUP BY source_id
            )
            INSERT INTO meta.meta_rejected_record
                   (pipeline_name, source_collection, source_id, business_key, error_ids, error_messages,
                    field_values, raw_record, attempt_count, first_job_run_id, last_job_run_id,
                    first_rejected_at, last_rejected_at)
            SELECT ?, ?, source_id, business_key, error_ids, error_messages, field_values,
                   CAST(raw_record AS JSONB), 1, ?, ?, now(), now()
              FROM records
            %s
            """;

    private static final String NEW_ERROR = """
            INSERT INTO meta.meta_error_master (error_type, field_name, error_message)
            SELECT ?, ?, ?
             WHERE NOT EXISTS (SELECT 1 FROM meta.meta_error_master WHERE error_type = ? AND error_message = ?)
            ON CONFLICT (error_type, error_message) DO NOTHING
            """;

    private static final String ONE_RECORD = """
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
            """;

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

    // ---------------------------------------------------------------- save

    /** Saves rejects from a staging table with one row per record + error (QualityColumns.oneRowPerError). */
    public void saveFromStaging(String stagingTable, String pipelineName, String collection, long pipelineRunId) {
        transaction.executeWithoutResult(status -> {
            jdbc.update(NEW_ERRORS_FROM_STAGING.formatted(stagingTable));
            jdbc.update(RECORDS_FROM_STAGING.formatted(stagingTable, MESSAGE_SEPARATOR, ON_CONFLICT_UPDATE),
                    pipelineName, collection, pipelineRunId, pipelineRunId);
        });
    }

    /**
     * Saves rejects without staging (directLoad pipelines), in the caller's transaction.
     * Rows: one per record + error (QualityColumns.oneRowPerError); they are few, so grouped in memory.
     */
    public void saveDirect(Iterator<Row> errorRows, String pipelineName, String collection, long pipelineRunId) {
        Map<String, Row> distinctErrors = new LinkedHashMap<>();
        List<Map<String, Row>> records = groupErrorsByRecord(errorRows, distinctErrors);
        if (records.isEmpty()) {
            return;
        }
        transaction.executeWithoutResult(status -> {
            insertNewErrors(distinctErrors.values());
            upsertRecords(records, pipelineName, collection, pipelineRunId);
        });
    }

    /** @return per record: its errors (type|message -&gt; row); distinctErrors gets every distinct error */
    private static List<Map<String, Row>> groupErrorsByRecord(Iterator<Row> errorRows, Map<String, Row> distinctErrors) {
        Map<String, Map<String, Row>> errorsBySource = new LinkedHashMap<>();
        errorRows.forEachRemaining(row -> {
            String errorKey = errorKey(row.getAs(QualityColumns.ERROR_TYPE), row.getAs(QualityColumns.ERROR_MESSAGE));
            errorsBySource.computeIfAbsent(row.getAs(QualityColumns.REJECTED_SOURCE_ID), id -> new LinkedHashMap<>())
                    .putIfAbsent(errorKey, row);
            distinctErrors.putIfAbsent(errorKey, row);
        });
        return new ArrayList<>(errorsBySource.values());
    }

    private void insertNewErrors(Collection<Row> errors) {
        jdbc.batchUpdate(NEW_ERROR, new ArrayList<>(errors), BATCH, (ps, row) -> {
            ps.setString(1, row.getAs(QualityColumns.ERROR_TYPE));
            ps.setString(2, row.getAs(QualityColumns.FIELD_NAME));
            ps.setString(3, row.getAs(QualityColumns.ERROR_MESSAGE));
            ps.setString(4, row.getAs(QualityColumns.ERROR_TYPE));
            ps.setString(5, row.getAs(QualityColumns.ERROR_MESSAGE));
        });
    }

    /** One row per record; its error ids / messages are looked up in the master, sorted by error_id. */
    private void upsertRecords(List<Map<String, Row>> records, String pipelineName, String collection,
                               long pipelineRunId) {
        String sql = ONE_RECORD.formatted(MESSAGE_SEPARATOR, ON_CONFLICT_UPDATE);
        jdbc.batchUpdate(sql, records, BATCH, (ps, errors) -> {
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
    }

    // ---------------------------------------------------------------- read

    /** Filters are optional (null = no filter). errorType = records with at least one error of that type. */
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

    /** Number of rejected records per pipeline / collection / error. */
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

    // ---------------------------------------------------------------- helpers

    private static String errorKey(String errorType, String errorMessage) {
        return errorType + "|" + errorMessage;
    }

    /** {"field": "bad value"} of the errors that name a field; null when none does. */
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
            throw new EtlException("Could not write field_values as JSON", e);
        }
    }

    private Map<String, String> fieldValues(String json) {
        if (json == null) {
            return Map.of();
        }
        try {
            return objectMapper.readValue(json, FIELD_VALUES_TYPE);
        } catch (JsonProcessingException e) {
            throw new EtlException("field_values is not a JSON object: " + json, e);
        }
    }

    private static List<Integer> errorIds(ResultSet rs) throws SQLException {
        Array array = rs.getArray("error_ids");
        return array == null ? List.of() : Arrays.asList((Integer[]) array.getArray());
    }
}
