package com.demo.etl.framework.audit;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

/**
 * SQL for etl_audit.etl_data_warning.
 */
@Repository
public class DataWarningRepository {

    private static final RowMapper<DataWarning> ROW_MAPPER = (rs, rowNum) -> new DataWarning(
            rs.getLong("data_warning_id"),
            rs.getLong("job_run_id"),
            rs.getString("pipeline_name"),
            rs.getString("source_collection"),
            rs.getString("warning_type"),
            rs.getString("source_id"),
            rs.getString("business_key"),
            rs.getString("warning_message"),
            rs.getObject("created_at", LocalDateTime.class));

    private final JdbcTemplate jdbc;

    public DataWarningRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** One collection-level warning (SCHEMA_DRIFT / VOLUME): no source_id / business_key. */
    public void insert(long jobRunId, String pipelineName, String collection, WarningType type, String message) {
        jdbc.update("""
                INSERT INTO etl_audit.etl_data_warning
                       (job_run_id, pipeline_name, source_collection, warning_type, warning_message)
                VALUES (?, ?, ?, ?, ?)
                """, jobRunId, pipelineName, collection, type.name(), message);
    }

    /**
     * RULE warnings of many records at once. The staging table (written by Spark) has the columns
     * source_id, business_key, warning_message.
     */
    public int insertRuleWarningsFromStaging(String stagingTable, String pipelineName, String collection, long jobRunId) {
        return jdbc.update("""
                INSERT INTO etl_audit.etl_data_warning
                       (job_run_id, pipeline_name, source_collection, warning_type, source_id, business_key, warning_message)
                SELECT ?, ?, ?, ?, s.source_id, s.business_key, s.warning_message
                  FROM %s s
                """.formatted(stagingTable), jobRunId, pipelineName, collection, WarningType.RULE.name());
    }

    public List<DataWarning> findByJobRunId(long jobRunId) {
        return jdbc.query("""
                SELECT * FROM etl_audit.etl_data_warning
                 WHERE job_run_id = ?
                 ORDER BY source_collection, warning_type, business_key NULLS FIRST
                """, ROW_MAPPER, jobRunId);
    }
}
