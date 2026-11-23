package com.tcs.dmj.dwh.repository;

import com.tcs.dmj.dwh.domain.MetaPipelineRun;
import com.tcs.dmj.dwh.enums.PipelineStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * SQL for meta.meta_pipeline_run (plain JdbcTemplate: easy to read, no magic).
 */
@Repository
public class MetaPipelineRunRepository {

    private static final int MAX_ERROR_CODE_LENGTH = 100;
    private static final int MAX_ERROR_MESSAGE_LENGTH = 1000;

    private static final String SELECT = """
            SELECT r.*, p.pipeline_name
              FROM meta.meta_pipeline_run r
              JOIN meta.meta_pipeline p ON p.pipeline_id = r.pipeline_id
            """;

    private static final RowMapper<MetaPipelineRun> ROW_MAPPER = (rs, rowNum) -> new MetaPipelineRun(
            rs.getLong("pipeline_run_id"),
            rs.getLong("pipeline_id"),
            rs.getString("pipeline_name"),
            rs.getObject("last_run_time", OffsetDateTime.class),
            rs.getObject("start_time", OffsetDateTime.class),
            rs.getObject("end_time", OffsetDateTime.class),
            rs.getString("pipeline_status"),
            rs.getObject("total_records", Long.class),
            rs.getObject("success_records", Long.class),
            rs.getObject("failed_records", Long.class),
            rs.getString("error_code"),
            rs.getString("error_message"),
            rs.getObject("created_at", OffsetDateTime.class),
            rs.getObject("updated_at", OffsetDateTime.class));

    private final JdbcTemplate jdbc;

    public MetaPipelineRunRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Inserts the row of a new run (status RUNNING) and returns the new pipeline_run_id.
     *
     * @param windowEnd the run reads data up to this time (stored as last_run_time of the run)
     */
    public long createRun(String pipelineName, Instant windowEnd) {
        List<Long> ids = jdbc.queryForList("""
                INSERT INTO meta.meta_pipeline_run (pipeline_id, pipeline_status, last_run_time, start_time)
                SELECT pipeline_id, ?, ?, clock_timestamp()
                  FROM meta.meta_pipeline
                 WHERE pipeline_name = ?
                RETURNING pipeline_run_id
                """, Long.class, PipelineStatus.RUNNING.name(), OffsetDateTime.ofInstant(windowEnd, ZoneOffset.UTC),
                pipelineName);
        if (ids.isEmpty()) {
            throw new IllegalStateException("Pipeline " + pipelineName + " is not registered in meta.meta_pipeline");
        }
        return ids.get(0);
    }

    /**
     * @param totalRecords   records read from the source
     * @param successRecords records handled successfully (valid + soft-deleted)
     * @param failedRecords  records rejected
     */
    public void finishRun(long pipelineRunId, PipelineStatus status, long totalRecords, long successRecords,
                          long failedRecords, String errorCode, String errorMessage) {
        jdbc.update("""
                UPDATE meta.meta_pipeline_run
                   SET pipeline_status = ?,
                       end_time        = clock_timestamp(),
                       total_records   = ?,
                       success_records = ?,
                       failed_records  = ?,
                       error_code      = ?,
                       error_message   = ?
                 WHERE pipeline_run_id = ?
                """,
                status.name(), totalRecords, successRecords, failedRecords,
                truncate(errorCode, MAX_ERROR_CODE_LENGTH), truncate(errorMessage, MAX_ERROR_MESSAGE_LENGTH),
                pipelineRunId);
    }

    public Optional<MetaPipelineRun> findById(long pipelineRunId) {
        return jdbc.query(SELECT + " WHERE r.pipeline_run_id = ?", ROW_MAPPER, pipelineRunId).stream().findFirst();
    }

    /** Latest runs first. Both filters are optional (null = no filter). */
    public List<MetaPipelineRun> findRecent(String pipelineName, String status, int limit) {
        StringBuilder sql = new StringBuilder(SELECT).append(" WHERE 1 = 1");
        List<Object> params = new ArrayList<>();
        if (pipelineName != null) {
            sql.append(" AND p.pipeline_name = ?");
            params.add(pipelineName);
        }
        if (status != null) {
            sql.append(" AND r.pipeline_status = ?");
            params.add(status.toUpperCase());
        }
        sql.append(" ORDER BY r.pipeline_run_id DESC LIMIT ?");
        params.add(limit);
        return jdbc.query(sql.toString(), ROW_MAPPER, params.toArray());
    }

    /** Pipelines that have a run with status RUNNING (busy now, or left behind by a process that died). */
    public List<String> pipelinesWithRunningRuns() {
        return jdbc.queryForList("""
                SELECT DISTINCT p.pipeline_name
                  FROM meta.meta_pipeline_run r
                  JOIN meta.meta_pipeline p ON p.pipeline_id = r.pipeline_id
                 WHERE r.pipeline_status = ?
                """, String.class, PipelineStatus.RUNNING.name());
    }

    public boolean isRunning(String pipelineName) {
        return Boolean.TRUE.equals(jdbc.queryForObject("""
                SELECT EXISTS (SELECT 1 FROM meta.meta_pipeline_run r
                                 JOIN meta.meta_pipeline p ON p.pipeline_id = r.pipeline_id
                                WHERE p.pipeline_name = ? AND r.pipeline_status = ?)
                """, Boolean.class, pipelineName, PipelineStatus.RUNNING.name()));
    }

    /**
     * Runs of this pipeline that stayed RUNNING because their process died: they become FAILED.
     * Call it ONLY while holding the pipeline's run lock (then no live run of this pipeline exists anywhere).
     *
     * @return number of runs closed
     */
    public int failInterruptedRuns(String pipelineName) {
        return jdbc.update("""
                UPDATE meta.meta_pipeline_run r
                   SET pipeline_status = ?, end_time = clock_timestamp(), error_code = 'INTERRUPTED',
                       error_message = 'The process stopped while the run was busy'
                  FROM meta.meta_pipeline p
                 WHERE p.pipeline_id = r.pipeline_id AND p.pipeline_name = ? AND r.pipeline_status = ?
                """, PipelineStatus.FAILED.name(), pipelineName, PipelineStatus.RUNNING.name());
    }

    private static String truncate(String text, int maxLength) {
        return text == null || text.length() <= maxLength ? text : text.substring(0, maxLength);
    }
}
