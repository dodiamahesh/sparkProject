package com.demo.etl.framework.audit;

import com.demo.etl.framework.pipeline.JobMetrics;
import com.demo.etl.framework.pipeline.LoadType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * SQL for etl_audit.etl_job_run (plain JdbcTemplate: easy to read, no magic).
 */
@Repository
public class JobRunRepository {

    private static final RowMapper<JobRun> ROW_MAPPER = (rs, rowNum) -> new JobRun(
            rs.getLong("job_run_id"),
            rs.getString("pipeline_name"),
            rs.getString("load_type"),
            rs.getString("status"),
            rs.getString("triggered_by"),
            rs.getObject("start_time", LocalDateTime.class),
            rs.getObject("end_time", LocalDateTime.class),
            rs.getObject("duration_ms", Long.class),
            rs.getLong("source_count"),
            rs.getLong("valid_count"),
            rs.getLong("rejected_count"),
            rs.getLong("duplicate_count"),
            rs.getLong("loaded_count"),
            rs.getLong("resolved_count"),
            rs.getString("error_message"),
            rs.getString("error_detail"));

    private final JdbcTemplate jdbc;

    public JobRunRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Inserts a RUNNING row and returns the new job_run_id. */
    public long createRun(String pipelineName, LoadType loadType, String triggeredBy) {
        return jdbc.queryForObject("""
                INSERT INTO etl_audit.etl_job_run (pipeline_name, load_type, status, triggered_by, start_time)
                VALUES (?, ?, ?, ?, now())
                RETURNING job_run_id
                """, Long.class, pipelineName, loadType.name(), JobStatus.RUNNING.name(), triggeredBy);
    }

    public void finishRun(long jobRunId, JobStatus status, JobMetrics metrics, String errorMessage, String errorDetail) {
        jdbc.update("""
                UPDATE etl_audit.etl_job_run
                   SET status          = ?,
                       end_time        = clock_timestamp(),
                       duration_ms     = (EXTRACT(EPOCH FROM (clock_timestamp() - start_time)) * 1000)::BIGINT,
                       source_count    = ?,
                       valid_count     = ?,
                       rejected_count  = ?,
                       duplicate_count = ?,
                       loaded_count    = ?,
                       resolved_count  = ?,
                       error_message   = ?,
                       error_detail    = ?
                 WHERE job_run_id = ?
                """,
                status.name(),
                metrics.sourceCount(), metrics.validCount(), metrics.rejectedCount(),
                metrics.duplicateCount(), metrics.loadedCount(), metrics.resolvedCount(),
                errorMessage, errorDetail, jobRunId);
    }

    public Optional<JobRun> findById(long jobRunId) {
        return jdbc.query("SELECT * FROM etl_audit.etl_job_run WHERE job_run_id = ?", ROW_MAPPER, jobRunId)
                .stream().findFirst();
    }

    /** Latest runs first. Both filters are optional (null = no filter). */
    public List<JobRun> findRecent(String pipelineName, String status, int limit) {
        StringBuilder sql = new StringBuilder("SELECT * FROM etl_audit.etl_job_run WHERE 1 = 1");
        List<Object> params = new ArrayList<>();
        if (pipelineName != null) {
            sql.append(" AND pipeline_name = ?");
            params.add(pipelineName);
        }
        if (status != null) {
            sql.append(" AND status = ?");
            params.add(status.toUpperCase());
        }
        sql.append(" ORDER BY job_run_id DESC LIMIT ?");
        params.add(limit);
        return jdbc.query(sql.toString(), ROW_MAPPER, params.toArray());
    }

    /**
     * If the application was killed while a job was running, that job stays RUNNING forever.
     * Called at startup to close such jobs.
     */
    public int failInterruptedRuns() {
        return jdbc.update("""
                UPDATE etl_audit.etl_job_run
                   SET status = ?, end_time = now(),
                       error_message = 'Application stopped while the job was running'
                 WHERE status = ?
                """, JobStatus.FAILED.name(), JobStatus.RUNNING.name());
    }
}
