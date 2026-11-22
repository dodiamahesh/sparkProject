package com.demo.etl.framework.audit;

import com.demo.etl.framework.pipeline.JobMetrics;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * SQL for meta.meta_pipeline_run (plain JdbcTemplate: easy to read, no magic).
 */
@Repository
public class JobRunRepository {

    private static final RowMapper<JobRun> ROW_MAPPER = (rs, rowNum) -> new JobRun(
            rs.getLong("job_run_id"),
            rs.getString("pipeline_name"),
            rs.getObject("window_end", OffsetDateTime.class),
            rs.getString("status"),
            rs.getString("triggered_by"),
            rs.getObject("start_time", LocalDateTime.class),
            rs.getObject("end_time", LocalDateTime.class),
            rs.getObject("duration_ms", Long.class),
            rs.getLong("source_count"),
            rs.getLong("valid_count"),
            rs.getLong("rejected_count"),
            rs.getLong("duplicate_count"),
            rs.getLong("deleted_count"),
            rs.getLong("loaded_count"),
            rs.getString("error_message"),
            rs.getString("error_detail"));

    private final JdbcTemplate jdbc;

    public JobRunRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Inserts the row of a new run (status NULL = still running) and returns the new job_run_id. */
    public long createRun(String pipelineName, Instant windowEnd, String triggeredBy) {
        return jdbc.queryForObject("""
                INSERT INTO meta.meta_pipeline_run (pipeline_name, window_end, triggered_by, start_time)
                VALUES (?, ?, ?, now())
                RETURNING job_run_id
                """, Long.class, pipelineName, OffsetDateTime.ofInstant(windowEnd, ZoneOffset.UTC), triggeredBy);
    }

    public void finishRun(long jobRunId, JobStatus status, JobMetrics metrics, String errorMessage, String errorDetail) {
        jdbc.update("""
                UPDATE meta.meta_pipeline_run
                   SET status          = ?,
                       end_time        = clock_timestamp(),
                       duration_ms     = (EXTRACT(EPOCH FROM (clock_timestamp() - start_time)) * 1000)::BIGINT,
                       source_count    = ?,
                       valid_count     = ?,
                       rejected_count  = ?,
                       duplicate_count = ?,
                       deleted_count   = ?,
                       loaded_count    = ?,
                       error_message   = ?,
                       error_detail    = ?
                 WHERE job_run_id = ?
                """,
                status.name(),
                metrics.sourceCount(), metrics.validCount(), metrics.rejectedCount(),
                metrics.duplicateCount(), metrics.deletedCount(), metrics.loadedCount(),
                errorMessage, errorDetail, jobRunId);
    }

    public Optional<JobRun> findById(long jobRunId) {
        return jdbc.query("SELECT * FROM meta.meta_pipeline_run WHERE job_run_id = ?", ROW_MAPPER, jobRunId)
                .stream().findFirst();
    }

    /** Latest runs first. Both filters are optional (null = no filter). */
    public List<JobRun> findRecent(String pipelineName, String status, int limit) {
        StringBuilder sql = new StringBuilder("SELECT * FROM meta.meta_pipeline_run WHERE 1 = 1");
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
     * If the application was killed while a job was running, its row keeps status NULL forever.
     * Called at startup: such runs (and their unfinished steps) become FAILED.
     *
     * @return number of runs closed
     */
    public int failInterruptedRuns() {
        String message = "Interrupted: the application stopped while the job was running";
        jdbc.update("""
                UPDATE meta.meta_pipeline_step
                   SET status = ?, end_time = now(), error_message = ?
                 WHERE status IS NULL
                """, StepStatus.FAILED.name(), message);
        return jdbc.update("""
                UPDATE meta.meta_pipeline_run
                   SET status = ?, end_time = now(), error_message = ?
                 WHERE status IS NULL
                """, JobStatus.FAILED.name(), message);
    }
}
