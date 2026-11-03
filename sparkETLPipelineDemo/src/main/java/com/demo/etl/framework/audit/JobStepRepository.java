package com.demo.etl.framework.audit;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

/**
 * SQL for etl_audit.etl_job_step.
 */
@Repository
public class JobStepRepository {

    private static final RowMapper<JobStep> ROW_MAPPER = (rs, rowNum) -> new JobStep(
            rs.getLong("job_step_id"),
            rs.getInt("step_order"),
            rs.getString("step_name"),
            rs.getString("status"),
            rs.getObject("record_count", Long.class),
            rs.getObject("start_time", LocalDateTime.class),
            rs.getObject("end_time", LocalDateTime.class),
            rs.getObject("duration_ms", Long.class),
            rs.getString("error_message"));

    private final JdbcTemplate jdbc;

    public JobStepRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public long startStep(long jobRunId, int stepOrder, String stepName) {
        return jdbc.queryForObject("""
                INSERT INTO etl_audit.etl_job_step (job_run_id, step_order, step_name, status, start_time)
                VALUES (?, ?, ?, ?, clock_timestamp())
                RETURNING job_step_id
                """, Long.class, jobRunId, stepOrder, stepName, StepStatus.RUNNING.name());
    }

    public void finishStep(long jobStepId, StepStatus status, Long recordCount, String errorMessage) {
        jdbc.update("""
                UPDATE etl_audit.etl_job_step
                   SET status        = ?,
                       record_count  = ?,
                       end_time      = clock_timestamp(),
                       duration_ms   = (EXTRACT(EPOCH FROM (clock_timestamp() - start_time)) * 1000)::BIGINT,
                       error_message = ?
                 WHERE job_step_id = ?
                """, status.name(), recordCount, errorMessage, jobStepId);
    }

    public List<JobStep> findByJobRunId(long jobRunId) {
        return jdbc.query("SELECT * FROM etl_audit.etl_job_step WHERE job_run_id = ? ORDER BY step_order",
                ROW_MAPPER, jobRunId);
    }
}
