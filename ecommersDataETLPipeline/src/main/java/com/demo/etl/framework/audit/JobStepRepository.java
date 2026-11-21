package com.demo.etl.framework.audit;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.LocalDateTime;
import java.util.List;

/**
 * SQL for meta.meta_pipeline_step.
 *
 * Every write runs in its OWN short transaction (REQUIRES_NEW): a pipeline with directLoad() runs inside one big
 * all-or-nothing transaction, but its steps must be visible while it runs, and a FAILED step must stay saved
 * when that big transaction is rolled back.
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
    private final TransactionTemplate ownTransaction;

    public JobStepRepository(JdbcTemplate jdbc, PlatformTransactionManager transactionManager) {
        this.jdbc = jdbc;
        this.ownTransaction = new TransactionTemplate(transactionManager);
        this.ownTransaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    /** Inserts the row of a new step (status NULL = still running) and returns its id. */
    public long startStep(long jobRunId, int stepOrder, String stepName) {
        return ownTransaction.execute(status -> jdbc.queryForObject("""
                INSERT INTO meta.meta_pipeline_step (job_run_id, step_order, step_name, start_time)
                VALUES (?, ?, ?, clock_timestamp())
                RETURNING job_step_id
                """, Long.class, jobRunId, stepOrder, stepName));
    }

    public void finishStep(long jobStepId, StepStatus stepStatus, Long recordCount, String errorMessage) {
        ownTransaction.executeWithoutResult(status -> jdbc.update("""
                UPDATE meta.meta_pipeline_step
                   SET status        = ?,
                       record_count  = ?,
                       end_time      = clock_timestamp(),
                       duration_ms   = (EXTRACT(EPOCH FROM (clock_timestamp() - start_time)) * 1000)::BIGINT,
                       error_message = ?
                 WHERE job_step_id = ?
                """, stepStatus.name(), recordCount, errorMessage, jobStepId));
    }

    public List<JobStep> findByJobRunId(long jobRunId) {
        return jdbc.query("SELECT * FROM meta.meta_pipeline_step WHERE job_run_id = ? ORDER BY step_order",
                ROW_MAPPER, jobRunId);
    }
}
