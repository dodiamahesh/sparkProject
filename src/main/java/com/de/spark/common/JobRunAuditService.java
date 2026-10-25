package com.de.spark.common;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;

/** Writes one row per job run into de_silver.etl_job_run (what ran, when, how many rows, errors). */
@Service
public class JobRunAuditService {

    private final JdbcTemplate silverJdbc;

    public JobRunAuditService(JdbcTemplate silverJdbc) {
        this.silverJdbc = silverJdbc;
    }

    /** Inserts a RUNNING row and returns its run id (also used as batch_id in the data tables). */
    public String start(String jobName, long watermarkFrom, long watermarkTo) {
        String runId = UUID.randomUUID().toString();
        silverJdbc.update("INSERT INTO etl_job_run (run_id, job_name, status, started_at, watermark_from, watermark_to)"
                + " VALUES (?, ?, 'RUNNING', NOW(3), ?, ?)", runId, jobName, watermarkFrom, watermarkTo);
        return runId;
    }

    public void success(String runId, long rowsRead, long rowsWritten, long rowsRejected) {
        silverJdbc.update("UPDATE etl_job_run SET status = 'SUCCESS', finished_at = NOW(3),"
                + " rows_read = ?, rows_written = ?, rows_rejected = ? WHERE run_id = ?",
                rowsRead, rowsWritten, rowsRejected, runId);
    }

    public void failed(String runId, Exception e) {
        String msg = String.valueOf(e.getMessage());
        silverJdbc.update("UPDATE etl_job_run SET status = 'FAILED', finished_at = NOW(3), error_message = ? WHERE run_id = ?",
                msg.length() > 1000 ? msg.substring(0, 1000) : msg, runId);
    }

    /** Records that a job was not started (e.g. Job #2 because Job #1 failed). */
    public void skipped(String jobName, String reason) {
        silverJdbc.update("INSERT INTO etl_job_run (run_id, job_name, status, started_at, finished_at, error_message)"
                + " VALUES (?, ?, 'SKIPPED', NOW(3), NOW(3), ?)", UUID.randomUUID().toString(), jobName, reason);
    }
}
