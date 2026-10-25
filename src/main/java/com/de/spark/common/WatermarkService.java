package com.de.spark.common;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Remembers how far each job has processed (de_silver.etl_watermark, epoch milliseconds).
 * The next run only reads data newer than this value, so old data is never processed again.
 */
@Service
public class WatermarkService {

    private final JdbcTemplate silverJdbc;

    public WatermarkService(JdbcTemplate silverJdbc) {
        this.silverJdbc = silverJdbc;
    }

    /** Last processed value; 0 (= 1970-01-01) on the very first run. */
    public long get(String jobName) {
        List<Long> v = silverJdbc.queryForList(
                "SELECT watermark_value FROM etl_watermark WHERE job_name = ?", Long.class, jobName);
        return v.isEmpty() ? 0L : v.get(0);
    }

    /**
     * Saves the new watermark. Call it inside the same transaction as the data load,
     * so the watermark only moves forward when the data is really committed.
     */
    public void set(String jobName, long value) {
        set(silverJdbc, jobName, value);
    }

    public void set(JdbcTemplate jdbc, String jobName, long value) {
        jdbc.update("INSERT INTO etl_watermark (job_name, watermark_value, updated_at) VALUES (?, ?, NOW(3)) AS n"
                + " ON DUPLICATE KEY UPDATE watermark_value = n.watermark_value, updated_at = n.updated_at", jobName, value);
    }
}
