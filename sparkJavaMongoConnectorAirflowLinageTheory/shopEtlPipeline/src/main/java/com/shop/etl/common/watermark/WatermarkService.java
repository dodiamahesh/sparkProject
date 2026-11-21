package com.shop.etl.common.watermark;

import com.shop.etl.common.config.EtlProperties;
import com.shop.etl.common.io.MongoSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * {@code meta.etl_watermark} sambhale che: "darek job e darek collection kya sudhi vanchi".
 *
 * Design doc Section 3.3 na niyam:
 *   1. last_watermark NULL → FULL LOAD
 *   2. Bije vakhte: field > last_watermark AND <= window end
 *   3. Job SUCCESS thay pachhi j watermark aagal vadhe (commit)
 *   4. Darek job no potano watermark (orders: Job-2 ni row alag, Job-4 ni nathi)
 *
 * Fail thay to? {@link #markFailed} fakt status FAILED kare, last_watermark NATHI badlatu.
 * Etle aaglo run e j juni jagyae thi shuru thay ane koi data gumatu nathi (doc 11.7).
 */
@Service
public class WatermarkService {

    private final JdbcTemplate jdbc;
    private final String table;

    public WatermarkService(JdbcTemplate jdbc, EtlProperties properties) {
        this.jdbc = jdbc;
        this.table = properties.postgres().schema().meta() + ".etl_watermark";
    }

    /**
     * Job na darek source ni window banave che: start = last_watermark, end = windowEnd.
     * Source ni row table ma na hoy to NULL watermark sathe banave (navo source = full load).
     *
     * @return source naam → window (sources na order ma)
     */
    public Map<String, LoadWindow> windows(String jobName, List<MongoSource> sources, Instant windowEnd) {
        Map<String, LoadWindow> windows = new LinkedHashMap<>();
        for (MongoSource source : sources) {
            jdbc.update("INSERT INTO " + table + " (job_name, source_collection, watermark_column, last_watermark) "
                            + "VALUES (?, ?, ?, NULL) ON CONFLICT (job_name, source_collection) DO NOTHING",
                    jobName, source.name(), source.watermarkField());

            List<OffsetDateTime> last = jdbc.queryForList(
                    "SELECT last_watermark FROM " + table + " WHERE job_name = ? AND source_collection = ?",
                    OffsetDateTime.class, jobName, source.name());
            Instant start = (last.isEmpty() || last.get(0) == null) ? null : last.get(0).toInstant();
            windows.put(source.name(), new LoadWindow(start, windowEnd));
        }
        return windows;
    }

    /**
     * Job SUCCESS thayo: badha sources no watermark = window end.
     * Aa method JobRunner ni transaction ma chale che (audit SUCCESS sathe), etle banne sathe
     * thay ke ek pan nahi.
     */
    public void commit(String jobName, Iterable<String> sources, Instant windowEnd, String runId) {
        for (String source : sources) {
            jdbc.update("UPDATE " + table + " SET last_watermark = ?, last_run_id = ?, last_status = 'SUCCESS', "
                            + "updated_at = now() WHERE job_name = ? AND source_collection = ?",
                    Timestamp.from(windowEnd), runId, jobName, source);
        }
    }

    /** Job FAILED: fakt status. last_watermark same rahe (re-run e j window thi). */
    public void markFailed(String jobName, String runId) {
        jdbc.update("UPDATE " + table + " SET last_run_id = ?, last_status = 'FAILED', updated_at = now() "
                + "WHERE job_name = ?", runId, jobName);
    }

    /** Job na badha watermarks (API GET /api/jobs mate). */
    public List<Map<String, Object>> findAll(String jobName) {
        return jdbc.queryForList("SELECT source_collection, watermark_column, last_watermark, last_run_id, last_status "
                + "FROM " + table + " WHERE job_name = ? ORDER BY source_collection", jobName);
    }
}
