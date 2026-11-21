package com.shop.etl.common.audit;

import com.shop.etl.common.config.EtlProperties;
import com.shop.etl.common.job.JobContext;
import com.shop.etl.common.watermark.LoadWindow;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * {@code meta.etl_run_audit} table sambhale che: darek run no hisab.
 *
 * Darek run ma rows:
 * <pre>
 *   run_id            source_name  status   total processed rejected partial inserted updated
 *   J2_20261021_0000  JOB          SUCCESS  70    60        10       2       ...      ...
 *   J2_20261021_0000  customers    SUCCESS  23    20        3        0       20       0
 *   J2_20261021_0000  orders       SUCCESS  24    20        4        2       30       0
 *   J2_20261021_0000  payments     SUCCESS  23    20        3        0       20       0
 * </pre>
 *
 * Lifecycle:
 *   startJob    → JOB row RUNNING (API ne turant runId male, ane status joi shakay)
 *   startSource → source row RUNNING (kai collection par atkyu e khabar pade)
 *   finishSource→ source row SUCCESS + counts  /  failSource → FAILED + error
 *   finishJob   → JOB row SUCCESS (sources no saravalo) ke FAILED + error
 *
 * Same run_id farithi chale (fail pachhi re-run) to rows UPSERT thay: juna counts reset.
 *
 * Kem JdbcTemplate (Spark nahi)? Aa nana, driver-side statements che. Spark job aa mate
 * ghanu bharu che.
 */
@Service
public class AuditService {

    /** JOB summary row nu source_name. */
    public static final String JOB_ROW = "JOB";

    private static final int MAX_ERROR_LENGTH = 2000;

    private final JdbcTemplate jdbc;
    private final String table;
    private final String silverSchema;

    public AuditService(JdbcTemplate jdbc, EtlProperties properties) {
        this.jdbc = jdbc;
        this.table = properties.postgres().schema().meta() + ".etl_run_audit";
        this.silverSchema = properties.postgres().schema().silver();
    }

    /** JOB row ne RUNNING banave (insert ke re-run hoy to reset). */
    public void startJob(JobContext ctx) {
        upsertRunning(ctx, JOB_ROW, ctx.loadType());
    }

    /** Source row ne RUNNING banave. */
    public void startSource(JobContext ctx, String source, LoadWindow window) {
        upsertRunning(ctx, source, window.loadType());
    }

    /** Source SUCCESS + counts. */
    public void finishSource(JobContext ctx, String source, SourceStats stats) {
        finish(ctx.runId(), source, "SUCCESS", stats, null);
    }

    /** Source FAILED + error message. Counts 0 rahe (adhura numbers bhram pade). */
    public void failSource(JobContext ctx, String source, Throwable error) {
        finish(ctx.runId(), source, "FAILED", SourceStats.empty(), message(error));
    }

    /** JOB row SUCCESS: badha sources no saravalo. */
    public void finishJob(JobContext ctx, SourceStats total) {
        finish(ctx.runId(), JOB_ROW, "SUCCESS", total, null);
    }

    /** JOB row FAILED + error. */
    public void failJob(JobContext ctx, Throwable error) {
        finish(ctx.runId(), JOB_ROW, "FAILED", SourceStats.empty(), message(error));
    }

    /**
     * Aa run e target table ma ketli rows INSERT ane ketli UPDATE kari e gane che.
     *
     * Kevi rite? Badhi silver tables ma etl_run_id, etl_inserted_at, etl_updated_at che:
     *   - navi row:        etl_inserted_at = etl_updated_at  (banne same now())
     *   - update thayeli:  trigger etl_updated_at ne navu now() aape → etl_inserted_at < etl_updated_at
     * Upsert ma value same hoy to update j nathi thatu (IS DISTINCT FROM), etle e row
     * gani nathi: "kharekhar badlayeli" rows j ganay.
     *
     * {@code etl_updated_at >= run start} filter thi index (ix_*_etl) vapray, aakhi table scan nahi.
     *
     * @param table silver table naam (schema vagar), jem ke "fact_sales"
     * @return [inserted, updated]
     */
    public long[] countWritten(JobContext ctx, String table) {
        Map<String, Object> r = jdbc.queryForMap(
                "SELECT count(*) FILTER (WHERE etl_inserted_at = etl_updated_at) AS inserted, "
                        + "       count(*) FILTER (WHERE etl_inserted_at <> etl_updated_at) AS updated "
                        + "FROM " + silverSchema + "." + table + " "
                        + "WHERE etl_run_id = ? "
                        + "  AND etl_updated_at >= (SELECT start_time FROM " + this.table
                        + "                          WHERE run_id = ? AND source_name = '" + JOB_ROW + "')",
                ctx.runId(), ctx.runId());
        return new long[]{((Number) r.get("inserted")).longValue(), ((Number) r.get("updated")).longValue()};
    }

    /** Ek run ni badhi audit rows (API status mate). JOB row pehla. */
    public List<Map<String, Object>> findRun(String runId) {
        return jdbc.queryForList(
                "SELECT run_id, job_name, source_name, load_type, triggered_by, status, start_time, end_time, "
                        + "total_record, processed_record, rejected_record, partial_record, "
                        + "inserted_record, updated_record, error_message "
                        + "FROM " + table + " WHERE run_id = ? "
                        + "ORDER BY CASE WHEN source_name = '" + JOB_ROW + "' THEN 0 ELSE 1 END, audit_id",
                runId);
    }

    /** INSERT ... ON CONFLICT: navo run hoy to insert, re-run hoy to row reset kari RUNNING. */
    private void upsertRunning(JobContext ctx, String source, String loadType) {
        jdbc.update("INSERT INTO " + table + " (run_id, job_name, source_name, load_type, triggered_by, status, start_time) "
                        + "VALUES (?, ?, ?, ?, ?, 'RUNNING', now()) "
                        + "ON CONFLICT (run_id, source_name) DO UPDATE SET "
                        + "  load_type = EXCLUDED.load_type, triggered_by = EXCLUDED.triggered_by, status = 'RUNNING', "
                        + "  start_time = now(), end_time = NULL, total_record = 0, processed_record = 0, "
                        + "  rejected_record = 0, partial_record = 0, inserted_record = 0, updated_record = 0, "
                        + "  error_message = NULL",
                ctx.runId(), ctx.jobName(), source, loadType, ctx.triggeredBy());
    }

    /** Row ne final status + counts sathe band kare che. */
    private void finish(String runId, String source, String status, SourceStats s, String error) {
        jdbc.update("UPDATE " + table + " SET status = ?, end_time = now(), total_record = ?, processed_record = ?, "
                        + "rejected_record = ?, partial_record = ?, inserted_record = ?, updated_record = ?, "
                        + "error_message = ? WHERE run_id = ? AND source_name = ?",
                status, s.total(), s.processed(), s.rejected(), s.partial(), s.inserted(), s.updated(),
                error, runId, source);
    }

    /** Error message: root cause sathe, ane column ma samay evdu tunku. */
    private static String message(Throwable error) {
        Throwable root = error;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String text = error.getMessage();
        if (root != error && root.getMessage() != null) {
            text = text + " | root: " + root.getMessage();
        }
        if (text == null) {
            text = error.getClass().getName();
        }
        return text.length() > MAX_ERROR_LENGTH ? text.substring(0, MAX_ERROR_LENGTH) : text;
    }
}
