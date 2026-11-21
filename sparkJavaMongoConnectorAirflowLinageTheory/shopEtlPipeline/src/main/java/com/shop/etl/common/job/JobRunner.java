package com.shop.etl.common.job;

import com.shop.etl.common.audit.AuditService;
import com.shop.etl.common.audit.SourceStats;
import com.shop.etl.common.quality.RejectWriter;
import com.shop.etl.common.watermark.WatermarkService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

/**
 * Ek run ne SHARUAT thi ANT sudhi chalave che (lock JobLauncher pase che).
 *
 * Kram:
 * <pre>
 *   1. meta_rejected ma aa run_id na juna rejects kadho (re-run safe)
 *   2. job.run(ctx)  → darek source: read → sanitize → validate → rejects → load → audit
 *   3. SUCCESS: EK transaction ma { watermark = window end, JOB audit = SUCCESS }
 *      FAILED : JOB audit = FAILED + error, watermark status FAILED (value nahi badlati)
 * </pre>
 *
 * Step 3 ek transaction ma kem? "Watermark aagal vadhyo pan audit RUNNING rahyo" ke ulat
 * evi adhuri sthiti na thay. Banne sathe thay ke ek pan nahi.
 *
 * Aa class exception BAHAR NATHI fekto: result (SUCCESS/FAILED) pacho aape che. Async thread
 * ma exception khovai jay, etle badhu ahiya j pakdi ne audit ma lakhiye chhiye.
 */
@Component
public class JobRunner {

    private static final Logger log = LoggerFactory.getLogger(JobRunner.class);

    private final AuditService audit;
    private final WatermarkService watermarks;
    private final RejectWriter rejectWriter;
    private final TransactionTemplate transaction;

    public JobRunner(AuditService audit, WatermarkService watermarks, RejectWriter rejectWriter,
                     TransactionTemplate transaction) {
        this.audit = audit;
        this.watermarks = watermarks;
        this.rejectWriter = rejectWriter;
        this.transaction = transaction;
    }

    /**
     * Run chalave. JobLauncher e pehla j lock lidho che ane JOB audit row RUNNING kari che.
     *
     * @return SUCCESS ke FAILED result
     */
    public LaunchResult execute(EtlJob job, JobContext ctx) {
        Instant started = Instant.now();
        log.info("▶ Run start: job={}, runId={}, windows={}, trigger={}",
                ctx.jobName(), ctx.runId(), ctx.windows(), ctx.triggeredBy());
        try {
            rejectWriter.deleteForRun(ctx.runId());

            Map<String, SourceStats> stats = job.run(ctx);
            SourceStats total = stats.values().stream().reduce(SourceStats.empty(), SourceStats::plus);

            transaction.executeWithoutResult(status -> {
                watermarks.commit(ctx.jobName(), ctx.windows().keySet(), ctx.windowEnd(), ctx.runId());
                audit.finishJob(ctx, total);
            });

            String took = Duration.between(started, Instant.now()).toSeconds() + "s";
            log.info("✔ Run SUCCESS: runId={}, took={}, stats={}", ctx.runId(), took, stats);
            return new LaunchResult(ctx.jobName(), ctx.runId(), "SUCCESS", ctx.windowEnd(),
                    "Run puro thayo (" + took + "): total=" + total.total() + ", rejected=" + total.rejected());
        } catch (Exception e) {
            log.error("✘ Run FAILED: runId={}", ctx.runId(), e);
            try {
                audit.failJob(ctx, e);
                watermarks.markFailed(ctx.jobName(), ctx.runId());
            } catch (Exception auditError) {
                log.error("Audit ma FAILED lakhvama pan error (DB down?): runId={}", ctx.runId(), auditError);
            }
            return new LaunchResult(ctx.jobName(), ctx.runId(), "FAILED", ctx.windowEnd(), e.getMessage());
        }
    }
}
