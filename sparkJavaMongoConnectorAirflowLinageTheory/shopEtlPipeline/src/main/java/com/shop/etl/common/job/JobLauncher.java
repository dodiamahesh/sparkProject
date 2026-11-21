package com.shop.etl.common.job;

import com.shop.etl.common.audit.AuditService;
import com.shop.etl.common.config.EtlProperties;
import com.shop.etl.common.config.EtlProperties.JobConfig;
import com.shop.etl.common.watermark.LoadWindow;
import com.shop.etl.common.watermark.WatermarkService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.task.TaskRejectedException;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Badha TRIGGERS (API, Airflow, Scheduler, CLI) no EK j entry point.
 *
 * Kem ek j? Lock, run id, window, ane audit no niyam darek trigger ma alag lakhiye to kyarek
 * ne kyarek ek ma bhul thay. Aahi ek j vaar lakhyu che: trigger koi pan hoy, job same rite chale.
 *
 * Flow:
 * <pre>
 *   1. Job shodho (naam thi), yml ma enabled che?
 *   2. Window end nakki karo: aapyo hoy to e (Airflow data_interval_end), nahi to "have" (minute sudhi)
 *   3. LOCK lo (bijo run chale che → 409)
 *   4. Darek source ni window (watermark thi). Badhi khali → SKIPPED
 *   5. Run id banavo + JOB audit row RUNNING (API ne turant status male)
 *   6. async: background thread ma JobRunner, turant STARTED pacho
 *      sync : aaj thread ma JobRunner, puru thay pachhi SUCCESS/FAILED
 *   7. Run puro thay etle lock chhuto
 * </pre>
 */
@Component
public class JobLauncher {

    private static final Logger log = LoggerFactory.getLogger(JobLauncher.class);
    private static final DateTimeFormatter RUN_ID_TIME = DateTimeFormatter.ofPattern("yyyyMMdd_HHmm");

    private final Map<String, EtlJob> jobs;
    private final EtlProperties properties;
    private final JobLock jobLock;
    private final WatermarkService watermarks;
    private final AuditService audit;
    private final JobRunner runner;
    private final ThreadPoolTaskExecutor executor;

    public JobLauncher(List<EtlJob> jobs, EtlProperties properties, JobLock jobLock, WatermarkService watermarks,
                       AuditService audit, JobRunner runner,
                       @Qualifier("etlJobExecutor") ThreadPoolTaskExecutor executor) {
        this.jobs = jobs.stream().collect(Collectors.toMap(EtlJob::name, Function.identity(), (a, b) -> a, TreeMap::new));
        this.properties = properties;
        this.jobLock = jobLock;
        this.watermarks = watermarks;
        this.audit = audit;
        this.runner = runner;
        this.executor = executor;
        log.info("Registered ETL jobs: {}", this.jobs.keySet());
    }

    /** Registered job naam (API GET /api/jobs). */
    public List<String> jobNames() {
        return List.copyOf(jobs.keySet());
    }

    /**
     * Job chalave.
     *
     * @param jobName       J2_SALES
     * @param windowEndText "2026-10-21T04:00+05:30" / "2026-10-21T04:00" (IST mani levay) / null (have)
     * @param async         true: background (API), false: puru thay tya sudhi rah juo (CLI, scheduler)
     * @param triggeredBy   API / AIRFLOW / SCHEDULER / CLI (audit mate)
     */
    public LaunchResult launch(String jobName, String windowEndText, boolean async, String triggeredBy) {
        EtlJob job = Optional.ofNullable(jobs.get(jobName)).orElseThrow(() -> new JobExceptions.JobNotFound(jobName));
        JobConfig config = properties.job(jobName);
        if (!config.enabled()) {
            throw new JobExceptions.JobDisabled(jobName);
        }
        Instant windowEnd = parseWindowEnd(windowEndText, properties.zoneId());

        JobLock.Handle lock = jobLock.tryAcquire(jobName)
                .orElseThrow(() -> new JobExceptions.JobAlreadyRunning(jobName));
        try {
            Map<String, LoadWindow> windows = watermarks.windows(jobName, job.sources(), windowEnd);
            if (windows.values().stream().allMatch(LoadWindow::isEmpty)) {
                lock.close();
                log.info("SKIPPED: {} no data {} sudhi pehla j load thai gayo che", jobName, windowEnd);
                return new LaunchResult(jobName, null, "SKIPPED", windowEnd,
                        "Window khali che: aa samay sudhi no data pehla j load thayo che");
            }

            JobContext ctx = new JobContext(jobName, runId(jobName, windowEnd, properties.zoneId()),
                    windowEnd, windows, config, triggeredBy);
            audit.startJob(ctx);

            if (!async) {
                try (lock) {
                    return runner.execute(job, ctx);
                }
            }
            try {
                executor.execute(() -> {
                    try (lock) {
                        runner.execute(job, ctx);
                    }
                });
            } catch (TaskRejectedException e) {
                audit.failJob(ctx, e);
                throw new JobExceptions.JobQueueFull(jobName);
            }
            return new LaunchResult(jobName, ctx.runId(), "STARTED", windowEnd,
                    "Run background ma shuru thayo. Status: GET /api/jobs/runs/" + ctx.runId());
        } catch (RuntimeException e) {
            lock.close();   // Handle.close() be vaar call thay to pan safe che
            throw e;
        }
    }

    /**
     * Run id = job short code + window end (IST). "J2_SALES" + 2026-10-21 04:00 → "J2_20261021_0400".
     * Same window farithi chale (fail pachhi re-run) to same id: audit ane rejects overwrite thay.
     */
    static String runId(String jobName, Instant windowEnd, ZoneId zone) {
        String shortCode = jobName.contains("_") ? jobName.substring(0, jobName.indexOf('_')) : jobName;
        return shortCode + "_" + RUN_ID_TIME.format(windowEnd.atZone(zone));
    }

    /**
     * Window end parse kare che.
     *   null / blank          → have (seconds kadhi ne: 10:37:52 → 10:37)
     *   offset sathe          → "2026-10-20T22:30:00+00:00" (Airflow data_interval_end)
     *   offset vagar          → "2026-10-21T04:00" business timezone (IST) mani levay
     */
    static Instant parseWindowEnd(String text, ZoneId zone) {
        if (text == null || text.isBlank()) {
            return Instant.now().truncatedTo(ChronoUnit.MINUTES);
        }
        try {
            return OffsetDateTime.parse(text).toInstant();
        } catch (Exception notOffset) {
            try {
                return LocalDateTime.parse(text).atZone(zone).toInstant();
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        "windowEnd '" + text + "' samjayu nahi. Example: 2026-10-21T04:00 ke 2026-10-21T04:00+05:30");
            }
        }
    }
}
