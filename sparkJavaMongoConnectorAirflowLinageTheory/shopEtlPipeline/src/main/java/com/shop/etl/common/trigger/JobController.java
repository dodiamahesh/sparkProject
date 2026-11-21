package com.shop.etl.common.trigger;

import com.shop.etl.common.audit.AuditService;
import com.shop.etl.common.job.LaunchResult;
import com.shop.etl.common.job.JobLauncher;
import com.shop.etl.common.watermark.WatermarkService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * TRIGGER-1: REST API. Manas (Postman/curl), Airflow (HttpOperator) ke koi biji system
 * job chalavi shake.
 *
 * <pre>
 *   POST /api/jobs/J2_SALES/runs                          → have sudhi no data, background (202)
 *   POST /api/jobs/J2_SALES/runs?windowEnd=2026-10-21T04:00
 *   POST /api/jobs/J2_SALES/runs?mode=sync                → puru thay pachhi j response (200 / 500)
 *        Header X-Triggered-By: AIRFLOW                    → audit ma kone chalavyo (default API)
 *
 *   GET  /api/jobs                                        → jobs ane emna watermarks
 *   GET  /api/jobs/runs/J2_20261021_0400                  → run no status + source wise counts
 * </pre>
 *
 * Default mode ASYNC kem? Job ne minuto lage. HTTP request etlo samay khullo na rakhay
 * (proxy/load balancer timeout). Etle turant runId aapo, ane caller status poll kare.
 */
@RestController
@RequestMapping("/api/jobs")
public class JobController {

    private final JobLauncher launcher;
    private final AuditService audit;
    private final WatermarkService watermarks;

    public JobController(JobLauncher launcher, AuditService audit, WatermarkService watermarks) {
        this.launcher = launcher;
        this.audit = audit;
        this.watermarks = watermarks;
    }

    /**
     * Job chalavo.
     *
     * @param jobName     J2_SALES
     * @param windowEnd   optional: data kya sudhi (Airflow data_interval_end aape)
     * @param mode        async (default) ke sync
     * @param triggeredBy optional header: AIRFLOW / API / ...
     * @return 202 STARTED, 200 SUCCESS/SKIPPED, 500 FAILED (sync)
     */
    @PostMapping("/{jobName}/runs")
    public ResponseEntity<LaunchResult> run(@PathVariable String jobName,
                                            @RequestParam(required = false) String windowEnd,
                                            @RequestParam(defaultValue = "async") String mode,
                                            @RequestHeader(value = "X-Triggered-By", defaultValue = "API") String triggeredBy) {
        boolean async = !"sync".equalsIgnoreCase(mode);
        LaunchResult result = launcher.launch(jobName, windowEnd, async, triggeredBy.toUpperCase());
        HttpStatus status = switch (result.status()) {
            case "STARTED" -> HttpStatus.ACCEPTED;
            case "FAILED" -> HttpStatus.INTERNAL_SERVER_ERROR;
            default -> HttpStatus.OK;
        };
        return ResponseEntity.status(status).body(result);
    }

    /**
     * Run no status. Airflow HttpSensor aa poll kare che: status SUCCESS → aagal, FAILED → fail.
     * Run id haju audit ma na hoy to 404.
     */
    @GetMapping("/runs/{runId}")
    public ResponseEntity<Map<String, Object>> runStatus(@PathVariable String runId) {
        List<Map<String, Object>> rows = audit.findRun(runId);
        if (rows.isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("runId", runId);
        body.put("status", rows.get(0).get("status"));
        body.put("job", rows.get(0));
        body.put("sources", rows.subList(1, rows.size()));
        return ResponseEntity.ok(body);
    }

    /** Badha registered jobs ane emna watermarks. */
    @GetMapping
    public List<Map<String, Object>> jobs() {
        return launcher.jobNames().stream()
                .map(name -> Map.<String, Object>of("job", name, "watermarks", watermarks.findAll(name)))
                .toList();
    }
}
