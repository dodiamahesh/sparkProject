package com.shop.etl.common.job;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.time.Instant;

/**
 * Job trigger nu result (API response, CLI exit code, scheduler log).
 *
 * Status:
 *   STARTED → async run background ma shuru (API). Status GET /api/jobs/runs/{runId} thi jovo.
 *   SUCCESS → sync run puro, badhu barabar (CLI / scheduler / API mode=sync)
 *   FAILED  → sync run fail. Karan message ane audit table ma
 *   SKIPPED → window khali: aa samay sudhi no data pehla j load thai gayo che
 *
 * @param jobName   J2_SALES
 * @param runId     J2_20261021_0400 (SKIPPED hoy to null)
 * @param status    upar mujab
 * @param windowEnd aa run kya sudhi vanche
 * @param message   manas mate tunku message
 */
public record LaunchResult(String jobName, String runId, String status, Instant windowEnd, String message) {

    /** CLI exit code mate: fakt FAILED kharab che. (API JSON ma nathi aavtu.) */
    @JsonIgnore
    public boolean isFailure() {
        return "FAILED".equals(status);
    }
}
