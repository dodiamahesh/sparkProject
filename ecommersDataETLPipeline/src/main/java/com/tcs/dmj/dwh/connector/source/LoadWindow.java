package com.tcs.dmj.dwh.connector.source;

import java.time.Instant;

/**
 * Which documents ONE source reads in ONE run, based on its watermark (timestamp) field:
 * <pre>
 *   watermarkField &gt; start  AND  watermarkField &lt;= end
 * </pre>
 *
 * - start = last_run_time of the pipeline after its last SUCCESSFUL run (meta.meta_pipeline). null on the first run:
 *           then there is no lower bound and everything up to {@code end} is read.
 * - end   = start time of THIS run (fixed for all sources of the run). A document changed while the job
 *           is running has a later timestamp, so it is read by the NEXT run: nothing is read twice or missed.
 *
 * @param start lower bound, exclusive (null = first run)
 * @param end   upper bound, inclusive
 */
public record LoadWindow(Instant start, Instant end) {

    public boolean isFirstRun() {
        return start == null;
    }

    /** "updatedAt &gt; 2026-11-20T10:00:00Z AND &lt;= 2026-11-21T09:30:00Z", for step names and logs. */
    public String describe(String field) {
        return isFirstRun()
                ? field + " <= " + end + " (first run)"
                : field + " > " + start + " AND <= " + end;
    }
}
