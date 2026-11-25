package com.tcs.dmj.dwh.connector.source;

import java.time.Instant;

/**
 * The documents one run reads: timestamp &gt; start AND &lt;= end.
 * start = last_run_time of the last successful run (null on the first run), end = start time of this run.
 */
public record LoadWindow(Instant start, Instant end) {

    public boolean isFirstRun() {
        return start == null;
    }

    /** For logs: "updatedAt &gt; ... AND &lt;= ..." */
    public String describe(String field) {
        return isFirstRun()
                ? field + " <= " + end + " (first run)"
                : field + " > " + start + " AND <= " + end;
    }
}
