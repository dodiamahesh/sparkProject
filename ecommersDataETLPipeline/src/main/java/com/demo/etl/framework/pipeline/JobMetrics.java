package com.demo.etl.framework.pipeline;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Counters for one job run. They are saved in etl_audit.etl_job_run when the job ends.
 */
public class JobMetrics {

    private final AtomicLong sourceCount = new AtomicLong();
    private final AtomicLong validCount = new AtomicLong();
    private final AtomicLong rejectedCount = new AtomicLong();
    private final AtomicLong duplicateCount = new AtomicLong();
    private final AtomicLong loadedCount = new AtomicLong();
    private final AtomicLong resolvedCount = new AtomicLong();
    private final AtomicLong warningCount = new AtomicLong();

    public void addSource(long count) {
        sourceCount.addAndGet(count);
    }

    public void addValid(long count) {
        validCount.addAndGet(count);
    }

    public void addRejected(long count) {
        rejectedCount.addAndGet(count);
    }

    public void addDuplicates(long count) {
        duplicateCount.addAndGet(count);
    }

    public void addLoaded(long count) {
        loadedCount.addAndGet(count);
    }

    public void addResolved(long count) {
        resolvedCount.addAndGet(count);
    }

    /** Rule warnings + schema-drift warnings + volume warnings. */
    public void addWarnings(long count) {
        warningCount.addAndGet(count);
    }

    public long warningCount() {
        return warningCount.get();
    }

    public long sourceCount() {
        return sourceCount.get();
    }

    public long validCount() {
        return validCount.get();
    }

    public long rejectedCount() {
        return rejectedCount.get();
    }

    public long duplicateCount() {
        return duplicateCount.get();
    }

    public long loadedCount() {
        return loadedCount.get();
    }

    public long resolvedCount() {
        return resolvedCount.get();
    }
}
