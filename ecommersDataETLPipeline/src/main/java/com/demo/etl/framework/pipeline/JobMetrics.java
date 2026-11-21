package com.demo.etl.framework.pipeline;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Counters for one job run. They are saved in meta.meta_pipeline_run when the job ends.
 */
public class JobMetrics {

    private final AtomicLong sourceCount = new AtomicLong();
    private final AtomicLong validCount = new AtomicLong();
    private final AtomicLong rejectedCount = new AtomicLong();
    private final AtomicLong duplicateCount = new AtomicLong();
    private final AtomicLong loadedCount = new AtomicLong();

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

    /** A rolled-back run (directLoad) wrote nothing: its loaded count goes back to 0. */
    public void resetLoaded() {
        loadedCount.set(0);
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
}
