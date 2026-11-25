package com.tcs.dmj.dwh.core;

import java.util.concurrent.atomic.AtomicLong;

/** Counters of one run, saved in meta.meta_pipeline_run. */
public class JobMetrics {

    private final AtomicLong sourceCount = new AtomicLong();
    private final AtomicLong validCount = new AtomicLong();
    private final AtomicLong rejectedCount = new AtomicLong();
    private final AtomicLong duplicateCount = new AtomicLong();
    private final AtomicLong deletedCount = new AtomicLong();
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

    public void addDeleted(long count) {
        deletedCount.addAndGet(count);
    }

    public void addLoaded(long count) {
        loadedCount.addAndGet(count);
    }

    /** A rolled-back run wrote nothing. */
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

    public long deletedCount() {
        return deletedCount.get();
    }

    public long loadedCount() {
        return loadedCount.get();
    }
}
