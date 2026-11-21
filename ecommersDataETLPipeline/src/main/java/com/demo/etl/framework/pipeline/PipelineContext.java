package com.demo.etl.framework.pipeline;

import com.demo.etl.framework.source.SourceBatch;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Everything that belongs to ONE job run: id, window end, counters, processed sources and cached data.
 * A new context is created for every run and thrown away at the end.
 */
public class PipelineContext {

    private final long jobRunId;
    private final String pipelineName;
    private final Instant windowEnd;
    private final JobMetrics metrics = new JobMetrics();
    private final List<SourceBatch> sourceBatches = new ArrayList<>();
    private final List<Dataset<Row>> cachedDatasets = new ArrayList<>();
    private final AtomicInteger stepCounter = new AtomicInteger();
    private final AtomicInteger stagingTableCounter = new AtomicInteger();

    public PipelineContext(long jobRunId, String pipelineName, Instant windowEnd) {
        this.jobRunId = jobRunId;
        this.pipelineName = pipelineName;
        this.windowEnd = windowEnd;
    }

    public long jobRunId() {
        return jobRunId;
    }

    public String pipelineName() {
        return pipelineName;
    }

    /** Upper bound of the incremental window of EVERY source in this run (the run's start time). */
    public Instant windowEnd() {
        return windowEnd;
    }

    public JobMetrics metrics() {
        return metrics;
    }

    /** Every source processed in this run. Used at the end to save rejects and move the watermarks. */
    public void addSourceBatch(SourceBatch batch) {
        sourceBatches.add(batch);
    }

    public List<SourceBatch> sourceBatches() {
        return Collections.unmodifiableList(sourceBatches);
    }

    /**
     * Caches a dataset for the rest of this run.
     * IMPORTANT: Spark is lazy. Without caching, every action (count, load, save rejects) would read
     * MongoDB again. Caching reads it ONCE and freezes what we read for the rest of the run.
     */
    public Dataset<Row> cache(Dataset<Row> dataset) {
        Dataset<Row> cached = dataset.persist(StorageLevel.MEMORY_AND_DISK());
        cachedDatasets.add(cached);
        return cached;
    }

    /** Called by the framework when the job ends (success or failure). */
    public void releaseResources() {
        cachedDatasets.forEach(Dataset::unpersist);
        cachedDatasets.clear();
    }

    public int nextStepOrder() {
        return stepCounter.incrementAndGet();
    }

    public int nextStagingTableNumber() {
        return stagingTableCounter.incrementAndGet();
    }
}
