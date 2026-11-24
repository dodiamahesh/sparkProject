package com.tcs.dmj.dwh.core;

import com.tcs.dmj.dwh.connector.source.SourceBatch;
import com.tcs.dmj.dwh.domain.MetaAsset;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Everything that belongs to ONE job run: id, window end, counters, processed sources and cached data.
 * A new context is created for every run and thrown away at the end.
 */
public class PipelineContext {

    private final long pipelineRunId;
    private final String pipelineName;
    private final Instant windowEnd;
    private final boolean directLoad;
    private final JobMetrics metrics = new JobMetrics();
    private final List<SourceBatch> sourceBatches = new ArrayList<>();
    private final List<Dataset<Row>> cachedDatasets = new ArrayList<>();
    private final AtomicInteger stagingTableCounter = new AtomicInteger();
    private final Set<String> deletesApplied = ConcurrentHashMap.newKeySet();
    private final Map<String, LineageEdge> lineage = new ConcurrentHashMap<>();

    public PipelineContext(long pipelineRunId, String pipelineName, Instant windowEnd, boolean directLoad) {
        this.pipelineRunId = pipelineRunId;
        this.pipelineName = pipelineName;
        this.windowEnd = windowEnd;
        this.directLoad = directLoad;
    }

    public long pipelineRunId() {
        return pipelineRunId;
    }

    public String pipelineName() {
        return pipelineName;
    }

    /** Upper bound of the incremental window of EVERY source in this run (the run's start time). */
    public Instant windowEnd() {
        return windowEnd;
    }

    /**
     * true = the pipeline writes straight into its tables (no staging) and the whole run is ONE transaction:
     * every table, the rejected records and last_run_time are committed together, or nothing at all.
     */
    public boolean directLoad() {
        return directLoad;
    }

    public JobMetrics metrics() {
        return metrics;
    }

    /** Every source processed in this run. Used at the end to save rejects and move last_run_time. */
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

    /** The pipeline applied the soft deletes of this collection (AbstractEtlPipeline.applyDeletes). */
    public void markDeletesApplied(String collection) {
        deletesApplied.add(collection);
    }

    public boolean deletesApplied(String collection) {
        return deletesApplied.contains(collection);
    }

    public int nextStagingTableNumber() {
        return stagingTableCounter.incrementAndGet();
    }

    /** Table-level lineage seen in this run (written to meta.meta_lineage when the run succeeds). */
    public void addLineage(MetaAsset source, MetaAsset target) {
        lineage.putIfAbsent(source.key() + " -> " + target.key(), new LineageEdge(source, target));
    }

    public List<LineageEdge> lineage() {
        return List.copyOf(lineage.values());
    }

    /** source asset -&gt; target asset, written by this run. */
    public record LineageEdge(MetaAsset source, MetaAsset target) {
    }
}
