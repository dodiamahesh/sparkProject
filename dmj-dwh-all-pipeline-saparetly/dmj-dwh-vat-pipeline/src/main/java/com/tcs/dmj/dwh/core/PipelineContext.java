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

/** Everything of ONE run: id, window end, counters, processed sources, cached data and lineage. */
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

    /** One source -> target edge of the table lineage. */
    public record LineageEdge(MetaAsset source, MetaAsset target) {
    }

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

    /** Upper bound of the incremental window of every source of this run (the run's start time). */
    public Instant windowEnd() {
        return windowEnd;
    }

    /** true = the whole run is one transaction (see EtlPipeline.directLoad). */
    public boolean directLoad() {
        return directLoad;
    }

    public JobMetrics metrics() {
        return metrics;
    }

    public void addSourceBatch(SourceBatch batch) {
        sourceBatches.add(batch);
    }

    public List<SourceBatch> sourceBatches() {
        return Collections.unmodifiableList(sourceBatches);
    }

    /** Caches a dataset for the rest of the run, so MongoDB is read only once (Spark is lazy). */
    public Dataset<Row> cache(Dataset<Row> dataset) {
        Dataset<Row> cached = dataset.persist(StorageLevel.MEMORY_AND_DISK());
        cachedDatasets.add(cached);
        return cached;
    }

    /** Called when the run ends. */
    public void releaseResources() {
        cachedDatasets.forEach(Dataset::unpersist);
        cachedDatasets.clear();
    }

    public void markDeletesApplied(String collection) {
        deletesApplied.add(collection);
    }

    public boolean deletesApplied(String collection) {
        return deletesApplied.contains(collection);
    }

    public int nextStagingTableNumber() {
        return stagingTableCounter.incrementAndGet();
    }

    public void addLineage(MetaAsset source, MetaAsset target) {
        lineage.putIfAbsent(source.key() + " -> " + target.key(), new LineageEdge(source, target));
    }

    public List<LineageEdge> lineage() {
        return List.copyOf(lineage.values());
    }
}
