package com.tcs.dmj.dwh.service.impl;

import com.tcs.dmj.dwh.connector.source.SourceBatch;
import com.tcs.dmj.dwh.core.ColumnLineage;
import com.tcs.dmj.dwh.core.EtlPipeline;
import com.tcs.dmj.dwh.core.LineageCollector;
import com.tcs.dmj.dwh.core.PipelineContext;
import com.tcs.dmj.dwh.domain.MetaAsset;
import com.tcs.dmj.dwh.enums.Platform;
import com.tcs.dmj.dwh.exception.PipelineConfigurationException;
import com.tcs.dmj.dwh.repository.MetaAssetRepository;
import com.tcs.dmj.dwh.repository.MetaColumnLineageRepository;
import com.tcs.dmj.dwh.repository.MetaLineageRepository;
import com.tcs.dmj.dwh.repository.MetaPipelineRepository;
import com.tcs.dmj.dwh.service.LineageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * After a successful run, in its own transaction: assets -> meta_asset, table edges -> meta_lineage,
 * declared column mappings -> meta_column_lineage. Lineage never fails a run (errors are only logged).
 */
@Service
public class LineageServiceImpl implements LineageService {

    private static final Logger log = LoggerFactory.getLogger(LineageServiceImpl.class);

    private final MetaPipelineRepository metaPipelineRepository;
    private final MetaAssetRepository assetRepository;
    private final MetaLineageRepository lineageRepository;
    private final MetaColumnLineageRepository columnLineageRepository;
    private final LineageCollector lineageCollector;
    private final TransactionTemplate transaction;

    public LineageServiceImpl(MetaPipelineRepository metaPipelineRepository, MetaAssetRepository assetRepository,
                              MetaLineageRepository lineageRepository,
                              MetaColumnLineageRepository columnLineageRepository,
                              LineageCollector lineageCollector, TransactionTemplate transaction) {
        this.metaPipelineRepository = metaPipelineRepository;
        this.assetRepository = assetRepository;
        this.lineageRepository = lineageRepository;
        this.columnLineageRepository = columnLineageRepository;
        this.lineageCollector = lineageCollector;
        this.transaction = transaction;
    }

    @Override
    public void record(EtlPipeline pipeline, PipelineContext context) {
        try {
            transaction.executeWithoutResult(status -> write(pipeline, context));
        } catch (RuntimeException e) {
            log.warn("[job {}] lineage of {} not saved: {}", context.pipelineRunId(), pipeline.name(), e.toString());
        }
    }

    private void write(EtlPipeline pipeline, PipelineContext context) {
        long pipelineId = metaPipelineRepository.findByName(pipeline.name())
                .orElseThrow(() -> new PipelineConfigurationException(pipeline.name() + " is not in meta.meta_pipeline"))
                .pipelineId();
        AssetIds assetIds = new AssetIds(collectionDescriptions(context));
        writeTableLineage(context, pipelineId, assetIds);
        writeColumnLineage(pipeline, context, pipelineId, assetIds);
    }

    /** Edges found automatically in this run; older edges of the pipeline are deactivated. */
    private void writeTableLineage(PipelineContext context, long pipelineId, AssetIds assetIds) {
        List<PipelineContext.LineageEdge> edges = context.lineage();
        if (edges.isEmpty()) {
            return;
        }
        List<Long> lineageIds = new ArrayList<>();
        for (PipelineContext.LineageEdge edge : edges) {
            lineageIds.add(lineageRepository.upsert(assetIds.of(edge.source()), assetIds.of(edge.target()), pipelineId));
        }
        int deactivated = lineageRepository.deactivateAllExcept(pipelineId, lineageIds);
        log.info("[job {}] lineage: {} edge(s) saved, {} old edge(s) deactivated",
                context.pipelineRunId(), lineageIds.size(), deactivated);
    }

    /** Mappings declared in EtlPipeline.columnLineage(); mappings removed from the code are deactivated. */
    private void writeColumnLineage(EtlPipeline pipeline, PipelineContext context, long pipelineId, AssetIds assetIds) {
        List<Long> columnIds = new ArrayList<>();
        for (ColumnLineage column : pipeline.columnLineage()) {
            long sourceId = assetIds.of(lineageCollector.mongoCollection(column.sourceCollection()));
            long targetId = assetIds.of(lineageCollector.postgresTable(column.targetTable()));
            columnIds.add(columnLineageRepository.upsert(pipelineId, sourceId, column.sourceField(), targetId,
                    column.targetColumn(), column.mappingType()));
        }
        int deactivated = columnLineageRepository.deactivateAllExcept(pipelineId, columnIds);
        if (!columnIds.isEmpty() || deactivated > 0) {
            log.info("[job {}] column lineage: {} mapping(s) saved, {} deactivated",
                    context.pipelineRunId(), columnIds.size(), deactivated);
        }
    }

    /** collection -&gt; description of its SourceDefinition (used as asset_description). */
    private static Map<String, String> collectionDescriptions(PipelineContext context) {
        Map<String, String> descriptions = new HashMap<>();
        for (SourceBatch batch : context.sourceBatches()) {
            String description = batch.source().metadata().description();
            if (description != null) {
                descriptions.putIfAbsent(batch.collection(), description);
            }
        }
        return descriptions;
    }

    /** Upserts each asset once per run and remembers its asset_id. */
    private final class AssetIds {

        private final Map<String, String> collectionDescriptions;
        private final Map<String, Long> idsByKey = new HashMap<>();

        private AssetIds(Map<String, String> collectionDescriptions) {
            this.collectionDescriptions = collectionDescriptions;
        }

        long of(MetaAsset asset) {
            return idsByKey.computeIfAbsent(asset.key(), key -> assetRepository.upsert(withDescription(asset)));
        }

        private MetaAsset withDescription(MetaAsset asset) {
            boolean collectionWithoutDescription = asset.platform() == Platform.MONGODB && asset.assetDescription() == null;
            return collectionWithoutDescription
                    ? asset.withDescription(collectionDescriptions.get(asset.assetName()))
                    : asset;
        }
    }
}
