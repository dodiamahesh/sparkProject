package com.tcs.dmj.dwh.service.impl;

import com.tcs.dmj.dwh.connector.source.SourceBatch;
import com.tcs.dmj.dwh.core.ColumnLineage;
import com.tcs.dmj.dwh.core.EtlPipeline;
import com.tcs.dmj.dwh.core.LineageCollector;
import com.tcs.dmj.dwh.core.PipelineContext;
import com.tcs.dmj.dwh.domain.MetaAsset;
import com.tcs.dmj.dwh.enums.Platform;
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
 * Called after a run SUCCEEDED (its data is committed), in its own transaction:
 *
 *   1. every asset of the run          -> meta.meta_asset          (insert / update)
 *   2. every source -&gt; target edge    -> meta.meta_lineage        (insert; edges of this pipeline that were not
 *                                                                     seen in this run become active_flag 'N')
 *   3. the declared column mappings     -> meta.meta_column_lineage (insert; mappings removed from the code
 *                                                                     become active_flag 'N')
 *
 * Lineage is metadata: when writing it fails, the run stays successful and the error is only logged.
 * The next successful run writes it again.
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
                .orElseThrow(() -> new IllegalStateException(pipeline.name() + " is not in meta.meta_pipeline"))
                .pipelineId();
        Map<String, String> descriptions = collectionDescriptions(context);
        Map<String, Long> assetIds = new HashMap<>();

        // 1 + 2: table lineage, found automatically during the run
        List<PipelineContext.LineageEdge> edges = context.lineage();
        if (!edges.isEmpty()) {
            List<Long> lineageIds = new ArrayList<>();
            for (PipelineContext.LineageEdge edge : edges) {
                long sourceId = assetId(edge.source(), descriptions, assetIds);
                long targetId = assetId(edge.target(), descriptions, assetIds);
                lineageIds.add(lineageRepository.upsert(sourceId, targetId, pipelineId));
            }
            int deactivated = lineageRepository.deactivateAllExcept(pipelineId, lineageIds);
            log.info("[job {}] lineage: {} edge(s) saved, {} old edge(s) deactivated",
                    context.pipelineRunId(), lineageIds.size(), deactivated);
        }

        // 3: column lineage, declared by the pipeline
        List<Long> columnIds = new ArrayList<>();
        for (ColumnLineage column : pipeline.columnLineage()) {
            long sourceId = assetId(lineageCollector.mongoCollection(column.sourceCollection()), descriptions, assetIds);
            long targetId = assetId(lineageCollector.postgresTable(column.targetTable()), descriptions, assetIds);
            columnIds.add(columnLineageRepository.upsert(pipelineId, sourceId, column.sourceField(), targetId,
                    column.targetColumn(), column.mappingType()));
        }
        int deactivated = columnLineageRepository.deactivateAllExcept(pipelineId, columnIds);
        if (!columnIds.isEmpty() || deactivated > 0) {
            log.info("[job {}] column lineage: {} mapping(s) saved, {} deactivated",
                    context.pipelineRunId(), columnIds.size(), deactivated);
        }
    }

    private long assetId(MetaAsset asset, Map<String, String> descriptions, Map<String, Long> cache) {
        return cache.computeIfAbsent(asset.key(), key -> {
            MetaAsset withDescription = asset.platform() == Platform.MONGODB && asset.assetDescription() == null
                    ? asset.withDescription(descriptions.get(asset.assetName()))
                    : asset;
            return assetRepository.upsert(withDescription);
        });
    }

    /** collection -&gt; description of its SourceDefinition, used as asset_description. */
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
}
