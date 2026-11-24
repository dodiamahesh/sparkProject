package com.tcs.dmj.dwh.repository;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.Collection;

/**
 * SQL for meta.meta_lineage: asset-level lineage edges (source asset -&gt; target asset, by pipeline).
 */
@Repository
public class MetaLineageRepository {

    private final JdbcTemplate jdbc;

    public MetaLineageRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Inserts the edge (only when it is new: lineage_id has no gaps) or makes it active again; returns its id. */
    public long upsert(long sourceAssetId, long targetAssetId, long pipelineId) {
        jdbc.update("""
                INSERT INTO meta.meta_lineage (source_asset_id, target_asset_id, pipeline_id)
                SELECT CAST(? AS BIGINT), CAST(? AS BIGINT), CAST(? AS BIGINT)
                 WHERE NOT EXISTS (SELECT 1 FROM meta.meta_lineage
                                    WHERE source_asset_id = ? AND target_asset_id = ? AND pipeline_id = ?)
                ON CONFLICT DO NOTHING
                """, sourceAssetId, targetAssetId, pipelineId, sourceAssetId, targetAssetId, pipelineId);
        jdbc.update("""
                UPDATE meta.meta_lineage SET active_flag = 'Y'
                 WHERE source_asset_id = ? AND target_asset_id = ? AND pipeline_id = ? AND active_flag <> 'Y'
                """, sourceAssetId, targetAssetId, pipelineId);
        return jdbc.queryForObject("""
                SELECT lineage_id FROM meta.meta_lineage
                 WHERE source_asset_id = ? AND target_asset_id = ? AND pipeline_id = ?
                """, Long.class, sourceAssetId, targetAssetId, pipelineId);
    }

    /** Edges of the pipeline that were not seen in its last successful run: active_flag = 'N'. */
    public int deactivateAllExcept(long pipelineId, Collection<Long> lineageIds) {
        return jdbc.update("UPDATE meta.meta_lineage SET active_flag = 'N' "
                        + "WHERE pipeline_id = ? AND active_flag = 'Y' AND NOT (lineage_id = ANY (?))",
                pipelineId, lineageIds.toArray(Long[]::new));
    }
}
