package com.tcs.dmj.dwh.repository;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.Collection;

/** meta.meta_lineage: table-level edges source asset -&gt; target asset, per pipeline. */
@Repository
public class MetaLineageRepository {

    private final JdbcTemplate jdbc;

    public MetaLineageRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Inserts the edge (or makes it active again) and returns its lineage_id. */
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

    /** Edges not seen in the pipeline's last successful run get active_flag = 'N'. */
    public int deactivateAllExcept(long pipelineId, Collection<Long> lineageIds) {
        return jdbc.update("UPDATE meta.meta_lineage SET active_flag = 'N' "
                        + "WHERE pipeline_id = ? AND active_flag = 'Y' AND NOT (lineage_id = ANY (?))",
                pipelineId, lineageIds.toArray(Long[]::new));
    }
}
