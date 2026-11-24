package com.tcs.dmj.dwh.repository;

import com.tcs.dmj.dwh.enums.MappingType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.Collection;

/**
 * SQL for meta.meta_column_lineage: selected field-to-field lineage (critical / regulatory columns only).
 */
@Repository
public class MetaColumnLineageRepository {

    private static final String SAME_MAPPING = """
            pipeline_id = ? AND source_asset_id = ? AND source_column_name = ?
            AND target_asset_id = ? AND target_column_name = ?""";

    private final JdbcTemplate jdbc;

    public MetaColumnLineageRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Inserts the mapping (only when it is new: column_lineage_id has no gaps) or updates its mapping type /
     * makes it active again; returns its id.
     */
    public long upsert(long pipelineId, long sourceAssetId, String sourceColumn, long targetAssetId,
                       String targetColumn, MappingType mappingType) {
        Object[] key = {pipelineId, sourceAssetId, sourceColumn, targetAssetId, targetColumn};
        jdbc.update("""
                INSERT INTO meta.meta_column_lineage
                       (pipeline_id, source_asset_id, source_column_name, target_asset_id, target_column_name, mapping_type)
                SELECT CAST(? AS BIGINT), CAST(? AS BIGINT), CAST(? AS VARCHAR), CAST(? AS BIGINT),
                       CAST(? AS VARCHAR), CAST(? AS VARCHAR)
                 WHERE NOT EXISTS (SELECT 1 FROM meta.meta_column_lineage WHERE %s)
                ON CONFLICT DO NOTHING
                """.formatted(SAME_MAPPING),
                pipelineId, sourceAssetId, sourceColumn, targetAssetId, targetColumn, mappingType.name(),
                pipelineId, sourceAssetId, sourceColumn, targetAssetId, targetColumn);
        jdbc.update("""
                UPDATE meta.meta_column_lineage SET mapping_type = ?, active_flag = 'Y'
                 WHERE %s AND (mapping_type IS DISTINCT FROM CAST(? AS VARCHAR) OR active_flag <> 'Y')
                """.formatted(SAME_MAPPING),
                mappingType.name(), pipelineId, sourceAssetId, sourceColumn, targetAssetId, targetColumn,
                mappingType.name());
        return jdbc.queryForObject("SELECT column_lineage_id FROM meta.meta_column_lineage WHERE " + SAME_MAPPING,
                Long.class, key);
    }

    /** Mappings of the pipeline that are no longer declared in the code: active_flag = 'N'. */
    public int deactivateAllExcept(long pipelineId, Collection<Long> columnLineageIds) {
        return jdbc.update("UPDATE meta.meta_column_lineage SET active_flag = 'N' "
                        + "WHERE pipeline_id = ? AND active_flag = 'Y' AND NOT (column_lineage_id = ANY (?))",
                pipelineId, columnLineageIds.toArray(Long[]::new));
    }
}
