package com.tcs.dmj.dwh.repository;

import com.tcs.dmj.dwh.domain.MetaAsset;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * SQL for meta.meta_asset: every MongoDB collection and PostgreSQL table that appears in the lineage.
 * An asset is unique by platform + database + schema + name.
 */
@Repository
public class MetaAssetRepository {

    public static final int MAX_NAME_LENGTH = 100;
    public static final int MAX_DESCRIPTION_LENGTH = 100;

    private static final String SAME_ASSET = """
            platform = ? AND COALESCE(database_name, '') = COALESCE(CAST(? AS VARCHAR), '')
            AND COALESCE(schema_name, '') = COALESCE(CAST(? AS VARCHAR), '') AND asset_name = ?""";

    private final JdbcTemplate jdbc;

    public MetaAssetRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Inserts the asset (only when it is new, so asset_id has no gaps) or updates it when its type / layer /
     * description changed, and returns its asset_id. A NULL description never overwrites an existing one.
     */
    public long upsert(MetaAsset asset) {
        String name = truncate(asset.assetName(), MAX_NAME_LENGTH);
        String description = truncate(asset.assetDescription(), MAX_DESCRIPTION_LENGTH);
        Object[] key = {asset.platform().name(), asset.databaseName(), asset.schemaName(), name};

        jdbc.update("""
                INSERT INTO meta.meta_asset
                       (asset_name, asset_type, platform, database_name, schema_name, layer_name, asset_description)
                SELECT CAST(? AS VARCHAR), CAST(? AS VARCHAR), CAST(? AS VARCHAR), CAST(? AS VARCHAR),
                       CAST(? AS VARCHAR), CAST(? AS VARCHAR), CAST(? AS VARCHAR)
                 WHERE NOT EXISTS (SELECT 1 FROM meta.meta_asset WHERE %s)
                ON CONFLICT DO NOTHING
                """.formatted(SAME_ASSET),
                concat(new Object[]{name, asset.assetType().name(), asset.platform().name(), asset.databaseName(),
                        asset.schemaName(), asset.layerName().name(), description}, key));

        jdbc.update("""
                UPDATE meta.meta_asset
                   SET asset_type = ?, layer_name = ?,
                       asset_description = COALESCE(CAST(? AS VARCHAR), asset_description), active_flag = 'Y'
                 WHERE %s
                   AND (asset_type IS DISTINCT FROM CAST(? AS VARCHAR)
                        OR layer_name IS DISTINCT FROM CAST(? AS VARCHAR)
                        OR (CAST(? AS VARCHAR) IS NOT NULL AND asset_description IS DISTINCT FROM CAST(? AS VARCHAR))
                        OR active_flag <> 'Y')
                """.formatted(SAME_ASSET),
                concat(new Object[]{asset.assetType().name(), asset.layerName().name(), description}, key,
                        new Object[]{asset.assetType().name(), asset.layerName().name(), description, description}));

        return jdbc.queryForObject("SELECT asset_id FROM meta.meta_asset WHERE " + SAME_ASSET, Long.class, key);
    }

    private static Object[] concat(Object[]... parts) {
        int length = 0;
        for (Object[] part : parts) {
            length += part.length;
        }
        Object[] all = new Object[length];
        int i = 0;
        for (Object[] part : parts) {
            System.arraycopy(part, 0, all, i, part.length);
            i += part.length;
        }
        return all;
    }

    private static String truncate(String text, int maxLength) {
        return text == null || text.length() <= maxLength ? text : text.substring(0, maxLength);
    }
}
