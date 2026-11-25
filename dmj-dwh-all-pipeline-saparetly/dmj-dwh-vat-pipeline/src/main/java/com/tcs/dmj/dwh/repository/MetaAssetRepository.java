package com.tcs.dmj.dwh.repository;

import com.tcs.dmj.dwh.domain.MetaAsset;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.Arrays;
import java.util.stream.Stream;

/** meta.meta_asset: collections and tables of the lineage (unique by platform + database + schema + name). */
@Repository
public class MetaAssetRepository {

    public static final int MAX_NAME_LENGTH = 100;
    public static final int MAX_DESCRIPTION_LENGTH = 100;

    private static final String SAME_ASSET = """
            platform = ? AND COALESCE(database_name, '') = COALESCE(CAST(? AS VARCHAR), '')
            AND COALESCE(schema_name, '') = COALESCE(CAST(? AS VARCHAR), '') AND asset_name = ?""";

    private static final String INSERT_IF_NEW = """
            INSERT INTO meta.meta_asset
                   (asset_name, asset_type, platform, database_name, schema_name, layer_name, asset_description)
            SELECT CAST(? AS VARCHAR), CAST(? AS VARCHAR), CAST(? AS VARCHAR), CAST(? AS VARCHAR),
                   CAST(? AS VARCHAR), CAST(? AS VARCHAR), CAST(? AS VARCHAR)
             WHERE NOT EXISTS (SELECT 1 FROM meta.meta_asset WHERE %s)
            ON CONFLICT DO NOTHING
            """.formatted(SAME_ASSET);

    private static final String UPDATE_IF_CHANGED = """
            UPDATE meta.meta_asset
               SET asset_type = ?, layer_name = ?,
                   asset_description = COALESCE(CAST(? AS VARCHAR), asset_description), active_flag = 'Y'
             WHERE %s
               AND (asset_type IS DISTINCT FROM CAST(? AS VARCHAR)
                    OR layer_name IS DISTINCT FROM CAST(? AS VARCHAR)
                    OR (CAST(? AS VARCHAR) IS NOT NULL AND asset_description IS DISTINCT FROM CAST(? AS VARCHAR))
                    OR active_flag <> 'Y')
            """.formatted(SAME_ASSET);

    private final JdbcTemplate jdbc;

    public MetaAssetRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Inserts or updates the asset and returns its asset_id. A NULL description never overwrites one. */
    public long upsert(MetaAsset asset) {
        String name = truncate(asset.assetName(), MAX_NAME_LENGTH);
        String description = truncate(asset.assetDescription(), MAX_DESCRIPTION_LENGTH);
        String type = asset.assetType().name();
        String layer = asset.layerName().name();
        Object[] key = {asset.platform().name(), asset.databaseName(), asset.schemaName(), name};

        Object[] insertValues = {name, type, asset.platform().name(), asset.databaseName(), asset.schemaName(),
                layer, description};
        jdbc.update(INSERT_IF_NEW, concat(insertValues, key));
        jdbc.update(UPDATE_IF_CHANGED, concat(new Object[]{type, layer, description}, key,
                new Object[]{type, layer, description, description}));

        return jdbc.queryForObject("SELECT asset_id FROM meta.meta_asset WHERE " + SAME_ASSET, Long.class, key);
    }

    private static Object[] concat(Object[]... parts) {
        return Stream.of(parts).flatMap(Arrays::stream).toArray();
    }

    private static String truncate(String text, int maxLength) {
        return text == null || text.length() <= maxLength ? text : text.substring(0, maxLength);
    }
}
