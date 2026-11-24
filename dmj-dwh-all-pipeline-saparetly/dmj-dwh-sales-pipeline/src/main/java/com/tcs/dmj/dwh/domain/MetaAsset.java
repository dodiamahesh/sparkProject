package com.tcs.dmj.dwh.domain;

import com.tcs.dmj.dwh.enums.AssetType;
import com.tcs.dmj.dwh.enums.LayerName;
import com.tcs.dmj.dwh.enums.Platform;

/**
 * A lineage asset (meta.meta_asset): a MongoDB collection or a PostgreSQL table.
 * An asset is identified by platform + database + schema + name (unique index in meta.meta_asset).
 *
 * @param schemaName NULL for MongoDB (a collection lives directly in a database)
 */
public record MetaAsset(
        String assetName,
        AssetType assetType,
        Platform platform,
        String databaseName,
        String schemaName,
        LayerName layerName,
        String assetDescription) {

    public static MetaAsset mongoCollection(String database, String collection, String description) {
        return new MetaAsset(collection, AssetType.COLLECTION, Platform.MONGODB, database, null, LayerName.RAW,
                description);
    }

    /** @param table "schema.table" or "table" (schema public) */
    public static MetaAsset postgresTable(String database, String table) {
        int dot = table.indexOf('.');
        String schema = dot < 0 ? "public" : table.substring(0, dot);
        String name = dot < 0 ? table : table.substring(dot + 1);
        return new MetaAsset(name, AssetType.TABLE, Platform.EDB_PAS, database, schema, LayerName.CURATED, null);
    }

    /** The same asset, regardless of the description (used to compare assets). */
    public String key() {
        return platform + "|" + databaseName + "|" + schemaName + "|" + assetName;
    }

    public String displayName() {
        return schemaName == null ? databaseName + "." + assetName : schemaName + "." + assetName;
    }

    public MetaAsset withDescription(String description) {
        return new MetaAsset(assetName, assetType, platform, databaseName, schemaName, layerName, description);
    }
}
