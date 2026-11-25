package com.tcs.dmj.dwh.enums;

/** Layer of a lineage asset (meta.meta_asset.layer_name). RAW = MongoDB, CURATED = PostgreSQL silver. */
public enum LayerName {
    SOURCE,
    RAW,
    CURATED,
    AGGREGATED
}
