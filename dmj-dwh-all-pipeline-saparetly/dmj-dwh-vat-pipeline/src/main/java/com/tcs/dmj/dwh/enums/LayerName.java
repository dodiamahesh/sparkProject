package com.tcs.dmj.dwh.enums;

/** Medallion layer of a lineage asset (meta.meta_asset.layer_name). */
public enum LayerName {

    SOURCE,

    /** MongoDB collections read by the pipelines (bronze). */
    RAW,

    /** PostgreSQL silver / curated tables written by the pipelines. */
    CURATED,

    AGGREGATED
}
