package com.tcs.dmj.dwh.enums;

/** How a target column is derived from its source field (meta.meta_column_lineage.mapping_type). */
public enum MappingType {
    DIRECT,
    RENAMED,
    DERIVED,
    AGGREGATED,
    LOOKUP,
    CONSTANT,
    CONDITIONAL,
    MASKED,
    HASHED
}
