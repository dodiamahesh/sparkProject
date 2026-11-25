package com.tcs.dmj.dwh.core;

import com.tcs.dmj.dwh.enums.MappingType;

/**
 * One field-to-field mapping for meta.meta_column_lineage (declare only critical fields).
 * sourceField may be nested with dots; targetTable is "schema.table".
 */
public record ColumnLineage(String sourceCollection, String sourceField, String targetTable, String targetColumn,
                            MappingType mappingType) {

    public static ColumnLineage of(String sourceCollection, String sourceField, String targetTable,
                                   String targetColumn, MappingType mappingType) {
        return new ColumnLineage(sourceCollection, sourceField, targetTable, targetColumn, mappingType);
    }
}
