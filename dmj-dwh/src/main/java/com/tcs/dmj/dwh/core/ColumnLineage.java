package com.tcs.dmj.dwh.core;

import com.tcs.dmj.dwh.enums.MappingType;

/**
 * One field-to-field mapping for meta.meta_column_lineage. Column lineage is SELECTIVE: declare it only for
 * critical fields (regulatory, reporting, calculated, aggregated or masked), in {@link EtlPipeline#columnLineage()}.
 *
 * <pre>
 *   ColumnLineage.of("orders", "items.unit_price",
 *                    "curated.fact_sales", "unit_price", MappingType.DIRECT)
 * </pre>
 *
 * @param sourceCollection MongoDB collection (raw layer)
 * @param sourceField      field, nested fields with dots
 * @param targetTable      "schema.table" in PostgreSQL
 * @param targetColumn     column of the target table
 */
public record ColumnLineage(String sourceCollection, String sourceField, String targetTable, String targetColumn,
                            MappingType mappingType) {

    public static ColumnLineage of(String sourceCollection, String sourceField, String targetTable,
                                   String targetColumn, MappingType mappingType) {
        return new ColumnLineage(sourceCollection, sourceField, targetTable, targetColumn, mappingType);
    }
}
