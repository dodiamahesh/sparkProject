package com.tcs.dmj.dwh.connector.source;

import com.tcs.dmj.dwh.exception.PipelineConfigurationException;
import com.tcs.dmj.dwh.validation.QualityColumns;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * The data contract of one MongoDB collection (example: VatReturnSources.VAT_RETURNS).
 * Every field is read as String, so a corrupt value ("12abc") is reported by a CORRUPT rule.
 */
public record SourceDefinition(
        String collection,
        List<String> fields,
        String businessKey,
        String latestRecordColumn,
        Double maxRejectPercent,
        String sourceFilter,
        String softDeleteField,
        SourceMetadata metadata) {

    public static final String TEST_RECORD_FIELD = "isTest";
    public static final String DEFAULT_SOFT_DELETE_FIELD = "isDeleted";

    public record SourceMetadata(String description, String owner, String sla) {
    }

    public SourceDefinition {
        requireField(fields, businessKey, "businessKey", collection);
        requireField(fields, latestRecordColumn, "latestRecordColumn", collection);
        if (maxRejectPercent != null && (maxRejectPercent < 0 || maxRejectPercent > 100)) {
            throw new PipelineConfigurationException("maxRejectPercent of " + collection + " must be between 0 and 100");
        }
        fields = List.copyOf(fields);
    }

    private static void requireField(List<String> fields, String field, String role, String collection) {
        if (!fields.contains(field)) {
            throw new PipelineConfigurationException(role + " '" + field + "' must be one of the fields of " + collection);
        }
    }

    /** _id + every field (+ the soft-delete flag), all String. */
    public StructType schema() {
        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField(QualityColumns.SOURCE_ID, DataTypes.StringType, false));
        for (String field : fields) {
            structFields.add(DataTypes.createStructField(field, DataTypes.StringType, true));
        }
        if (softDeleteField != null && !fields.contains(softDeleteField)) {
            structFields.add(DataTypes.createStructField(softDeleteField, DataTypes.StringType, true));
        }
        return DataTypes.createStructType(structFields);
    }

    /** " Source owner: x." for error messages, or "". */
    public String ownerHint() {
        return metadata.owner() == null ? "" : " Source owner: " + metadata.owner() + ".";
    }

    public static Builder builder(String collection) {
        return new Builder(collection);
    }

    public static final class Builder {
        private final String collection;
        private List<String> fields = List.of();
        private String businessKey;
        private String latestRecordColumn;
        private Double maxRejectPercent;
        private String sourceFilter;
        private String softDeleteField;
        private String description;
        private String owner;
        private String sla;

        private Builder(String collection) {
            this.collection = collection;
        }

        public Builder fields(String... fields) {
            this.fields = List.of(fields);
            return this;
        }

        /** Identifies a record: duplicate check, upsert key, rejected table. */
        public Builder businessKey(String businessKey) {
            this.businessKey = businessKey;
            return this;
        }

        /** Timestamp field (BSON Date, indexed): incremental watermark + "latest wins" on duplicates. */
        public Builder latestRecordColumn(String latestRecordColumn) {
            this.latestRecordColumn = latestRecordColumn;
            return this;
        }

        /** More than this % bad rows (duplicates not counted) -> the run FAILS before loading. */
        public Builder maxRejectPercent(double percent) {
            this.maxRejectPercent = percent;
            return this;
        }

        /** Extra MongoDB $match, e.g. {@code {"country": "India"}}. */
        public Builder sourceFilter(String mongoMatchJson) {
            this.sourceFilter = mongoMatchJson;
            return this;
        }

        /** Skips {"isTest": true} documents. */
        public Builder excludeTestRecords() {
            return sourceFilter("{\"" + TEST_RECORD_FIELD + "\": {\"$ne\": true}}");
        }

        /** isDeleted = true at the source -> the key is soft-deleted in the target tables (not validated). */
        public Builder softDelete() {
            return softDelete(DEFAULT_SOFT_DELETE_FIELD);
        }

        /** Same with another flag field. true / 1 / Y / yes mean deleted. */
        public Builder softDelete(String flagField) {
            this.softDeleteField = flagField;
            return this;
        }

        public Builder description(String description) {
            this.description = description;
            return this;
        }

        public Builder owner(String owner) {
            this.owner = owner;
            return this;
        }

        public Builder sla(String sla) {
            this.sla = sla;
            return this;
        }

        public SourceDefinition build() {
            return new SourceDefinition(collection, fields, businessKey, latestRecordColumn, maxRejectPercent,
                    sourceFilter, softDeleteField, new SourceMetadata(description, owner, sla));
        }
    }
}
