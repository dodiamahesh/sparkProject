package com.demo.etl.framework.source;

import com.demo.etl.framework.quality.PiiType;
import com.demo.etl.framework.quality.QualityColumns;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Describes ONE MongoDB collection that a pipeline reads: the "data contract" of that source.
 *
 * All fields are read as STRING on purpose (standard "bronze" practice):
 * a corrupt value like "12abc" in a number field is still read, and our CORRUPT rules can report it,
 * instead of Spark failing or silently turning it into NULL.
 *
 * <pre>
 *   SourceDefinition.builder("payments")
 *       .fields("paymentId", "payerEmail", "amount", "updatedAt")
 *       .businessKey("paymentId")
 *       .latestRecordColumn("updatedAt")
 *       // ---- optional ----
 *       .excludeTestRecords()                     // source filter: skip {"isTest": true}
 *       .maxRejectPercent(30)                     // quality gate: more bad rows -> job FAILS
 *       .expectedRows(1, 50_000)                  // volume check (INCREMENTAL): outside -> warning
 *       .maskEmail("payerEmail")                  // PII: masked in silver + rejected table
 *       .owner("payments-team@demo.com")          // metadata: who to ask when it breaks
 *       .sla("new data every hour")
 *       .description("Bill payments from the payment app")
 *       .build();
 * </pre>
 *
 * @param pii           field -> how to mask it (empty = no PII)
 * @param sourceFilter  extra MongoDB $match (JSON), applied for every load type; null = none
 * @param ignoredFields fields that exist in MongoDB but are not read on purpose (no schema-drift warning)
 */
public record SourceDefinition(
        String collection,
        List<String> fields,
        String businessKey,
        String latestRecordColumn,
        QualityChecks checks,
        Map<String, PiiType> pii,
        String sourceFilter,
        Set<String> ignoredFields,
        SourceMetadata metadata) {

    /** Field set on test documents in MongoDB; {@link Builder#excludeTestRecords()} skips them. */
    public static final String TEST_RECORD_FIELD = "isTest";

    /**
     * @param maxRejectPercent  null = no gate. Duplicates do NOT count as bad rows.
     * @param minRows           null = no minimum (volume check, INCREMENTAL only)
     * @param maxRows           null = no maximum (volume check, INCREMENTAL only)
     * @param failOnSchemaDrift false = new / missing fields give a warning, true = the job fails
     */
    public record QualityChecks(Double maxRejectPercent, Long minRows, Long maxRows, boolean failOnSchemaDrift) {
    }

    /** Documentation of the source; shown in GET /api/etl/pipelines and in quality-gate errors. */
    public record SourceMetadata(String description, String owner, String sla) {
    }

    public SourceDefinition {
        if (!fields.contains(businessKey)) {
            throw new IllegalArgumentException("businessKey '" + businessKey + "' must be one of the fields of " + collection);
        }
        if (!fields.contains(latestRecordColumn)) {
            throw new IllegalArgumentException("latestRecordColumn '" + latestRecordColumn + "' must be one of the fields of " + collection);
        }
        for (String piiField : pii.keySet()) {
            if (!fields.contains(piiField)) {
                throw new IllegalArgumentException("PII field '" + piiField + "' must be one of the fields of " + collection);
            }
        }
        if (checks.maxRejectPercent() != null && (checks.maxRejectPercent() < 0 || checks.maxRejectPercent() > 100)) {
            throw new IllegalArgumentException("maxRejectPercent of " + collection + " must be between 0 and 100");
        }
        fields = List.copyOf(fields);
        pii = Map.copyOf(pii);
        ignoredFields = Set.copyOf(ignoredFields);
    }

    /** Spark schema: _id + every field, all as String. */
    public StructType schema() {
        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField(QualityColumns.SOURCE_ID, DataTypes.StringType, false));
        for (String field : fields) {
            structFields.add(DataTypes.createStructField(field, DataTypes.StringType, true));
        }
        return DataTypes.createStructType(structFields);
    }

    /** "Source owner: x" for error messages, or "" when no owner is set. */
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
        private Long minRows;
        private Long maxRows;
        private boolean failOnSchemaDrift;
        private final Map<String, PiiType> pii = new LinkedHashMap<>();
        private String sourceFilter;
        private final Set<String> ignoredFields = new LinkedHashSet<>();
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

        /** Column that uniquely identifies a record (used for duplicate check and rejected table). */
        public Builder businessKey(String businessKey) {
            this.businessKey = businessKey;
            return this;
        }

        /** When the same business key appears twice, the row with the highest value here is kept. */
        public Builder latestRecordColumn(String latestRecordColumn) {
            this.latestRecordColumn = latestRecordColumn;
            return this;
        }

        // ------------------------------------------------------------ quality gate + checks

        /**
         * QUALITY GATE: if more than {@code percent} % of the rows read are rejected (duplicates not counted),
         * the job FAILS before this collection is loaded, so a broken source cannot fill silver with gaps.
         * Not applied to RETRY_REJECTED (it reads only bad rows) or to very small batches
         * (fewer than etl.quality.reject-gate-min-records rows).
         */
        public Builder maxRejectPercent(double percent) {
            this.maxRejectPercent = percent;
            return this;
        }

        /** VOLUME CHECK (INCREMENTAL only): fewer than min or more than max rows read -> warning. */
        public Builder expectedRows(long min, long max) {
            this.minRows = min;
            this.maxRows = max;
            return this;
        }

        /** SCHEMA DRIFT: by default new / missing fields give a warning; with this the job fails instead. */
        public Builder failOnSchemaDrift() {
            this.failOnSchemaDrift = true;
            return this;
        }

        /** Fields that exist in MongoDB but are not read on purpose (no schema-drift warning for them). */
        public Builder ignoreFields(String... fieldNames) {
            ignoredFields.addAll(List.of(fieldNames));
            return this;
        }

        // ------------------------------------------------------------ PII

        /** "rahul.shah@gmail.com" -> "r****@gmail.com" in silver, in the rejected table and in error messages. */
        public Builder maskEmail(String... fieldNames) {
            for (String field : fieldNames) {
                pii.put(field, PiiType.EMAIL);
            }
            return this;
        }

        /** "9876543210" -> "XXXXXX3210" in silver, in the rejected table and in error messages. */
        public Builder maskPhone(String... fieldNames) {
            for (String field : fieldNames) {
                pii.put(field, PiiType.PHONE);
            }
            return this;
        }

        // ------------------------------------------------------------ source filter

        /** Extra MongoDB $match, e.g. {@code {"country": "India"}}. Documents that do not match are never read. */
        public Builder sourceFilter(String mongoMatchJson) {
            this.sourceFilter = mongoMatchJson;
            return this;
        }

        /** Skips test documents ({"isTest": true}). Documents without the field are read normally. */
        public Builder excludeTestRecords() {
            ignoredFields.add(TEST_RECORD_FIELD);
            return sourceFilter("{\"" + TEST_RECORD_FIELD + "\": {\"$ne\": true}}");
        }

        // ------------------------------------------------------------ metadata

        public Builder description(String description) {
            this.description = description;
            return this;
        }

        /** Team / person to contact when this source sends bad data. */
        public Builder owner(String owner) {
            this.owner = owner;
            return this;
        }

        /** When new data is expected, e.g. "every hour" or "daily by 06:00". */
        public Builder sla(String sla) {
            this.sla = sla;
            return this;
        }

        public SourceDefinition build() {
            return new SourceDefinition(collection, fields, businessKey, latestRecordColumn,
                    new QualityChecks(maxRejectPercent, minRows, maxRows, failOnSchemaDrift),
                    pii, sourceFilter, ignoredFields, new SourceMetadata(description, owner, sla));
        }
    }
}
