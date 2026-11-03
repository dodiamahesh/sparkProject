package com.demo.etl.framework.source;

import com.demo.etl.framework.quality.QualityColumns;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * Describes ONE MongoDB collection that a pipeline reads.
 *
 * All fields are read as STRING on purpose (standard "bronze" practice):
 * a corrupt value like "12abc" in a number field is still read, and our CORRUPT rules can report it,
 * instead of Spark failing or silently turning it into NULL.
 *
 * <pre>
 *   SourceDefinition.builder("payments")
 *       .fields("paymentId", "amount", "updatedAt")
 *       .businessKey("paymentId")
 *       .latestRecordColumn("updatedAt")
 *       .build();
 * </pre>
 */
public record SourceDefinition(String collection, List<String> fields, String businessKey, String latestRecordColumn) {

    public SourceDefinition {
        if (!fields.contains(businessKey)) {
            throw new IllegalArgumentException("businessKey '" + businessKey + "' must be one of the fields of " + collection);
        }
        if (!fields.contains(latestRecordColumn)) {
            throw new IllegalArgumentException("latestRecordColumn '" + latestRecordColumn + "' must be one of the fields of " + collection);
        }
        fields = List.copyOf(fields);
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

    public static Builder builder(String collection) {
        return new Builder(collection);
    }

    public static final class Builder {
        private final String collection;
        private List<String> fields = List.of();
        private String businessKey;
        private String latestRecordColumn;

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

        public SourceDefinition build() {
            return new SourceDefinition(collection, fields, businessKey, latestRecordColumn);
        }
    }
}
