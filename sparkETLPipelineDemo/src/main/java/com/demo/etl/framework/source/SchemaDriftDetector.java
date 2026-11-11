package com.demo.etl.framework.source;

import com.demo.etl.config.EtlProperties;
import com.demo.etl.framework.pipeline.LoadType;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * SCHEMA DRIFT check: compares the fields really present in MongoDB with the fields of the
 * {@link SourceDefinition}, BEFORE the data is read.
 *
 *   new field     = present in MongoDB, unknown to the definition  -> data we silently ignore
 *   missing field = expected by the definition, present in NO sampled document -> source renamed / dropped it
 *
 * Only a sample (etl.quality.schema-sample-size documents) of the documents of THIS run is inspected,
 * with one aggregation that runs inside MongoDB.
 */
@Component
public class SchemaDriftDetector {

    /** Result of the check. Both lists are empty when the schema is as expected. */
    public record SchemaDrift(long sampledDocuments, Map<String, Long> newFields, List<String> missingFields) {

        public boolean hasDrift() {
            return !newFields.isEmpty() || !missingFields.isEmpty();
        }

        /** One readable line per drifted field. */
        public List<String> messages() {
            List<String> messages = new ArrayList<>();
            newFields.forEach((field, count) -> messages.add("New field '" + field + "' found in " + count + " of "
                    + sampledDocuments + " sampled documents (not in the source definition, so it is not loaded)"));
            missingFields.forEach(field -> messages.add("Expected field '" + field + "' is missing in all "
                    + sampledDocuments + " sampled documents"));
            return messages;
        }
    }

    private final MongoTemplate mongoTemplate;
    private final MongoSourceReader reader;
    private final int sampleSize;

    public SchemaDriftDetector(MongoTemplate mongoTemplate, MongoSourceReader reader, EtlProperties properties) {
        this.mongoTemplate = mongoTemplate;
        this.reader = reader;
        this.sampleSize = properties.quality().schemaSampleSize();
    }

    public SchemaDrift detect(SourceDefinition source, LoadType loadType) {
        // field name -> number of sampled documents that have it
        List<Document> pipeline = new ArrayList<>(reader.matchStages(source, loadType));
        pipeline.add(new Document("$limit", sampleSize));
        pipeline.add(Document.parse("{\"$project\": {\"f\": {\"$objectToArray\": \"$$ROOT\"}}}"));
        pipeline.add(new Document("$unwind", "$f"));
        pipeline.add(Document.parse("{\"$group\": {\"_id\": \"$f.k\", \"n\": {\"$sum\": 1}}}"));

        Map<String, Long> fieldCounts = new TreeMap<>();
        for (Document row : mongoTemplate.getCollection(source.collection()).aggregate(pipeline)) {
            fieldCounts.put(row.getString("_id"), ((Number) row.get("n")).longValue());
        }

        // every document has _id, so its count = number of sampled documents
        long sampled = fieldCounts.getOrDefault("_id", 0L);
        if (sampled == 0) {
            return new SchemaDrift(0, Map.of(), List.of());   // nothing to read in this run
        }

        Set<String> known = new HashSet<>(source.fields());
        known.addAll(source.ignoredFields());
        known.addAll(reader.technicalFields());

        Map<String, Long> newFields = new TreeMap<>();
        fieldCounts.forEach((field, count) -> {
            if (!known.contains(field)) {
                newFields.put(field, count);
            }
        });
        List<String> missingFields = source.fields().stream()
                .filter(field -> !fieldCounts.containsKey(field))
                .toList();
        return new SchemaDrift(sampled, newFields, missingFields);
    }
}
