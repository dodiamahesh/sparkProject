package com.de.cdc;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.conversions.Bson;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Option A of the duplicate check: before the writes of one destination collection are saved, find the
 * new records whose rule values already belong to a LIVE bronze document (or to an earlier record of the batch).
 *
 * <pre>
 *   checked:      writes that CREATE a new document = they have a new document and their _id is not in bronze yet
 *                 (insert, or an update / replace of a record that was rejected before)
 *   duplicate:    all rule fields equal to a live (_deleted: false) bronze document with another _id,
 *                 or to an earlier record of the same batch                -> first one wins
 *   not checked:  writes of an _id that is already in bronze (normal update / replace / delete, Kafka replay)
 *   field missing in the new record:  not checked, written normally, warning
 *   delete of a rejected _id (not in bronze): dropped, so no stub document is created in bronze
 * </pre>
 * Per destination and batch this costs 2 queries: which _ids exist, and which rule values exist.
 */
public class DuplicateDetector {

    private static final Logger log = LoggerFactory.getLogger(DuplicateDetector.class);
    private static final JsonWriterSettings JSON = JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build();

    /** Finds the record ids of a source that are already stored as duplicates (meta.meta_duplicate_record). */
    @FunctionalInterface
    public interface StoredLookup {
        Set<String> find(String source, Collection<String> recordIds);
    }

    /**
     * @param keep            writes to save, in their original order
     * @param duplicates      rejected writes (go to meta.meta_duplicate_record)
     * @param droppedDeletes  deletes of rejected records (not written, nothing to do)
     */
    public record Result(List<PendingWrite> keep, List<PendingWrite> duplicates, List<PendingWrite> droppedDeletes) { }

    private final long slowQueryMillis;

    public DuplicateDetector(long slowQueryMillis) {
        this.slowQueryMillis = slowQueryMillis;
    }

    /**
     * @param collection  the destination collection
     * @param writes      the writes of this destination in arrival order
     * @param rules       source -> rule fields (only active rules)
     */
    public Result filter(MongoCollection<BsonDocument> collection, List<PendingWrite> writes,
                         Map<String, List<String>> rules, StoredLookup storedLookup) {
        List<PendingWrite> ruled = new ArrayList<>();
        for (PendingWrite write : writes) {
            if (rules.containsKey(write.source())) {
                ruled.add(write);
            }
        }
        if (ruled.isEmpty()) {
            return new Result(writes, List.of(), List.of());
        }

        // 1. which _ids of the batch are already in bronze
        Set<BsonValue> ids = new LinkedHashSet<>();
        ruled.forEach(write -> ids.add(write.id()));
        Set<BsonValue> existingIds = new HashSet<>();
        collection.find(Filters.in("_id", new ArrayList<>(ids))).projection(Projections.include("_id"))
                .forEach(doc -> existingIds.add(doc.get("_id")));

        // 2. the writes that create a new document, and their rule key
        Map<PendingWrite, String> keyOfWrite = new IdentityHashMap<>();
        List<Bson> sameValues = new ArrayList<>();
        Set<String> projection = new LinkedHashSet<>();
        Set<List<String>> usedRules = new LinkedHashSet<>();
        for (PendingWrite write : ruled) {
            if (write.newDocument() == null || existingIds.contains(write.id())) {
                continue;
            }
            List<String> fields = rules.get(write.source());
            List<BsonValue> values = values(write.newDocument(), fields);
            if (values == null) {
                log.warn("{}: record {} has no value for a field of rule {}, written without duplicate check",
                        collection.getNamespace().getCollectionName(), recordId(write.id()), fields);
                continue;
            }
            keyOfWrite.put(write, key(fields, values));
            sameValues.add(filterFor(fields, values));
            projection.addAll(fields);
            usedRules.add(fields);
        }

        // 3. live bronze documents with the same rule values (one query)
        Map<String, BsonValue> ownerOfKey = new HashMap<>();
        Map<BsonValue, List<String>> keysOfId = new HashMap<>();
        if (!sameValues.isEmpty()) {
            projection.add("_id");
            long start = System.nanoTime();
            Bson query = Filters.and(Filters.eq("_deleted", false), Filters.or(sameValues));
            for (BsonDocument doc : collection.find(query).projection(Projections.include(new ArrayList<>(projection)))) {
                for (List<String> fields : usedRules) {
                    List<BsonValue> values = values(doc, fields);
                    if (values != null) {
                        claim(ownerOfKey, keysOfId, key(fields, values), doc.get("_id"));
                    }
                }
            }
            long millis = (System.nanoTime() - start) / 1_000_000;
            if (millis > slowQueryMillis) {
                log.warn("{}: duplicate check took {} ms for {} record(s). Create the unique index "
                        + "(sample-data/<pipeline>/03_bronze_unique_indexes.js)",
                        collection.getNamespace().getCollectionName(), millis, sameValues.size());
            }
        }

        // 4. deletes of _ids that are not in bronze: were they rejected in an earlier batch?
        Map<String, Set<String>> storedBySource = new HashMap<>();
        Map<String, List<String>> deleteIdsBySource = new LinkedHashMap<>();
        for (PendingWrite write : ruled) {
            if (write.isDelete() && !existingIds.contains(write.id())) {
                deleteIdsBySource.computeIfAbsent(write.source(), s -> new ArrayList<>()).add(recordId(write.id()));
            }
        }
        deleteIdsBySource.forEach((source, recordIds) -> storedBySource.put(source, storedLookup.find(source, recordIds)));

        // 5. decide, in arrival order (first one wins)
        List<PendingWrite> keep = new ArrayList<>();
        List<PendingWrite> duplicates = new ArrayList<>();
        List<PendingWrite> droppedDeletes = new ArrayList<>();
        Set<BsonValue> rejected = new HashSet<>();
        Set<BsonValue> writtenInBatch = new HashSet<>();
        for (PendingWrite write : writes) {
            if (!rules.containsKey(write.source())) {
                keep.add(write);
                continue;
            }
            BsonValue id = write.id();
            if (write.isDelete()) {
                boolean inBronze = existingIds.contains(id) || writtenInBatch.contains(id);
                boolean rejectedBefore = storedBySource.getOrDefault(write.source(), Set.of()).contains(recordId(id));
                if (!inBronze && (rejected.contains(id) || rejectedBefore)) {
                    droppedDeletes.add(write);
                    continue;
                }
                release(ownerOfKey, keysOfId, id); // a soft-deleted document no longer blocks anything
                keep.add(write);
                continue;
            }
            String key = keyOfWrite.get(write);
            if (key != null) {
                BsonValue owner = ownerOfKey.get(key);
                if (owner != null && !owner.equals(id)) {
                    duplicates.add(write);
                    rejected.add(id);
                    continue;
                }
                claim(ownerOfKey, keysOfId, key, id);
                rejected.remove(id);
            }
            if (write.newDocument() != null) {
                writtenInBatch.add(id);
            }
            keep.add(write);
        }
        return new Result(keep, duplicates, droppedDeletes);
    }

    private static void claim(Map<String, BsonValue> ownerOfKey, Map<BsonValue, List<String>> keysOfId,
                              String key, BsonValue id) {
        if (ownerOfKey.putIfAbsent(key, id) == null) {
            keysOfId.computeIfAbsent(id, i -> new ArrayList<>()).add(key);
        }
    }

    private static void release(Map<String, BsonValue> ownerOfKey, Map<BsonValue, List<String>> keysOfId, BsonValue id) {
        List<String> keys = keysOfId.remove(id);
        if (keys != null) {
            keys.forEach(key -> ownerOfKey.remove(key, id));
        }
    }

    /** { f1: v1, f2: v2, ... } for the query. */
    private static Bson filterFor(List<String> fields, List<BsonValue> values) {
        List<Bson> parts = new ArrayList<>();
        for (int i = 0; i < fields.size(); i++) {
            parts.add(Filters.eq(fields.get(i), values.get(i)));
        }
        return parts.size() == 1 ? parts.get(0) : Filters.and(parts);
    }

    /** The values of the rule fields, or null when a field is missing (an explicit null counts as a value). */
    static List<BsonValue> values(BsonDocument doc, List<String> fields) {
        List<BsonValue> values = new ArrayList<>();
        for (String field : fields) {
            BsonValue value = valueAt(doc, field);
            if (value == null) {
                return null;
            }
            values.add(value);
        }
        return values;
    }

    /** Value at a dot path ("address.pincode"), or null when it does not exist. Arrays are not walked into. */
    static BsonValue valueAt(BsonDocument doc, String path) {
        BsonValue current = doc;
        for (String part : path.split("\\.")) {
            if (current == null || !current.isDocument()) {
                return null;
            }
            current = current.asDocument().get(part);
        }
        return current;
    }

    /** Key of a record for one rule: "field=value" pairs; numbers compare by value (5 == 5L == 5.0). */
    static String key(List<String> fields, List<BsonValue> values) {
        StringBuilder key = new StringBuilder();
        for (int i = 0; i < fields.size(); i++) {
            key.append(fields.get(i)).append('=').append(canonical(values.get(i))).append('\u0001');
        }
        return key.toString();
    }

    static String canonical(BsonValue value) {
        try {
            if (value.isNumber()) {
                return "n:" + normalize(value.isDouble() ? BigDecimal.valueOf(value.asDouble().getValue())
                        : BigDecimal.valueOf(value.asNumber().longValue()));
            }
            if (value.isDecimal128()) {
                return "n:" + normalize(value.asDecimal128().getValue().bigDecimalValue());
            }
        } catch (NumberFormatException | ArithmeticException e) {
            // NaN, Infinity: compare as JSON below
        }
        if (value.isString()) {
            return "s:" + value.asString().getValue();
        }
        return "j:" + value.getBsonType() + ":" + toJson(value);
    }

    private static String normalize(BigDecimal number) {
        return number.signum() == 0 ? "0" : number.stripTrailingZeros().toPlainString();
    }

    /** Source _id as text for meta.meta_duplicate_record: a string as it is, anything else as relaxed JSON. */
    static String recordId(BsonValue id) {
        return id.isString() ? id.asString().getValue() : toJson(id);
    }

    /** Payload for meta.meta_duplicate_record: the new document, or the raw change event when there is none. */
    static String payloadJson(PendingWrite write) {
        return write.newDocument() != null ? write.newDocument().toJson(JSON) : write.message().value();
    }

    /** Relaxed Extended JSON of a single value, for example {"$oid": "6ae0..."} or 5. */
    private static String toJson(BsonValue value) {
        String wrapped = new BsonDocument("v", value).toJson(JSON);   // {"v": <value>}
        return wrapped.substring(wrapped.indexOf(':') + 1, wrapped.length() - 1).trim();
    }
}
