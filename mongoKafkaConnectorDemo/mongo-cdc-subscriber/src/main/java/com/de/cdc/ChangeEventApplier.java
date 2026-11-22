package com.de.cdc;

import com.de.cdc.DuplicateRecordStore.DuplicateRecord;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Applies MongoDB change events to the target (bronze) database.
 *
 * Each Kafka message is one change event from the MongoDB source connector, for example:
 * <pre>
 * {
 *   "operationType": "update",
 *   "wallTime":     { "$date": { "$numberLong": "1793128500123" } },
 *   "ns":           { "db": "sourceMongo_db", "coll": "student" },
 *   "documentKey":  { "_id": { "$oid": "6ae0e6da..." } },
 *   "fullDocument": { "_id": { "$oid": "6ae0e6da..." }, "name": "Dhanasvi", "marks": { "$numberInt": "98" } }
 * }
 * </pre>
 *
 * Messages from a SQL table use the same format ("ns.coll" = table name, "documentKey._id" = primary key).
 *
 * The target collection comes from the mapping table (see CollectionMappings): source "ns.coll" -> destination,
 * using the rows whose db_type = source.db.type (NOSQL = MongoDB collection, SQL = SQL table).
 * Before a message is written, two checks are done; if one fails, the message is NOT written and goes to the DLQ:
 * <pre>
 *   1. an ACTIVE mapping of this source type exists for the source collection/table (else MappingException)
 *   2. the destination collection already EXISTS in the target database           (else MappingException)
 * </pre>
 * The subscriber never creates a collection: create it yourself (db.createCollection) before you map it.
 *
 * Duplicate check (only for sources with an active rule in meta.cdc_duplicate_rule, see DuplicateDetector):
 * <pre>
 *   A  before writing: a new record whose rule values belong to a live bronze document (another _id)
 *      is not written                                                   -> meta.meta_duplicate_record
 *   C  while writing: the partial unique index "cdc_dup_rule" rejects a write (E11000)
 *      -> that write is not applied, the writes after it are sent again -> meta.meta_duplicate_record
 * </pre>
 *
 * Then the event is applied to the destination collection:
 * <pre>
 *   insert / replace / update  ->  replace the whole document (insert it if it is missing)
 *   delete                     ->  keep the document, mark it _deleted: true (soft delete)
 *   drop                       ->  drop the destination collection (only when apply.drop=true)
 * </pre>
 *
 * After the original fields, every bronze document gets 4 metadata fields, used by Spark (Bronze -> Silver):
 * <pre>
 *   _op           last operation: "insert", "update", "replace" or "delete"
 *   _deleted      true after a delete, otherwise false
 *   _source_ts    when the change happened in the source (epoch millis, from wallTime)
 *   _ingested_at  when this subscriber wrote it to bronze (epoch millis); Spark reads new data by this field
 * </pre>
 *
 * Replacing or updating by _id gives the same result even if a message is applied twice,
 * so reading the same messages again after a crash is safe.
 */
public class ChangeEventApplier {

    private static final Logger log = LoggerFactory.getLogger(ChangeEventApplier.class);

    /**
     * A message that could not be applied (bad JSON, missing fields, no mapping, destination collection missing).
     * SubscriberApp sends it to the DLQ topic.
     */
    public record FailedMessage(ConsumerRecord<String, String> message, Exception error) { }

    /** No active mapping for the source collection, or the destination collection does not exist. */
    public static class MappingException extends RuntimeException {
        public MappingException(String message) {
            super(message);
        }
    }

    private final MongoDatabase targetDb;
    private final boolean applyDropEvents;
    private final CollectionMappings mappings;
    private final DuplicateRules duplicateRules;
    private final DuplicateRecordStore duplicateStore;
    private final DuplicateDetector duplicateDetector;

    public ChangeEventApplier(MongoDatabase targetDb, boolean applyDropEvents, CollectionMappings mappings,
                              DuplicateRules duplicateRules, DuplicateRecordStore duplicateStore,
                              DuplicateDetector duplicateDetector) {
        this.targetDb = targetDb;
        this.applyDropEvents = applyDropEvents;
        this.mappings = mappings;
        this.duplicateRules = duplicateRules;
        this.duplicateStore = duplicateStore;
        this.duplicateDetector = duplicateDetector;
    }

    /**
     * Applies all messages from one Kafka poll.
     *
     * The writes are collected per collection first and then saved with one bulk write per collection.
     * That is much faster than one database call per message. The bulk write keeps the list order,
     * so several changes to the same document are applied in the order they happened.
     *
     * The mapping table, the duplicate rules and the list of existing target collections are read once per batch.
     * If a meta table cannot be read or the duplicate records cannot be saved, this throws and the batch is
     * not committed: it comes again after a restart (all writes are idempotent).
     *
     * @return the messages that could not be applied
     */
    public List<FailedMessage> applyAll(ConsumerRecords<String, String> messages) {
        // source collection/table -> destination collection, and the collections that exist in the target database
        Map<String, String> activeMappings = mappings.loadActive();
        // source collection/table -> fields of its duplicate rule
        Map<String, List<String>> rules = duplicateRules.loadActive();
        Set<String> existingCollections = targetDb.listCollectionNames().into(new HashSet<>());

        // destination collection -> writes for that collection, in arrival order
        Map<String, List<PendingWrite>> writesByCollection = new LinkedHashMap<>();
        List<DuplicateRecord> duplicates = new ArrayList<>();
        List<FailedMessage> failedMessages = new ArrayList<>();
        // one ingestion time for the whole batch
        long ingestedAt = System.currentTimeMillis();

        for (ConsumerRecord<String, String> message : messages) {
            if (message.value() == null) {
                continue; // empty message (tombstone): nothing to apply
            }

            // Step 1: read the event and find its destination. A message we cannot apply is set aside;
            // it must not stop the others.
            String operation;
            String destination = null;
            PendingWrite pending = null;
            try {
                // parse() turns Extended JSON back into the original types: {"$oid"} -> ObjectId, {"$numberInt"} -> Int32
                BsonDocument event = BsonDocument.parse(message.value());
                operation = event.getString("operationType").getValue();
                String collection = getCollectionName(event);
                WriteModel<BsonDocument> write = buildWrite(operation, event, ingestedAt);
                if (write != null || operation.equals("drop")) {
                    destination = resolveDestination(collection, activeMappings, existingCollections);
                }
                if (write != null) {
                    pending = new PendingWrite(message, collection, operation,
                            event.getDocument("documentKey").get("_id"), newDocument(operation, event), write);
                }
            } catch (RuntimeException e) {
                failedMessages.add(new FailedMessage(message, e));
                continue;
            }

            // Step 2: queue the write, or handle events that are not about a single document
            if (pending != null) {
                writesByCollection.computeIfAbsent(destination, d -> new ArrayList<>()).add(pending);
            } else if (operation.equals("drop")) {
                // save the changes that came before the drop first, then drop
                flush(destination, writesByCollection.remove(destination), rules, duplicates);
                if (dropCollection(destination)) {
                    existingCollections.remove(destination); // later events of this batch: destination is gone
                }
            } else {
                log.warn("Skipping '{}' event (partition {}, offset {})", operation, message.partition(), message.offset());
            }
        }

        // Step 3: duplicate check + save everything that was queued
        for (Map.Entry<String, List<PendingWrite>> entry : writesByCollection.entrySet()) {
            flush(entry.getKey(), entry.getValue(), rules, duplicates);
        }

        // Step 4: the rejected duplicates -> PostgreSQL (before the Kafka commit, so none is lost)
        duplicateStore.save(duplicates);
        return failedMessages;
    }

    /**
     * Duplicate check (A) of one destination, then the bulk write with the unique index safety net (C).
     * The rejected records are added to {@code duplicates}.
     */
    private void flush(String destination, List<PendingWrite> writes, Map<String, List<String>> rules,
                       List<DuplicateRecord> duplicates) {
        if (writes == null || writes.isEmpty()) {
            return;
        }
        MongoCollection<BsonDocument> collection = targetDb.getCollection(destination, BsonDocument.class);
        String dbType = mappings.sourceDbType();

        DuplicateDetector.Result checked = duplicateDetector.filter(collection, writes, rules,
                (source, recordIds) -> duplicateStore.findStored(source, dbType, recordIds));
        report(destination, checked.duplicates(), "rule check", dbType, duplicates);
        if (!checked.droppedDeletes().isEmpty()) {
            log.info("{}: {} delete(s) of rejected duplicate record(s) skipped (they are only in {})",
                    destination, checked.droppedDeletes().size(), duplicateStore.table());
        }

        List<PendingWrite> rejectedByIndex = saveWrites(collection, checked.keep());
        report(destination, rejectedByIndex, "unique index " + DuplicateRules.INDEX_NAME, dbType, duplicates);
    }

    /** Logs the rejected records (at most 10 lines) and adds them to the list for meta.meta_duplicate_record. */
    private void report(String destination, List<PendingWrite> rejected, String foundBy, String dbType,
                        List<DuplicateRecord> duplicates) {
        if (rejected.isEmpty()) {
            return;
        }
        log.info("{}: {} duplicate record(s) not written (found by {}), stored in {}",
                destination, rejected.size(), foundBy, duplicateStore.table());
        for (int i = 0; i < rejected.size(); i++) {
            PendingWrite write = rejected.get(i);
            String recordId = DuplicateDetector.recordId(write.id());
            if (i < 10) {
                log.info("  duplicate: {} {} ({})", write.source(), recordId, write.operation());
            }
            duplicates.add(new DuplicateRecord(recordId, write.source(), DuplicateDetector.payloadJson(write), dbType));
        }
    }

    /** The new full document of the event (insert, replace, update with fullDocument), else null. */
    private static BsonDocument newDocument(String operation, BsonDocument event) {
        if (operation.equals("delete")) {
            return null;
        }
        BsonValue fullDocument = event.get("fullDocument");
        return fullDocument != null && fullDocument.isDocument() ? fullDocument.asDocument() : null;
    }

    /**
     * Finds the destination collection of a source collection/table:
     * an active mapping of this subscriber's source type must exist, and the destination collection must already exist.
     * The subscriber never creates a collection, so it checks this before every write.
     */
    private String resolveDestination(String sourceCollection, Map<String, String> activeMappings,
                                      Set<String> existingCollections) {
        String dbType = mappings.sourceDbType();
        if (sourceCollection == null) {
            throw new MappingException("Event has no collection/table name (ns.coll)");
        }
        String destination = activeMappings.get(sourceCollection);
        if (destination == null) {
            throw new MappingException("No active " + dbType + " mapping for source '" + sourceCollection + "' in "
                    + mappings.table() + ". Add a row (source='" + sourceCollection + "', db_type='" + dbType
                    + "', active=true)");
        }
        if (!existingCollections.contains(destination)) {
            throw new MappingException("Destination collection '" + targetDb.getName() + "." + destination
                    + "' does not exist (mapped from '" + sourceCollection + "'). The subscriber does not create collections: "
                    + "run db.getSiblingDB('" + targetDb.getName() + "').createCollection('" + destination + "') first");
        }
        return destination;
    }

    /** Builds the MongoDB write for one event, or returns null when there is no document to write. */
    private static WriteModel<BsonDocument> buildWrite(String operation, BsonDocument event, long ingestedAt) {
        BsonDocument metadata = buildMetadata(operation, event, ingestedAt);
        switch (operation) {
            case "insert":
            case "replace":
                return replaceDocument(event, event.getDocument("fullDocument"), metadata);
            case "update":
                return updateDocument(event, metadata);
            case "delete":
                return markDeleted(event, metadata);
            default:
                // drop, rename, dropDatabase, invalidate: these are not changes to a single document
                return null;
        }
    }

    /** The 4 metadata fields that are added after the original fields of every bronze document. */
    private static BsonDocument buildMetadata(String operation, BsonDocument event, long ingestedAt) {
        BsonDocument metadata = new BsonDocument();
        metadata.put("_op", new BsonString(operation));
        metadata.put("_deleted", BsonBoolean.valueOf(operation.equals("delete")));
        metadata.put("_source_ts", new BsonInt64(getSourceTime(event, ingestedAt)));
        metadata.put("_ingested_at", new BsonInt64(ingestedAt));
        return metadata;
    }

    /**
     * Replaces the document with newDocument + metadata. upsert = insert it if it does not exist yet.
     * clone() keeps the event unchanged; the original fields stay first, the metadata comes after them.
     */
    private static WriteModel<BsonDocument> replaceDocument(BsonDocument event, BsonDocument newDocument,
                                                           BsonDocument metadata) {
        BsonDocument bronzeDocument = newDocument.clone();
        bronzeDocument.putAll(metadata);
        return new ReplaceOneModel<>(matchById(event), bronzeDocument, new ReplaceOptions().upsert(true));
    }

    /**
     * The source connector asks MongoDB for the full document after every update (updateLookup),
     * so normally we simply replace the whole document.
     *
     * If the document was deleted right after the update, MongoDB sends "fullDocument": null.
     * Then we apply only what the update changed: the changed fields ($set) and the removed fields ($unset).
     */
    private static WriteModel<BsonDocument> updateDocument(BsonDocument event, BsonDocument metadata) {
        BsonValue fullDocument = event.get("fullDocument");
        if (fullDocument != null && fullDocument.isDocument()) {
            return replaceDocument(event, fullDocument.asDocument(), metadata);
        }

        BsonDocument updateDescription = event.getDocument("updateDescription");

        // changed fields + metadata, for example { "marks": 99 }  ->  { "$set": { "marks": 99, "_op": "update", ... } }
        BsonDocument setFields = updateDescription.getDocument("updatedFields", new BsonDocument()).clone();
        setFields.putAll(metadata);
        BsonDocument changes = new BsonDocument("$set", setFields);

        // removed fields, for example [ "course" ]  ->  { "$unset": { "course": "" } }
        BsonDocument removedFields = new BsonDocument();
        for (BsonValue fieldName : updateDescription.getArray("removedFields", new BsonArray())) {
            removedFields.put(fieldName.asString().getValue(), new BsonString(""));
        }
        if (!removedFields.isEmpty()) {
            changes.put("$unset", removedFields);
        }
        return new UpdateOneModel<>(matchById(event), changes);
    }

    /**
     * Soft delete: the document stays in bronze with its last values, only the metadata changes
     * (_deleted: true). Spark sees it in the next run and marks the silver row as deleted.
     * upsert: if bronze never had the document, a small { _id, metadata } document is created.
     */
    private static WriteModel<BsonDocument> markDeleted(BsonDocument event, BsonDocument metadata) {
        return new UpdateOneModel<>(matchById(event), new BsonDocument("$set", metadata), new UpdateOptions().upsert(true));
    }

    /** Filter that finds the changed document: { "_id": documentKey._id } */
    private static Bson matchById(BsonDocument event) {
        BsonValue id = event.getDocument("documentKey").get("_id");
        return Filters.eq("_id", id);
    }

    /**
     * When the change happened in the source: wallTime, or clusterTime (seconds) on old MongoDB versions.
     * Events from startup.mode=copy_existing ("_id": { "copyingData": true }) have neither,
     * so for them we use the time the document was copied (ingestedAt).
     */
    private static long getSourceTime(BsonDocument event, long ingestedAt) {
        if (event.containsKey("wallTime")) {
            return event.getDateTime("wallTime").getValue();
        }
        if (event.containsKey("clusterTime")) {
            return event.getTimestamp("clusterTime").getTime() * 1000L;
        }
        return ingestedAt;
    }

    /** Collection name from "ns.coll". dropDatabase and invalidate events have no collection, so this returns null. */
    private static String getCollectionName(BsonDocument event) {
        if (!event.containsKey("ns")) {
            return null;
        }
        BsonDocument namespace = event.getDocument("ns");
        if (!namespace.containsKey("coll")) {
            return null;
        }
        return namespace.getString("coll").getValue();
    }

    /**
     * Saves the writes with as few calls as possible. ordered = apply them in list order and stop at the first error,
     * so several changes of one document keep their order.
     *
     * Option C: when the unique index of the duplicate rule rejects a write (E11000 on index "cdc_dup_rule"),
     * the writes before it are already applied; that write is set aside as a duplicate and the writes after it
     * are sent again. Any other error is thrown (the batch is then not committed).
     *
     * @return the writes rejected by the unique index
     */
    private List<PendingWrite> saveWrites(MongoCollection<BsonDocument> collection, List<PendingWrite> writes) {
        List<PendingWrite> rejected = new ArrayList<>();
        if (writes.isEmpty()) {
            return rejected;
        }
        int matched = 0;
        int inserted = 0;
        int start = 0;
        while (start < writes.size()) {
            List<PendingWrite> part = writes.subList(start, writes.size());
            List<WriteModel<BsonDocument>> models = new ArrayList<>(part.size());
            part.forEach(write -> models.add(write.write()));
            try {
                BulkWriteResult result = collection.bulkWrite(models, new BulkWriteOptions().ordered(true));
                matched += result.getMatchedCount();
                inserted += result.getUpserts().size();
                break;
            } catch (MongoBulkWriteException e) {
                BulkWriteError error = e.getWriteErrors().get(0);
                if (!isDuplicateRuleViolation(error)) {
                    throw e;
                }
                matched += e.getWriteResult().getMatchedCount();
                inserted += e.getWriteResult().getUpserts().size();
                rejected.add(part.get(error.getIndex()));
                start += error.getIndex() + 1;
            }
        }
        log.info("{}: {} writes (matched={}, inserted={}{})", collection.getNamespace().getCollectionName(),
                writes.size() - rejected.size(), matched, inserted,
                rejected.isEmpty() ? "" : ", rejected by unique index=" + rejected.size());
        return rejected;
    }

    /** E11000 duplicate key on the duplicate rule index (not on _id or another index). */
    private static boolean isDuplicateRuleViolation(BulkWriteError error) {
        return error.getCode() == 11000 && error.getMessage() != null
                && error.getMessage().contains(DuplicateRules.INDEX_NAME);
    }

    /** @return true when the destination collection was dropped */
    private boolean dropCollection(String collection) {
        if (applyDropEvents) {
            targetDb.getCollection(collection).drop();
            log.warn("{}: collection dropped, because its source collection was dropped. It is NOT created again: "
                    + "create it yourself before new events arrive", collection);
            return true;
        }
        log.warn("{}: source collection was dropped; target kept because apply.drop=false", collection);
        return false;
    }
}
