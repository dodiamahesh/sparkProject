package com.de.cdc;

import com.mongodb.bulk.BulkWriteResult;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Applies MongoDB change events to the target (bronze) database.
 *
 * Each Kafka message is one change event from the MongoDB source connector, for example:
 * <pre>
 * {
 *   "operationType": "update",
 *   "wallTime":     { "$date": { "$numberLong": "1793128500123" } },
 *   "ns":           { "db": "de_source_mongo", "coll": "student" },
 *   "documentKey":  { "_id": { "$oid": "6ae0e6da..." } },
 *   "fullDocument": { "_id": { "$oid": "6ae0e6da..." }, "name": "Dhanasvi", "marks": { "$numberInt": "98" } }
 * }
 * </pre>
 *
 * Every event is written to the collection with the same name ("ns.coll") in the target database:
 * <pre>
 *   insert / replace / update  ->  replace the whole document (insert it if it is missing)
 *   delete                     ->  keep the document, mark it _deleted: true (soft delete)
 *   drop                       ->  drop the collection (only when apply.drop=true)
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

    /** A message that could not be read (bad JSON or missing fields). SubscriberApp sends it to the DLQ topic. */
    public record FailedMessage(ConsumerRecord<String, String> message, Exception error) { }

    private final MongoDatabase targetDb;
    private final boolean applyDropEvents;

    public ChangeEventApplier(MongoDatabase targetDb, boolean applyDropEvents) {
        this.targetDb = targetDb;
        this.applyDropEvents = applyDropEvents;
    }

    /**
     * Applies all messages from one Kafka poll.
     *
     * The writes are collected per collection first and then saved with one bulk write per collection.
     * That is much faster than one database call per message. The bulk write keeps the list order,
     * so several changes to the same document are applied in the order they happened.
     *
     * @return the messages that could not be read
     */
    public List<FailedMessage> applyAll(ConsumerRecords<String, String> messages) {
        // collection name -> writes for that collection, in arrival order
        Map<String, List<WriteModel<BsonDocument>>> writesByCollection = new LinkedHashMap<>();
        List<FailedMessage> failedMessages = new ArrayList<>();
        // one ingestion time for the whole batch
        long ingestedAt = System.currentTimeMillis();

        for (ConsumerRecord<String, String> message : messages) {
            if (message.value() == null) {
                continue; // empty message (tombstone): nothing to apply
            }

            // Step 1: read the event. A message we cannot read is set aside; it must not stop the others.
            String operation;
            String collection;
            WriteModel<BsonDocument> write;
            try {
                // parse() turns Extended JSON back into the original types: {"$oid"} -> ObjectId, {"$numberInt"} -> Int32
                BsonDocument event = BsonDocument.parse(message.value());
                operation = event.getString("operationType").getValue();
                collection = getCollectionName(event);
                write = buildWrite(operation, event, ingestedAt);
            } catch (RuntimeException e) {
                failedMessages.add(new FailedMessage(message, e));
                continue;
            }

            // Step 2: queue the write, or handle events that are not about a single document
            if (write != null) {
                addWrite(writesByCollection, collection, write);
            } else if (operation.equals("drop")) {
                // save the changes that came before the drop first, then drop
                saveWrites(collection, writesByCollection.remove(collection));
                dropCollection(collection);
            } else {
                log.warn("Skipping '{}' event (partition {}, offset {})", operation, message.partition(), message.offset());
            }
        }

        // Step 3: save everything that was queued
        for (Map.Entry<String, List<WriteModel<BsonDocument>>> entry : writesByCollection.entrySet()) {
            saveWrites(entry.getKey(), entry.getValue());
        }
        return failedMessages;
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

    private static void addWrite(Map<String, List<WriteModel<BsonDocument>>> writesByCollection,
                                 String collection, WriteModel<BsonDocument> write) {
        List<WriteModel<BsonDocument>> writes = writesByCollection.get(collection);
        if (writes == null) {
            writes = new ArrayList<>();
            writesByCollection.put(collection, writes);
        }
        writes.add(write);
    }

    /** Saves the writes in one call. ordered = apply them in list order and stop at the first error. */
    private void saveWrites(String collection, List<WriteModel<BsonDocument>> writes) {
        if (writes == null || writes.isEmpty()) {
            return;
        }
        BulkWriteResult result = targetDb.getCollection(collection, BsonDocument.class)
                .bulkWrite(writes, new BulkWriteOptions().ordered(true));

        log.info("{}: {} writes (matched={}, inserted={})",
                collection, writes.size(), result.getMatchedCount(), result.getUpserts().size());
    }

    private void dropCollection(String collection) {
        if (applyDropEvents) {
            targetDb.getCollection(collection).drop();
            log.warn("{}: collection dropped, because it was dropped in the source", collection);
        } else {
            log.warn("{}: collection was dropped in the source; target kept because apply.drop=false", collection);
        }
    }
}
