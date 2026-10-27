package com.de.cdc;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Turns MongoDB change events (canonical Extended JSON from the MongoDB source connector)
 * into writes on the target database. Every write is idempotent, so replaying a message is safe.
 */
public class ChangeEventApplier {

    private static final Logger log = LoggerFactory.getLogger(ChangeEventApplier.class);
    private static final ReplaceOptions UPSERT = new ReplaceOptions().upsert(true);
    private static final BulkWriteOptions ORDERED = new BulkWriteOptions().ordered(true);

    private final MongoDatabase target;
    private final boolean applyDrop;

    /** op = operationType, coll = ns.coll, write = null when the event needs no document write */
    private record Change(String op, String coll, WriteModel<BsonDocument> write) { }

    public ChangeEventApplier(MongoClient mongo, String targetDatabase, boolean applyDrop) {
        this.target = mongo.getDatabase(targetDatabase);
        this.applyDrop = applyDrop;
    }

    /**
     * Applies one poll's records. Writes are grouped per collection and sent as ordered bulk writes.
     * All changes to one document share a key, so they sit in one partition and keep their order.
     * Records that cannot be parsed go to onBadRecord; MongoDB errors are thrown to the caller.
     */
    public void apply(ConsumerRecords<String, String> records,
                      BiConsumer<ConsumerRecord<String, String>, Exception> onBadRecord) {
        Map<String, List<WriteModel<BsonDocument>>> pending = new LinkedHashMap<>();

        for (ConsumerRecord<String, String> rec : records) {
            if (rec.value() == null) {
                continue; // tombstone
            }
            Change change;
            try {
                change = parse(rec.value());
            } catch (RuntimeException e) {
                onBadRecord.accept(rec, e);
                continue;
            }

            if (change.write() != null) {
                pending.computeIfAbsent(change.coll(), c -> new ArrayList<>()).add(change.write());
            } else if ("drop".equals(change.op())) {
                // writes queued before the drop must land first
                flush(change.coll(), pending.remove(change.coll()));
                drop(change.coll());
            } else {
                log.warn("Skipping '{}' event (partition {}, offset {})", change.op(), rec.partition(), rec.offset());
            }
        }
        pending.forEach(this::flush);
    }

    private static Change parse(String json) {
        BsonDocument event = BsonDocument.parse(json); // canonical Extended JSON -> original BSON types
        String op = event.getString("operationType").getValue();
        String coll = event.containsKey("ns") && event.getDocument("ns").containsKey("coll")
                ? event.getDocument("ns").getString("coll").getValue()
                : null;

        WriteModel<BsonDocument> write = switch (op) {
            case "insert", "replace" -> new ReplaceOneModel<>(byId(event), event.getDocument("fullDocument"), UPSERT);
            case "update" -> {
                BsonValue full = event.get("fullDocument");
                // updateLookup gives the whole document; it is null when the document was deleted meanwhile
                yield full != null && full.isDocument()
                        ? new ReplaceOneModel<>(byId(event), full.asDocument(), UPSERT)
                        : fromUpdateDescription(byId(event), event.getDocument("updateDescription"));
            }
            case "delete" -> new DeleteOneModel<>(byId(event));
            default -> null; // drop, rename, dropDatabase, invalidate
        };
        return new Change(op, coll, write);
    }

    private static Bson byId(BsonDocument event) {
        return Filters.eq("_id", event.getDocument("documentKey").get("_id"));
    }

    private static WriteModel<BsonDocument> fromUpdateDescription(Bson byId, BsonDocument desc) {
        BsonDocument update = new BsonDocument();
        BsonDocument set = desc.getDocument("updatedFields", new BsonDocument());
        if (!set.isEmpty()) {
            update.put("$set", set);
        }
        BsonDocument unset = new BsonDocument();
        for (BsonValue field : desc.getArray("removedFields", new BsonArray())) {
            unset.put(field.asString().getValue(), new BsonString(""));
        }
        if (!unset.isEmpty()) {
            update.put("$unset", unset);
        }
        return update.isEmpty() ? null : new UpdateOneModel<>(byId, update);
    }

    private void flush(String coll, List<WriteModel<BsonDocument>> writes) {
        if (writes == null || writes.isEmpty()) {
            return;
        }
        BulkWriteResult r = target.getCollection(coll, BsonDocument.class).bulkWrite(writes, ORDERED);
        log.info("{}: {} writes (matched={}, upserted={}, deleted={})",
                coll, writes.size(), r.getMatchedCount(), r.getUpserts().size(), r.getDeletedCount());
    }

    private void drop(String coll) {
        if (applyDrop) {
            target.getCollection(coll).drop();
            log.warn("{}: collection dropped (source collection was dropped)", coll);
        } else {
            log.warn("{}: source collection was dropped; target kept because apply.drop=false", coll);
        }
    }
}
