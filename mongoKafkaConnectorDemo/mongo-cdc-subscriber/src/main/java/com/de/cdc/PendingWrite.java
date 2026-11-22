package com.de.cdc;

import com.mongodb.client.model.WriteModel;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/**
 * One change event, ready to be written to its destination collection.
 *
 * @param message      the Kafka message it came from (for the DLQ / duplicate payload)
 * @param source       source collection / table (ns.coll)
 * @param operation    insert, update, replace or delete
 * @param id           documentKey._id
 * @param newDocument  the full new document (insert, replace, update with fullDocument); null for delete and
 *                     for an update without fullDocument. Only writes with a newDocument can create a document.
 * @param write        the MongoDB write
 */
public record PendingWrite(ConsumerRecord<String, String> message, String source, String operation, BsonValue id,
                           BsonDocument newDocument, WriteModel<BsonDocument> write) {

    public boolean isDelete() {
        return "delete".equals(operation);
    }
}
