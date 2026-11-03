package com.demo.etl.framework.source;

import com.demo.etl.config.EtlProperties;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.WriteModel;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * Writes the processedFlag back to MongoDB after a successful job, so the next INCREMENTAL
 * run does not read the same documents again.
 *
 * Runs on the Spark executors (foreachPartition): every partition opens its own MongoDB
 * connection and sends bulk updates of 1000 documents. This scales to millions of rows
 * because no ids are collected on the driver.
 */
@Component
public class MongoFlagUpdater {

    private final SparkSession spark;
    private final EtlProperties.Mongo mongo;
    private final String flagField;

    public MongoFlagUpdater(SparkSession spark, EtlProperties properties) {
        this.spark = spark;
        this.mongo = properties.mongo();
        this.flagField = properties.source().flagField();
    }

    /**
     * @param ids dataset whose FIRST column is the MongoDB _id
     * @return number of documents matched
     */
    public long updateFlag(String collection, Dataset<Row> ids, SourceFlag flag, long jobRunId) {
        LongAccumulator matched = spark.sparkContext().longAccumulator("flag-update-" + collection);
        ids.foreachPartition(new FlagUpdateTask(mongo.uri(), mongo.database(), collection,
                flagField, flag.code(), jobRunId, matched));
        return matched.value();
    }

    /**
     * Code that runs on the executors. It must be Serializable, so it only holds simple values
     * (Strings / numbers) and creates the MongoClient inside call().
     */
    private static final class FlagUpdateTask implements ForeachPartitionFunction<Row> {

        private static final int BATCH_SIZE = 1000;

        private final String uri;
        private final String database;
        private final String collection;
        private final String flagField;
        private final String flagValue;
        private final long jobRunId;
        private final LongAccumulator matched;

        FlagUpdateTask(String uri, String database, String collection, String flagField,
                       String flagValue, long jobRunId, LongAccumulator matched) {
            this.uri = uri;
            this.database = database;
            this.collection = collection;
            this.flagField = flagField;
            this.flagValue = flagValue;
            this.jobRunId = jobRunId;
            this.matched = matched;
        }

        @Override
        public void call(Iterator<Row> rows) {
            if (!rows.hasNext()) {
                return;
            }
            try (MongoClient client = MongoClients.create(uri)) {
                MongoCollection<Document> target = client.getDatabase(database).getCollection(collection);
                Bson update = Updates.combine(
                        Updates.set(flagField, flagValue),
                        Updates.set("etlJobRunId", jobRunId),
                        Updates.set("etlProcessedAt", new Date()));

                List<WriteModel<Document>> batch = new ArrayList<>(BATCH_SIZE);
                while (rows.hasNext()) {
                    Object id = toMongoId(rows.next().getString(0));
                    batch.add(new UpdateOneModel<>(Filters.eq("_id", id), update));
                    if (batch.size() == BATCH_SIZE) {
                        flush(target, batch);
                    }
                }
                flush(target, batch);
            }
        }

        private void flush(MongoCollection<Document> target, List<WriteModel<Document>> batch) {
            if (!batch.isEmpty()) {
                matched.add(target.bulkWrite(batch, new BulkWriteOptions().ordered(false)).getMatchedCount());
                batch.clear();
            }
        }

        /** Spark gives us the _id as a hex String. MongoDB needs the real ObjectId to find the document. */
        private static Object toMongoId(String id) {
            return ObjectId.isValid(id) ? new ObjectId(id) : id;
        }
    }
}
