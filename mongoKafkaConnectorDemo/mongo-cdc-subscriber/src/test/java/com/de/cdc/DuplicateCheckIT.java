package com.de.cdc;

import com.de.cdc.ChangeEventApplier.FailedMessage;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Duplicate check (A + C) end to end, without Kafka: change events go straight into ChangeEventApplier.
 * Uses its own MongoDB database and PostgreSQL schema, both removed at the end. Skipped when MongoDB
 * (localhost:27017, rs0) or PostgreSQL (localhost:5433, user postgres, MAPPING_JDBC_PASSWORD or "root") is not running.
 * Run:  mvn test
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DuplicateCheckIT {

    private static final String MONGO_URI = "mongodb://localhost:27017/?replicaSet=rs0&serverSelectionTimeoutMS=3000";
    private static final String BRONZE = "zz_cdc_duplicate_it";
    private static final String JDBC = "jdbc:postgresql://localhost:5433/postgres";
    private static final String SCHEMA = "zz_cdc_duplicate_it";
    private static final String PASSWORD = System.getenv().getOrDefault("MAPPING_JDBC_PASSWORD", "root");

    private static MongoClient mongo;
    private static MongoDatabase bronze;
    private static ChangeEventApplier applier;
    private static long offset = 0;

    // ids of the test records
    private static final ObjectId A1 = new ObjectId();   // product P003, first one
    private static final ObjectId B = new ObjectId();    // product P003, duplicate of A1
    private static final ObjectId C1 = new ObjectId();   // product P004
    private static final ObjectId D = new ObjectId();    // product without product_code

    @BeforeAll
    static void setUp() throws Exception {
        try {
            mongo = MongoClients.create(MONGO_URI);
            mongo.getDatabase("admin").runCommand(new Document("ping", 1));
            try (Connection c = DriverManager.getConnection(JDBC, "postgres", PASSWORD)) {
                c.isValid(2);
            }
        } catch (Exception e) {
            assumeTrue(false, "MongoDB or PostgreSQL not running: " + e.getMessage());
        }
        bronze = mongo.getDatabase(BRONZE);
        bronze.drop();
        bronze.createCollection("products");

        sql("DROP SCHEMA IF EXISTS " + SCHEMA + " CASCADE",
            "CREATE SCHEMA " + SCHEMA,
            "CREATE TABLE " + SCHEMA + ".mapping (source VARCHAR(255), destination VARCHAR(255), db_type VARCHAR(10), active BOOLEAN)",
            "INSERT INTO " + SCHEMA + ".mapping VALUES ('products', 'products', 'NOSQL', TRUE)",
            "CREATE TABLE " + SCHEMA + ".rule (source VARCHAR(255), db_type VARCHAR(10), fields TEXT, active BOOLEAN, PRIMARY KEY (source, db_type))",
            "INSERT INTO " + SCHEMA + ".rule VALUES ('products', 'NOSQL', 'product_code', TRUE)",
            "CREATE TABLE " + SCHEMA + ".dup (id_pk BIGSERIAL PRIMARY KEY, record_id TEXT NOT NULL, data_store_name VARCHAR(255) NOT NULL,"
                    + " object_payload JSONB NOT NULL, db_type VARCHAR(10) NOT NULL, UNIQUE (db_type, data_store_name, record_id))");

        Properties config = new Properties();
        config.setProperty("mapping.jdbc.url", JDBC);
        config.setProperty("mapping.jdbc.user", "postgres");
        config.setProperty("mapping.jdbc.password", PASSWORD);
        config.setProperty("mapping.table", SCHEMA + ".mapping");
        config.setProperty("duplicate.rule.table", SCHEMA + ".rule");
        config.setProperty("duplicate.record.table", SCHEMA + ".dup");
        MetaDatabase metaDb = new MetaDatabase(config);
        applier = new ChangeEventApplier(bronze, false, new CollectionMappings(config, metaDb),
                new DuplicateRules(config, metaDb), new DuplicateRecordStore(config, metaDb), new DuplicateDetector(200));
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (bronze != null) {
            bronze.drop();
            sql("DROP SCHEMA IF EXISTS " + SCHEMA + " CASCADE");
            mongo.close();
        }
    }

    @Test
    @Order(1)
    void firstOneWinsInsideTheBatchAndMissingFieldIsNotChecked() throws Exception {
        apply(insert(A1, "P003", 100), insert(B, "P003", 90), insert(C1, "P004", 50), insertWithoutCode(D));

        assertEquals(3, products().countDocuments(), "A1, C1, D in bronze");
        assertNull(products().find(Filters.eq("_id", B)).first(), "B is a duplicate of A1");
        assertEquals(List.of(B.toHexString()), duplicateIds());
    }

    @Test
    @Order(2)
    void replayOfTheSameMessagesChangesNothing() throws Exception {
        apply(insert(A1, "P003", 100), insert(B, "P003", 90), insert(C1, "P004", 50), insertWithoutCode(D));

        assertEquals(3, products().countDocuments());
        assertEquals(1, duplicateRows(), "still one row for B (upsert)");
    }

    @Test
    @Order(3)
    void updateAndDeleteOfARejectedRecordDoNotReachBronze() throws Exception {
        apply(update(B, "P003", 95));                       // A1 still live -> still a duplicate
        assertNull(products().find(Filters.eq("_id", B)).first());
        assertEquals("95", duplicatePayload(B, "price"), "payload of the meta row updated");

        apply(delete(B));                                   // no stub document in bronze
        assertNull(products().find(Filters.eq("_id", B)).first());
        assertEquals(1, duplicateRows(), "meta row stays (history)");
    }

    @Test
    @Order(4)
    void afterTheFirstOneIsDeletedTheRejectedRecordCanComeIn() throws Exception {
        apply(delete(A1));
        assertTrue(products().find(Filters.eq("_id", A1)).first().getBoolean("_deleted"));

        apply(update(B, "P003", 96));                       // P003 has no live document now
        Document b = products().find(Filters.eq("_id", B)).first();
        assertEquals(96, b.getInteger("price"));
        assertFalse(b.getBoolean("_deleted"));
    }

    @Test
    @Order(5)
    void uniqueIndexRejectsAnUpdateThatMakesARecordEqualToAnother() throws Exception {
        products().createIndex(new Document("product_code", 1), new IndexOptions().name(DuplicateRules.INDEX_NAME)
                .unique(true).partialFilterExpression(new Document("_deleted", false)
                        .append("product_code", new Document("$exists", true))));

        // C1 is in bronze, so the query check (A) does not look at this update; the index (C) blocks it
        apply(update(C1, "P003", 55), insert(new ObjectId(), "P005", 10));

        assertEquals("P004", products().find(Filters.eq("_id", C1)).first().getString("product_code"),
                "C1 stays on its old version");
        assertEquals(1, products().countDocuments(Filters.eq("product_code", "P005")), "the write after it was applied");
        assertTrue(duplicateIds().contains(C1.toHexString()), "C1 update stored as duplicate");
    }

    // ---------------------------------------------------------------- helpers

    private static MongoCollection<Document> products() {
        return bronze.getCollection("products");
    }

    private static void apply(String... events) {
        List<ConsumerRecord<String, String>> list = new ArrayList<>();
        for (String event : events) {
            list.add(new ConsumerRecord<>("sourceMongo_db", 0, offset++, null, event));
        }
        List<FailedMessage> failed = applier.applyAll(
                new ConsumerRecords<>(Map.of(new TopicPartition("sourceMongo_db", 0), list), Map.of()));
        assertEquals(0, failed.size(), "no message should go to the DLQ: " + failed);
    }

    private static String insert(ObjectId id, String code, int price) {
        return event("insert", id, "{\"_id\": {\"$oid\": \"" + id.toHexString() + "\"}, \"product_code\": \"" + code
                + "\", \"price\": " + price + "}");
    }

    private static String update(ObjectId id, String code, int price) {
        return event("update", id, "{\"_id\": {\"$oid\": \"" + id.toHexString() + "\"}, \"product_code\": \"" + code
                + "\", \"price\": " + price + "}");
    }

    private static String insertWithoutCode(ObjectId id) {
        return event("insert", id, "{\"_id\": {\"$oid\": \"" + id.toHexString() + "\"}, \"price\": 1}");
    }

    private static String delete(ObjectId id) {
        return event("delete", id, null);
    }

    private static String event(String operation, ObjectId id, String fullDocument) {
        return "{\"operationType\": \"" + operation + "\", \"ns\": {\"db\": \"sourceMongo_db\", \"coll\": \"products\"},"
                + " \"documentKey\": {\"_id\": {\"$oid\": \"" + id.toHexString() + "\"}}"
                + (fullDocument == null ? "" : ", \"fullDocument\": " + fullDocument)
                + (operation.equals("update") ? ", \"updateDescription\": {\"updatedFields\": {}}" : "") + "}";
    }

    private static List<String> duplicateIds() throws Exception {
        List<String> ids = new ArrayList<>();
        query("SELECT record_id FROM " + SCHEMA + ".dup ORDER BY id_pk", rows -> {
            // record_id of an ObjectId is {"$oid": "<hex>"}
            ids.add(BsonDocument.parse("{\"v\": " + rows.getString(1) + "}").getObjectId("v").getValue().toHexString());
        });
        return ids;
    }

    private static int duplicateRows() throws Exception {
        int[] count = {0};
        query("SELECT count(*) FROM " + SCHEMA + ".dup", rows -> count[0] = rows.getInt(1));
        return count[0];
    }

    private static String duplicatePayload(ObjectId id, String field) throws Exception {
        String[] value = {null};
        query("SELECT object_payload->>'" + field + "' FROM " + SCHEMA + ".dup WHERE record_id LIKE '%" + id.toHexString() + "%'",
                rows -> value[0] = rows.getString(1));
        return value[0];
    }

    private interface RowHandler {
        void handle(ResultSet rows) throws Exception;
    }

    private static void query(String sql, RowHandler handler) throws Exception {
        try (Connection c = DriverManager.getConnection(JDBC, "postgres", PASSWORD);
             Statement s = c.createStatement();
             ResultSet rows = s.executeQuery(sql)) {
            while (rows.next()) {
                handler.handle(rows);
            }
        }
    }

    private static void sql(String... statements) throws Exception {
        try (Connection c = DriverManager.getConnection(JDBC, "postgres", PASSWORD);
             Statement s = c.createStatement()) {
            for (String statement : statements) {
                s.execute(statement);
            }
        }
    }
}
