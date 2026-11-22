package com.de.cdc;

import com.mongodb.client.MongoDatabase;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Reads the duplicate rules from the PostgreSQL meta table (default meta.cdc_duplicate_rule):
 * <pre>
 *   source       db_type   fields                     active
 *   products     NOSQL     product_code               true
 *   customers    NOSQL     name,email,phone           true    -> duplicate only when ALL 3 values are equal
 * </pre>
 * A new record (a write that creates a new document in bronze) is a duplicate when a LIVE bronze document
 * (_deleted: false) with a different _id has the same values for all fields of the rule.
 * A source without an active rule is not checked.
 *
 * Fields are comma separated; nested fields use dot notation ("address.pincode"). _id and the subscriber's
 * metadata fields cannot be part of a rule (the same _id is the same record, not a duplicate).
 *
 * Safety net (option C): a partial unique index named {@link #INDEX_NAME} on the same fields in the
 * destination collection (sample-data/&lt;pipeline&gt;/03_bronze_unique_indexes.js). The subscriber never creates it,
 * it only warns at start when the index is missing or does not match the rule.
 */
public class DuplicateRules {

    private static final Logger log = LoggerFactory.getLogger(DuplicateRules.class);

    /** Name of the partial unique index (option C) in every destination collection that has a rule. */
    public static final String INDEX_NAME = "cdc_dup_rule";

    /** Fields that can never be part of a rule. */
    private static final Set<String> FORBIDDEN_FIELDS =
            Set.of("_id", "_op", "_deleted", "_source_ts", "_ingested_at");

    private final MetaDatabase metaDb;
    private final String table;

    public DuplicateRules(Properties config, MetaDatabase metaDb) {
        this.metaDb = metaDb;
        this.table = MetaDatabase.tableName(config, "duplicate.rule.table", "meta.cdc_duplicate_rule");
    }

    public String table() {
        return table;
    }

    /**
     * Loads the active rules of this subscriber's source type (source.db.type).
     *
     * @return source collection/table -> rule fields in their order, for example "customers" -> [name, email, phone]
     * @throws IllegalStateException when the table cannot be read; the batch is then not applied and not committed
     */
    public Map<String, List<String>> loadActive() {
        String sql = "SELECT source, fields FROM " + table + " WHERE active AND UPPER(db_type) = ?";
        Map<String, List<String>> rules = new LinkedHashMap<>();
        try (Connection connection = metaDb.connect();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, metaDb.sourceDbType());
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    String source = rows.getString(1);
                    List<String> fields = parseFields(source, rows.getString(2));
                    if (!fields.isEmpty()) {
                        rules.put(source, fields);
                    }
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot read the duplicate rule table " + table
                    + " (" + metaDb.jdbcUrl() + "): " + e.getMessage(), e);
        }
        return rules;
    }

    /** "name, email ,phone" -> [name, email, phone]; forbidden and repeated fields are dropped with a warning. */
    static List<String> parseFields(String source, String text) {
        List<String> fields = new ArrayList<>();
        if (text == null) {
            return fields;
        }
        for (String part : text.split(",")) {
            String field = part.trim();
            if (field.isEmpty()) {
                continue;
            }
            if (FORBIDDEN_FIELDS.contains(field)) {
                log.warn("Duplicate rule of '{}': field '{}' cannot be part of a rule, ignored", source, field);
            } else if (!fields.contains(field)) {
                fields.add(field);
            }
        }
        if (fields.isEmpty()) {
            log.warn("Duplicate rule of '{}' has no usable fields, the source is not checked", source);
        }
        return fields;
    }

    /**
     * Option C check at start: does every destination with a rule have the partial unique index on the same fields?
     * Only logs warnings; the query check (option A) works without the index.
     */
    public static void checkIndexes(MongoDatabase targetDb, Map<String, String> mappings, Map<String, List<String>> rules) {
        Set<String> existingCollections = targetDb.listCollectionNames().into(new HashSet<>());
        rules.forEach((source, fields) -> {
            String destination = mappings.get(source);
            if (destination == null) {
                log.warn("Duplicate rule for '{}' but no active mapping: the rule is not used", source);
                return;
            }
            if (!existingCollections.contains(destination)) {
                return; // its messages go to the DLQ anyway (destination missing)
            }
            BsonDocument index = null;
            for (BsonDocument candidate : targetDb.getCollection(destination).listIndexes(BsonDocument.class)) {
                if (INDEX_NAME.equals(candidate.getString("name").getValue())) {
                    index = candidate;
                }
            }
            if (index == null) {
                log.warn("{}: no unique index '{}' (option C) for rule {}. The query check still works; "
                        + "create it with sample-data/<pipeline>/03_bronze_unique_indexes.js", destination, INDEX_NAME, fields);
                return;
            }
            List<String> indexFields = new ArrayList<>(index.getDocument("key").keySet());
            boolean unique = index.getBoolean("unique", org.bson.BsonBoolean.FALSE).getValue();
            if (!indexFields.equals(fields) || !unique) {
                log.warn("{}: index '{}' {}{} does not match the rule {}. Run the 03_bronze_unique_indexes.js script again",
                        destination, INDEX_NAME, indexFields, unique ? "" : " (not unique)", fields);
            }
        });
    }
}
