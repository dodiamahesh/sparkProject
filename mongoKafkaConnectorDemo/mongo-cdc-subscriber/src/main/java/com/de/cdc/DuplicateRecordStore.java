package com.de.cdc;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * Writes rejected (duplicate) records to the PostgreSQL table meta.meta_duplicate_record:
 * <pre>
 *   id_pk | record_id | data_store_name | object_payload (JSONB) | db_type
 * </pre>
 * Unique (db_type, data_store_name, record_id): the same rejected record again (Kafka replay, a later update)
 * does not create a new row, it replaces object_payload with the latest copy.
 */
public class DuplicateRecordStore {

    /** One rejected record. recordId = source _id as text (see DuplicateDetector.recordId). */
    public record DuplicateRecord(String recordId, String dataStoreName, String payloadJson, String dbType) { }

    private final MetaDatabase metaDb;
    private final String table;

    public DuplicateRecordStore(Properties config, MetaDatabase metaDb) {
        this.metaDb = metaDb;
        this.table = MetaDatabase.tableName(config, "duplicate.record.table", "meta.meta_duplicate_record");
    }

    public String table() {
        return table;
    }

    /** Inserts the records, or updates object_payload when the record is already there. One transaction. */
    public void save(List<DuplicateRecord> records) {
        if (records.isEmpty()) {
            return;
        }
        String sql = "INSERT INTO " + table + " (record_id, data_store_name, object_payload, db_type) "
                + "VALUES (?, ?, ?::jsonb, ?) "
                + "ON CONFLICT (db_type, data_store_name, record_id) DO UPDATE SET object_payload = EXCLUDED.object_payload";
        try (Connection connection = metaDb.connect()) {
            connection.setAutoCommit(false);
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                for (DuplicateRecord record : records) {
                    statement.setString(1, record.recordId());
                    statement.setString(2, record.dataStoreName());
                    statement.setString(3, record.payloadJson());
                    statement.setString(4, record.dbType());
                    statement.addBatch();
                }
                statement.executeBatch();
                connection.commit();
            } catch (SQLException e) {
                connection.rollback();
                throw e;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot write duplicate records to " + table + ": " + e.getMessage(), e);
        }
    }

    /** Which of these record ids of one source are already stored as duplicates (rejected in an earlier batch)? */
    public Set<String> findStored(String dataStoreName, String dbType, Collection<String> recordIds) {
        Set<String> found = new HashSet<>();
        if (recordIds.isEmpty()) {
            return found;
        }
        String sql = "SELECT record_id FROM " + table + " WHERE db_type = ? AND data_store_name = ? AND record_id = ANY (?)";
        try (Connection connection = metaDb.connect();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            Array ids = connection.createArrayOf("text", recordIds.toArray());
            statement.setString(1, dbType);
            statement.setString(2, dataStoreName);
            statement.setArray(3, ids);
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    found.add(rows.getString(1));
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot read duplicate records from " + table + ": " + e.getMessage(), e);
        }
        return found;
    }
}
