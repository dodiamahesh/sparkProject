package com.de.cdc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Reads the source -> destination mapping from the PostgreSQL meta table (default meta.cdc_collection_mapping):
 * <pre>
 *   source       destination     db_type   active
 *   products     raw_products    NOSQL     true      -> MongoDB collection "products" is written to bronze_db.raw_products
 *   customers    customers       NOSQL     true
 *   station      sql_station     SQL       true      -> SQL table "station" is written to bronze_db.sql_station
 *   orders       orders          NOSQL     false     -> not active: events of "orders" are rejected (DLQ)
 * </pre>
 * db_type = the type of the SOURCE: NOSQL = a MongoDB collection, SQL = a SQL table. The destination is always
 * a collection in the target (bronze) database. One subscriber reads one source system (one topic), so it uses only
 * the rows of its own type (setting source.db.type). The same name can exist as a collection AND as a table.
 *
 * Only rows with active = true are loaded. The table is read again for every Kafka batch, so a change in the
 * table (new row, active on/off, other destination) is used from the next batch on, without a restart.
 */
public class CollectionMappings {

    private final MetaDatabase metaDb;
    private final String table;

    public CollectionMappings(Properties config, MetaDatabase metaDb) {
        this.metaDb = metaDb;
        this.table = MetaDatabase.tableName(config, "mapping.table", "meta.cdc_collection_mapping");
    }

    public String table() {
        return table;
    }

    /** Type of the source this subscriber reads: NOSQL (MongoDB collections) or SQL (SQL tables). */
    public String sourceDbType() {
        return metaDb.sourceDbType();
    }

    /**
     * Loads the active mappings of this subscriber's source type (source.db.type).
     *
     * @return source collection/table -> destination collection, for example "products" -> "raw_products"
     * @throws IllegalStateException when the table cannot be read (PostgreSQL down, table missing).
     *         The batch is then not applied and not committed, so no message is lost.
     */
    public Map<String, String> loadActive() {
        String sql = "SELECT source, destination FROM " + table + " WHERE active AND UPPER(db_type) = ?";
        Map<String, String> mappings = new LinkedHashMap<>();
        try (Connection connection = metaDb.connect();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, metaDb.sourceDbType());
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    mappings.put(rows.getString(1), rows.getString(2));
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot read the collection mapping table " + table
                    + " (" + metaDb.jdbcUrl() + "): " + e.getMessage(), e);
        }
        return mappings;
    }
}
