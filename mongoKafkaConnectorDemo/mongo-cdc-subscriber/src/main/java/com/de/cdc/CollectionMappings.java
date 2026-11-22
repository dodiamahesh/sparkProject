package com.de.cdc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

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

    /** Allowed values of db_type (and of the setting source.db.type). */
    public static final Set<String> DB_TYPES = Set.of("NOSQL", "SQL");

    private final String jdbcUrl;
    private final String user;
    private final String password;
    private final String table;
    private final String sourceDbType;

    public CollectionMappings(Properties config) {
        this.jdbcUrl = required(config, "mapping.jdbc.url");
        this.user = required(config, "mapping.jdbc.user");
        // the environment variable wins, so the real password does not have to be in the properties file
        String envPassword = System.getenv("MAPPING_JDBC_PASSWORD");
        this.password = envPassword != null ? envPassword : config.getProperty("mapping.jdbc.password", "");
        this.table = config.getProperty("mapping.table", "meta.cdc_collection_mapping");
        // the table name is put into the SQL text, so allow only "schema.table" characters
        if (!table.matches("[A-Za-z0-9_]+(\\.[A-Za-z0-9_]+)?")) {
            throw new IllegalArgumentException("mapping.table is not a valid table name: " + table);
        }
        this.sourceDbType = config.getProperty("source.db.type", "NOSQL").trim().toUpperCase(Locale.ROOT);
        if (!DB_TYPES.contains(sourceDbType)) {
            throw new IllegalArgumentException("source.db.type must be one of " + DB_TYPES + ", not: " + sourceDbType);
        }
    }

    public String table() {
        return table;
    }

    /** Type of the source this subscriber reads: NOSQL (MongoDB collections) or SQL (SQL tables). */
    public String sourceDbType() {
        return sourceDbType;
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
        try (Connection connection = DriverManager.getConnection(jdbcUrl, user, password);
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, sourceDbType);
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    mappings.put(rows.getString(1), rows.getString(2));
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot read the collection mapping table " + table
                    + " (" + jdbcUrl + "): " + e.getMessage(), e);
        }
        return mappings;
    }

    private static String required(Properties config, String key) {
        String value = config.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing setting '" + key + "' in subscriber.properties");
        }
        return value;
    }
}
