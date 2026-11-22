package com.de.cdc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;

/**
 * Connection settings of the PostgreSQL meta database (mapping, duplicate rules, duplicate records),
 * and the type of the source this subscriber reads (source.db.type).
 */
public class MetaDatabase {

    /** Allowed values of db_type (and of the setting source.db.type). */
    public static final Set<String> DB_TYPES = Set.of("NOSQL", "SQL");

    private final String jdbcUrl;
    private final String user;
    private final String password;
    private final String sourceDbType;

    public MetaDatabase(Properties config) {
        this.jdbcUrl = required(config, "mapping.jdbc.url");
        this.user = required(config, "mapping.jdbc.user");
        // the environment variable wins, so the real password does not have to be in the properties file
        String envPassword = System.getenv("MAPPING_JDBC_PASSWORD");
        this.password = envPassword != null ? envPassword : config.getProperty("mapping.jdbc.password", "");
        this.sourceDbType = config.getProperty("source.db.type", "NOSQL").trim().toUpperCase(Locale.ROOT);
        if (!DB_TYPES.contains(sourceDbType)) {
            throw new IllegalArgumentException("source.db.type must be one of " + DB_TYPES + ", not: " + sourceDbType);
        }
    }

    /** A new connection; the caller closes it (try-with-resources). */
    public Connection connect() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, user, password);
    }

    public String jdbcUrl() {
        return jdbcUrl;
    }

    /** Type of the source this subscriber reads: NOSQL (MongoDB collections) or SQL (SQL tables). */
    public String sourceDbType() {
        return sourceDbType;
    }

    /** A table name from the config. It is put into SQL text, so only "schema.table" characters are allowed. */
    static String tableName(Properties config, String key, String defaultValue) {
        String table = config.getProperty(key, defaultValue).trim();
        if (!table.matches("[A-Za-z0-9_]+(\\.[A-Za-z0-9_]+)?")) {
            throw new IllegalArgumentException(key + " is not a valid table name: " + table);
        }
        return table;
    }

    private static String required(Properties config, String key) {
        String value = config.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing setting '" + key + "' in subscriber.properties");
        }
        return value;
    }
}
