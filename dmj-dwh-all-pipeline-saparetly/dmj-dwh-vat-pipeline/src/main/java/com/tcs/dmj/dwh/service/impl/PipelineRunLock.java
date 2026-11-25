package com.tcs.dmj.dwh.service.impl;

import com.tcs.dmj.dwh.config.ApplicationProperties;
import com.tcs.dmj.dwh.exception.EtlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * The same pipeline never runs twice at the same time, also across processes (REST service + Airflow JVMs).
 * PostgreSQL advisory lock on a dedicated connection: if the process dies, the lock is released.
 */
@Component
public class PipelineRunLock {

    private static final Logger log = LoggerFactory.getLogger(PipelineRunLock.class);
    private static final String KEY_PREFIX = "dmj-dwh:pipeline:";

    private final ApplicationProperties.Postgres postgres;

    public PipelineRunLock(ApplicationProperties properties) {
        this.postgres = properties.postgres();
    }

    /** @return the held lock (close it when the run ends), or null when the pipeline is running elsewhere */
    public Held tryAcquire(String pipelineName) {
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(postgres.url(), postgres.username(), postgres.password());
            if (lockTaken(connection, pipelineName)) {
                return new Held(connection, pipelineName);
            }
            connection.close();
            return null;
        } catch (SQLException e) {
            closeQuietly(connection);
            throw new EtlException("Could not take the run lock of " + pipelineName + ": " + e.getMessage(), e);
        }
    }

    private static boolean lockTaken(Connection connection, String pipelineName) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("SELECT pg_try_advisory_lock(hashtext(?))")) {
            statement.setString(1, KEY_PREFIX + pipelineName);
            try (ResultSet result = statement.executeQuery()) {
                result.next();
                return result.getBoolean(1);
            }
        }
    }

    private static void closeQuietly(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException ignored) {
                // closing releases the lock; nothing else to do
            }
        }
    }

    /** A held lock: close() ends the session, which releases the lock. */
    public static final class Held implements AutoCloseable {

        private final Connection connection;
        private final String pipelineName;

        private Held(Connection connection, String pipelineName) {
            this.connection = connection;
            this.pipelineName = pipelineName;
        }

        @Override
        public void close() {
            try {
                connection.close();
            } catch (SQLException e) {
                log.warn("Closing the run lock connection of {} failed: {}", pipelineName, e.getMessage());
            }
        }
    }
}
