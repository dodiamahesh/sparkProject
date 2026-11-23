package com.tcs.dmj.dwh.service.impl;

import com.tcs.dmj.dwh.config.ApplicationProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Makes sure the SAME pipeline never runs twice at the same time, also across processes: the REST service and
 * every Airflow task (each one its own JVM, see PipelineBatchRunner) share this lock. Different pipelines run
 * in parallel.
 *
 * It is a PostgreSQL session advisory lock held on a DEDICATED connection (not from the pool) for the whole run.
 * When the process dies, PostgreSQL closes the connection and the lock is released automatically, so a crashed
 * run never blocks the next one.
 */
@Component
public class PipelineRunLock {

    private static final Logger log = LoggerFactory.getLogger(PipelineRunLock.class);
    private static final String KEY_PREFIX = "dmj-dwh:pipeline:";

    private final ApplicationProperties.Postgres postgres;

    public PipelineRunLock(ApplicationProperties properties) {
        this.postgres = properties.postgres();
    }

    /** @return the held lock (close it when the run ends), or null when the pipeline is running somewhere else */
    public Held tryAcquire(String pipelineName) {
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(postgres.url(), postgres.username(), postgres.password());
            try (PreparedStatement statement = connection.prepareStatement("SELECT pg_try_advisory_lock(hashtext(?))")) {
                statement.setString(1, KEY_PREFIX + pipelineName);
                try (ResultSet result = statement.executeQuery()) {
                    result.next();
                    if (result.getBoolean(1)) {
                        return new Held(connection, pipelineName);
                    }
                }
            }
            connection.close();
            return null;
        } catch (SQLException e) {
            closeQuietly(connection);
            throw new IllegalStateException("Could not take the run lock of " + pipelineName + ": " + e.getMessage(), e);
        }
    }

    private static void closeQuietly(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException ignored) {
                // closing the connection is what releases the lock; nothing else to do
            }
        }
    }

    /** A held lock. close() ends the session, which releases the lock. */
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
