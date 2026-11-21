package com.shop.etl.common.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

/**
 * Same job EK SATHE 2 vaar na chale e mate lock. PostgreSQL ADVISORY LOCK vapare che.
 *
 * Kem jaruri? Aapda job ne 3 jagyae thi trigger kari shakay (API, Airflow, scheduler).
 * 04:00 e scheduler chalave ane e j samaye koi API hit kare, to banne same data lakhe,
 * watermark gadbad thay. Lock hoy to bijo trigger turant "already running" (HTTP 409) male.
 *
 * Kem DB advisory lock (Java synchronized nahi)?
 *   - App na 2 instance (2 server / 2 pod) chalta hoy to pan lock kaam kare (DB common che).
 *   - App crash thay to DB connection bandh thay, ane lock POTE chhuti jay. "Atkelo lock"
 *     manually saaf karvo nathi padto (table vala lock ma aa problem hoy che).
 *   - Koi navi table nathi joti.
 *
 * Lock ek DB connection sathe jodayelo che, etle run chale tya sudhi e connection khullu rahe che.
 */
@Component
public class JobLock {

    private static final Logger log = LoggerFactory.getLogger(JobLock.class);

    private final DataSource dataSource;

    public JobLock(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Lock levano try kare (wait nathi kartu).
     *
     * @return lock malyo to Handle (close() thi chhodo), nahi to empty (bijo run chale che)
     */
    public Optional<Handle> tryAcquire(String jobName) {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            try (PreparedStatement ps = connection.prepareStatement("SELECT pg_try_advisory_lock(hashtext(?))")) {
                ps.setString(1, jobName);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    if (rs.getBoolean(1)) {
                        log.debug("Lock malyo: {}", jobName);
                        return Optional.of(new Handle(jobName, connection));
                    }
                }
            }
            connection.close();
            return Optional.empty();
        } catch (SQLException e) {
            closeQuietly(connection);
            throw new IllegalStateException("Job lock levama error: " + jobName, e);
        }
    }

    private static void closeQuietly(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException ignored) {
                // close vakhte error ne avgani shakay
            }
        }
    }

    /**
     * Malelo lock. try-with-resources ma vapro: run puro thay (success ke fail) etle close()
     * lock chhode ane connection pool ma pachhu aape.
     */
    public static final class Handle implements AutoCloseable {

        private final String jobName;
        private final Connection connection;
        private boolean closed;

        private Handle(String jobName, Connection connection) {
            this.jobName = jobName;
            this.connection = connection;
        }

        /** Lock chhode. Be vaar call thay to pan safe (bijo call kai nathi kartu). */
        @Override
        public synchronized void close() {
            if (closed) {
                return;
            }
            closed = true;
            try (PreparedStatement ps = connection.prepareStatement("SELECT pg_advisory_unlock(hashtext(?))")) {
                ps.setString(1, jobName);
                ps.execute();
            } catch (SQLException e) {
                log.warn("Lock chhodta error ({}): connection bandh thata lock pote chhuti jashe", jobName, e);
            } finally {
                closeQuietly(connection);
            }
        }
    }
}
