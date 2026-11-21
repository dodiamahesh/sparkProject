package com.shop.etl.common.io;

import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Row;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Spark EXECUTOR par chalto code: ek partition ni badhi rows Postgres ma batch ma lakhe che.
 *
 * Flow (ek partition mate):
 * <pre>
 *   1 connection kholo (autoCommit = false)
 *   badha steps na PreparedStatement EK j vaar banavo
 *   rows ne batchSize na tukda ma bhego karo
 *     darek tukda mate: step-1 batch → step-2 batch → ... → COMMIT
 *   connection bandh
 * </pre>
 *
 * Kem aavu? (performance)
 *   - ROW DIITH EK CONNECTION nahi: partition diith ek j connection.
 *   - ROW DIITH EK ROUND-TRIP nahi: 1000 rows ek executeBatch ma (ane reWriteBatchedInserts
 *     thi INSERT multi-row bane che).
 *   - Darek tukda pachhi COMMIT: transaction nanu rahe, locks lambo samay na rahe.
 *
 * Fail thay to? Tukdo rollback thay ane exception Spark ne jay (task fail → job fail).
 * Badha SQL idempotent che (ON CONFLICT / IS DISTINCT FROM / NOT EXISTS), etle job farithi
 * chalavo to pehla commit thayela tukda pharithi lakhay to pan duplicate nathi banta.
 *
 * Steps nu order kem sachvay che? Ek tukda na row X mate step-1 badha rows mate pehla chale,
 * pachhi step-2. Aapda data ma ek partition ma ek business key ek j vaar che (dedupe pachhi),
 * etle alag rows na steps ek bija ne asar nathi karta.
 */
public class JdbcPartitionWriter implements ForeachPartitionFunction<Row> {

    private final JdbcConnectionInfo connection;
    private final List<JdbcStep> steps;

    public JdbcPartitionWriter(JdbcConnectionInfo connection, List<JdbcStep> steps) {
        this.connection = connection;
        this.steps = List.copyOf(steps);
    }

    /** Spark darek partition mate aa method executor par call kare che. */
    @Override
    public void call(Iterator<Row> rows) throws Exception {
        if (!rows.hasNext()) {
            return;   // khali partition: connection pan na kholo
        }
        RowValues values = new RowValues(ZoneId.of(connection.zoneId()));
        try (Connection c = DriverManager.getConnection(connection.url(), connection.username(), connection.password())) {
            c.setAutoCommit(false);
            List<PreparedStatement> statements = new ArrayList<>(steps.size());
            try {
                for (JdbcStep step : steps) {
                    statements.add(c.prepareStatement(step.sql()));
                }
                List<Row> buffer = new ArrayList<>(connection.batchSize());
                while (rows.hasNext()) {
                    buffer.add(rows.next());
                    if (buffer.size() >= connection.batchSize()) {
                        flush(c, statements, buffer, values);
                    }
                }
                flush(c, statements, buffer, values);
            } finally {
                for (PreparedStatement ps : statements) {
                    ps.close();
                }
            }
        }
    }

    /**
     * Ek tukdo (buffer) lakhe che: darek step no batch chalave, pachhi commit.
     * Error aave to rollback kari ne kayo step fail thayo e message sathe exception aape che.
     */
    private void flush(Connection c, List<PreparedStatement> statements, List<Row> buffer, RowValues values)
            throws SQLException {
        if (buffer.isEmpty()) {
            return;
        }
        for (int i = 0; i < steps.size(); i++) {
            JdbcStep step = steps.get(i);
            PreparedStatement ps = statements.get(i);
            try {
                for (Row row : buffer) {
                    step.binder().bind(ps, row, values);
                    ps.addBatch();
                }
                ps.executeBatch();
            } catch (SQLException e) {
                c.rollback();
                SQLException root = e.getNextException() != null ? e.getNextException() : e;
                throw new SQLException("JDBC step '" + step.name() + "' fail thayo: " + root.getMessage(), root);
            }
        }
        c.commit();
        buffer.clear();
    }
}
