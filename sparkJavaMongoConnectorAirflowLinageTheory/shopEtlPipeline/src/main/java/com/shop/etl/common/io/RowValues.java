package com.shop.etl.common.io;

import org.apache.spark.sql.Row;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * Spark Row mathi value lai ne PreparedStatement ma NULL-SAFE ane TYPE-SAFE rite set kare che.
 *
 * Kem? Darek loader ma "null hoy to setNull, nahi to setString" jevo code varamvar na lakhvo pade.
 * Ane timestamp IST ma j lakhay e niyam ek j jagyae rahe.
 *
 * Spark config {@code spark.sql.datetime.java8API.enabled=true} che, etle Row ma:
 *   TimestampType → java.time.Instant,  DateType → java.time.LocalDate
 *
 * Aa class darek partition mate ek vaar bane che (executor par), etle Serializable nathi joie.
 */
public final class RowValues {

    private final ZoneId zone;

    public RowValues(ZoneId zone) {
        this.zone = zone;
    }

    /** String column → VARCHAR. Null hoy to SQL NULL. */
    public void string(PreparedStatement ps, int index, Row row, String column) throws SQLException {
        Object value = row.getAs(column);
        if (value == null) {
            ps.setNull(index, Types.VARCHAR);
        } else {
            ps.setString(index, value.toString());
        }
    }

    /** Fix string (jem ke run_id) → VARCHAR. */
    public void constant(PreparedStatement ps, int index, String value) throws SQLException {
        if (value == null) {
            ps.setNull(index, Types.VARCHAR);
        } else {
            ps.setString(index, value);
        }
    }

    /** Koi pan number column (int/long/short/decimal) → INTEGER. */
    public void integer(PreparedStatement ps, int index, Row row, String column) throws SQLException {
        Object value = row.getAs(column);
        if (value == null) {
            ps.setNull(index, Types.INTEGER);
        } else {
            ps.setInt(index, ((Number) value).intValue());
        }
    }

    /** Decimal / number column → NUMERIC (paisa mate double nahi, BigDecimal). */
    public void decimal(PreparedStatement ps, int index, Row row, String column) throws SQLException {
        Object value = row.getAs(column);
        if (value == null) {
            ps.setNull(index, Types.NUMERIC);
        } else if (value instanceof BigDecimal bd) {
            ps.setBigDecimal(index, bd);
        } else {
            ps.setBigDecimal(index, new BigDecimal(value.toString()));
        }
    }

    /**
     * Timestamp column (Instant, UTC) → Postgres TIMESTAMP (timezone vagar) IST local time ma.
     * Example: 2026-10-20T19:50:00Z → 2026-10-21 01:20:00 (IST). Design doc ma badhe IST che.
     */
    public void timestamp(PreparedStatement ps, int index, Row row, String column) throws SQLException {
        Object value = row.getAs(column);
        if (value == null) {
            ps.setNull(index, Types.TIMESTAMP);
        } else {
            ps.setObject(index, LocalDateTime.ofInstant((Instant) value, zone));
        }
    }

    /**
     * Date column (LocalDate) ne divas ni shuruat (00:00) na TIMESTAMP tarike set kare che.
     * SCD2 ma pehli version no valid_from = signup_date 00:00 mate vapray che.
     */
    public void dateAsTimestamp(PreparedStatement ps, int index, Row row, String column) throws SQLException {
        Object value = row.getAs(column);
        if (value == null) {
            ps.setNull(index, Types.TIMESTAMP);
        } else {
            ps.setObject(index, ((LocalDate) value).atStartOfDay());
        }
    }

    /** Date column (LocalDate) → Postgres DATE. */
    public void date(PreparedStatement ps, int index, Row row, String column) throws SQLException {
        Object value = row.getAs(column);
        if (value == null) {
            ps.setNull(index, Types.DATE);
        } else {
            ps.setObject(index, (LocalDate) value);
        }
    }
}
