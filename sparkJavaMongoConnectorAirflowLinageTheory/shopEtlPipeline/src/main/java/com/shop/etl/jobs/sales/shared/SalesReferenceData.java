package com.shop.etl.jobs.sales.shared;

import com.shop.etl.common.config.EtlProperties;
import com.shop.etl.common.quality.Clean;
import com.shop.etl.dataset.silver.DimOrderStatus;
import com.shop.etl.dataset.silver.DimPaymentMethod;
import com.shop.etl.dataset.silver.SilverTable;
import org.apache.spark.sql.Column;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.regexp_replace;

/**
 * Job-2 na REFERENCE values (order status, payment method) ane emna mapping. Orders ane
 * payments BANNE aa vapare che, etle Job-2 na "shared" package ma che.
 *
 * Aa values ek j jagyae che:
 *   - Seed: dim_order_status ane dim_payment_method ma "insert if not exists"
 *   - Sanitize: raw text ("Net Banking", "Cash on Delivery", "upi ") → standard code
 *   - Validate: allowed list (aa list ma nathi → reject)
 *
 * Navo payment method (jem ke WALLET) aave to fakt aa enum ma umero. Seed, mapping ane
 * validation pote update thai jashe.
 */
@Component
public class SalesReferenceData {

    /** Order status: code, description, final che? (final = have badlashe nahi). */
    public enum OrderStatus {
        PLACED("Order mukayo", false),
        SHIPPED("Courier ne apayo", false),
        DELIVERED("Customer ne malyo", true),
        CANCELLED("Radd thayo", true),
        RETURNED("Pachho aavyo", true);

        public final String description;
        public final boolean isFinal;

        OrderStatus(String description, boolean isFinal) {
            this.description = description;
            this.isFinal = isFinal;
        }

        /** Validation mate allowed codes. */
        public static Object[] codes() {
            return Arrays.stream(values()).map(Enum::name).toArray();
        }
    }

    /** Payment method: code, naam, online che? */
    public enum PaymentMethod {
        UPI("Unified Payments Interface", true),
        CARD("Credit / Debit Card", true),
        NETBANKING("Net Banking", true),
        COD("Cash on Delivery", false);

        public final String methodName;
        public final boolean online;

        PaymentMethod(String methodName, boolean online) {
            this.methodName = methodName;
            this.online = online;
        }

        /** Validation mate allowed codes. */
        public static Object[] codes() {
            return Arrays.stream(values()).map(Enum::name).toArray();
        }
    }

    /**
     * Raw text (trim + UPPER + ek space pachhi) → standard code. Design doc rule J2-S07.
     * "upi " → UPI, "Net Banking" → NETBANKING, "Cash on Delivery" → COD.
     */
    private static final Map<String, String> PAYMENT_METHOD_ALIASES = new LinkedHashMap<>();

    static {
        PAYMENT_METHOD_ALIASES.put("NET BANKING", "NETBANKING");
        PAYMENT_METHOD_ALIASES.put("INTERNET BANKING", "NETBANKING");
        PAYMENT_METHOD_ALIASES.put("CASH ON DELIVERY", "COD");
        PAYMENT_METHOD_ALIASES.put("CREDIT CARD", "CARD");
        PAYMENT_METHOD_ALIASES.put("DEBIT CARD", "CARD");
    }

    private final JdbcTemplate jdbc;
    private final String silver;

    public SalesReferenceData(JdbcTemplate jdbc, EtlProperties properties) {
        this.jdbc = jdbc;
        this.silver = properties.postgres().schema().silver();
    }

    /**
     * Payment method sanitize: trim, UPPER, vacche ni ghani space ek, pachhi alias mapping.
     * Mapping ma na hoy evi value jem che tem rahe (pachhi allowed-list rule pakadse).
     */
    public static Column normalizePaymentMethod(Column raw) {
        Column cleaned = regexp_replace(Clean.upperTrim(raw), "\\s+", " ");
        return Clean.map(cleaned, PAYMENT_METHOD_ALIASES);
    }

    /**
     * Reference dims seed kare che: hoy to kai nahi, na hoy to insert.
     * Darek run ni shuruat ma chale che. 9 nana statements, driver par (Spark nahi).
     *
     * {@code ON CONFLICT DO NOTHING} ne badle {@code WHERE NOT EXISTS} kem? ON CONFLICT row
     * insert na thay to pan IDENTITY sequence no number vapri nakhe. Darek run ma 9 number
     * bagadat. NOT EXISTS pehla check kare, etle number fakt navi row mate j vapray.
     */
    public void seed() {
        String statusTable = SilverTable.qualified(silver, DimOrderStatus.TABLE);
        String statusSql = "INSERT INTO " + statusTable + " (status_code, description, is_final) "
                + "SELECT ?, ?, ? WHERE NOT EXISTS (SELECT 1 FROM " + statusTable + " WHERE status_code = ?)";
        for (OrderStatus s : OrderStatus.values()) {
            jdbc.update(statusSql, s.name(), s.description, s.isFinal ? "Y" : "N", s.name());
        }
        String methodTable = SilverTable.qualified(silver, DimPaymentMethod.TABLE);
        String methodSql = "INSERT INTO " + methodTable + " (method_code, method_name, is_online) "
                + "SELECT ?, ?, ? WHERE NOT EXISTS (SELECT 1 FROM " + methodTable + " WHERE method_code = ?)";
        for (PaymentMethod m : PaymentMethod.values()) {
            jdbc.update(methodSql, m.name(), m.methodName, m.online ? "Y" : "N", m.name());
        }
    }
}
