package com.shop.etl.jobs.sales.customer;

import com.shop.etl.common.quality.Clean;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

import java.util.Map;

import static com.shop.etl.dataset.raw.RawCustomer.CITY;
import static com.shop.etl.dataset.raw.RawCustomer.CUSTOMER_CODE;
import static com.shop.etl.dataset.raw.RawCustomer.EMAIL;
import static com.shop.etl.dataset.raw.RawCustomer.NAME;
import static com.shop.etl.dataset.raw.RawCustomer.PHONE;
import static com.shop.etl.dataset.raw.RawCustomer.SIGNUP_DATE;
import static com.shop.etl.dataset.raw.RawCustomer.STATE;
import static com.shop.etl.dataset.raw.RawCustomer.TIER;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.to_date;
import static org.apache.spark.sql.functions.when;

/**
 * CUSTOMERS SANITIZATION SERVICE (Job-2, Part A). Design doc Section 6.4 rules J2-S01 thi S05.
 *
 * Sanitize = data SUDHARVO, reject nahi. Aa class koi row kadhti nathi.
 *
 * | Rule   | Field | Before                   | After               |
 * |--------|-------|--------------------------|---------------------|
 * | J2-S01 | email | "  PRIYA.SHAH@MAIL.COM " | priya.shah@mail.com |
 * | J2-S02 | phone | "+91-98765 00004"        | 9876500004          |
 * | J2-S03 | city  | "lucknow "               | Lucknow             |
 * | J2-S04 | state | RJ                       | Rajasthan           |
 * | J2-S05 | tier  | null                     | Bronze              |
 *
 * Phone ni original value {@code phone_raw} ma rakhiye: phone kharab hoy ane reject thay to
 * bad_value ma "user e shu moklyu hatu" e dekhay.
 */
@Component
public class CustomerSanitizer {

    static final String PHONE_RAW = "phone_raw";

    /** State code → full naam (J2-S04). Navo code aave to aahi umero. */
    private static final Map<String, String> STATE_CODES = Map.ofEntries(
            Map.entry("GJ", "Gujarat"), Map.entry("MH", "Maharashtra"), Map.entry("RJ", "Rajasthan"),
            Map.entry("DL", "Delhi"), Map.entry("KA", "Karnataka"), Map.entry("TN", "Tamil Nadu"),
            Map.entry("TS", "Telangana"), Map.entry("UP", "Uttar Pradesh"), Map.entry("WB", "West Bengal"),
            Map.entry("MP", "Madhya Pradesh"), Map.entry("KL", "Kerala"));

    /**
     * Raw customers par badha sanitize rules lagave che.
     *
     * @param raw RawCustomer.SCHEMA vado dataset
     * @return same columns (saaf karela) + phone_raw
     */
    public Dataset<Row> sanitize(Dataset<Row> raw) {
        return raw
                .withColumn(CUSTOMER_CODE, Clean.upperTrim(col(CUSTOMER_CODE)))
                .withColumn(NAME, Clean.trimToNull(col(NAME)))
                .withColumn(EMAIL, Clean.lowerTrim(col(EMAIL)))                         // J2-S01
                .withColumn(PHONE_RAW, col(PHONE))
                .withColumn(PHONE, Clean.phone10(col(PHONE)))                           // J2-S02
                .withColumn(CITY, Clean.titleCase(col(CITY)))                           // J2-S03
                .withColumn(STATE, state(col(STATE)))                                   // J2-S04
                .withColumn(TIER, coalesce(Clean.titleCase(col(TIER)), lit("Bronze"))) // J2-S05
                .withColumn(SIGNUP_DATE, to_date(Clean.trimToNull(col(SIGNUP_DATE))));
    }

    /**
     * State: 2 akshar no code hoy to full naam, nahi to Title Case.
     * "RJ" → "Rajasthan",  "uttar pradesh " → "Uttar Pradesh".
     */
    private static Column state(Column raw) {
        Column code = Clean.upperTrim(raw);
        Column result = null;
        for (Map.Entry<String, String> e : STATE_CODES.entrySet()) {
            result = (result == null)
                    ? when(code.equalTo(e.getKey()), lit(e.getValue()))
                    : result.when(code.equalTo(e.getKey()), lit(e.getValue()));
        }
        return result.otherwise(Clean.titleCase(raw));
    }
}
