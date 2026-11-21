package com.shop.etl.common.quality;

import org.apache.spark.sql.Column;

import java.util.Map;

import static org.apache.spark.sql.functions.initcap;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.lower;
import static org.apache.spark.sql.functions.nullif;
import static org.apache.spark.sql.functions.regexp_extract;
import static org.apache.spark.sql.functions.regexp_replace;
import static org.apache.spark.sql.functions.trim;
import static org.apache.spark.sql.functions.upper;
import static org.apache.spark.sql.functions.when;

/**
 * SANITIZE mate common (reusable) Spark column functions. Design doc rule prakar "S".
 *
 * Sanitize = data ne SUDHARVO, reject nahi. Je sudhari shakay e sudharo; je na sudhare e
 * pachhi Validator reject karshe.
 *
 * Badha functions Spark built-in expressions che (UDF nahi), etle Catalyst optimize kare che.
 * Badha jobs (Job 1–5) aa j functions vapare, etle "email kevi rite saaf karvo" no niyam
 * aakha project ma ek j che.
 */
public final class Clean {

    private Clean() {
    }

    /** Trim kare, ane khali string hoy to NULL. {@code "  "} → null, {@code " Rahul "} → "Rahul". */
    public static Column trimToNull(Column c) {
        return nullif(trim(c), lit(""));
    }

    /** Trim + UPPERCASE. {@code " delivered "} → "DELIVERED". Codes ane status mate. */
    public static Column upperTrim(Column c) {
        return upper(trimToNull(c));
    }

    /** Trim + lowercase. {@code "  PRIYA.SHAH@MAIL.COM "} → "priya.shah@mail.com". Email mate. */
    public static Column lowerTrim(Column c) {
        return lower(trimToNull(c));
    }

    /** Trim + Title Case. {@code "lucknow "} → "Lucknow". City / naam mate. */
    public static Column titleCase(Column c) {
        return initcap(lower(trimToNull(c)));
    }

    /**
     * Phone: fakt digits rakho ane CHHELLA 10 digit lo. Na male to NULL.
     * {@code "+91-98765 00004"} → "9876500004", {@code "09876500004"} → "9876500004".
     */
    public static Column phone10(Column c) {
        Column digits = regexp_replace(c, "[^0-9]", "");
        return nullif(regexp_extract(digits, "([0-9]{10})$", 1), lit(""));
    }

    /**
     * Amount string mathi ₹, comma ane space kadhi ne DECIMAL(14,2). Number na bane to NULL
     * (Validator pachhi ene CORRUPT ganse).
     * {@code "₹29,000"} → 29000.00, {@code "abc"} → null.
     */
    public static Column amount(Column c) {
        return nullif(regexp_replace(trim(c), "[₹,\\s]", ""), lit("")).cast("decimal(14,2)");
    }

    /**
     * Value mapping (code → standard value). Mapping ma na hoy to cleaned value j rahe
     * (pachhi "allowed list" rule ene pakadse).
     * Example: {@code Map.of("RJ","Rajasthan")}.
     *
     * @param cleaned pehla thi trim/upper thayelo column
     */
    public static Column map(Column cleaned, Map<String, String> mapping) {
        Column result = null;
        for (Map.Entry<String, String> e : mapping.entrySet()) {
            result = (result == null)
                    ? when(cleaned.equalTo(lit(e.getKey())), lit(e.getValue()))
                    : result.when(cleaned.equalTo(lit(e.getKey())), lit(e.getValue()));
        }
        return result == null ? cleaned : result.otherwise(cleaned);
    }
}
