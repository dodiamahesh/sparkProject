package com.shop.etl.common.quality;

import org.apache.spark.sql.Column;

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.struct;

/**
 * Ek data quality rule: "kyare fail ganvo" + "fail thay to shu lakhvu".
 *
 * Rule ne Spark {@link Column} expression tarike lakhiye chhiye (UDF nahi). Kem?
 *   - Spark Catalyst optimizer expression ne samje che ane fast code generate kare che.
 *   - UDF Catalyst mate "black box" che: optimization nathi thatu, ane row diith Java call thay.
 *   - Badha rules ek j pass ma (ek j Spark stage ma) check thay che.
 *
 * Rule business manas pan vanchi shake evo rakhvo: code, reason ane condition ek line ma.
 *
 * <pre>
 *   Rule.of("J2-V02", RejectType.VALIDATION, "Email format khoto",
 *           not(col("email").rlike(EMAIL_REGEX)),   // kyare FAIL
 *           "email", col("email"))                  // bad_value: email=test@@mail
 * </pre>
 *
 * ⚠️ NULL dhyan: Spark ma {@code null > 0} = null, ane null ne "fail nahi" ganay.
 * Etle condition ma null case pote lakho: {@code col("qty").isNull().or(col("qty").leq(0))}.
 *
 * @param code      rule code (J2-V02) → meta_rejected.rule_code
 * @param type      reject prakar
 * @param reason    manas samji shake evo message
 * @param failWhen  true hoy to row REJECT
 * @param badValue  kai value kharab hati (meta_rejected.bad_value)
 */
public record Rule(String code, RejectType type, String reason, Column failWhen, Column badValue) {

    /**
     * Rule banave che. bad_value "field=value" format ma bane che (null hoy to "field=null").
     *
     * @param field kharab field nu naam (bad_value ma dekhase)
     * @param value e field ni value (raw value aapvi saru, jethi khabar pade shu aavyu hatu)
     */
    public static Rule of(String code, RejectType type, String reason, Column failWhen, String field, Column value) {
        Column bad = concat(lit(field + "="), coalesce(value.cast("string"), lit("null")));
        return new Rule(code, type, reason, failWhen, bad);
    }

    /** Reject column mate struct banave che: rule_code, reject_type, reason, bad_value. */
    Column toRejectStruct() {
        return struct(
                lit(code).as(RuleEngine.F_RULE_CODE),
                lit(type.name()).as(RuleEngine.F_REJECT_TYPE),
                lit(reason).as(RuleEngine.F_REASON),
                badValue.cast("string").as(RuleEngine.F_BAD_VALUE));
    }
}
