package com.shop.etl.common.quality;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.List;

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

/**
 * Rules ne Dataset par lagu karto common engine.
 *
 * IDEA: row ne turant alag dataset ma nathi nakhta. Darek row par ek column {@code _reject}
 * rakhiye chhiye:
 *   - {@code _reject = null}              → row SARI che
 *   - {@code _reject = {rule_code, ...}}  → row REJECT, ane kem e pan
 *
 * Fayda (performance + simplicity):
 *   1. Mongo mathi data EK J VAAR vanchay. Pachhi valid = {@code _reject IS NULL}, ane
 *      rejected = {@code _reject IS NOT NULL}, banne same (persisted) dataset mathi.
 *      (Design doc: "single write ... double Mongo read avoid")
 *   2. Pehlo fail thayelo rule j rahe che: {@code coalesce(_reject, navo_rule)}. Etle ek row
 *      nu ek j sachu reason lakhay, ane rules nu ORDER e priority che.
 *   3. Badha rules ek {@code CASE WHEN} expression bane che: ek pass, koi shuffle nahi.
 */
public final class RuleEngine {

    /** Reject info rakhto column. Underscore thi shuru: target table ma kyarey na jay. */
    public static final String REJECT = "_reject";

    static final String F_RULE_CODE = "rule_code";
    static final String F_REJECT_TYPE = "reject_type";
    static final String F_REASON = "reason";
    static final String F_BAD_VALUE = "bad_value";

    /** {@code _reject} column no type: struct of 4 strings. */
    public static final StructType REJECT_TYPE = new StructType()
            .add(F_RULE_CODE, DataTypes.StringType)
            .add(F_REJECT_TYPE, DataTypes.StringType)
            .add(F_REASON, DataTypes.StringType)
            .add(F_BAD_VALUE, DataTypes.StringType);

    private RuleEngine() {
    }

    /** Khali {@code _reject} column umere che (badhi rows shuru ma sari). Validator pehla aa call kare. */
    public static Dataset<Row> init(Dataset<Row> data) {
        return data.withColumn(REJECT, lit(null).cast(REJECT_TYPE));
    }

    /**
     * Rules ne list na ORDER mujab lagu kare che.
     * <pre>
     *   _reject = COALESCE(_reject,
     *                      CASE WHEN rule1 fail THEN rule1_info
     *                           WHEN rule2 fail THEN rule2_info ... END)
     * </pre>
     * Pehla thi reject thayeli row ne aa rules badlta nathi.
     */
    public static Dataset<Row> apply(Dataset<Row> data, List<Rule> rules) {
        if (rules.isEmpty()) {
            return data;
        }
        Column chain = null;
        for (Rule rule : rules) {
            chain = (chain == null)
                    ? when(rule.failWhen(), rule.toRejectStruct())
                    : chain.when(rule.failWhen(), rule.toRejectStruct());
        }
        return data.withColumn(REJECT, coalesce(col(REJECT), chain));
    }

    /** Sari rows (je koi rule ma fail nathi) ane {@code _reject} column kadhi ne. */
    public static Dataset<Row> valid(Dataset<Row> checked) {
        return checked.filter(col(REJECT).isNull()).drop(REJECT);
    }

    /** Reject thayeli rows ({@code _reject} column sathe, RejectWriter mate). */
    public static Dataset<Row> rejected(Dataset<Row> checked) {
        return checked.filter(col(REJECT).isNotNull());
    }

    /** {@code _reject} na andar no field (RejectWriter vapare che). */
    static Column field(String name) {
        return col(REJECT).getField(name);
    }
}
