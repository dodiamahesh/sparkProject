package com.shop.etl.common.quality;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.when;

/**
 * Same business key vala duplicate records shodhe che: LATEST rakhe, baki DUPLICATE reject.
 *
 * Kevi rite? Window function:
 * <pre>
 *   ROW_NUMBER() OVER (PARTITION BY business_key, (pehla thi sari row?)
 *                      ORDER BY updated_at DESC, _id DESC)
 *   rn = 1 → rakho,   rn > 1 → DUPLICATE
 * </pre>
 *
 * - "pehla thi sari row?" partition ma kem? Corrupt thayeli row (jeno updated_at bharoso
 *   layak nathi) latest ganay nahi. Fakt sari rows vacche j latest shodhay. (Doc: Corrupt → Dedupe)
 * - {@code _id DESC} tie-breaker: updated_at same hoy (email 2 vaar process) to pan result
 *   hammesha same aave (deterministic). Re-run ma alag row reject na thay.
 *
 * Performance: {@code dropDuplicates} karta window thodo bhare che (shuffle by key), pan
 * dropDuplicates "kayo rakhvo" control nathi aaptu, ane rejected row pan nathi aaptu.
 * Aapde audit mate rejected row joie che, etle window j sachu che.
 */
public final class Deduplicator {

    private static final String RN = "_rn";

    private Deduplicator() {
    }

    /**
     * @param data       RuleEngine.init thayelo dataset
     * @param keyColumns business key (jem ke customer_code)
     * @param orderBy    latest nakki karva column (updated_at)
     * @param tieBreaker same time hoy to (mostly _id)
     * @param rule       duplicate mate rule code / reason / bad_value (failWhen ignore thay)
     */
    public static Dataset<Row> markDuplicates(Dataset<Row> data, List<String> keyColumns,
                                              String orderBy, String tieBreaker, Rule rule) {
        List<Column> partition = new ArrayList<>();
        keyColumns.forEach(k -> partition.add(col(k)));
        partition.add(col(RuleEngine.REJECT).isNull());

        WindowSpec latestFirst = Window
                .partitionBy(partition.toArray(new Column[0]))
                .orderBy(col(orderBy).desc_nulls_last(), col(tieBreaker).desc());

        return data
                .withColumn(RN, row_number().over(latestFirst))
                .withColumn(RuleEngine.REJECT,
                        when(col(RuleEngine.REJECT).isNull().and(col(RN).gt(1)), rule.toRejectStruct())
                                .otherwise(col(RuleEngine.REJECT)))
                .drop(RN);
    }
}
