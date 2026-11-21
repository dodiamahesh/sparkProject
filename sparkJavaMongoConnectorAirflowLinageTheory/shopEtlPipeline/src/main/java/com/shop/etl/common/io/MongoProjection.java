package com.shop.etl.common.io;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * MongoDB aggregation na {@code $project} stage ne saral rite banavva mate builder.
 *
 * Kem jaruri che? (performance + safety)
 *   1. COLUMN PRUNING AT SOURCE: fakt jaruri fields j Mongo mathi bahar aave. Network ane
 *      Spark memory ma ochho data. (Document ma 50 field hoy ane aapde 10 joie to 80% bachat.)
 *   2. TYPE SAFETY: raw data ma ek j field ma kyarek number, kyarek string hoy
 *      (price 2000 ane "₹1,200", order_ts Date ane "2026-13-45"). {@code $toString} badha ne
 *      string banave che. Pachhi Spark ma aapde pote parse kariye: parse na thay to e CORRUPT.
 *      Aam schema mismatch ne lidhe aakho job crash nathi thato.
 *   3. FLATTEN: nested field (address.city) ne sidho column (city) banave che.
 *
 * Example:
 * <pre>
 *   MongoProjection.builder()
 *       .asString("customer_code")
 *       .asString("city", "address.city")
 *       .keep("updated_at")
 *       .build()
 *   → {"$project": {"customer_code": {"$toString": "$customer_code"},
 *                   "city": {"$toString": "$address.city"},
 *                   "updated_at": 1}}
 * </pre>
 *
 * Aa class SERIALIZABLE nathi kem ke e fakt driver par JSON string banavva vapray che.
 */
public final class MongoProjection {

    private final String stageJson;

    private MongoProjection(String stageJson) {
        this.stageJson = stageJson;
    }

    /** Navo builder. */
    public static Builder builder() {
        return new Builder();
    }

    /** {@code $project} stage nu JSON (aggregation pipeline ma mukva mate). */
    public String toStageJson() {
        return stageJson;
    }

    /**
     * Builder: darek method ek output field umere che. Order jalvai rahe che (LinkedHashMap).
     */
    public static final class Builder {

        private final Map<String, String> fields = new LinkedHashMap<>();

        /** Same naam ni field ne string tarike lo: {@code name: {$toString: "$name"}}. */
        public Builder asString(String field) {
            return asString(field, field);
        }

        /**
         * Source path (nested pan chale, jem ke "address.city") ne string banavi ne
         * output field naam thi lo. Field na hoy ke null hoy to output null.
         */
        public Builder asString(String outputField, String sourcePath) {
            fields.put(outputField, "{\"$toString\": \"$" + sourcePath + "\"}");
            return this;
        }

        /**
         * Field ne jem che tem rakho (type badlya vagar). Watermark field (BSON Date) mate vapro,
         * jethi Spark ene Timestamp tarike vanche ane index vali $match sathe match rahe.
         */
        public Builder keep(String field) {
            fields.put(field, "1");
            return this;
        }

        /**
         * Array of sub-documents ne, darek field string banavi ne, lo. Field na hoy to khali array.
         * Example: items → [{product_code, qty, unit_price}] (badhu string).
         *
         * @param field     array field naam (output pan same naam)
         * @param subFields array element ni andar na fields
         */
        public Builder arrayOfStrings(String field, String... subFields) {
            String inner = java.util.Arrays.stream(subFields)
                    .map(f -> "\"" + f + "\": {\"$toString\": \"$$it." + f + "\"}")
                    .collect(Collectors.joining(", "));
            fields.put(field, "{\"$map\": {\"input\": {\"$ifNull\": [\"$" + field + "\", []]}, "
                    + "\"as\": \"it\", \"in\": {" + inner + "}}}");
            return this;
        }

        /** Final {@code $project} stage banave che. */
        public MongoProjection build() {
            String body = fields.entrySet().stream()
                    .map(e -> "\"" + e.getKey() + "\": " + e.getValue())
                    .collect(Collectors.joining(", "));
            return new MongoProjection("{\"$project\": {" + body + "}}");
        }
    }
}
