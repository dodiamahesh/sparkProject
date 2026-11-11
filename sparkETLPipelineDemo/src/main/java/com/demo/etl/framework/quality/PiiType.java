package com.demo.etl.framework.quality;

import org.apache.spark.sql.Column;

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.regexp_replace;
import static org.apache.spark.sql.functions.when;

/**
 * How a personal-data (PII) field is masked. Masking keeps the length / shape, so masked values
 * still fit the existing PostgreSQL columns and are still useful to read ("which domain?", "last 4 digits").
 *
 * Masking runs AFTER validation (rules see the real value), and BEFORE anything is written:
 * silver tables, the rejected table (raw_record + error_reason) never contain the real value.
 */
public enum PiiType {

    /** "rahul.shah@gmail.com" -> "r****@gmail.com"; a value without "@" -> "****". */
    EMAIL {
        @Override
        public Column mask(Column value) {
            return when(value.isNull(), lit(null))
                    .when(value.contains("@"), regexp_replace(value, "^(.)[^@]*", "$1****"))
                    .otherwise(lit("****"));
        }
    },

    /** "9876543210" -> "XXXXXX3210" (every character except the last 4). */
    PHONE {
        @Override
        public Column mask(Column value) {
            return regexp_replace(value, ".(?=.{4})", "X");
        }
    };

    public abstract Column mask(Column value);
}
