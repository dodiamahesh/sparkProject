package com.demo.etl.framework.common;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

/**
 * Data types used by all pipelines, so every pipeline casts money/dates the same way.
 */
public final class SparkTypes {

    /** Money columns: NUMERIC(18,2) in PostgreSQL. */
    public static final DataType MONEY = DataTypes.createDecimalType(18, 2);
    public static final DataType RATE = DataTypes.createDecimalType(9, 4);
    public static final DataType TIMESTAMP = DataTypes.TimestampType;
    public static final DataType DATE = DataTypes.DateType;
    public static final DataType INT = DataTypes.IntegerType;

    private SparkTypes() {
    }
}
