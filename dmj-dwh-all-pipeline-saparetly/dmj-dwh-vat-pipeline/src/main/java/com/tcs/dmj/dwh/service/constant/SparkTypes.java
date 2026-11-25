package com.tcs.dmj.dwh.service.constant;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

/** Spark types shared by every pipeline. MONEY = NUMERIC(18,2) in PostgreSQL. */
public final class SparkTypes {

    public static final DataType MONEY = DataTypes.createDecimalType(18, 2);
    public static final DataType RATE = DataTypes.createDecimalType(9, 4);
    public static final DataType TIMESTAMP = DataTypes.TimestampType;
    public static final DataType DATE = DataTypes.DateType;
    public static final DataType INT = DataTypes.IntegerType;

    private SparkTypes() {
    }
}
