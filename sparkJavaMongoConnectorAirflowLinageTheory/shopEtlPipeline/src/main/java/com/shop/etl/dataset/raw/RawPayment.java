package com.shop.etl.dataset.raw;

import com.shop.etl.common.io.MongoProjection;
import com.shop.etl.common.io.MongoSource;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * DATASET CLASS: MongoDB {@code payments} collection (RAW layer). Design doc Section 2.6.
 *
 * {@code amount} kyarek number (2400), kyarek text ("₹29,000"), kyarek kachro ("abc").
 * String tarike vanchi ne Sanitizer {@code Clean.amount} thi number banave che.
 */
public final class RawPayment {

    public static final String ID = "_id";
    public static final String PAYMENT_ID = "payment_id";
    public static final String ORDER_NO = "order_no";
    public static final String METHOD = "method";
    public static final String AMOUNT = "amount";
    public static final String STATUS = "status";
    public static final String PAID_AT = "paid_at";
    public static final String UPDATED_AT = "updated_at";

    public static final StructType SCHEMA = new StructType()
            .add(ID, DataTypes.StringType)
            .add(PAYMENT_ID, DataTypes.StringType)
            .add(ORDER_NO, DataTypes.StringType)
            .add(METHOD, DataTypes.StringType)
            .add(AMOUNT, DataTypes.StringType)
            .add(STATUS, DataTypes.StringType)
            .add(PAID_AT, DataTypes.StringType)
            .add(UPDATED_AT, DataTypes.TimestampType);

    public static final MongoProjection PROJECTION = MongoProjection.builder()
            .asString(ID)
            .asString(PAYMENT_ID)
            .asString(ORDER_NO)
            .asString(METHOD)
            .asString(AMOUNT)
            .asString(STATUS)
            .asString(PAID_AT)
            .keep(UPDATED_AT)
            .build();

    public static final MongoSource SOURCE = new MongoSource("payments", UPDATED_AT, PROJECTION, SCHEMA);

    private RawPayment() {
    }
}
