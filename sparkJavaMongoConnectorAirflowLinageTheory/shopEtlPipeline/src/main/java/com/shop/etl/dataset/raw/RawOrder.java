package com.shop.etl.dataset.raw;

import com.shop.etl.common.io.MongoProjection;
import com.shop.etl.common.io.MongoSource;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * DATASET CLASS: MongoDB {@code orders} collection (RAW layer). Design doc Section 2.5.
 *
 * Order ma {@code items} ARRAY che: [{product_code, qty, unit_price}, ...]. Array no darek
 * field pan string tarike aave che (qty "0", unit_price "999"). OrderLoader ene explode kari ne
 * line wise rows banave che.
 *
 * {@code order_ts} Mongo ma mostly Date che, pan kyarek kharab string ("2026-13-45").
 * {@code $toString} Date ne ISO text ("2026-10-01T04:45:00.000Z") banave che, ane kharab text
 * ne jem che tem rakhe che. Pachhi Sanitizer parse kare: parse na thay → CORRUPT.
 *
 * shipping ane order_total Job-2 ma nathi joita, etle projection ma nathi (ochho data).
 */
public final class RawOrder {

    public static final String ID = "_id";
    public static final String ORDER_NO = "order_no";
    public static final String CUSTOMER_CODE = "customer_code";
    public static final String ORDER_TS = "order_ts";
    public static final String STATUS = "status";
    public static final String PAYMENT_METHOD = "payment_method";
    public static final String ITEMS = "items";
    public static final String UPDATED_AT = "updated_at";

    /** items array ni andar na fields. */
    public static final String ITEM_PRODUCT_CODE = "product_code";
    public static final String ITEM_QTY = "qty";
    public static final String ITEM_UNIT_PRICE = "unit_price";

    private static final StructType ITEM = new StructType()
            .add(ITEM_PRODUCT_CODE, DataTypes.StringType)
            .add(ITEM_QTY, DataTypes.StringType)
            .add(ITEM_UNIT_PRICE, DataTypes.StringType);

    public static final StructType SCHEMA = new StructType()
            .add(ID, DataTypes.StringType)
            .add(ORDER_NO, DataTypes.StringType)
            .add(CUSTOMER_CODE, DataTypes.StringType)
            .add(ORDER_TS, DataTypes.StringType)
            .add(STATUS, DataTypes.StringType)
            .add(PAYMENT_METHOD, DataTypes.StringType)
            .add(ITEMS, DataTypes.createArrayType(ITEM))
            .add(UPDATED_AT, DataTypes.TimestampType);

    public static final MongoProjection PROJECTION = MongoProjection.builder()
            .asString(ID)
            .asString(ORDER_NO)
            .asString(CUSTOMER_CODE)
            .asString(ORDER_TS)
            .asString(STATUS)
            .asString(PAYMENT_METHOD)
            .arrayOfStrings(ITEMS, ITEM_PRODUCT_CODE, ITEM_QTY, ITEM_UNIT_PRICE)
            .keep(UPDATED_AT)
            .build();

    public static final MongoSource SOURCE = new MongoSource("orders", UPDATED_AT, PROJECTION, SCHEMA);

    private RawOrder() {
    }
}
