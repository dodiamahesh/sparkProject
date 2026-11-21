package com.shop.etl.dataset.raw;

import com.shop.etl.common.io.MongoProjection;
import com.shop.etl.common.io.MongoSource;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * DATASET CLASS: MongoDB {@code customers} collection (RAW layer). Design doc Section 2.4.
 *
 * Aa class ma 3 vaat che:
 *   1. Column naam (constants) → code ma "customer_code" string varamvar na lakhvi pade, ane
 *      typo compile time par pakdai jay.
 *   2. Spark SCHEMA → Mongo mathi vanchti vakhte fix type.
 *   3. Mongo PROJECTION → kaya fields lavva, nested field (address.city) flat karvo.
 *
 * Kem Java bean (getter/setter POJO) nahi? Spark ma {@code Dataset<RawCustomer>} (bean encoder)
 * vapro to darek row Java object ma convert thay, ane map/filter lambda Catalyst mate black box
 * bane. {@code Dataset<Row>} + column expressions sauthi fast che. Etle aa "schema class" che:
 * typed column naam + schema, pan processing Row par.
 *
 * RAW niyam: badha business fields STRING tarike vanchiye ({@code $toString}). Kharab type
 * (number ni jagyae text) thi job crash na thay; Sanitizer / Validator pachhi parse kare.
 * Fakt watermark field (updated_at) BSON Date → Timestamp rahe che.
 */
public final class RawCustomer {

    public static final String ID = "_id";
    public static final String CUSTOMER_CODE = "customer_code";
    public static final String NAME = "name";
    public static final String EMAIL = "email";
    public static final String PHONE = "phone";
    public static final String CITY = "city";
    public static final String STATE = "state";
    public static final String PINCODE = "pincode";
    public static final String TIER = "tier";
    public static final String SIGNUP_DATE = "signup_date";
    public static final String UPDATED_AT = "updated_at";

    /** Spark schema (projection na output sathe same order / type). */
    public static final StructType SCHEMA = new StructType()
            .add(ID, DataTypes.StringType)
            .add(CUSTOMER_CODE, DataTypes.StringType)
            .add(NAME, DataTypes.StringType)
            .add(EMAIL, DataTypes.StringType)
            .add(PHONE, DataTypes.StringType)
            .add(CITY, DataTypes.StringType)
            .add(STATE, DataTypes.StringType)
            .add(PINCODE, DataTypes.StringType)
            .add(TIER, DataTypes.StringType)
            .add(SIGNUP_DATE, DataTypes.StringType)
            .add(UPDATED_AT, DataTypes.TimestampType);

    /** Mongo $project: address.city → city (flat), badhu string, updated_at Date j rahe. */
    public static final MongoProjection PROJECTION = MongoProjection.builder()
            .asString(ID)
            .asString(CUSTOMER_CODE)
            .asString(NAME)
            .asString(EMAIL)
            .asString(PHONE)
            .asString(CITY, "address.city")
            .asString(STATE, "address.state")
            .asString(PINCODE, "address.pincode")
            .asString(TIER)
            .asString(SIGNUP_DATE)
            .keep(UPDATED_AT)
            .build();

    /** MongoSourceReader ane watermark mate source definition. */
    public static final MongoSource SOURCE = new MongoSource("customers", UPDATED_AT, PROJECTION, SCHEMA);

    private RawCustomer() {
    }
}
