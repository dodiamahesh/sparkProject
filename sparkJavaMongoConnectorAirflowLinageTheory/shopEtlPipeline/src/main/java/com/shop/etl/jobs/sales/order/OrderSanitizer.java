package com.shop.etl.jobs.sales.order;

import com.shop.etl.common.quality.Clean;
import com.shop.etl.jobs.sales.shared.SalesReferenceData;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

import static com.shop.etl.dataset.raw.RawOrder.CUSTOMER_CODE;
import static com.shop.etl.dataset.raw.RawOrder.ORDER_NO;
import static com.shop.etl.dataset.raw.RawOrder.ORDER_TS;
import static com.shop.etl.dataset.raw.RawOrder.PAYMENT_METHOD;
import static com.shop.etl.dataset.raw.RawOrder.STATUS;
import static org.apache.spark.sql.functions.col;

/**
 * ORDERS SANITIZATION SERVICE (Job-2, Part B). Design doc rules J2-S06, J2-S07.
 *
 * | Rule   | Field          | Before              | After        |
 * |--------|----------------|---------------------|--------------|
 * | J2-S06 | status         | "delivered"         | DELIVERED    |
 * | J2-S07 | payment_method | "upi ", Net Banking | UPI, NETBANKING |
 * |        | order_ts       | ISO text            | Timestamp    |
 *
 * order_ts parse: Mongo $toString Date ne "2026-10-01T04:45:00.000Z" aape. Spark cast thi
 * Timestamp bane. Kharab text ("2026-13-45") → NULL. Original {@code order_ts_raw} ma rahe,
 * jethi Validator ene CORRUPT kahi ne sachi kharab value lakhi shake.
 *
 * Items array ahiya NATHI sudhartu: e line level che, explode pachhi OrderLoader / validator ma.
 */
@Component
public class OrderSanitizer {

    static final String ORDER_TS_RAW = "order_ts_raw";

    /** Raw orders saaf kare che. Koi row kadhti nathi. */
    public Dataset<Row> sanitize(Dataset<Row> raw) {
        return raw
                .withColumn(ORDER_NO, Clean.upperTrim(col(ORDER_NO)))
                .withColumn(CUSTOMER_CODE, Clean.upperTrim(col(CUSTOMER_CODE)))
                .withColumn(STATUS, Clean.upperTrim(col(STATUS)))                                        // J2-S06
                .withColumn(PAYMENT_METHOD, SalesReferenceData.normalizePaymentMethod(col(PAYMENT_METHOD))) // J2-S07
                .withColumn(ORDER_TS_RAW, col(ORDER_TS))
                .withColumn(ORDER_TS, Clean.trimToNull(col(ORDER_TS)).cast("timestamp"));
    }
}
