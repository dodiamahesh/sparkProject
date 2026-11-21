package com.shop.etl.jobs.sales.payment;

import com.shop.etl.common.quality.Clean;
import com.shop.etl.jobs.sales.shared.SalesReferenceData;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

import static com.shop.etl.dataset.raw.RawPayment.AMOUNT;
import static com.shop.etl.dataset.raw.RawPayment.METHOD;
import static com.shop.etl.dataset.raw.RawPayment.ORDER_NO;
import static com.shop.etl.dataset.raw.RawPayment.PAID_AT;
import static com.shop.etl.dataset.raw.RawPayment.PAYMENT_ID;
import static com.shop.etl.dataset.raw.RawPayment.STATUS;
import static org.apache.spark.sql.functions.col;

/**
 * PAYMENTS SANITIZATION SERVICE (Job-2, Part C). Design doc rules J2-S08, J2-S09.
 *
 * | Rule   | Field  | Before        | After      |
 * |--------|--------|---------------|------------|
 * | J2-S08 | amount | "₹29,000"     | 29000.00   |
 * | J2-S09 | status | "success"     | SUCCESS    |
 * | J2-S09 | method | "Net Banking" | NETBANKING |
 *
 * amount ane paid_at ni ORIGINAL value {@code *_raw} column ma rahe: parse na thay ("abc")
 * to Validator CORRUPT kahe ane bad_value ma "abc" lakhe.
 */
@Component
public class PaymentSanitizer {

    static final String AMOUNT_RAW = "amount_raw";
    static final String PAID_AT_RAW = "paid_at_raw";

    /** Raw payments saaf kare che. Koi row kadhti nathi. */
    public Dataset<Row> sanitize(Dataset<Row> raw) {
        return raw
                .withColumn(PAYMENT_ID, Clean.upperTrim(col(PAYMENT_ID)))
                .withColumn(ORDER_NO, Clean.upperTrim(col(ORDER_NO)))
                .withColumn(STATUS, Clean.upperTrim(col(STATUS)))                                 // J2-S09
                .withColumn(METHOD, SalesReferenceData.normalizePaymentMethod(col(METHOD)))       // J2-S09
                .withColumn(AMOUNT_RAW, col(AMOUNT))
                .withColumn(AMOUNT, Clean.amount(col(AMOUNT)))                                    // J2-S08
                .withColumn(PAID_AT_RAW, col(PAID_AT))
                .withColumn(PAID_AT, Clean.trimToNull(col(PAID_AT)).cast("timestamp"));
    }
}
