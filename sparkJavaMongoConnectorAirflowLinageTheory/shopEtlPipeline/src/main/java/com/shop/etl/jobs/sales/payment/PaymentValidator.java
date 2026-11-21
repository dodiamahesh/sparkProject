package com.shop.etl.jobs.sales.payment;

import com.shop.etl.common.quality.Deduplicator;
import com.shop.etl.common.quality.RejectType;
import com.shop.etl.common.quality.Rule;
import com.shop.etl.common.quality.RuleEngine;
import com.shop.etl.dataset.silver.FactPayment;
import com.shop.etl.dataset.silver.FactSales;
import com.shop.etl.jobs.sales.shared.SalesReferenceData;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

import java.util.List;

import static com.shop.etl.dataset.raw.RawPayment.AMOUNT;
import static com.shop.etl.dataset.raw.RawPayment.ID;
import static com.shop.etl.dataset.raw.RawPayment.METHOD;
import static com.shop.etl.dataset.raw.RawPayment.ORDER_NO;
import static com.shop.etl.dataset.raw.RawPayment.PAID_AT;
import static com.shop.etl.dataset.raw.RawPayment.PAYMENT_ID;
import static com.shop.etl.dataset.raw.RawPayment.STATUS;
import static com.shop.etl.dataset.raw.RawPayment.UPDATED_AT;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.not;

/**
 * PAYMENTS VALIDATION SERVICE (Job-2, Part C). Design doc Section 6.4.
 *
 * | Code   | Prakar     | Rule                                         | Aa data ma          |
 * |--------|------------|----------------------------------------------|---------------------|
 * | J2-C02 | Corrupt    | amount number banvo joie                     | PAY22 "abc"         |
 * | J2-C03 | Corrupt    | paid_at valid date (hoy to)                  | (koi nahi)          |
 * | J2-D03 | Duplicate  | same payment_id: latest (gateway callback)   | PAY05 20:11 vado    |
 * | J2-V09 | Validation | amount > 0                                   | (koi nahi)          |
 * | J2-V10 | Validation | status SUCCESS/FAILED/REFUNDED/PENDING       | (koi nahi)          |
 * | J2-V12 | Validation | method allowed list (navo)                   | (koi nahi)          |
 * | J2-F03 | FK         | order CURATED fact_sales ma hovo joie        | PAY21 (O1021 reject)|
 *
 * J2-F03 kem aakhi curated table ma, fakt aa batch ma nahi? Incremental ma PAY10 (04:00 e
 * SUCCESS) no order O1010 10-07 e load thayo hato. E aa batch ma nathi, pan fact_sales ma che.
 */
@Component
public class PaymentValidator {

    static final List<String> ALLOWED_STATUS = List.of("SUCCESS", "FAILED", "REFUNDED", "PENDING");

    private static final List<Rule> CORRUPT = List.of(
            Rule.of("J2-C02", RejectType.CORRUPT, "Amount number nathi",
                    col(AMOUNT).isNull(), AMOUNT, col(PaymentSanitizer.AMOUNT_RAW)),
            Rule.of("J2-C03", RejectType.CORRUPT, "paid_at valid date nathi",
                    col(PAID_AT).isNull().and(col(PaymentSanitizer.PAID_AT_RAW).isNotNull()),
                    PAID_AT, col(PaymentSanitizer.PAID_AT_RAW)));

    private static final Rule DUPLICATE = Rule.of("J2-D03", RejectType.DUPLICATE,
            "Same payment_id 2 vaar (gateway callback). Juno chhodyo.", null, UPDATED_AT, col(UPDATED_AT));

    private static final List<Rule> VALIDATION = List.of(
            Rule.of("J2-V09", RejectType.VALIDATION, "Amount 0 thi moto hovo joie",
                    col(AMOUNT).leq(0), AMOUNT, col(AMOUNT)),
            Rule.of("J2-V10", RejectType.VALIDATION, "Payment status allowed list ma nathi",
                    coalesce(not(col(STATUS).isin(ALLOWED_STATUS.toArray())), col(STATUS).isNull()),
                    STATUS, col(STATUS)),
            Rule.of("J2-V12", RejectType.VALIDATION, "Payment method allowed list ma nathi",
                    coalesce(not(col(METHOD).isin(SalesReferenceData.PaymentMethod.codes())), col(METHOD).isNull()),
                    METHOD, col(METHOD)));

    private static final List<Rule> FOREIGN_KEY = List.of(
            Rule.of("J2-F03", RejectType.FK_MISSING, "Order curated ma nathi",
                    col(FactPayment.CUSTOMER_KEY).isNull(), ORDER_NO, col(ORDER_NO)));

    /**
     * @param sanitized      PaymentSanitizer output
     * @param orderCustomers SalesLookups.orderCustomers(): order_no → customer_key
     * @return same data + customer_key + {@code _reject}
     */
    public Dataset<Row> validate(Dataset<Row> sanitized, Dataset<Row> orderCustomers) {
        Dataset<Row> checked = RuleEngine.init(sanitized);
        checked = RuleEngine.apply(checked, CORRUPT);
        checked = Deduplicator.markDuplicates(checked, List.of(PAYMENT_ID), UPDATED_AT, ID, DUPLICATE);
        checked = RuleEngine.apply(checked, VALIDATION);

        Dataset<Row> orders = orderCustomers.select(
                col(FactSales.ORDER_NO).as("_order_no"), col(FactSales.CUSTOMER_KEY));
        checked = checked.join(orders, col(ORDER_NO).equalTo(col("_order_no")), "left").drop("_order_no");
        return RuleEngine.apply(checked, FOREIGN_KEY);
    }
}
