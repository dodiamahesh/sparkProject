package com.shop.etl.jobs.sales.order;

import com.shop.etl.common.quality.Deduplicator;
import com.shop.etl.common.quality.RejectType;
import com.shop.etl.common.quality.Rule;
import com.shop.etl.common.quality.RuleEngine;
import com.shop.etl.dataset.silver.DimCustomer;
import com.shop.etl.dataset.silver.DimProduct;
import com.shop.etl.dataset.silver.FactSales;
import com.shop.etl.jobs.sales.shared.SalesReferenceData;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

import java.util.List;

import static com.shop.etl.dataset.raw.RawOrder.CUSTOMER_CODE;
import static com.shop.etl.dataset.raw.RawOrder.ID;
import static com.shop.etl.dataset.raw.RawOrder.ITEMS;
import static com.shop.etl.dataset.raw.RawOrder.ORDER_NO;
import static com.shop.etl.dataset.raw.RawOrder.ORDER_TS;
import static com.shop.etl.dataset.raw.RawOrder.PAYMENT_METHOD;
import static com.shop.etl.dataset.raw.RawOrder.STATUS;
import static com.shop.etl.dataset.raw.RawOrder.UPDATED_AT;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.not;
import static org.apache.spark.sql.functions.size;

/**
 * ORDERS VALIDATION SERVICE (Job-2, Part B). Design doc Section 6.4. Be level par:
 *
 * DOCUMENT LEVEL ({@link #validateOrders}) — order kharab hoy to AAKHO order reject:
 * | Code   | Prakar     | Rule                                   | Aa data ma         |
 * |--------|------------|----------------------------------------|--------------------|
 * | J2-C01 | Corrupt    | order_ts valid date                    | O1022 "2026-13-45" |
 * | J2-D02 | Duplicate  | same order_no: latest updated_at       | O1005 PLACED       |
 * | J2-V05 | Validation | items khali na hova joie               | O1023              |
 * | J2-V08 | Validation | status allowed list                    | (koi nahi)         |
 * | J2-V11 | Validation | payment_method allowed list (navo)     | (koi nahi)         |
 * | J2-F01 | FK         | customer dim_customer ma hovo joie     | O1021 (C099)       |
 *
 * LINE LEVEL ({@link #validateLines}) — fakt e LINE reject, baki order load thay:
 * | J2-V06 | Validation | qty > 0                                | O1008 line-2       |
 * | J2-V07 | Validation | unit_price > 0                         | (koi nahi)         |
 * | J2-F02 | FK         | product dim_product ma hovo joie       | O1012 line-2 P022  |
 *
 * FK check kem chhelle? Lookup mate Postgres join joie (moghu). Pehla sasta rules thi
 * kharab rows alag kari daiye, pachhi join. (Spark ma badhu ek plan ma che, pan rule order
 * reason nakki kare che.)
 */
@Component
public class OrderValidator {

    static final String QTY_RAW = "qty_raw";
    static final String PRICE_RAW = "unit_price_raw";

    private static final List<Rule> CORRUPT = List.of(
            Rule.of("J2-C01", RejectType.CORRUPT, "Order date valid nathi",
                    col(ORDER_TS).isNull(), ORDER_TS, col(OrderSanitizer.ORDER_TS_RAW)));

    private static final Rule DUPLICATE = Rule.of("J2-D02", RejectType.DUPLICATE,
            "Same order_no 2 vaar. Juno version chhodyo.", null, STATUS, col(STATUS));

    private static final List<Rule> VALIDATION = List.of(
            Rule.of("J2-V05", RejectType.VALIDATION, "Order ma items nathi",
                    size(col(ITEMS)).leq(0), ITEMS, size(col(ITEMS))),
            Rule.of("J2-V08", RejectType.VALIDATION, "Order status allowed list ma nathi",
                    coalesce(not(col(STATUS).isin(SalesReferenceData.OrderStatus.codes())), col(STATUS).isNull()),
                    STATUS, col(STATUS)),
            Rule.of("J2-V11", RejectType.VALIDATION, "Payment method allowed list ma nathi",
                    coalesce(not(col(PAYMENT_METHOD).isin(SalesReferenceData.PaymentMethod.codes())),
                            col(PAYMENT_METHOD).isNull()),
                    PAYMENT_METHOD, col(PAYMENT_METHOD)));

    private static final List<Rule> FOREIGN_KEY = List.of(
            Rule.of("J2-F01", RejectType.FK_MISSING, "Customer dim_customer ma nathi",
                    col(FactSales.CUSTOMER_KEY).isNull(), CUSTOMER_CODE, col(CUSTOMER_CODE)));

    private static final List<Rule> LINE_RULES = List.of(
            Rule.of("J2-V06", RejectType.VALIDATION, "Quantity 0 thi moti nathi (line)",
                    col(FactSales.QUANTITY).isNull().or(col(FactSales.QUANTITY).leq(0)), "qty", col(QTY_RAW)),
            Rule.of("J2-V07", RejectType.VALIDATION, "Unit price 0 thi moti nathi (line)",
                    col(FactSales.UNIT_PRICE).isNull().or(col(FactSales.UNIT_PRICE).leq(0)), "unit_price", col(PRICE_RAW)),
            Rule.of("J2-F02", RejectType.FK_MISSING, "Product dim_product ma nathi (line)",
                    col(DimProduct.PRODUCT_KEY).isNull(), "product", col(DimProduct.PRODUCT_CODE)));

    /**
     * Document level rules + customer FK.
     *
     * Customer key POINT-IN-TIME lookup (SCD2): order vakhte je version chalu hati e key.
     * <pre>
     *   order.customer_code = dim.customer_code
     *   AND order_ts >= valid_from AND order_ts < valid_to
     * </pre>
     * Na male (jem ke order signup pehla no hoy) to CURRENT version ni key (fallback).
     * Banne na male → customer j nathi → J2-F01.
     *
     * @param sanitized        OrderSanitizer output
     * @param customerVersions SalesLookups.customerVersions()
     */
    public Dataset<Row> validateOrders(Dataset<Row> sanitized, Dataset<Row> customerVersions) {
        Dataset<Row> checked = RuleEngine.init(sanitized);
        checked = RuleEngine.apply(checked, CORRUPT);
        checked = Deduplicator.markDuplicates(checked, List.of(ORDER_NO), UPDATED_AT, ID, DUPLICATE);
        checked = RuleEngine.apply(checked, VALIDATION);
        checked = withCustomerKey(checked, customerVersions);
        return RuleEngine.apply(checked, FOREIGN_KEY);
    }

    /**
     * Line level rules (explode pachhi) + product FK.
     *
     * @param lines    OrderLoader.explodeLines output
     * @param products SalesLookups.products() (broadcast)
     */
    public Dataset<Row> validateLines(Dataset<Row> lines, Dataset<Row> products) {
        Dataset<Row> withProduct = lines.join(products,
                lines.col(DimProduct.PRODUCT_CODE).equalTo(products.col(DimProduct.PRODUCT_CODE)), "left")
                .drop(products.col(DimProduct.PRODUCT_CODE));
        return RuleEngine.apply(RuleEngine.init(withProduct), LINE_RULES);
    }

    /** SCD2 point-in-time join + current fallback. Output ma {@code customer_key} column. */
    private static Dataset<Row> withCustomerKey(Dataset<Row> orders, Dataset<Row> customerVersions) {
        Dataset<Row> versions = customerVersions.select(
                col(DimCustomer.CUSTOMER_CODE).as("_pit_code"),
                col(DimCustomer.CUSTOMER_KEY).as("_pit_key"),
                col(DimCustomer.VALID_FROM).as("_pit_from"),
                col(DimCustomer.VALID_TO).as("_pit_to"));
        Dataset<Row> current = customerVersions
                .filter(col(DimCustomer.IS_CURRENT).equalTo("Y"))
                .select(col(DimCustomer.CUSTOMER_CODE).as("_cur_code"), col(DimCustomer.CUSTOMER_KEY).as("_cur_key"));

        return orders
                .join(versions, col(CUSTOMER_CODE).equalTo(col("_pit_code"))
                        .and(col(ORDER_TS).geq(col("_pit_from")))
                        .and(col(ORDER_TS).lt(col("_pit_to"))), "left")
                .join(current, col(CUSTOMER_CODE).equalTo(col("_cur_code")), "left")
                .withColumn(FactSales.CUSTOMER_KEY, coalesce(col("_pit_key"), col("_cur_key")))
                .drop("_pit_code", "_pit_key", "_pit_from", "_pit_to", "_cur_code", "_cur_key");
    }
}
