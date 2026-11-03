package com.demo.etl.pipelines.payment;

import com.demo.etl.framework.common.SparkTypes;
import com.demo.etl.framework.common.SparkUtils;
import com.demo.etl.framework.target.DateDimension;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;

/**
 * TRANSFORM step: splits one flat "payments" collection into star-schema tables.
 * Pure functions (DataFrame in -> DataFrame out), so each method can be unit tested alone.
 */
final class PaymentStarSchemaMapper {

    private PaymentStarSchemaMapper() {
    }

    /** MongoDB names (camelCase, String) -> PostgreSQL names (snake_case, typed). */
    static Dataset<Row> toTypedPayments(Dataset<Row> validPayments) {
        return validPayments.select(
                col("paymentId").as("payment_id"),
                col("payerId").as("payer_id"),
                col("payerName").as("payer_name"),
                col("payerEmail").as("payer_email"),
                col("payerPhone").as("payer_phone"),
                col("payerCity").as("payer_city"),
                col("paymentMethod").as("payment_method"),
                col("paymentProvider").as("payment_provider"),
                col("billerName").as("biller_name"),
                col("billerCategory").as("biller_category"),
                col("amount").cast(SparkTypes.MONEY).as("amount"),
                col("currency"),
                col("status").as("payment_status"),
                col("referenceNumber").as("reference_number"),
                col("paymentDate").cast(SparkTypes.TIMESTAMP).as("payment_ts"),
                col("updatedAt").cast(SparkTypes.TIMESTAMP).as("updated_at"));
    }

    /** One row per payer. A payer appears in many payments, keep the latest details. */
    static Dataset<Row> toDimPayer(Dataset<Row> payments) {
        return SparkUtils.latestRecordPerKey(payments, "payer_id", "updated_at")
                .select("payer_id", "payer_name", "payer_email", "payer_phone", "payer_city");
    }

    static Dataset<Row> toDimPaymentMethod(Dataset<Row> payments) {
        return payments.select("payment_method", "payment_provider").distinct();
    }

    static Dataset<Row> toDimBiller(Dataset<Row> payments) {
        return SparkUtils.latestRecordPerKey(payments, "biller_name", "updated_at")
                .select("biller_name", "biller_category");
    }

    /** Expects the surrogate keys (payer_key, payment_method_key, biller_key) to be added already. */
    static Dataset<Row> toFactPayment(Dataset<Row> paymentsWithKeys) {
        return paymentsWithKeys.select(
                col("payment_id"),
                col("payer_key"),
                col("payment_method_key"),
                col("biller_key"),
                DateDimension.dateKey(col("payment_ts")).as("date_key"),
                col("amount"),
                col("currency"),
                col("payment_status"),
                col("reference_number"),
                col("payment_ts"));
    }
}
