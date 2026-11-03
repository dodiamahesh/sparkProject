package com.demo.etl.pipelines.payment;

import com.demo.etl.framework.pipeline.AbstractEtlPipeline;
import com.demo.etl.framework.pipeline.PipelineContext;
import com.demo.etl.framework.pipeline.PipelineToolkit;
import com.demo.etl.framework.source.SourceBatch;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

import java.util.List;

import static com.demo.etl.pipelines.payment.PaymentStarSchemaMapper.toDimBiller;
import static com.demo.etl.pipelines.payment.PaymentStarSchemaMapper.toDimPayer;
import static com.demo.etl.pipelines.payment.PaymentStarSchemaMapper.toDimPaymentMethod;
import static com.demo.etl.pipelines.payment.PaymentStarSchemaMapper.toFactPayment;
import static com.demo.etl.pipelines.payment.PaymentStarSchemaMapper.toTypedPayments;

/**
 * EXAMPLE 1 - ONE collection -> star schema.
 *
 *   MongoDB: payments
 *        |
 *        v  (validate, clean, de-duplicate)
 *   PostgreSQL: dim_payer, dim_payment_method, dim_biller, dim_date  ->  fact_payment
 */
@Component
public class PaymentPipeline extends AbstractEtlPipeline {

    public PaymentPipeline(PipelineToolkit toolkit) {
        super(toolkit);
    }

    @Override
    public String name() {
        return "payment-pipeline";
    }

    @Override
    public String description() {
        return "Single collection example: payments -> dim_payer, dim_payment_method, dim_biller, dim_date, fact_payment";
    }

    @Override
    public List<String> sourceCollections() {
        return List.of("payments");
    }

    @Override
    public List<String> targetTables() {
        return List.of("silver.dim_payer", "silver.dim_payment_method", "silver.dim_biller",
                "silver.dim_date", "silver.fact_payment");
    }

    @Override
    public List<String> fullLoadTruncateTables() {
        return List.of("silver.fact_payment");
    }

    @Override
    public void run(PipelineContext context) {
        // 1. EXTRACT + DATA QUALITY (bad rows are kept aside and saved automatically)
        SourceBatch payments = extractAndValidate(context,
                PaymentSource.PAYMENTS, PaymentSource.sanitizer(), PaymentSource.rules());

        // 2. TRANSFORM: rename + cast to real types
        Dataset<Row> typedPayments = toTypedPayments(payments.valid());

        // 3. LOAD DIMENSIONS first, because the fact needs their surrogate keys
        loadDateDimension(context, typedPayments, "payment_ts");
        load(context, toDimPayer(typedPayments), "silver.dim_payer", "payer_id");
        load(context, toDimPaymentMethod(typedPayments), "silver.dim_payment_method", "payment_method", "payment_provider");
        load(context, toDimBiller(typedPayments), "silver.dim_biller", "biller_name");

        // 4. LOAD FACT: look up surrogate keys, then upsert on payment_id
        Dataset<Row> withKeys = addSurrogateKey(typedPayments, "silver.dim_payer", "payer_key", "payer_id");
        withKeys = addSurrogateKey(withKeys, "silver.dim_payment_method", "payment_method_key", "payment_method", "payment_provider");
        withKeys = addSurrogateKey(withKeys, "silver.dim_biller", "biller_key", "biller_name");
        load(context, toFactPayment(withKeys), "silver.fact_payment", "payment_id");
    }
}
