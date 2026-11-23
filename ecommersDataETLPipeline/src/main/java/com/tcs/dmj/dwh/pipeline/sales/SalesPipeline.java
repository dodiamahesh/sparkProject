package com.tcs.dmj.dwh.pipeline.sales;

import com.tcs.dmj.dwh.connector.source.SourceBatch;
import com.tcs.dmj.dwh.connector.source.SourceDefinition;
import com.tcs.dmj.dwh.connector.target.Scd2Writer;
import com.tcs.dmj.dwh.core.AbstractEtlPipeline;
import com.tcs.dmj.dwh.core.PipelineContext;
import com.tcs.dmj.dwh.core.PipelineToolkit;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

import java.util.List;

import static com.tcs.dmj.dwh.pipeline.sales.SalesStarSchemaMapper.toDimCustomer;
import static com.tcs.dmj.dwh.pipeline.sales.SalesStarSchemaMapper.toDimProduct;
import static com.tcs.dmj.dwh.pipeline.sales.SalesStarSchemaMapper.toFactPayment;
import static com.tcs.dmj.dwh.pipeline.sales.SalesStarSchemaMapper.toFactSales;
import static com.tcs.dmj.dwh.pipeline.sales.SalesStarSchemaMapper.toOrderLines;
import static com.tcs.dmj.dwh.pipeline.sales.SalesStarSchemaMapper.toTypedOrderLines;
import static com.tcs.dmj.dwh.pipeline.sales.SalesStarSchemaMapper.toTypedPayments;
import static com.tcs.dmj.dwh.pipeline.sales.SalesStarSchemaMapper.withCustomerKeyAt;
import static com.tcs.dmj.dwh.pipeline.sales.SalesStarSchemaMapper.withOrderCustomerKey;

/**
 * JOB-2 (J2_SALES) - multiple collections -> multiple dimensions + facts (STAR schema).
 *
 *   products   ------------------------------------------------> dim_product   (SCD1)
 *   customers  ------------------------------------------------> dim_customer  (SCD2: city / state / tier)
 *   orders --+-- status / payment_method --- (seeded dims) ----> dim_order_status, dim_payment_method
 *            +-- items[] explode, one row per line -----------> fact_sales    (grain: one order line)
 *                 lookup dim_customer (point in time), dim_product, dim_date
 *   payments --- lookup order in fact_sales (+ its customer) --> fact_payment  (grain: one payment)
 *
 * Self-contained: dim_product (Job-1 in the design document) is loaded by this pipeline first,
 * so no other job has to run before it.
 *
 * Order matters: products + customers first (orders need their keys), then orders, then payments
 * (a payment needs its order loaded). Rejects (document AND line level) end up in
 * meta.meta_rejected_record; last_run_time moves only after everything was loaded.
 *
 * Trigger: POST /api/admin/dwh/pipelines/sales-pipeline/run   (incremental: updated_at after last_run_time)
 */
@Component
public class SalesPipeline extends AbstractEtlPipeline {

    static final String DIM_PRODUCT = "curated.dim_product";
    static final String DIM_CUSTOMER = "curated.dim_customer";
    static final String DIM_ORDER_STATUS = "curated.dim_order_status";
    static final String DIM_PAYMENT_METHOD = "curated.dim_payment_method";
    static final String FACT_SALES = "curated.fact_sales";
    static final String FACT_PAYMENT = "curated.fact_payment";

    /**
     * SCD2: the address (city / state / pincode) and tier create a new version, so old orders keep the old
     * address. name / email / phone / signup_date are overwritten in every version (SCD1).
     */
    private static final Scd2Writer.Scd2Spec CUSTOMER_SCD2 = new Scd2Writer.Scd2Spec(
            DIM_CUSTOMER, "customer_code", List.of("city", "state", "pincode", "tier"), "changed_at", "first_valid_from");

    public SalesPipeline(PipelineToolkit toolkit) {
        super(toolkit);
    }

    @Override
    public String name() {
        return "sales-pipeline";
    }

    @Override
    public String description() {
        return "Sales star (J2_SALES): products, customers (SCD2), orders, payments -> fact_sales, fact_payment";
    }

    @Override
    public List<SourceDefinition> sources() {
        return List.of(SalesSources.PRODUCTS, SalesSources.CUSTOMERS, SalesSources.ORDERS, SalesSources.PAYMENTS);
    }

    @Override
    public List<String> targetTables() {
        return List.of(DIM_PRODUCT, DIM_CUSTOMER, DIM_ORDER_STATUS, DIM_PAYMENT_METHOD, "silver.dim_date",
                FACT_SALES, FACT_PAYMENT);
    }

    @Override
    public void run(PipelineContext context) {
        // ---------------- PART 0: products -> dim_product (replaces the Job-1 dependency) ----------------
        SourceBatch products = extractAndValidate(context, SalesSources.PRODUCTS,
                SalesSources.productSanitizer(), SalesSources.productRules());
        load(context, toDimProduct(products.valid()), DIM_PRODUCT, "product_code");

        // ---------------- PART A: customers -> dim_customer (SCD2) ----------------
        SourceBatch customers = extractAndValidate(context, SalesSources.CUSTOMERS,
                SalesSources.customerSanitizer(), SalesSources.customerRules());
        loadScd2(context, toDimCustomer(customers.valid()), CUSTOMER_SCD2);

        // ---------------- PART B: orders -> fact_sales (one row per line) ----------------
        SourceBatch orders = extractAndValidate(context, SalesSources.ORDERS,
                SalesSources.orderSanitizer(), SalesSources.orderRules(readTable(DIM_CUSTOMER, "customer_code")));

        // items[] explode: a bad LINE is rejected alone, the rest of the order is still loaded
        SourceBatch lines = validateDerived(context, SalesSources.ORDER_LINES, toOrderLines(orders.valid()),
                SalesSources.orderLineRules(readTable(DIM_PRODUCT, "product_code")));

        Dataset<Row> typedLines = toTypedOrderLines(lines.valid());
        loadDateDimension(context, typedLines, "order_ts");
        Dataset<Row> linesWithKeys = withCustomerKeyAt(typedLines,
                readTable(DIM_CUSTOMER, "customer_key", "customer_code", "valid_from", "valid_to", "is_current"),
                "order_ts");
        linesWithKeys = addSurrogateKey(linesWithKeys, DIM_PRODUCT, "product_key", "product_code");
        linesWithKeys = addSurrogateKey(linesWithKeys, DIM_ORDER_STATUS, "order_status_key", "status_code");
        linesWithKeys = addSurrogateKey(linesWithKeys, DIM_PAYMENT_METHOD, "payment_method_key", "method_code");
        load(context, toFactSales(linesWithKeys), FACT_SALES, "order_no", "line_no");

        // ---------------- PART C: payments -> fact_payment ----------------
        // FK check against the WHOLE fact_sales table (this run + earlier runs), not only this batch
        SourceBatch payments = extractAndValidate(context, SalesSources.PAYMENTS,
                SalesSources.paymentSanitizer(), SalesSources.paymentRules(readTable(FACT_SALES, "order_no")));

        Dataset<Row> typedPayments = toTypedPayments(payments.valid());
        loadDateDimension(context, typedPayments, "paid_at");
        Dataset<Row> paymentsWithKeys = withOrderCustomerKey(typedPayments, readTable(FACT_SALES, "order_no", "customer_key"));
        paymentsWithKeys = addSurrogateKey(paymentsWithKeys, DIM_PAYMENT_METHOD, "payment_method_key", "method_code");
        load(context, toFactPayment(paymentsWithKeys), FACT_PAYMENT, "payment_id");

        // ---------------- PART D: SOFT DELETES (rows are marked, never removed) ----------------
        applyDeletes(context, products, DIM_PRODUCT, "product_code");
        applyDeletesScd2(context, customers, CUSTOMER_SCD2);                        // closes the current version
        applyDeletes(context, orders, FACT_SALES, "order_no");                        // every line of a deleted order
        applyDeletes(context, payments, FACT_PAYMENT, "payment_id");
    }
}
