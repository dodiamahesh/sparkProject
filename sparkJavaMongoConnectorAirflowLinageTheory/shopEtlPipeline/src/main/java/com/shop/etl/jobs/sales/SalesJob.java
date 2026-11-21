package com.shop.etl.jobs.sales;

import com.shop.etl.common.audit.SourceStats;
import com.shop.etl.common.io.MongoSource;
import com.shop.etl.common.job.EtlJob;
import com.shop.etl.common.job.JobContext;
import com.shop.etl.dataset.raw.RawCustomer;
import com.shop.etl.dataset.raw.RawOrder;
import com.shop.etl.dataset.raw.RawPayment;
import com.shop.etl.jobs.sales.customer.CustomerPipeline;
import com.shop.etl.jobs.sales.order.OrderPipeline;
import com.shop.etl.jobs.sales.payment.PaymentPipeline;
import com.shop.etl.jobs.sales.shared.SalesReferenceData;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * JOB-2: J2_SALES. Multiple collections → Star schema (dim + fact). Design doc Section 6.
 *
 * <pre>
 *   customers ──────────────────────────────────────► dim_customer (SCD2)
 *   orders ──┬── status / payment_method ───────────► dim_order_status, dim_payment_method (seed)
 *            └── items[] explode + customer/product lookup ─► fact_sales
 *   payments ──── order lookup (fact_sales) ─────────► fact_payment
 * </pre>
 *
 * Job ni andar no ORDER jaruri che:
 *   1. reference dims seed   (order ane payment ne status/method key joie)
 *   2. customers             (order ne customer_key joie)
 *   3. orders                (payment ne order loaded joie)
 *   4. payments
 * Koi pan part fail thay to aagla parts nathi chalta, ane watermark nathi vadhato. Je part
 * pehla load thai gaya e re-run ma pharithi aavshe pan UPSERT ne lidhe duplicate nahi bane.
 *
 * Dependency: Job-1 (dim_product) pehla chalvo joie.
 */
@Component
public class SalesJob implements EtlJob {

    public static final String NAME = "J2_SALES";

    private final SalesReferenceData referenceData;
    private final CustomerPipeline customers;
    private final OrderPipeline orders;
    private final PaymentPipeline payments;

    public SalesJob(SalesReferenceData referenceData, CustomerPipeline customers,
                    OrderPipeline orders, PaymentPipeline payments) {
        this.referenceData = referenceData;
        this.customers = customers;
        this.orders = orders;
        this.payments = payments;
    }

    @Override
    public String name() {
        return NAME;
    }

    /** Watermark rows: customers, orders, payments (aa j processing order). */
    @Override
    public List<MongoSource> sources() {
        return List.of(RawCustomer.SOURCE, RawOrder.SOURCE, RawPayment.SOURCE);
    }

    /** Upar lakhela order ma 3 parts chalave che. */
    @Override
    public Map<String, SourceStats> run(JobContext ctx) {
        referenceData.seed();
        Map<String, SourceStats> stats = new LinkedHashMap<>();
        stats.put(RawCustomer.SOURCE.name(), customers.run(ctx));
        stats.put(RawOrder.SOURCE.name(), orders.run(ctx));
        stats.put(RawPayment.SOURCE.name(), payments.run(ctx));
        return stats;
    }
}
