package com.shop.etl.jobs.sales.payment;

import com.shop.etl.common.audit.AuditService;
import com.shop.etl.common.io.MongoSource;
import com.shop.etl.common.io.MongoSourceReader;
import com.shop.etl.common.job.JobContext;
import com.shop.etl.common.pipeline.SourcePipeline;
import com.shop.etl.common.quality.RejectWriter;
import com.shop.etl.dataset.raw.RawPayment;
import com.shop.etl.dataset.silver.FactPayment;
import com.shop.etl.jobs.sales.shared.SalesLookups;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import static org.apache.spark.sql.functions.col;

/**
 * Job-2 PART C: payments → fact_payment.
 *
 * Orders part PACHHI chale che: payment no order fact_sales ma loaded hovo joie (J2-F03).
 *
 * Lookback (yml {@code payment-order-lookback-days}, default 90):
 *   Incremental ma fact_sales AAKHI nathi vanchti. Fakt chhella 90 divas na orders, kem ke
 *   payment order pachhi thoda j divas ma aave. Moti fact table ma aa bahu motu bachat che.
 *   Full load ma koi limit nahi (badha orders).
 *
 * Expected (full load): read 23, reject 3 (PAY05 dup, PAY21 FK, PAY22 corrupt), load 20.
 */
@Component
public class PaymentPipeline extends SourcePipeline {

    private static final DateTimeFormatter DATE_KEY = DateTimeFormatter.ofPattern("yyyyMMdd");

    private final PaymentSanitizer sanitizer;
    private final PaymentValidator validator;
    private final PaymentLoader loader;
    private final SalesLookups lookups;

    public PaymentPipeline(MongoSourceReader reader, RejectWriter rejectWriter, AuditService audit,
                           PaymentSanitizer sanitizer, PaymentValidator validator, PaymentLoader loader,
                           SalesLookups lookups) {
        super(reader, rejectWriter, audit);
        this.sanitizer = sanitizer;
        this.validator = validator;
        this.loader = loader;
        this.lookups = lookups;
    }

    @Override
    protected MongoSource source() {
        return RawPayment.SOURCE;
    }

    @Override
    protected String targetTable() {
        return FactPayment.TABLE;
    }

    @Override
    protected Column sourceKey() {
        return col(RawPayment.PAYMENT_ID);
    }

    @Override
    protected Dataset<Row> sanitize(Dataset<Row> raw) {
        return sanitizer.sanitize(raw);
    }

    @Override
    protected Dataset<Row> validate(JobContext ctx, Dataset<Row> sanitized) {
        return validator.validate(sanitized, lookups.orderCustomers(lookbackDateKey(ctx)));
    }

    @Override
    protected long load(JobContext ctx, Dataset<Row> valid) {
        loader.load(ctx, valid, lookups.paymentMethods());
        return 0;
    }

    /** Full load → null (limit nahi). Incremental → (window end − lookback divas) no date key, IST ma. */
    private static Integer lookbackDateKey(JobContext ctx) {
        if (ctx.window(RawPayment.SOURCE.name()).isFullLoad()) {
            return null;
        }
        int days = Integer.parseInt(ctx.config().param("payment-order-lookback-days", "90"));
        ZoneId zone = ZoneId.systemDefault();   // SparkConfig e business timezone (IST) set karyo che
        return Integer.parseInt(DATE_KEY.format(ctx.windowEnd().atZone(zone).minusDays(days)));
    }
}
