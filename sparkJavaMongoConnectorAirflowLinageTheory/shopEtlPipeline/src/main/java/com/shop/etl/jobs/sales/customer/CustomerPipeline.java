package com.shop.etl.jobs.sales.customer;

import com.shop.etl.common.audit.AuditService;
import com.shop.etl.common.io.MongoSource;
import com.shop.etl.common.io.MongoSourceReader;
import com.shop.etl.common.job.JobContext;
import com.shop.etl.common.pipeline.SourcePipeline;
import com.shop.etl.common.quality.RejectWriter;
import com.shop.etl.dataset.raw.RawCustomer;
import com.shop.etl.dataset.silver.DimCustomer;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

import static org.apache.spark.sql.functions.col;

/**
 * Job-2 PART A: customers → dim_customer (SCD2).
 *
 * Aa class fakt 3 service ne JODE che. Kharekhar kaam:
 *   CustomerSanitizer → saaf karo
 *   CustomerValidator → dedupe + rules
 *   CustomerLoader    → SCD2 upsert
 * Read, persist, count, rejects, gate ane audit common SourcePipeline kare che.
 *
 * Expected (full load 2026-10-21 00:00): read 23, reject 3 (C005 dup, C021, C022), load 20.
 */
@Component
public class CustomerPipeline extends SourcePipeline {

    private final CustomerSanitizer sanitizer;
    private final CustomerValidator validator;
    private final CustomerLoader loader;

    public CustomerPipeline(MongoSourceReader reader, RejectWriter rejectWriter, AuditService audit,
                            CustomerSanitizer sanitizer, CustomerValidator validator, CustomerLoader loader) {
        super(reader, rejectWriter, audit);
        this.sanitizer = sanitizer;
        this.validator = validator;
        this.loader = loader;
    }

    @Override
    protected MongoSource source() {
        return RawCustomer.SOURCE;
    }

    @Override
    protected String targetTable() {
        return DimCustomer.TABLE;
    }

    @Override
    protected Column sourceKey() {
        return col(RawCustomer.CUSTOMER_CODE);
    }

    @Override
    protected Dataset<Row> sanitize(Dataset<Row> raw) {
        return sanitizer.sanitize(raw);
    }

    @Override
    protected Dataset<Row> validate(JobContext ctx, Dataset<Row> sanitized) {
        return validator.validate(sanitized);
    }

    /** Customers ma line level nathi, etle partial hammesha 0. */
    @Override
    protected long load(JobContext ctx, Dataset<Row> valid) {
        loader.load(ctx, valid);
        return 0;
    }
}
