package com.shop.etl.jobs.sales.order;

import com.shop.etl.common.audit.AuditService;
import com.shop.etl.common.io.MongoSource;
import com.shop.etl.common.io.MongoSourceReader;
import com.shop.etl.common.job.JobContext;
import com.shop.etl.common.pipeline.SourcePipeline;
import com.shop.etl.common.quality.RejectWriter;
import com.shop.etl.common.quality.RuleEngine;
import com.shop.etl.dataset.raw.RawOrder;
import com.shop.etl.dataset.silver.FactSales;
import com.shop.etl.jobs.sales.shared.SalesLookups;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;
import org.springframework.stereotype.Component;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

/**
 * Job-2 PART B: orders → fact_sales (line level).
 *
 * Document level (SourcePipeline template): sanitize → validateOrders → rejects → gate.
 * Line level (aa class nu load):
 * <pre>
 *   explode lines → validateLines (qty, price, product FK) → PERSIST
 *     ├─ rejected lines → meta_rejected (LINE level, source_key "O1008/line-2")
 *     │                   partial = ketla orders ni line reject thai
 *     └─ valid lines    → fact_sales upsert
 * </pre>
 *
 * Expected (full load): read 24, doc reject 4, processed 20, partial 2, 32 lines → 30 load.
 */
@Component
public class OrderPipeline extends SourcePipeline {

    private final OrderSanitizer sanitizer;
    private final OrderValidator validator;
    private final OrderLoader loader;
    private final SalesLookups lookups;
    private final RejectWriter rejectWriter;

    public OrderPipeline(MongoSourceReader reader, RejectWriter rejectWriter, AuditService audit,
                         OrderSanitizer sanitizer, OrderValidator validator, OrderLoader loader,
                         SalesLookups lookups) {
        super(reader, rejectWriter, audit);
        this.sanitizer = sanitizer;
        this.validator = validator;
        this.loader = loader;
        this.lookups = lookups;
        this.rejectWriter = rejectWriter;
    }

    @Override
    protected MongoSource source() {
        return RawOrder.SOURCE;
    }

    @Override
    protected String targetTable() {
        return FactSales.TABLE;
    }

    @Override
    protected Column sourceKey() {
        return col(RawOrder.ORDER_NO);
    }

    @Override
    protected Dataset<Row> sanitize(Dataset<Row> raw) {
        return sanitizer.sanitize(raw);
    }

    /** dim_customer ahiya vanchay che: customers part load thai gaya PACHHI (C023 jeva nava pan male). */
    @Override
    protected Dataset<Row> validate(JobContext ctx, Dataset<Row> sanitized) {
        return validator.validateOrders(sanitized, lookups.customerVersions());
    }

    /**
     * Line level: explode, line rules, line rejects, fact_sales upsert.
     *
     * @return partial orders count (line reject thai hoy eva orders)
     */
    @Override
    protected long load(JobContext ctx, Dataset<Row> validOrders) {
        Dataset<Row> lines = validator
                .validateLines(loader.explodeLines(validOrders), lookups.products())
                .persist(StorageLevel.MEMORY_AND_DISK());
        try {
            Dataset<Row> badLines = RuleEngine.rejected(lines);
            long partial = badLines.select(RawOrder.ORDER_NO).distinct().count();
            if (partial > 0) {
                Column key = concat(col(RawOrder.ORDER_NO), lit("/line-"), col(FactSales.LINE_NO));
                rejectWriter.write(ctx, source().name(), badLines, key, "LINE");
            }
            loader.write(ctx, RuleEngine.valid(lines), lookups.orderStatuses(), lookups.paymentMethods());
            return partial;
        } finally {
            lines.unpersist();
        }
    }
}
