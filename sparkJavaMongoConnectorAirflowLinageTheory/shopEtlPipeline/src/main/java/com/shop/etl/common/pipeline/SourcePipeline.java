package com.shop.etl.common.pipeline;

import com.shop.etl.common.audit.AuditService;
import com.shop.etl.common.audit.SourceStats;
import com.shop.etl.common.io.MongoSource;
import com.shop.etl.common.io.MongoSourceReader;
import com.shop.etl.common.job.JobContext;
import com.shop.etl.common.quality.QualityGate;
import com.shop.etl.common.quality.RejectWriter;
import com.shop.etl.common.quality.RuleEngine;
import com.shop.etl.common.watermark.LoadWindow;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.lit;

/**
 * EK SOURCE (collection) ne process karva no common TEMPLATE. Design doc Section 3.1 no
 * 12-step flow ahiya ek j vaar lakhyo che. Darek collection mate fakt 3 vaat alag che:
 * sanitize, validate ane load. E 3 method subclass lakhe che (CustomerPipeline, OrderPipeline...).
 *
 * <pre>
 *   ① window          (JobContext mathi, watermark thi)
 *   ② READ            MongoSourceReader: filter + projection Mongo ma j
 *   ③ SANITIZE        subclass (XxxSanitizer)
 *   ④–⑦ VALIDATE      subclass (XxxValidator): corrupt → dedupe → validation → FK
 *        ↓ PERSIST    ek j vaar compute, pachhi count + rejects + load teno j upyog
 *   COUNT             total ane rejected EK j Spark action ma
 *   ⑪ WRITE REJECTS   meta_rejected (DOCUMENT level)
 *   GATE              reject % vadhu → FAIL (silver ma kai nahi lakhay)
 *   ⑧–⑩ LOAD          subclass (XxxLoader): transform + upsert. Line rejects hoy to partial count aape
 *   AUDIT             inserted / updated target table mathi, source row SUCCESS
 *   UNPERSIST         memory chhodo
 * </pre>
 *
 * PERSIST kem? Spark lazy che. persist vagar "count", "rejects lakho" ane "load" darek vakhte
 * Mongo mathi FARITHI vanche (3 vaar read + 3 vaar validation). persist(MEMORY_AND_DISK) thi
 * pehli action vakhte result memory ma rahe (memory na hoy to disk), ane baki actions tya thi vanche.
 */
public abstract class SourcePipeline {

    private static final Logger log = LoggerFactory.getLogger(SourcePipeline.class);

    private final MongoSourceReader reader;
    private final RejectWriter rejectWriter;
    private final AuditService audit;

    protected SourcePipeline(MongoSourceReader reader, RejectWriter rejectWriter, AuditService audit) {
        this.reader = reader;
        this.rejectWriter = rejectWriter;
        this.audit = audit;
    }

    /** Kayo Mongo source (dataset class mathi). */
    protected abstract MongoSource source();

    /** Silver ni main target table (inserted/updated count mate), jem ke "fact_sales". */
    protected abstract String targetTable();

    /** meta_rejected.source_key mate business key column. */
    protected abstract Column sourceKey();

    /** ③ Raw data saaf karo (reject nahi). */
    protected abstract Dataset<Row> sanitize(Dataset<Row> raw);

    /** ④–⑦ Rules lagavo. Output ma {@code _reject} column hovo joie (RuleEngine). */
    protected abstract Dataset<Row> validate(JobContext ctx, Dataset<Row> sanitized);

    /**
     * ⑧–⑩ Sari rows ne silver ma lakho.
     *
     * @return partial documents count (je documents load thaya pan ek line reject thai). Line
     *         level validation na hoy to 0.
     */
    protected abstract long load(JobContext ctx, Dataset<Row> valid);

    /**
     * Template method: upar no aakho flow chalave che. Subclass aa method override NATHI karta.
     */
    public final SourceStats run(JobContext ctx) {
        String name = source().name();
        LoadWindow window = ctx.window(name);
        audit.startSource(ctx, name, window);
        if (window.isEmpty()) {
            log.info("[{}] window khali, skip", name);
            audit.finishSource(ctx, name, SourceStats.empty());
            return SourceStats.empty();
        }

        Dataset<Row> checked = null;
        try {
            Dataset<Row> raw = reader.read(source(), window);
            checked = validate(ctx, sanitize(raw)).persist(StorageLevel.MEMORY_AND_DISK());

            Row counts = checked.agg(count(lit(1)), count(col(RuleEngine.REJECT))).first();
            long total = counts.getLong(0);
            long rejected = counts.getLong(1);
            log.info("[{}] read={}, rejected={}", name, total, rejected);

            if (rejected > 0) {
                rejectWriter.write(ctx, name, RuleEngine.rejected(checked), sourceKey(), "DOCUMENT");
            }
            QualityGate.check(name, total, rejected, ctx.config());

            long partial = (total - rejected) > 0 ? load(ctx, RuleEngine.valid(checked)) : 0;

            long[] written = audit.countWritten(ctx, targetTable());
            SourceStats stats = new SourceStats(total, total - rejected, rejected, partial, written[0], written[1]);
            audit.finishSource(ctx, name, stats);
            log.info("[{}] SUCCESS {}", name, stats);
            return stats;
        } catch (RuntimeException e) {
            try {
                audit.failSource(ctx, name, e);
            } catch (RuntimeException auditError) {
                log.error("[{}] audit FAILED lakhvama error", name, auditError);
            }
            throw e;
        } finally {
            if (checked != null) {
                checked.unpersist();
            }
        }
    }
}
