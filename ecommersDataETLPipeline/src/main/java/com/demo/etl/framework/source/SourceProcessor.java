package com.demo.etl.framework.source;

import com.demo.etl.config.EtlProperties;
import com.demo.etl.framework.audit.StepTracker;
import com.demo.etl.framework.audit.WatermarkRepository;
import com.demo.etl.framework.exception.DataQualityGateException;
import com.demo.etl.framework.pipeline.PipelineContext;
import com.demo.etl.framework.quality.DataQualityEngine;
import com.demo.etl.framework.quality.DataQualityRules;
import com.demo.etl.framework.quality.DataSanitizer;
import com.demo.etl.framework.quality.ErrorType;
import com.demo.etl.framework.quality.QualityColumns;
import com.demo.etl.framework.quality.QualityResult;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.to_json;

/**
 * The standard "first half" of every pipeline, for one source collection:
 *
 *   WINDOW (watermark) -> EXTRACT (MongoDB) -> SANITIZE -> VALIDATE -> DE-DUPLICATE -> MASK PII
 *     -> split into valid / rejected -> REJECT GATE
 *
 * Every run is INCREMENTAL: only documents whose timestamp field is inside the window are read
 * (see {@link LoadWindow}). MongoDB is only READ, never updated.
 *
 * Rejected rows and the new watermark are NOT written here. They are written at the end of the job by
 * {@code SourceCommitService}, so a failed job changes nothing and the next run reads the same window again.
 */
@Component
public class SourceProcessor {

    private static final Logger log = LoggerFactory.getLogger(SourceProcessor.class);

    private final MongoSourceReader reader;
    private final DataQualityEngine qualityEngine;
    private final StepTracker stepTracker;
    private final WatermarkRepository watermarkRepository;
    private final int rejectGateMinRecords;

    public SourceProcessor(MongoSourceReader reader, DataQualityEngine qualityEngine, StepTracker stepTracker,
                           WatermarkRepository watermarkRepository, EtlProperties properties) {
        this.reader = reader;
        this.qualityEngine = qualityEngine;
        this.stepTracker = stepTracker;
        this.watermarkRepository = watermarkRepository;
        this.rejectGateMinRecords = properties.quality().rejectGateMinRecords();
    }

    public SourceBatch process(PipelineContext context, SourceDefinition source, DataSanitizer sanitizer, DataQualityRules rules) {
        String collection = source.collection();

        // 0. WINDOW: last watermark of this pipeline + collection -> start of this run
        LoadWindow window = watermarkRepository.window(context.pipelineName(), collection, context.windowEnd());

        // 1. EXTRACT: read + keep a JSON copy of the original document (PII masked) for the rejected table
        Dataset<Row> raw = context.cache(reader.read(source, window)
                .withColumn(QualityColumns.RAW_RECORD, to_json(struct(rawRecordColumns(source)))));
        long sourceCount = stepTracker.track(context,
                "Extract " + collection + " (" + window.describe(source.latestRecordColumn()) + ")", raw::count);

        // 2-4. SANITIZE + VALIDATE + DE-DUPLICATE, then mask PII before anything is stored
        SourceBatch batch = stepTracker.track(context, "Validate " + collection, () -> {
            Dataset<Row> cleaned = sanitizer.apply(raw);
            QualityResult result = qualityEngine.check(cleaned, rules, source.businessKey(),
                    source.latestRecordColumn(), source.pii());
            Dataset<Row> valid = context.cache(maskPii(result.valid(), source));
            return toBatch(context, source, valid, result.rejected(), sourceCount, false);
        }, SourceBatch::validCount);

        context.metrics().addSource(batch.sourceCount());
        context.metrics().addValid(batch.validCount());
        context.metrics().addRejected(batch.rejectedCount());
        context.metrics().addDuplicates(batch.duplicateCount());
        context.addSourceBatch(batch);

        log.info("[job {}] {}: read={}, valid={}, rejected={} (of which duplicates={})",
                context.jobRunId(), collection, batch.sourceCount(), batch.validCount(),
                batch.rejectedCount(), batch.duplicateCount());

        // 5. REJECT GATE: too many bad rows -> stop the job BEFORE the pipeline loads this collection
        if (source.maxRejectPercent() != null) {
            stepTracker.track(context, "Reject gate " + collection, () -> checkRejectGate(batch));
        }
        return batch;
    }

    /**
     * Validates rows that were BUILT from another batch instead of read from MongoDB, typically the
     * lines of an array field: one order document with items[] -> one row per line.
     *
     * A bad line is rejected on its own (with its own source_id / business_key in the rejected table),
     * while the rest of the document is still loaded. These rows have no watermark of their own.
     *
     * @param definition describes the line rows: collection (e.g. "orders.items"), fields, business key
     * @param rows       must contain {@link QualityColumns#SOURCE_ID} (unique per line, e.g. "&lt;_id&gt;#2")
     *                   and {@link QualityColumns#RAW_RECORD} (JSON of the line, for the rejected table)
     */
    public SourceBatch processDerived(PipelineContext context, SourceDefinition definition,
                                      Dataset<Row> rows, DataQualityRules rules) {
        String name = definition.collection();
        SourceBatch batch = stepTracker.track(context, "Validate " + name + " (lines)", () -> {
            QualityResult result = qualityEngine.check(rows, rules, definition.businessKey(),
                    definition.latestRecordColumn(), definition.pii());
            return toBatch(context, definition, context.cache(result.valid()), result.rejected(), -1, true);
        }, SourceBatch::validCount);

        // Lines are not extra SOURCE documents: only the rejected lines are added to the job counters
        context.metrics().addRejected(batch.rejectedCount());
        context.metrics().addDuplicates(batch.duplicateCount());
        context.addSourceBatch(batch);

        log.info("[job {}] {}: lines={}, valid={}, rejected={}", context.jobRunId(), name,
                batch.sourceCount(), batch.validCount(), batch.rejectedCount());
        return batch;
    }

    /** @param sourceCount -1 = derived rows: valid + rejected */
    private SourceBatch toBatch(PipelineContext context, SourceDefinition source, Dataset<Row> valid,
                                Dataset<Row> rejectedRows, long sourceCount, boolean derived) {
        Dataset<Row> rejected = context.cache(rejectedRows);
        long validCount = valid.count();
        long rejectedCount = rejected.count();
        long duplicateCount = rejected
                .filter(QualityColumns.mainErrorType().equalTo(ErrorType.DUPLICATE.name()))
                .count();
        long count = sourceCount >= 0 ? sourceCount : validCount + rejectedCount;
        return new SourceBatch(source, valid, rejected, count, validCount, rejectedCount, duplicateCount, derived);
    }

    // ================================================================== reject gate

    /** @return number of bad rows; throws when the reject percentage is above the source's limit */
    private long checkRejectGate(SourceBatch batch) {
        SourceDefinition source = batch.source();
        double maxPercent = source.maxRejectPercent();
        if (batch.sourceCount() < rejectGateMinRecords) {
            log.info("Reject gate skipped for {}: only {} rows (minimum {})",
                    source.collection(), batch.sourceCount(), rejectGateMinRecords);
            return batch.badRowCount();
        }
        double percent = 100.0 * batch.badRowCount() / batch.sourceCount();
        if (percent > maxPercent) {
            Dataset<Row> badErrors = QualityColumns.oneRowPerError(batch.rejected()
                    .filter(QualityColumns.mainErrorType().notEqual(ErrorType.DUPLICATE.name())));
            throw new DataQualityGateException(source.collection(), String.format(Locale.ROOT,
                    "Reject gate failed for %s: %d of %d rows rejected (%.1f%%), allowed at most %.1f%%. "
                            + "Errors: %s. Example: %s. Nothing from this collection was loaded and the "
                            + "watermark did not move.%s",
                    source.collection(), batch.badRowCount(), batch.sourceCount(), percent, maxPercent,
                    errorSummary(badErrors), exampleError(badErrors), source.ownerHint()));
        }
        return batch.badRowCount();
    }

    /** "ORPHAN=62, VALIDATION=3" (number of failed rules per error type) */
    private String errorSummary(Dataset<Row> errors) {
        return errors.groupBy(QualityColumns.ERROR_TYPE).count()
                .collectAsList().stream()
                .map(row -> row.getString(0) + "=" + row.getLong(1))
                .sorted()
                .collect(Collectors.joining(", "));
    }

    /** "[ORPHAN] carrierId not found in dim_carrier (value: CRR-99)" */
    private String exampleError(Dataset<Row> errors) {
        List<Row> example = errors.limit(1).collectAsList();
        if (example.isEmpty()) {
            return "-";
        }
        Row error = example.get(0);
        return "[" + error.getAs(QualityColumns.ERROR_TYPE) + "] " + error.getAs(QualityColumns.ERROR_MESSAGE)
                + " (value: " + error.getAs(QualityColumns.FIELD_VALUE) + ")";
    }

    // ================================================================== PII

    /** Fields for the raw JSON copy; PII fields are masked, so the rejected table never holds the real value. */
    private Column[] rawRecordColumns(SourceDefinition source) {
        return source.fields().stream()
                .map(field -> source.pii().containsKey(field)
                        ? source.pii().get(field).mask(col(field)).as(field)
                        : col(field))
                .toArray(Column[]::new);
    }

    /** Valid rows go to silver: replace every PII field with its masked value. */
    private Dataset<Row> maskPii(Dataset<Row> rows, SourceDefinition source) {
        Dataset<Row> masked = rows;
        for (var pii : source.pii().entrySet()) {
            masked = masked.withColumn(pii.getKey(), pii.getValue().mask(col(pii.getKey())));
        }
        return masked;
    }
}
