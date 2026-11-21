package com.demo.etl.framework.source;

import com.demo.etl.config.EtlProperties;
import com.demo.etl.framework.audit.DataWarningRepository;
import com.demo.etl.framework.audit.StepTracker;
import com.demo.etl.framework.audit.WarningType;
import com.demo.etl.framework.exception.DataQualityGateException;
import com.demo.etl.framework.pipeline.LoadType;
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
 *   SCHEMA CHECK -> EXTRACT (MongoDB) -> VOLUME CHECK -> SANITIZE -> VALIDATE -> DE-DUPLICATE
 *     -> MASK PII -> split into valid / rejected / warnings -> REJECT GATE
 *
 * Rejected rows and rule warnings are NOT written here. They are written at the end of the job by
 * {@code SourceCommitService}, together with the MongoDB flag update, so a failed job changes nothing.
 * Schema-drift and volume warnings describe the source itself, so they are saved right away.
 */
@Component
public class SourceProcessor {

    private static final Logger log = LoggerFactory.getLogger(SourceProcessor.class);

    private final MongoSourceReader reader;
    private final SchemaDriftDetector schemaDriftDetector;
    private final DataQualityEngine qualityEngine;
    private final StepTracker stepTracker;
    private final DataWarningRepository warningRepository;
    private final int rejectGateMinRecords;

    public SourceProcessor(MongoSourceReader reader, SchemaDriftDetector schemaDriftDetector,
                           DataQualityEngine qualityEngine, StepTracker stepTracker,
                           DataWarningRepository warningRepository, EtlProperties properties) {
        this.reader = reader;
        this.schemaDriftDetector = schemaDriftDetector;
        this.qualityEngine = qualityEngine;
        this.stepTracker = stepTracker;
        this.warningRepository = warningRepository;
        this.rejectGateMinRecords = properties.quality().rejectGateMinRecords();
    }

    public SourceBatch process(PipelineContext context, SourceDefinition source, DataSanitizer sanitizer, DataQualityRules rules) {
        String collection = source.collection();

        // 0. SCHEMA DRIFT: new / missing fields in MongoDB (warning, or FAIL with failOnSchemaDrift())
        stepTracker.track(context, "Schema check " + collection, () -> checkSchema(context, source));

        // 1. EXTRACT: read + keep a JSON copy of the original document (PII masked) for the rejected table
        Dataset<Row> raw = context.cache(reader.read(source, context.loadType())
                .withColumn(QualityColumns.RAW_RECORD, to_json(struct(rawRecordColumns(source)))));
        long sourceCount = stepTracker.track(context, "Extract " + collection + " (" + context.loadType() + ")", raw::count);

        // 1b. VOLUME: unusually few / many rows in an INCREMENTAL run -> warning
        checkVolume(context, source, sourceCount);

        // 2-4. SANITIZE + VALIDATE + DE-DUPLICATE (+ warning rules), then mask PII before anything is stored
        SourceBatch batch = stepTracker.track(context, "Validate " + collection, () -> {
            Dataset<Row> cleaned = sanitizer.apply(raw);
            QualityResult result = qualityEngine.check(cleaned, rules, source.businessKey(),
                    source.latestRecordColumn(), source.pii());

            Dataset<Row> valid = context.cache(maskPii(result.valid(), source));
            Dataset<Row> rejected = context.cache(result.rejected());
            Dataset<Row> warnings = context.cache(result.warnings());
            long validCount = valid.count();
            long rejectedCount = rejected.count();
            long duplicateCount = rejected
                    .filter(col(QualityColumns.REJECTED_ERROR_TYPE).equalTo(ErrorType.DUPLICATE.name()))
                    .count();
            long warningCount = warnings.count();
            return new SourceBatch(source, valid, rejected, warnings,
                    sourceCount, validCount, rejectedCount, duplicateCount, warningCount);
        }, SourceBatch::validCount);

        context.metrics().addSource(batch.sourceCount());
        context.metrics().addValid(batch.validCount());
        context.metrics().addRejected(batch.rejectedCount());
        context.metrics().addDuplicates(batch.duplicateCount());
        // rule warnings are counted when they are saved (commit phase), so a failed job counts only saved ones
        context.addSourceBatch(batch);

        log.info("[job {}] {}: read={}, valid={}, rejected={} (of which duplicates={}), warnings={}",
                context.jobRunId(), collection, batch.sourceCount(), batch.validCount(),
                batch.rejectedCount(), batch.duplicateCount(), batch.warningCount());

        // 5. REJECT GATE: too many bad rows -> stop the job BEFORE the pipeline loads this collection
        if (source.checks().maxRejectPercent() != null && context.loadType() != LoadType.RETRY_REJECTED) {
            stepTracker.track(context, "Reject gate " + collection, () -> checkRejectGate(batch));
        }
        return batch;
    }

    // ================================================================== checks

    /** @return number of drifted fields (saved as SCHEMA_DRIFT warnings) */
    private long checkSchema(PipelineContext context, SourceDefinition source) {
        SchemaDriftDetector.SchemaDrift drift = schemaDriftDetector.detect(source, context.loadType());
        if (!drift.hasDrift()) {
            return 0;
        }
        List<String> messages = drift.messages();
        if (source.checks().failOnSchemaDrift()) {
            throw new DataQualityGateException(source.collection(), "Schema drift in " + source.collection() + ": "
                    + String.join("; ", messages) + "." + source.ownerHint());
        }
        for (String message : messages) {
            log.warn("[job {}] SCHEMA DRIFT {}: {}", context.jobRunId(), source.collection(), message);
            warningRepository.insert(context.jobRunId(), context.pipelineName(), source.collection(),
                    WarningType.SCHEMA_DRIFT, message);
        }
        context.metrics().addWarnings(messages.size());
        return messages.size();
    }

    private void checkVolume(PipelineContext context, SourceDefinition source, long sourceCount) {
        SourceDefinition.QualityChecks checks = source.checks();
        if (context.loadType() != LoadType.INCREMENTAL) {
            return;   // FULL reads everything, RETRY_REJECTED only bad rows: no "normal" volume
        }
        String message = null;
        if (checks.minRows() != null && sourceCount < checks.minRows()) {
            message = "INCREMENTAL run read " + sourceCount + " documents, expected at least " + checks.minRows()
                    + " (is the source still sending data?)." + source.ownerHint();
        } else if (checks.maxRows() != null && sourceCount > checks.maxRows()) {
            message = "INCREMENTAL run read " + sourceCount + " documents, expected at most " + checks.maxRows()
                    + " (re-sent history or duplicate feed?)." + source.ownerHint();
        }
        if (message != null) {
            log.warn("[job {}] VOLUME {}: {}", context.jobRunId(), source.collection(), message);
            warningRepository.insert(context.jobRunId(), context.pipelineName(), source.collection(),
                    WarningType.VOLUME, message);
            context.metrics().addWarnings(1);
        }
    }

    /** @return number of bad rows; throws when the reject percentage is above the source's limit */
    private long checkRejectGate(SourceBatch batch) {
        SourceDefinition source = batch.source();
        double maxPercent = source.checks().maxRejectPercent();
        if (batch.sourceCount() < rejectGateMinRecords) {
            log.info("Reject gate skipped for {}: only {} rows (minimum {})",
                    source.collection(), batch.sourceCount(), rejectGateMinRecords);
            return batch.badRowCount();
        }
        double percent = 100.0 * batch.badRowCount() / batch.sourceCount();
        if (percent > maxPercent) {
            throw new DataQualityGateException(source.collection(), String.format(Locale.ROOT,
                    "Reject gate failed for %s: %d of %d rows rejected (%.1f%%), allowed at most %.1f%%. "
                            + "Reasons: %s. Example: %s. Nothing from this collection was loaded and MongoDB "
                            + "flags were not changed.%s",
                    source.collection(), batch.badRowCount(), batch.sourceCount(), percent, maxPercent,
                    reasonSummary(batch), exampleReason(batch), source.ownerHint()));
        }
        return batch.badRowCount();
    }

    /** "ORPHAN=62, VALIDATION=3" */
    private String reasonSummary(SourceBatch batch) {
        return badRows(batch)
                .groupBy(QualityColumns.REJECTED_ERROR_TYPE).count()
                .collectAsList().stream()
                .map(row -> row.getString(0) + "=" + row.getLong(1))
                .sorted()
                .collect(Collectors.joining(", "));
    }

    private String exampleReason(SourceBatch batch) {
        List<Row> example = badRows(batch).select(QualityColumns.REJECTED_ERROR_REASON).limit(1).collectAsList();
        return example.isEmpty() ? "-" : example.get(0).getString(0);
    }

    private Dataset<Row> badRows(SourceBatch batch) {
        return batch.rejected().filter(col(QualityColumns.REJECTED_ERROR_TYPE).notEqual(ErrorType.DUPLICATE.name()));
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
