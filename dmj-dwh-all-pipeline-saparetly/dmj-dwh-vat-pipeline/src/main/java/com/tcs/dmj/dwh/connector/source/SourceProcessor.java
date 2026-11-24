package com.tcs.dmj.dwh.connector.source;

import com.tcs.dmj.dwh.config.ApplicationProperties;
import com.tcs.dmj.dwh.core.PipelineContext;
import com.tcs.dmj.dwh.enums.ErrorType;
import com.tcs.dmj.dwh.repository.MetaPipelineRepository;
import com.tcs.dmj.dwh.service.DataQualityGateException;
import com.tcs.dmj.dwh.service.constant.SparkTypes;
import com.tcs.dmj.dwh.validation.DataQualityEngine;
import com.tcs.dmj.dwh.validation.DataQualityRules;
import com.tcs.dmj.dwh.validation.DataSanitizer;
import com.tcs.dmj.dwh.validation.QualityColumns;
import com.tcs.dmj.dwh.validation.QualityResult;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.first;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.lower;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.not;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.to_json;
import static org.apache.spark.sql.functions.trim;

/**
 * The standard "first half" of every pipeline, for one source collection:
 *
 *   WINDOW (last_run_time) -> EXTRACT (MongoDB) -> SANITIZE -> SPLIT SOFT DELETES -> VALIDATE -> DE-DUPLICATE
 *     -> split into valid / rejected -> REJECT GATE
 *
 * Every run is INCREMENTAL: only documents whose timestamp field is inside the window are read
 * (see {@link LoadWindow}). MongoDB is only READ, never updated.
 *
 * Rejected rows and the new last_run_time are NOT written here. They are written at the end of the job by
 * {@code SourceCommitServiceImpl}, so a failed job changes nothing and the next run reads the same window again.
 */
@Component
public class SourceProcessor {

    private static final Logger log = LoggerFactory.getLogger(SourceProcessor.class);

    private final MongoSourceReader reader;
    private final DataQualityEngine qualityEngine;
    private final MetaPipelineRepository metaPipelineRepository;
    private final int rejectGateMinRecords;

    private static final String DELETED_FLAG = "_sd_deleted";
    private static final String LATEST_IS_DELETED = "_sd_latest_deleted";

    public SourceProcessor(MongoSourceReader reader, DataQualityEngine qualityEngine,
                           MetaPipelineRepository metaPipelineRepository, ApplicationProperties properties) {
        this.reader = reader;
        this.qualityEngine = qualityEngine;
        this.metaPipelineRepository = metaPipelineRepository;
        this.rejectGateMinRecords = properties.quality().rejectGateMinRecords();
    }

    public SourceBatch process(PipelineContext context, SourceDefinition source, DataSanitizer sanitizer, DataQualityRules rules) {
        String collection = source.collection();

        // 0. WINDOW: last_run_time of this pipeline (meta.meta_pipeline) -> start of this run
        LoadWindow window = metaPipelineRepository.window(context.pipelineName(), context.windowEnd());

        // 1. EXTRACT: read + keep a JSON copy of the original document for the rejected table
        Dataset<Row> raw = context.cache(reader.read(source, window)
                .withColumn(QualityColumns.RAW_RECORD, to_json(struct(fieldColumns(source)))));
        log.info("[job {}] extract {} ({})", context.pipelineRunId(), collection, window.describe(source.latestRecordColumn()));
        long sourceCount = raw.count();

        // 2-4. SANITIZE + VALIDATE + DE-DUPLICATE
        Dataset<Row> cleaned = sanitizer.apply(raw);
        // 2b. SOFT DELETE: keys whose latest version is marked deleted are not validated, they are deleted
        ActiveAndDeleted split = splitSoftDeletes(context, cleaned, source);
        QualityResult result = qualityEngine.check(split.active(), rules, source.businessKey(),
                source.latestRecordColumn());
        Dataset<Row> valid = context.cache(result.valid());
        SourceBatch batch = toBatch(context, source, valid, result.rejected(), split.deleted(), sourceCount, false);

        context.metrics().addSource(batch.sourceCount());
        context.metrics().addValid(batch.validCount());
        context.metrics().addRejected(batch.rejectedCount());
        context.metrics().addDuplicates(batch.duplicateCount());
        context.metrics().addDeleted(batch.deletedCount());
        context.addSourceBatch(batch);

        log.info("[job {}] {}: read={}, valid={}, rejected={} (of which duplicates={}), deleted at source={}",
                context.pipelineRunId(), collection, batch.sourceCount(), batch.validCount(),
                batch.rejectedCount(), batch.duplicateCount(), batch.deletedCount());

        // 5. REJECT GATE: too many bad rows -> stop the job BEFORE the pipeline loads this collection
        if (source.maxRejectPercent() != null) {
            checkRejectGate(batch);
        }
        return batch;
    }

    /**
     * Validates rows that were BUILT from another batch instead of read from MongoDB, typically the
     * lines of an array field: one document with lines[] -> one row per line.
     *
     * A bad line is rejected on its own (with its own source_id / business_key in the rejected table),
     * while the rest of the document is still loaded. These rows have no window of their own.
     *
     * @param definition describes the line rows: collection (e.g. "vat_return_coll.lines"), fields, business key
     * @param rows       must contain {@link QualityColumns#SOURCE_ID} (unique per line, e.g. "&lt;_id&gt;#2")
     *                   and {@link QualityColumns#RAW_RECORD} (JSON of the line, for the rejected table)
     */
    public SourceBatch processDerived(PipelineContext context, SourceDefinition definition,
                                      Dataset<Row> rows, DataQualityRules rules) {
        String name = definition.collection();
        QualityResult result = qualityEngine.check(rows, rules, definition.businessKey(),
                definition.latestRecordColumn());
        SourceBatch batch = toBatch(context, definition, context.cache(result.valid()), result.rejected(), null, -1, true);

        // Lines are not extra SOURCE documents: only the rejected lines are added to the job counters
        context.metrics().addRejected(batch.rejectedCount());
        context.metrics().addDuplicates(batch.duplicateCount());
        context.addSourceBatch(batch);

        log.info("[job {}] {}: lines={}, valid={}, rejected={}", context.pipelineRunId(), name,
                batch.sourceCount(), batch.validCount(), batch.rejectedCount());
        return batch;
    }

    /**
     * @param deleted     soft-deleted keys (null = none)
     * @param sourceCount -1 = derived rows: valid + rejected
     */
    private SourceBatch toBatch(PipelineContext context, SourceDefinition source, Dataset<Row> valid,
                                Dataset<Row> rejectedRows, Dataset<Row> deleted, long sourceCount, boolean derived) {
        Dataset<Row> rejected = context.cache(rejectedRows);
        long validCount = valid.count();
        long rejectedCount = rejected.count();
        long duplicateCount = rejected
                .filter(QualityColumns.mainErrorType().equalTo(ErrorType.DUPLICATE.name()))
                .count();
        long deletedCount = deleted == null ? 0 : deleted.count();
        long count = sourceCount >= 0 ? sourceCount : validCount + rejectedCount;
        return new SourceBatch(source, valid, rejected, deleted, count, validCount, rejectedCount, duplicateCount,
                deletedCount, derived);
    }

    // ================================================================== soft delete

    /** Rows to validate + soft-deleted keys (deleted = null when the source has no soft delete). */
    private record ActiveAndDeleted(Dataset<Row> active, Dataset<Row> deleted) {
    }

    /**
     * Splits the cleaned rows on the soft-delete flag. The LATEST version of a business key decides (same order as
     * the duplicate check): deleted -> the key goes to the deleted list (not validated, never rejected);
     * not deleted -> all its rows are validated as usual (a deleted older copy is then just a duplicate).
     *
     * @return deleted = one row per key: business key + SourceBatch.DELETED_AT (when it was deleted)
     */
    private ActiveAndDeleted splitSoftDeletes(PipelineContext context, Dataset<Row> cleaned, SourceDefinition source) {
        if (source.softDeleteField() == null) {
            return new ActiveAndDeleted(cleaned, null);
        }
        String key = source.businessKey();
        Column flag = lower(trim(col(source.softDeleteField()))).isin("true", "1", "y", "yes");
        Dataset<Row> flagged = cleaned
                .withColumn(DELETED_FLAG, coalesce(flag, lit(false)).and(col(key).isNotNull()))
                .withColumn(LATEST_IS_DELETED, first(col(DELETED_FLAG)).over(Window.partitionBy(col(key))
                        .orderBy(col(source.latestRecordColumn()).cast(SparkTypes.TIMESTAMP).desc_nulls_last(),
                                col(QualityColumns.SOURCE_ID).asc())));

        Dataset<Row> active = flagged.filter(not(col(LATEST_IS_DELETED))).drop(DELETED_FLAG, LATEST_IS_DELETED);
        Dataset<Row> deleted = context.cache(flagged
                .filter(col(LATEST_IS_DELETED).and(col(DELETED_FLAG)))
                .groupBy(col(key))
                .agg(max(col(source.latestRecordColumn()).cast(SparkTypes.TIMESTAMP)).as(SourceBatch.DELETED_AT)));
        return new ActiveAndDeleted(active, deleted);
    }

    // ================================================================== reject gate

    /** Throws when the reject percentage is above the source's limit. */
    private void checkRejectGate(SourceBatch batch) {
        SourceDefinition source = batch.source();
        double maxPercent = source.maxRejectPercent();
        if (batch.activeCount() < rejectGateMinRecords) {
            log.info("Reject gate skipped for {}: only {} rows (minimum {})",
                    source.collection(), batch.activeCount(), rejectGateMinRecords);
            return;
        }
        double percent = 100.0 * batch.badRowCount() / batch.activeCount();   // deleted documents are not validated
        if (percent > maxPercent) {
            Dataset<Row> badErrors = QualityColumns.oneRowPerError(batch.rejected()
                    .filter(QualityColumns.mainErrorType().notEqual(ErrorType.DUPLICATE.name())));
            throw new DataQualityGateException(source.collection(), String.format(Locale.ROOT,
                    "Reject gate failed for %s: %d of %d rows rejected (%.1f%%), allowed at most %.1f%%. "
                            + "Errors: %s. Example: %s. Nothing from this collection was loaded and the "
                            + "last_run_time did not move.%s",
                    source.collection(), batch.badRowCount(), batch.activeCount(), percent, maxPercent,
                    errorSummary(badErrors), exampleError(badErrors), source.ownerHint()));
        }
    }

    /** "ORPHAN=62, VALIDATION=3" (number of failed rules per error type) */
    private String errorSummary(Dataset<Row> errors) {
        return errors.groupBy(QualityColumns.ERROR_TYPE).count()
                .collectAsList().stream()
                .map(row -> row.getString(0) + "=" + row.getLong(1))
                .sorted()
                .collect(Collectors.joining(", "));
    }

    /** "[ORPHAN] formId not found in dim_tax_form (value: 99999)" */
    private String exampleError(Dataset<Row> errors) {
        List<Row> example = errors.limit(1).collectAsList();
        if (example.isEmpty()) {
            return "-";
        }
        Row error = example.get(0);
        return "[" + error.getAs(QualityColumns.ERROR_TYPE) + "] " + error.getAs(QualityColumns.ERROR_MESSAGE)
                + " (value: " + error.getAs(QualityColumns.FIELD_VALUE) + ")";
    }

    /** Every field of the source, for the raw JSON copy of the document. */
    private Column[] fieldColumns(SourceDefinition source) {
        return source.fields().stream().map(field -> col(field)).toArray(Column[]::new);
    }
}
