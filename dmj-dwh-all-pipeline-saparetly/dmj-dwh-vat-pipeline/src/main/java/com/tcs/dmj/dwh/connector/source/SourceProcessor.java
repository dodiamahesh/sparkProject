package com.tcs.dmj.dwh.connector.source;

import com.tcs.dmj.dwh.config.ApplicationProperties;
import com.tcs.dmj.dwh.core.PipelineContext;
import com.tcs.dmj.dwh.enums.ErrorType;
import com.tcs.dmj.dwh.exception.DataQualityGateException;
import com.tcs.dmj.dwh.repository.MetaPipelineRepository;
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
 * First half of every pipeline, per source collection:
 * window -> extract -> sanitize -> split soft deletes -> validate + de-duplicate -> reject gate.
 * Rejects and last_run_time are saved later, in the commit phase (SourceCommitServiceImpl).
 */
@Component
public class SourceProcessor {

    private static final Logger log = LoggerFactory.getLogger(SourceProcessor.class);

    private static final String DELETED_FLAG = "_sd_deleted";
    private static final String LATEST_IS_DELETED = "_sd_latest_deleted";

    private final MongoSourceReader reader;
    private final DataQualityEngine qualityEngine;
    private final MetaPipelineRepository metaPipelineRepository;
    private final int rejectGateMinRecords;

    public SourceProcessor(MongoSourceReader reader, DataQualityEngine qualityEngine,
                           MetaPipelineRepository metaPipelineRepository, ApplicationProperties properties) {
        this.reader = reader;
        this.qualityEngine = qualityEngine;
        this.metaPipelineRepository = metaPipelineRepository;
        this.rejectGateMinRecords = properties.quality().rejectGateMinRecords();
    }

    public SourceBatch process(PipelineContext context, SourceDefinition source, DataSanitizer sanitizer,DataQualityRules rules) {
        LoadWindow window = metaPipelineRepository.window(context.pipelineName(), context.windowEnd());
        Dataset<Row> raw = extract(context, source, window);
        long sourceCount = raw.count();

        Dataset<Row> cleaned = sanitizer.apply(raw);
        ActiveAndDeleted split = splitSoftDeletes(context, cleaned, source);
        QualityResult result = qualityEngine.check(split.active(), rules, source.businessKey(),
                source.latestRecordColumn());

        SourceBatch batch = toBatch(context, source, context.cache(result.valid()), result.rejected(),
                split.deleted(), sourceCount, false);
        countSourceBatch(context, batch);
        context.addSourceBatch(batch);
        log.info("[job {}] {}: read={}, valid={}, rejected={} (duplicates={}), deleted at source={}",
                context.pipelineRunId(), source.collection(), batch.sourceCount(), batch.validCount(),
                batch.rejectedCount(), batch.duplicateCount(), batch.deletedCount());

        if (source.maxRejectPercent() != null) {
            checkRejectGate(batch);
        }
        return batch;
    }

    /**
     * Validates rows built from another batch (e.g. one row per element of an array field). A bad line is
     * rejected on its own. The rows need a unique _id per line and a _raw_record.
     */
    public SourceBatch processDerived(PipelineContext context, SourceDefinition definition,
                                      Dataset<Row> rows, DataQualityRules rules) {
        QualityResult result = qualityEngine.check(rows, rules, definition.businessKey(),
                definition.latestRecordColumn());
        SourceBatch batch = toBatch(context, definition, context.cache(result.valid()), result.rejected(), null,
                -1, true);

        // lines are not extra source documents: only their rejects are counted
        context.metrics().addRejected(batch.rejectedCount());
        context.metrics().addDuplicates(batch.duplicateCount());
        context.addSourceBatch(batch);
        log.info("[job {}] {}: lines={}, valid={}, rejected={}", context.pipelineRunId(), definition.collection(),
                batch.sourceCount(), batch.validCount(), batch.rejectedCount());
        return batch;
    }

    /** Reads the window and keeps a JSON copy of every original document (for the rejected table). */
    private Dataset<Row> extract(PipelineContext context, SourceDefinition source, LoadWindow window) {
        Column[] fields = source.fields().stream().map(field -> col(field)).toArray(Column[]::new);
        Dataset<Row> raw = reader.read(source, window).withColumn(QualityColumns.RAW_RECORD, to_json(struct(fields)));
        log.info("[job {}] extract {} ({})", context.pipelineRunId(), source.collection(),
                window.describe(source.latestRecordColumn()));
        return context.cache(raw);
    }

    /** @param sourceCount -1 for derived rows (then valid + rejected) */
    private SourceBatch toBatch(PipelineContext context, SourceDefinition source, Dataset<Row> valid,
                                Dataset<Row> rejectedRows, Dataset<Row> deleted, long sourceCount, boolean derived) {
        Dataset<Row> rejected = context.cache(rejectedRows);
        long validCount = valid.count();
        long rejectedCount = rejected.count();
        long duplicateCount = rejected.filter(QualityColumns.mainErrorType().equalTo(ErrorType.DUPLICATE.name())).count();
        long deletedCount = deleted == null ? 0 : deleted.count();
        long count = sourceCount >= 0 ? sourceCount : validCount + rejectedCount;
        return new SourceBatch(source, valid, rejected, deleted, count, validCount, rejectedCount, duplicateCount,
                deletedCount, derived);
    }

    private static void countSourceBatch(PipelineContext context, SourceBatch batch) {
        context.metrics().addSource(batch.sourceCount());
        context.metrics().addValid(batch.validCount());
        context.metrics().addRejected(batch.rejectedCount());
        context.metrics().addDuplicates(batch.duplicateCount());
        context.metrics().addDeleted(batch.deletedCount());
    }

    // ---------------------------------------------------------------- soft delete

    /** deleted = null when the source has no soft delete. */
    private record ActiveAndDeleted(Dataset<Row> active, Dataset<Row> deleted) {
    }

    /**
     * The LATEST version of a business key decides: deleted -> its key goes to the delete list (one row per key:
     * key + DELETED_AT), not deleted -> all its rows are validated as usual.
     */
    private ActiveAndDeleted splitSoftDeletes(PipelineContext context, Dataset<Row> cleaned, SourceDefinition source) {
        if (source.softDeleteField() == null) {
            return new ActiveAndDeleted(cleaned, null);
        }
        String key = source.businessKey();
        Column isDeleted = lower(trim(col(source.softDeleteField()))).isin("true", "1", "y", "yes");
        Column latestFirst = col(source.latestRecordColumn()).cast(SparkTypes.TIMESTAMP).desc_nulls_last();
        Dataset<Row> flagged = cleaned
                .withColumn(DELETED_FLAG, coalesce(isDeleted, lit(false)).and(col(key).isNotNull()))
                .withColumn(LATEST_IS_DELETED, first(col(DELETED_FLAG)).over(Window.partitionBy(col(key))
                        .orderBy(latestFirst, col(QualityColumns.SOURCE_ID).asc())));

        Dataset<Row> active = flagged.filter(not(col(LATEST_IS_DELETED))).drop(DELETED_FLAG, LATEST_IS_DELETED);
        Dataset<Row> deleted = context.cache(flagged
                .filter(col(LATEST_IS_DELETED).and(col(DELETED_FLAG)))
                .groupBy(col(key))
                .agg(max(col(source.latestRecordColumn()).cast(SparkTypes.TIMESTAMP)).as(SourceBatch.DELETED_AT)));
        return new ActiveAndDeleted(active, deleted);
    }

    // ---------------------------------------------------------------- reject gate

    private void checkRejectGate(SourceBatch batch) {
        SourceDefinition source = batch.source();
        if (batch.activeCount() < rejectGateMinRecords) {
            log.info("Reject gate skipped for {}: only {} rows (minimum {})",
                    source.collection(), batch.activeCount(), rejectGateMinRecords);
            return;
        }
        double percent = 100.0 * batch.badRowCount() / batch.activeCount();
        if (percent <= source.maxRejectPercent()) {
            return;
        }
        Dataset<Row> badErrors = QualityColumns.oneRowPerError(batch.rejected()
                .filter(QualityColumns.mainErrorType().notEqual(ErrorType.DUPLICATE.name())));
        throw new DataQualityGateException(source.collection(), String.format(Locale.ROOT,
                "Reject gate failed for %s: %d of %d rows rejected (%.1f%%), allowed at most %.1f%%. "
                        + "Errors: %s. Example: %s. Nothing from this collection was loaded and the "
                        + "last_run_time did not move.%s",
                source.collection(), batch.badRowCount(), batch.activeCount(), percent, source.maxRejectPercent(),
                errorSummary(badErrors), exampleError(badErrors), source.ownerHint()));
    }

    /** "ORPHAN=62, VALIDATION=3" */
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
}
