package com.demo.etl.framework.source;

import com.demo.etl.framework.audit.StepTracker;
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

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.to_json;

/**
 * The standard "first half" of every pipeline, for one source collection:
 *
 *   EXTRACT (MongoDB) -> SANITIZE -> VALIDATE -> DE-DUPLICATE -> split into valid / rejected
 *
 * Rejected rows are NOT written here. They are written at the end of the job by
 * {@code SourceCommitService}, together with the MongoDB flag update, so a failed job changes nothing.
 */
@Component
public class SourceProcessor {

    private static final Logger log = LoggerFactory.getLogger(SourceProcessor.class);

    private final MongoSourceReader reader;
    private final DataQualityEngine qualityEngine;
    private final StepTracker stepTracker;

    public SourceProcessor(MongoSourceReader reader, DataQualityEngine qualityEngine, StepTracker stepTracker) {
        this.reader = reader;
        this.qualityEngine = qualityEngine;
        this.stepTracker = stepTracker;
    }

    public SourceBatch process(PipelineContext context, SourceDefinition source,
                               DataSanitizer sanitizer, DataQualityRules rules) {
        String collection = source.collection();

        // 1. EXTRACT: read + keep a JSON copy of the original document (for the rejected table)
        Dataset<Row> raw = context.cache(reader.read(source, context.loadType())
                .withColumn(QualityColumns.RAW_RECORD, to_json(struct(fieldColumns(source)))));
        long sourceCount = stepTracker.track(context, "Extract " + collection + " (" + context.loadType() + ")", raw::count);

        // 2-4. SANITIZE + VALIDATE + DE-DUPLICATE
        SourceBatch batch = stepTracker.track(context, "Validate " + collection, () -> {
            Dataset<Row> cleaned = sanitizer.apply(raw);
            QualityResult result = qualityEngine.check(cleaned, rules, source.businessKey(), source.latestRecordColumn());

            Dataset<Row> valid = context.cache(result.valid());
            Dataset<Row> rejected = context.cache(result.rejected());
            long validCount = valid.count();
            long rejectedCount = rejected.count();
            long duplicateCount = rejected
                    .filter(col(QualityColumns.REJECTED_ERROR_TYPE).equalTo(ErrorType.DUPLICATE.name()))
                    .count();
            return new SourceBatch(source, valid, rejected, sourceCount, validCount, rejectedCount, duplicateCount);
        }, SourceBatch::validCount);

        context.metrics().addSource(batch.sourceCount());
        context.metrics().addValid(batch.validCount());
        context.metrics().addRejected(batch.rejectedCount());
        context.metrics().addDuplicates(batch.duplicateCount());
        context.addSourceBatch(batch);

        log.info("[job {}] {}: read={}, valid={}, rejected={} (of which duplicates={})",
                context.jobRunId(), collection, batch.sourceCount(), batch.validCount(),
                batch.rejectedCount(), batch.duplicateCount());
        return batch;
    }

    private Column[] fieldColumns(SourceDefinition source) {
        return source.fields().stream().map(field -> col(field)).toArray(Column[]::new);
    }
}
