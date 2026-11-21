package com.demo.etl.framework.source;

import com.demo.etl.config.EtlProperties;
import com.demo.etl.framework.pipeline.LoadType;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Reads a MongoDB collection into a Spark DataFrame.
 * WHICH documents are read is decided by:
 *   1. the load type, using the flag field (not timestamps)
 *   2. the optional source filter of the {@link SourceDefinition} (e.g. skip test documents)
 */
@Component
public class MongoSourceReader {

    private static final Logger log = LoggerFactory.getLogger(MongoSourceReader.class);

    private final SparkSession spark;
    private final EtlProperties.Mongo mongo;
    private final String flagField;

    public MongoSourceReader(SparkSession spark, EtlProperties properties) {
        this.spark = spark;
        this.mongo = properties.mongo();
        this.flagField = properties.source().flagField();
    }

    public Dataset<Row> read(SourceDefinition source, LoadType loadType) {
        DataFrameReader reader = spark.read()
                .format("mongodb")
                .option("connection.uri", mongo.uri())
                .option("database", mongo.database())
                .option("collection", source.collection())
                .schema(source.schema());

        List<Document> stages = matchStages(source, loadType);
        String pipeline = null;
        if (!stages.isEmpty()) {
            // The $match stages run INSIDE MongoDB, so only the needed documents travel to Spark
            pipeline = stages.stream().map(Document::toJson).collect(Collectors.joining(", ", "[", "]"));
            reader = reader.option("aggregation.pipeline", pipeline);
        }
        log.debug("Reading {}.{} with loadType={} pipeline={}", mongo.database(), source.collection(), loadType, pipeline);
        return reader.load();
    }

    /**
     * The $match stages for this source and load type (empty = read everything).
     * Also used by {@link SchemaDriftDetector}, so it inspects exactly the documents that will be read.
     */
    public List<Document> matchStages(SourceDefinition source, LoadType loadType) {
        List<Document> stages = new ArrayList<>();
        Document flagMatch = switch (loadType) {
            case FULL -> null;
            // {"$in": ["N", null]} also matches documents that do not have the field yet
            case INCREMENTAL -> new Document(flagField, new Document("$in", Arrays.asList(SourceFlag.NEW.code(), null)));
            case RETRY_REJECTED -> new Document(flagField, SourceFlag.REJECTED.code());
        };
        if (flagMatch != null) {
            stages.add(new Document("$match", flagMatch));
        }
        if (source.sourceFilter() != null) {
            stages.add(new Document("$match", Document.parse(source.sourceFilter())));
        }
        return stages;
    }

    /** Fields the framework itself writes into every document (never reported as schema drift). */
    public List<String> technicalFields() {
        return List.of("_id", flagField, MongoFlagUpdater.JOB_RUN_ID_FIELD, MongoFlagUpdater.PROCESSED_AT_FIELD);
    }
}
