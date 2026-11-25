package com.tcs.dmj.dwh.connector.source;

import com.tcs.dmj.dwh.config.ApplicationProperties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Reads the documents of one window from a MongoDB collection (read only). The $match runs inside MongoDB;
 * the timestamp field must be a BSON Date with an index.
 */
@Component
public class MongoSourceReader {

    private static final Logger log = LoggerFactory.getLogger(MongoSourceReader.class);

    private final SparkSession spark;
    private final ApplicationProperties.Mongo mongo;

    public MongoSourceReader(SparkSession spark, ApplicationProperties properties) {
        this.spark = spark;
        this.mongo = properties.mongo();
    }

    public Dataset<Row> read(SourceDefinition source, LoadWindow window) {
        String pipeline = matchStages(source, window).stream()
                .map(Document::toJson)
                .collect(Collectors.joining(", ", "[", "]"));
        log.debug("Reading {}.{} pipeline={}", mongo.database(), source.collection(), pipeline);
        return spark.read()
                .format("mongodb")
                .option("connection.uri", mongo.uri())
                .option("database", mongo.database())
                .option("collection", source.collection())
                .option("aggregation.pipeline", pipeline)
                .schema(source.schema())
                .load();
    }

    /** {"$match": {"updatedAt": {"$gt": start, "$lte": end}}} (+ the source filter). No $gt on the first run. */
    private List<Document> matchStages(SourceDefinition source, LoadWindow window) {
        Document range = new Document();
        if (!window.isFirstRun()) {
            range.append("$gt", Date.from(window.start()));
        }
        range.append("$lte", Date.from(window.end()));

        List<Document> stages = new ArrayList<>();
        stages.add(new Document("$match", new Document(source.latestRecordColumn(), range)));
        if (source.sourceFilter() != null) {
            stages.add(new Document("$match", Document.parse(source.sourceFilter())));
        }
        return stages;
    }
}
