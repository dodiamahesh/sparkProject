package com.demo.etl.framework.source;

import com.demo.etl.config.EtlProperties;
import org.apache.spark.sql.DataFrameReader;
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
 * Reads a MongoDB collection into a Spark DataFrame. READ ONLY: the ETL never writes to MongoDB.
 *
 * WHICH documents are read is decided by:
 *   1. the incremental window on the source's timestamp field ({@link SourceDefinition#latestRecordColumn()},
 *      e.g. updatedAt):  updatedAt &gt; last watermark AND &lt;= start of this run
 *   2. the optional source filter of the {@link SourceDefinition} (e.g. skip test documents)
 *
 * The $match stages run INSIDE MongoDB, so only the documents of the window travel to Spark.
 * The timestamp field must be a BSON Date and should have an index ({updatedAt: 1}) to make the filter fast.
 */
@Component
public class MongoSourceReader {

    private static final Logger log = LoggerFactory.getLogger(MongoSourceReader.class);

    private final SparkSession spark;
    private final EtlProperties.Mongo mongo;

    public MongoSourceReader(SparkSession spark, EtlProperties properties) {
        this.spark = spark;
        this.mongo = properties.mongo();
    }

    public Dataset<Row> read(SourceDefinition source, LoadWindow window) {
        String pipeline = matchStages(source, window).stream()
                .map(Document::toJson)
                .collect(Collectors.joining(", ", "[", "]"));
        log.debug("Reading {}.{} pipeline={}", mongo.database(), source.collection(), pipeline);
        DataFrameReader reader = spark.read()
                .format("mongodb")
                .option("connection.uri", mongo.uri())
                .option("database", mongo.database())
                .option("collection", source.collection())
                .option("aggregation.pipeline", pipeline)
                .schema(source.schema());
        return reader.load();
    }

    /**
     * The $match stages for this source and window. Also used by {@link SchemaDriftDetector}, so it
     * inspects exactly the documents that will be read.
     * <pre>
     *   {"$match": {"updatedAt": {"$gt": {"$date": start}, "$lte": {"$date": end}}}}   ($gt left out on the first run)
     * </pre>
     */
    public List<Document> matchStages(SourceDefinition source, LoadWindow window) {
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

    /** Fields present in every document that are not business data (never reported as schema drift). */
    public List<String> technicalFields() {
        return List.of("_id");
    }
}
