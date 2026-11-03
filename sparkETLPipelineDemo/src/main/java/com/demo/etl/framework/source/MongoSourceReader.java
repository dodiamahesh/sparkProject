package com.demo.etl.framework.source;

import com.demo.etl.config.EtlProperties;
import com.demo.etl.framework.pipeline.LoadType;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Reads a MongoDB collection into a Spark DataFrame.
 * The load type decides WHICH documents are read, using the flag field (not timestamps).
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

        String filter = flagFilter(loadType);
        if (filter != null) {
            // The $match runs INSIDE MongoDB, so only the needed documents travel to Spark
            reader = reader.option("aggregation.pipeline", filter);
        }
        log.debug("Reading {}.{} with loadType={} filter={}", mongo.database(), source.collection(), loadType, filter);
        return reader.load();
    }

    /** MongoDB aggregation pipeline for each load type. */
    String flagFilter(LoadType loadType) {
        return switch (loadType) {
            case FULL -> null;
            // {"$in": ["N", null]} also matches documents that do not have the field yet
            case INCREMENTAL -> "[{\"$match\": {\"%s\": {\"$in\": [\"%s\", null]}}}]"
                    .formatted(flagField, SourceFlag.NEW.code());
            case RETRY_REJECTED -> "[{\"$match\": {\"%s\": \"%s\"}}]"
                    .formatted(flagField, SourceFlag.REJECTED.code());
        };
    }
}
