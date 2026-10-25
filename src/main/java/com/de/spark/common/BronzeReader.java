package com.de.spark.common;

import com.de.spark.config.PipelineProperties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Component;

/** Reads collections of the Bronze MongoDB database (de_bronze) with the MongoDB Spark Connector. */
@Component
public class BronzeReader {

    private final SparkSession spark;
    private final PipelineProperties props;

    public BronzeReader(SparkSession spark, PipelineProperties props) {
        this.spark = spark;
        this.props = props;
    }

    /**
     * Only documents with fromMs < _ingested_at <= toMs.
     * The filter is an aggregation pipeline, so MongoDB itself filters (pushdown) and Spark
     * receives only the new documents, not the whole collection.
     */
    public Dataset<Row> readWindow(String collection, String schemaDdl, long fromMs, long toMs) {
        String pipeline = "[{\"$match\": {\"_ingested_at\": {"
                + "\"$gt\": {\"$numberLong\": \"" + fromMs + "\"}, "
                + "\"$lte\": {\"$numberLong\": \"" + toMs + "\"}}}}]";
        return reader(collection, schemaDdl).option("aggregation.pipeline", pipeline).load();
    }

    /** The whole collection (used for small master-data collections). */
    public Dataset<Row> readAll(String collection, String schemaDdl) {
        return reader(collection, schemaDdl).load();
    }

    private org.apache.spark.sql.DataFrameReader reader(String collection, String schemaDdl) {
        return spark.read()
                .format("mongodb")
                .schema(schemaDdl)   // explicit schema: no sampling of documents to guess types
                .option("connection.uri", props.getMongoUri())
                .option("database", props.getBronzeDatabase())
                .option("collection", collection);
    }
}
