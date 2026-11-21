package com.shop.etl.common.io;

import com.shop.etl.common.config.EtlProperties;
import com.shop.etl.common.watermark.LoadWindow;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * MongoDB mathi Spark Dataset vanchva no EK common (reusable) component.
 *
 * Badha jobs aa j class vapare che. Connection, partitioning ane window filter no logic ek j
 * jagyae che, etle koi job ma bhul thavani shakyata ochhi.
 *
 * Performance techniques je aa class vapare che:
 *   1. FILTER PUSHDOWN: watermark window ({@code $match}) aggregation pipeline ma PEHLA stage
 *      tarike jay che. Etle filter Mongo server par thay, ane updated_at index vapray.
 *      Spark ne fakt window na documents j male, aakhi collection nahi.
 *   2. PROJECTION PUSHDOWN: {@code $project} stage thi fakt jaruri fields aave (MongoProjection).
 *   3. EXPLICIT SCHEMA: {@code .schema(...)} aapiye etle connector sampling kari ne schema
 *      shodhva extra query nathi karto.
 *   4. PARALLEL READ: SamplePartitioner collection ne _id range ma todi ne partitions banave che.
 *      Darek Spark task potano tukdo alag thi vanche che (driver par aakho data nathi aavto).
 *      Tukda nu size yml {@code etl.mongo.partition-size-mb} (default 64 MB).
 *   5. READ PREFERENCE: prod uri ma {@code readPreference=secondaryPreferred} rakho, jethi
 *      bhare read primary (je app writes sambhale che) par na jay.
 */
@Component
public class MongoSourceReader {

    private static final Logger log = LoggerFactory.getLogger(MongoSourceReader.class);
    private static final String SAMPLE_PARTITIONER =
            "com.mongodb.spark.sql.connector.read.partitioner.SamplePartitioner";

    private final SparkSession spark;
    private final EtlProperties.Mongo mongo;

    public MongoSourceReader(SparkSession spark, EtlProperties properties) {
        this.spark = spark;
        this.mongo = properties.mongo();
    }

    /**
     * Ek source ne window mujab vanche che.
     *
     * Aa method LAZY che: koi data atyare nathi vanchato. Fakt Spark plan bane che.
     * Kharekhar read tyare thay jyare koi action (count, write) chale.
     *
     * @param source kai collection, schema, projection
     * @param window kaya samay no data (full load hoy to start null)
     * @return raw Dataset (schema = source.schema())
     */
    public Dataset<Row> read(MongoSource source, LoadWindow window) {
        String collection = mongo.collection(source.name());
        String pipeline = "[" + matchStage(source.watermarkField(), window) + ", "
                + source.projection().toStageJson() + "]";

        log.info("Mongo read: {}.{} window=({} , {}] pipeline={}",
                mongo.database(), collection, window.start(), window.end(), pipeline);

        return spark.read()
                .format("mongodb")
                .option("connection.uri", mongo.uri())
                .option("database", mongo.database())
                .option("collection", collection)
                .option("aggregation.pipeline", pipeline)
                .option("partitioner", SAMPLE_PARTITIONER)
                .option("partitioner.options.partition.field", "_id")
                .option("partitioner.options.partition.size", String.valueOf(mongo.partitionSizeMb()))
                .schema(source.schema())
                .load();
    }

    /**
     * Watermark window no {@code $match} stage banave che (Extended JSON {"$date": ...}).
     *
     * Full load: fakt {@code $lte end}. Incremental: {@code $gt start AND $lte end}.
     * Time Instant (UTC) ma moklay che, ane Mongo pan Date UTC ma rakhe che. Etle IST/UTC
     * ni gadbad nathi thati.
     */
    static String matchStage(String field, LoadWindow window) {
        String upper = "\"$lte\": {\"$date\": \"" + window.end() + "\"}";
        String range = window.isFullLoad()
                ? upper
                : "\"$gt\": {\"$date\": \"" + window.start() + "\"}, " + upper;
        return "{\"$match\": {\"" + field + "\": {" + range + "}}}";
    }
}
