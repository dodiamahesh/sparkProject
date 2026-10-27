package com.de.spark.pipeline.bronze2silver;

import java.util.List;

/**
 * Explicit schemas of the Bronze documents (Spark DDL), built from real documents in de_bronze.
 *
 * weather_observations = the source document as-is (written by mongo-cdc-subscriber) + 4 metadata fields:
 *            { _id: ObjectId, <original fields...>, _op, _deleted, _source_ts, _ingested_at }
 * mysql_*  = Debezium MySQL row after the unwrap SMT (old pipeline; no longer updated, read as it is):
 *            { _id:{<pk>}, <columns...>, __op, __deleted, __source_ts_ms, _ingested_at }
 */
public final class BronzeSchemas {

    /** Metadata fields that mongo-cdc-subscriber adds after the original fields of every document. */
    public static final List<String> METADATA_FIELDS = List.of("_op", "_deleted", "_source_ts", "_ingested_at");

    public static final String MYSQL_STATION = String.join(", ",
            "station_code STRING", "station_name STRING", "country STRING",
            "latitude STRING", "longitude STRING",            // DECIMAL arrives as string (decimal.handling.mode=string)
            "station_type STRING",
            "__op STRING", "__deleted STRING", "__source_ts_ms BIGINT", "_ingested_at BIGINT");

    public static final String MYSQL_STATION_OWNER = String.join(", ",
            "owner_id BIGINT", "station_code STRING", "owner_name STRING", "region STRING",
            "__op STRING", "__deleted STRING", "__source_ts_ms BIGINT", "_ingested_at BIGINT");

    private static String vq(String type) {
        return "STRUCT<`value`: " + type + ", quality: STRING>";
    }

    /**
     * A bronze weather_observations document: the original observation (BSON types read directly:
     * ObjectId -> STRING, Date -> TIMESTAMP) followed by the 4 metadata fields.
     */
    public static final String OBSERVATION = String.join(", ",
            "_id STRING",
            "st STRING",
            "ts TIMESTAMP",
            "position STRUCT<`type`: STRING, coordinates: ARRAY<DOUBLE>>",
            "elevation DOUBLE",
            "callLetters STRING",
            "qualityControlProcess STRING",
            "dataSource STRING",
            "`type` STRING",
            "airTemperature " + vq("DOUBLE"),
            "dewPoint " + vq("DOUBLE"),
            "pressure " + vq("DOUBLE"),
            "wind STRUCT<direction: STRUCT<angle: DOUBLE, quality: STRING>, `type`: STRING,"
                    + " speed: STRUCT<rate: DOUBLE, quality: STRING>>",
            "visibility STRUCT<distance: " + vq("DOUBLE") + ", variability: " + vq("STRING") + ">",
            "skyCondition STRUCT<ceilingHeight: STRUCT<`value`: DOUBLE, quality: STRING, determination: STRING>,"
                    + " cavok: STRING>",
            "sections ARRAY<STRING>",
            "precipitationEstimatedObservation STRUCT<discrepancy: STRING, estimatedWaterDepth: DOUBLE>",
            "atmosphericPressureChange STRUCT<tendency: STRUCT<code: STRING, quality: STRING>,"
                    + " quantity3Hours: " + vq("DOUBLE") + ", quantity24Hours: " + vq("DOUBLE") + ">",
            "seaSurfaceTemperature " + vq("DOUBLE"),
            "waveMeasurement STRUCT<method: STRING, waves: STRUCT<period: DOUBLE, height: DOUBLE,"
                    + " quality: STRING>, seaState: STRUCT<code: STRING, quality: STRING>>",
            "pastWeatherObservationManual ARRAY<STRUCT<atmosphericCondition: " + vq("STRING")
                    + ", period: " + vq("DOUBLE") + ">>",
            "skyConditionObservation STRUCT<totalCoverage: STRUCT<`value`: STRING, opaque: STRING, quality: STRING>,"
                    + " lowestCloudCoverage: " + vq("STRING") + ", lowCloudGenus: " + vq("STRING")
                    + ", lowestCloudBaseHeight: " + vq("DOUBLE") + ", midCloudGenus: " + vq("STRING")
                    + ", highCloudGenus: " + vq("STRING") + ">",
            "presentWeatherObservationManual ARRAY<STRUCT<`condition`: STRING, quality: STRING>>",
            "_op STRING",
            "_deleted BOOLEAN",
            "_source_ts BIGINT",
            "_ingested_at BIGINT");

    private BronzeSchemas() {
    }
}
