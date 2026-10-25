package com.de.spark.pipeline.bronze2silver;

/**
 * Explicit schemas of the Bronze documents (Spark DDL), built from real documents in de_bronze.
 *
 * mongo_*  = Debezium MongoDB change event, stored as-is:
 *            { _id:{id}, op, after:"<original document as JSON string>", source:{ts_ms}, _ingested_at }
 * mysql_*  = Debezium MySQL row after the unwrap SMT:
 *            { _id:{<pk>}, <columns...>, __op, __deleted, __source_ts_ms, _ingested_at }
 */
public final class BronzeSchemas {

    public static final String MONGO_EVENT = String.join(", ",
            "_id STRUCT<id: STRING>",
            "op STRING",
            "after STRING",
            "source STRUCT<ts_ms: BIGINT>",
            "_ingested_at BIGINT");

    public static final String MYSQL_STATION = String.join(", ",
            "station_code STRING", "station_name STRING", "country STRING",
            "latitude STRING", "longitude STRING",            // DECIMAL arrives as string (decimal.handling.mode=string)
            "station_type STRING",
            "__op STRING", "__deleted STRING", "__source_ts_ms BIGINT", "_ingested_at BIGINT");

    public static final String MYSQL_STATION_OWNER = String.join(", ",
            "owner_id BIGINT", "station_code STRING", "owner_name STRING", "region STRING",
            "__op STRING", "__deleted STRING", "__source_ts_ms BIGINT", "_ingested_at BIGINT");

    /** The original weather observation inside "after" (relaxed Extended JSON: plain numbers, $oid, $date as millis). */
    private static String vq(String type) {
        return "STRUCT<`value`: " + type + ", quality: STRING>";
    }

    public static final String OBSERVATION = String.join(", ",
            "_id STRUCT<`$oid`: STRING>",
            "st STRING",
            "ts STRUCT<`$date`: BIGINT>",
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
            "presentWeatherObservationManual ARRAY<STRUCT<`condition`: STRING, quality: STRING>>");

    private BronzeSchemas() {
    }
}
