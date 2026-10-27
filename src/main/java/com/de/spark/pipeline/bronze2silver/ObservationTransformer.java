package com.de.spark.pipeline.bronze2silver;

import com.de.spark.config.PipelineProperties;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.de.spark.pipeline.bronze2silver.Rejects.*;
import static org.apache.spark.sql.functions.*;

/**
 * Bronze weather_observations documents -> Silver weather_observation + 5 child tables.
 *
 * Steps:  split off the metadata -> split deletes -> VALIDATE -> remove DUPLICATES -> flatten.
 * Every record that is not loaded goes to the rejects with a reason (see Rejects).
 */
@Component
public class ObservationTransformer {

    public static final String SOURCE = "weather_observations";

    private final SparkSession spark;
    private final PipelineProperties props;

    public ObservationTransformer(SparkSession spark, PipelineProperties props) {
        this.spark = spark;
        this.props = props;
    }

    /** Everything Job #1 needs to load for observations. */
    public static class Result {
        public Dataset<Row> observations;                         // -> weather_observation (upsert)
        public Map<String, Dataset<Row>> children = new LinkedHashMap<>();  // table name -> rows (replace)
        public Dataset<Row> deleted;                              // ids deleted in the source (soft delete)
        public Dataset<Row> rejects;                              // -> etl_rejected_record
    }

    public Result transform(Dataset<Row> docs) {
        // "d" = the original observation fields (everything except the bronze metadata fields)
        Column[] observationFields = Arrays.stream(docs.columns())
                .filter(name -> !BronzeSchemas.METADATA_FIELDS.contains(name))
                .map(docs::col)
                .toArray(Column[]::new);
        Column observation = struct(observationFields);

        Dataset<Row> parsed = docs.select(
                // _id is the ObjectId hex string; "" when it is missing or not an ObjectId
                regexp_extract(col("_id"), "[0-9a-fA-F]{24}", 0).as("event_key"),
                coalesce(col("_deleted"), lit(false)).as("is_deleted"),
                to_json(observation).as("raw_payload"),
                observation.as("d"),
                millisToTimestamp(col("_source_ts")).as("source_ts"),
                millisToTimestamp(col("_ingested_at")).as("bronze_ingested_at"));

        Result r = new Result();

        // 1. Deletes (_deleted = true, soft delete in bronze) -> soft delete in silver
        r.deleted = parsed.filter(col("is_deleted"))
                .select(col("event_key").as("id"), col("source_ts"), col("bronze_ingested_at"));

        // 2. VALIDATION: every rule gives a reason; a row with a reason is rejected
        Dataset<Row> checked = parsed.filter(not(col("is_deleted")))
                .withColumn("call_letters", trim(col("d.callLetters")))
                .withColumn("obs_ts", col("d.ts"))
                .withColumn("lon", col("d.position.coordinates").getItem(0))
                .withColumn("lat", col("d.position.coordinates").getItem(1))
                .withColumn("reason", validationReason())
                .withColumn("details", validationDetails());

        Dataset<Row> invalid = checked.filter(col("reason").isNotNull());
        Dataset<Row> valid = checked.filter(col("reason").isNull()).drop("reason", "details");

        // 3. DUPLICATES inside this batch: same station + same time but different id.
        //    Keep the newest change (source_ts), reject the others.
        WindowSpec sameStationAndTime = Window.partitionBy("call_letters", "obs_ts")
                .orderBy(col("source_ts").desc(), col("event_key"));
        Dataset<Row> ranked = valid
                .withColumn("rn", row_number().over(sameStationAndTime))
                .withColumn("kept_id", first("event_key").over(sameStationAndTime));
        Dataset<Row> dupInBatch = ranked.filter(col("rn").gt(1));
        Dataset<Row> winners = ranked.filter(col("rn").equalTo(1)).drop("rn", "kept_id").cache();

        // 4. DUPLICATES of rows already in silver: same station + time stored under another id
        Dataset<Row> existing = existingStationTimeKeys(winners);
        Column sameKeyOtherId = winners.col("call_letters").equalTo(existing.col("e_call_letters"))
                .and(winners.col("obs_ts").equalTo(existing.col("e_obs_ts")))
                .and(winners.col("event_key").notEqual(existing.col("existing_id")));
        Dataset<Row> dupExisting = winners.join(existing, sameKeyOtherId, "inner");
        Dataset<Row> accepted = winners.join(existing, sameKeyOtherId, "left_anti").cache();

        r.rejects = Rejects.of(invalid, SOURCE, col("event_key"), col("reason"), col("details"), col("raw_payload"))
                .unionByName(Rejects.of(dupInBatch, SOURCE, col("event_key"), lit(DUPLICATE_IN_BATCH),
                        concat(lit("same station + obs time as id "), col("kept_id"), lit(" (newer change kept)")),
                        col("raw_payload")))
                .unionByName(Rejects.of(dupExisting, SOURCE, col("event_key"), lit(DUPLICATE_OF_EXISTING),
                        concat(lit("same station + obs time already in silver as id "), col("existing_id")),
                        col("raw_payload")));

        // 5. Flatten the accepted documents
        r.observations = observations(accepted);
        r.children.put("observation_section", accepted.select(
                col("event_key").as("observation_id"), explode(col("d.sections")).as("section_code")));
        r.children.put("past_weather_observation", pastWeather(accepted));
        r.children.put("present_weather_observation", presentWeather(accepted));
        r.children.put("sky_condition_observation", skyCondition(accepted));
        r.children.put("marine_observation", marine(accepted));
        return r;
    }

    // ---------------- validation rules ----------------

    private static Column validationReason() {
        Column lon = col("lon");
        Column lat = col("lat");
        return when(col("event_key").equalTo(""), lit(MISSING_ID))
                .when(col("obs_ts").isNull(), lit(MISSING_TIMESTAMP))
                .when(col("call_letters").isNull().or(col("call_letters").equalTo("")), lit(MISSING_STATION))
                .when(lon.isNull().or(lat.isNull()).or(abs(lon).gt(180)).or(abs(lat).gt(90)), lit(INVALID_COORDINATES));
    }

    private static Column validationDetails() {
        return when(col("reason").equalTo(MISSING_ID), lit("_id is missing or not an ObjectId"))
                .when(col("reason").equalTo(MISSING_TIMESTAMP), lit("ts is missing or not a date"))
                .when(col("reason").equalTo(MISSING_STATION), lit("callLetters is null or blank"))
                .when(col("reason").equalTo(INVALID_COORDINATES), concat(
                        lit("coordinates [lon, lat] = "),
                        coalesce(to_json(col("d.position.coordinates")), lit("null")),
                        lit(" (lon must be -180..180, lat -90..90)")));
    }

    /** (id, call_letters, obs_ts) of live silver rows in the time range of this batch. */
    private Dataset<Row> existingStationTimeKeys(Dataset<Row> winners) {
        Row range = winners.agg(min("obs_ts"), max("obs_ts")).first();
        String where = range.isNullAt(0) ? "1 = 0"
                : "is_deleted = 0 AND obs_ts BETWEEN '" + range.getTimestamp(0) + "' AND '" + range.getTimestamp(1) + "'";
        String query = "(SELECT id AS existing_id, call_letters AS e_call_letters, obs_ts AS e_obs_ts"
                + " FROM weather_observation WHERE " + where + ") t";
        return spark.read().jdbc(props.getSilverJdbcUrl(), query, props.sparkJdbcProps());
    }

    // ---------------- flatten: one method per table ----------------

    private static Dataset<Row> observations(Dataset<Row> df) {
        return df.select(
                col("event_key").as("id"),
                col("d.st").as("st"),
                col("obs_ts"),
                col("lon").as("longitude"),
                col("lat").as("latitude"),
                col("d.elevation").cast("int").as("elevation"),
                col("call_letters"),
                col("d.qualityControlProcess").as("quality_control_process"),
                col("d.dataSource").as("data_source"),
                col("d.type").as("report_type"),
                col("d.airTemperature.value").as("air_temperature"),
                col("d.airTemperature.quality").as("air_temperature_quality"),
                col("d.dewPoint.value").as("dew_point"),
                col("d.dewPoint.quality").as("dew_point_quality"),
                col("d.pressure.value").as("pressure"),
                col("d.pressure.quality").as("pressure_quality"),
                col("d.wind.direction.angle").cast("int").as("wind_direction_angle"),
                col("d.wind.direction.quality").as("wind_direction_quality"),
                col("d.wind.type").as("wind_type"),
                col("d.wind.speed.rate").as("wind_speed_rate"),
                col("d.wind.speed.quality").as("wind_speed_quality"),
                col("d.visibility.distance.value").cast("int").as("visibility_distance"),
                col("d.visibility.distance.quality").as("visibility_distance_quality"),
                col("d.visibility.variability.value").as("visibility_variability"),
                col("d.visibility.variability.quality").as("visibility_variability_quality"),
                col("d.skyCondition.ceilingHeight.value").cast("int").as("ceiling_height"),
                col("d.skyCondition.ceilingHeight.quality").as("ceiling_height_quality"),
                col("d.skyCondition.ceilingHeight.determination").as("ceiling_determination"),
                col("d.skyCondition.cavok").as("cavok"),
                col("d.precipitationEstimatedObservation.discrepancy").as("precipitation_discrepancy"),
                col("d.precipitationEstimatedObservation.estimatedWaterDepth").cast("int").as("estimated_water_depth"),
                col("d.atmosphericPressureChange.tendency.code").as("pressure_tendency_code"),
                col("d.atmosphericPressureChange.tendency.quality").as("pressure_tendency_quality"),
                col("d.atmosphericPressureChange.quantity3Hours.value").as("pressure_change_3h"),
                col("d.atmosphericPressureChange.quantity3Hours.quality").as("pressure_change_3h_quality"),
                col("d.atmosphericPressureChange.quantity24Hours.value").as("pressure_change_24h"),
                col("d.atmosphericPressureChange.quantity24Hours.quality").as("pressure_change_24h_quality"),
                lit(0).as("is_deleted"),
                col("source_ts"),
                col("bronze_ingested_at"));
    }

    private static Dataset<Row> pastWeather(Dataset<Row> df) {
        return df.select(col("event_key"), explode(col("d.pastWeatherObservationManual")).as("e")).select(
                col("event_key").as("observation_id"),
                col("e.atmosphericCondition.value").as("atmospheric_condition"),
                col("e.atmosphericCondition.quality").as("atmospheric_condition_quality"),
                col("e.period.value").cast("int").as("period_value"),
                col("e.period.quality").as("period_quality"));
    }

    private static Dataset<Row> presentWeather(Dataset<Row> df) {
        return df.select(col("event_key"), explode(col("d.presentWeatherObservationManual")).as("e")).select(
                col("event_key").as("observation_id"),
                col("e.condition").as("weather_condition"),
                col("e.quality").as("condition_quality"));
    }

    private static Dataset<Row> skyCondition(Dataset<Row> df) {
        String p = "d.skyConditionObservation.";
        return df.filter(col("d.skyConditionObservation").isNotNull()).select(
                col("event_key").as("observation_id"),
                col(p + "totalCoverage.value").as("total_coverage"),
                col(p + "totalCoverage.opaque").as("total_coverage_opaque"),
                col(p + "totalCoverage.quality").as("total_coverage_quality"),
                col(p + "lowestCloudCoverage.value").as("lowest_cloud_coverage"),
                col(p + "lowestCloudCoverage.quality").as("lowest_cloud_coverage_quality"),
                col(p + "lowCloudGenus.value").as("low_cloud_genus"),
                col(p + "lowCloudGenus.quality").as("low_cloud_genus_quality"),
                col(p + "lowestCloudBaseHeight.value").cast("int").as("lowest_cloud_base_height"),
                col(p + "lowestCloudBaseHeight.quality").as("lowest_cloud_base_height_quality"),
                col(p + "midCloudGenus.value").as("mid_cloud_genus"),
                col(p + "midCloudGenus.quality").as("mid_cloud_genus_quality"),
                col(p + "highCloudGenus.value").as("high_cloud_genus"),
                col(p + "highCloudGenus.quality").as("high_cloud_genus_quality"));
    }

    private static Dataset<Row> marine(Dataset<Row> df) {
        return df.filter(col("d.seaSurfaceTemperature").isNotNull().or(col("d.waveMeasurement").isNotNull())).select(
                col("event_key").as("observation_id"),
                col("d.seaSurfaceTemperature.value").as("sea_surface_temperature"),
                col("d.seaSurfaceTemperature.quality").as("sea_surface_temperature_quality"),
                col("d.waveMeasurement.method").as("wave_method"),
                col("d.waveMeasurement.waves.period").cast("int").as("wave_period"),
                col("d.waveMeasurement.waves.height").as("wave_height"),
                col("d.waveMeasurement.waves.quality").as("wave_quality"),
                col("d.waveMeasurement.seaState.code").as("sea_state_code"),
                col("d.waveMeasurement.seaState.quality").as("sea_state_quality"));
    }

    static Column millisToTimestamp(Column millis) {
        return millis.divide(1000).cast("timestamp");
    }
}
