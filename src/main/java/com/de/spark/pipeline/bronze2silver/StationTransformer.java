package com.de.spark.pipeline.bronze2silver;

import com.de.spark.common.BronzeReader;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.springframework.stereotype.Component;

import static com.de.spark.pipeline.bronze2silver.ObservationTransformer.millisToTimestamp;
import static com.de.spark.pipeline.bronze2silver.Rejects.*;
import static org.apache.spark.sql.functions.*;

/**
 * Bronze mysql_station + mysql_station_owner -> Silver dim_station (one row per station, with its owner/region).
 *
 * A change in EITHER collection affects the station, so the dim row of every affected station is rebuilt
 * from the full (small) master-data collections. That also handles owner changes and deletes correctly.
 */
@Component
public class StationTransformer {

    public static final String STATION_SOURCE = "mysql_station";
    public static final String OWNER_SOURCE = "mysql_station_owner";

    private final BronzeReader reader;

    public StationTransformer(BronzeReader reader) {
        this.reader = reader;
    }

    public static class Result {
        public Dataset<Row> dim;       // -> dim_station (upsert)
        public Dataset<Row> rejects;   // -> etl_rejected_record
    }

    public Result transform(Dataset<Row> stationEvents, Dataset<Row> ownerEvents) {
        Dataset<Row> affected = stationEvents.select("station_code")
                .union(ownerEvents.select("station_code"))
                .distinct();

        Dataset<Row> stations = reader.readAll(STATION_SOURCE, BronzeSchemas.MYSQL_STATION)
                .join(affected, "station_code")
                .withColumn("lat", col("latitude").cast("decimal(8,4)"))
                .withColumn("lon", col("longitude").cast("decimal(8,4)"));

        // current owner of each station = the newest live owner row
        Dataset<Row> owner = reader.readAll(OWNER_SOURCE, BronzeSchemas.MYSQL_STATION_OWNER)
                .filter(col("__deleted").notEqual("true"))
                .withColumn("rn", row_number().over(Window.partitionBy("station_code").orderBy(col("owner_id").desc())))
                .filter(col("rn").equalTo(1))
                .select("station_code", "owner_name", "region");

        // VALIDATION (deleted stations are not validated, they are only marked deleted)
        Column isDeleted = col("__deleted").equalTo("true");
        Column reason = when(isDeleted, lit(null))
                .when(col("station_name").isNull().or(trim(col("station_name")).equalTo("")), lit(MISSING_STATION_NAME))
                .when(col("lat").isNull().or(col("lon").isNull())
                        .or(abs(col("lat")).gt(90)).or(abs(col("lon")).gt(180)), lit(INVALID_COORDINATES));
        Dataset<Row> checked = stations.withColumn("reason", reason).cache();

        Dataset<Row> invalid = checked.filter(col("reason").isNotNull());
        Result r = new Result();
        r.rejects = Rejects.of(invalid, STATION_SOURCE, col("station_code"), col("reason"),
                when(col("reason").equalTo(INVALID_COORDINATES), concat(lit("latitude="), coalesce(col("latitude"), lit("null")),
                        lit(", longitude="), coalesce(col("longitude"), lit("null")), lit(" (lat -90..90, lon -180..180)")))
                        .otherwise(lit("station_name is null or blank")),
                to_json(struct(col("station_code"), col("station_name"), col("country"), col("latitude"),
                        col("longitude"), col("station_type"), col("__op"))));

        r.dim = checked.filter(col("reason").isNull())
                .join(owner, "station_code", "left")
                .select(
                        col("station_code"),
                        col("station_name"),
                        col("country"),
                        col("lat").as("latitude"),
                        col("lon").as("longitude"),
                        col("station_type"),
                        col("owner_name"),
                        col("region"),
                        when(isDeleted, 1).otherwise(0).as("is_deleted"),
                        millisToTimestamp(col("_ingested_at")).as("bronze_ingested_at"));
        return r;
    }
}
