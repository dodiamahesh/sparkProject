-- =====================================================================
-- de_silver : curated layer + pipeline control tables
-- Created/updated automatically by Flyway at application startup.
-- =====================================================================

-- ---------- Dimension: station (from bronze mysql_station + mysql_station_owner) ----------
CREATE TABLE dim_station (
    station_code        VARCHAR(20)  PRIMARY KEY,
    station_name        VARCHAR(100),
    country             VARCHAR(50),
    latitude            DECIMAL(8,4),
    longitude           DECIMAL(8,4),
    station_type        VARCHAR(20),
    owner_name          VARCHAR(100),
    region              VARCHAR(50),
    is_deleted          TINYINT(1)   NOT NULL DEFAULT 0,
    bronze_ingested_at  DATETIME(3),
    silver_loaded_at    DATETIME(3)  NOT NULL,
    batch_id            VARCHAR(36)  NOT NULL,
    KEY idx_dim_loaded (silver_loaded_at)
) ENGINE=InnoDB;

-- ---------- Fact: one row per observation (from bronze mongo_weather_observations) ----------
CREATE TABLE weather_observation (
    id                              VARCHAR(24)  PRIMARY KEY,   -- Mongo ObjectId
    st                              VARCHAR(50),
    obs_ts                          DATETIME(3)  NOT NULL,
    longitude                       DOUBLE,
    latitude                        DOUBLE,
    elevation                       INT,
    call_letters                    VARCHAR(20)  NOT NULL,      -- = dim_station.station_code
    quality_control_process         VARCHAR(20),
    data_source                     VARCHAR(20),
    report_type                     VARCHAR(20),
    air_temperature                 DOUBLE,
    air_temperature_quality         VARCHAR(5),
    dew_point                       DOUBLE,
    dew_point_quality               VARCHAR(5),
    pressure                        DOUBLE,
    pressure_quality                VARCHAR(5),
    wind_direction_angle            INT,
    wind_direction_quality          VARCHAR(5),
    wind_type                       VARCHAR(5),
    wind_speed_rate                 DOUBLE,
    wind_speed_quality              VARCHAR(5),
    visibility_distance             INT,
    visibility_distance_quality     VARCHAR(5),
    visibility_variability          VARCHAR(5),
    visibility_variability_quality  VARCHAR(5),
    ceiling_height                  INT,
    ceiling_height_quality          VARCHAR(5),
    ceiling_determination           VARCHAR(5),
    cavok                           VARCHAR(5),
    precipitation_discrepancy       VARCHAR(5),
    estimated_water_depth           INT,
    pressure_tendency_code          VARCHAR(5),
    pressure_tendency_quality       VARCHAR(5),
    pressure_change_3h              DOUBLE,
    pressure_change_3h_quality      VARCHAR(5),
    pressure_change_24h             DOUBLE,
    pressure_change_24h_quality     VARCHAR(5),
    is_deleted                      TINYINT(1)   NOT NULL DEFAULT 0,
    source_ts                       DATETIME(3),                -- when the change happened in the source DB
    bronze_ingested_at              DATETIME(3),                -- when it reached bronze (Kafka time)
    silver_loaded_at                DATETIME(3)  NOT NULL,      -- when this pipeline run wrote it
    batch_id                        VARCHAR(36)  NOT NULL,      -- = etl_job_run.run_id
    KEY idx_obs_station_ts (call_letters, obs_ts),
    KEY idx_obs_loaded (silver_loaded_at)
) ENGINE=InnoDB;

-- ---------- Child tables (replaced as a whole when their observation changes) ----------
CREATE TABLE observation_section (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    observation_id  VARCHAR(24) NOT NULL,
    section_code    VARCHAR(10),
    CONSTRAINT fk_section_obs FOREIGN KEY (observation_id) REFERENCES weather_observation (id)
) ENGINE=InnoDB;

CREATE TABLE past_weather_observation (
    id                             BIGINT AUTO_INCREMENT PRIMARY KEY,
    observation_id                 VARCHAR(24) NOT NULL,
    atmospheric_condition          VARCHAR(5),
    atmospheric_condition_quality  VARCHAR(5),
    period_value                   INT,
    period_quality                 VARCHAR(5),
    CONSTRAINT fk_past_obs FOREIGN KEY (observation_id) REFERENCES weather_observation (id)
) ENGINE=InnoDB;

CREATE TABLE present_weather_observation (
    id                 BIGINT AUTO_INCREMENT PRIMARY KEY,
    observation_id     VARCHAR(24) NOT NULL,
    weather_condition  VARCHAR(5),
    condition_quality  VARCHAR(5),
    CONSTRAINT fk_present_obs FOREIGN KEY (observation_id) REFERENCES weather_observation (id)
) ENGINE=InnoDB;

CREATE TABLE sky_condition_observation (
    observation_id                    VARCHAR(24) PRIMARY KEY,
    total_coverage                    VARCHAR(5),
    total_coverage_opaque             VARCHAR(5),
    total_coverage_quality            VARCHAR(5),
    lowest_cloud_coverage             VARCHAR(5),
    lowest_cloud_coverage_quality     VARCHAR(5),
    low_cloud_genus                   VARCHAR(5),
    low_cloud_genus_quality           VARCHAR(5),
    lowest_cloud_base_height          INT,
    lowest_cloud_base_height_quality  VARCHAR(5),
    mid_cloud_genus                   VARCHAR(5),
    mid_cloud_genus_quality           VARCHAR(5),
    high_cloud_genus                  VARCHAR(5),
    high_cloud_genus_quality          VARCHAR(5),
    CONSTRAINT fk_sky_obs FOREIGN KEY (observation_id) REFERENCES weather_observation (id)
) ENGINE=InnoDB;

CREATE TABLE marine_observation (
    observation_id                   VARCHAR(24) PRIMARY KEY,
    sea_surface_temperature          DOUBLE,
    sea_surface_temperature_quality  VARCHAR(5),
    wave_method                      VARCHAR(5),
    wave_period                      INT,
    wave_height                      DOUBLE,
    wave_quality                     VARCHAR(5),
    sea_state_code                   VARCHAR(5),
    sea_state_quality                VARCHAR(5),
    CONSTRAINT fk_marine_obs FOREIGN KEY (observation_id) REFERENCES weather_observation (id)
) ENGINE=InnoDB;

-- ---------- Pipeline control ----------
-- How far each job has processed (epoch milliseconds)
CREATE TABLE etl_watermark (
    job_name    VARCHAR(50)  PRIMARY KEY,
    watermark_value  BIGINT       NOT NULL,
    updated_at  DATETIME(3)  NOT NULL
) ENGINE=InnoDB;

-- One row per job run (audit)
CREATE TABLE etl_job_run (
    run_id          VARCHAR(36)  PRIMARY KEY,
    job_name        VARCHAR(50)  NOT NULL,
    status          VARCHAR(10)  NOT NULL,      -- RUNNING / SUCCESS / FAILED / SKIPPED
    started_at      DATETIME(3)  NOT NULL,
    finished_at     DATETIME(3),
    watermark_from  BIGINT,
    watermark_to    BIGINT,
    rows_read       BIGINT,
    rows_written    BIGINT,
    rows_rejected   BIGINT,
    error_message   VARCHAR(1000),
    KEY idx_run_job (job_name, started_at)
) ENGINE=InnoDB;

-- Records that failed validation or were dropped as duplicates (with the reason + original data)
CREATE TABLE etl_rejected_record (
    id                 BIGINT AUTO_INCREMENT PRIMARY KEY,
    run_id             VARCHAR(36)  NOT NULL,
    job_name           VARCHAR(50)  NOT NULL,
    source_collection  VARCHAR(100) NOT NULL,
    record_key         VARCHAR(100),
    reason             VARCHAR(50)  NOT NULL,
    details            VARCHAR(500),
    raw_payload        MEDIUMTEXT,
    rejected_at        DATETIME(3)  NOT NULL,
    KEY idx_rej_run (run_id),
    KEY idx_rej_reason (reason)
) ENGINE=InnoDB;

-- ---------- Staging tables: Spark writes here, then one SQL transaction merges into the real tables ----------
CREATE TABLE stg_dim_station LIKE dim_station;
ALTER TABLE stg_dim_station DROP COLUMN silver_loaded_at, DROP COLUMN batch_id;

CREATE TABLE stg_weather_observation LIKE weather_observation;
ALTER TABLE stg_weather_observation DROP COLUMN silver_loaded_at, DROP COLUMN batch_id;

CREATE TABLE stg_observation_section LIKE observation_section;
ALTER TABLE stg_observation_section DROP COLUMN id;

CREATE TABLE stg_past_weather_observation LIKE past_weather_observation;
ALTER TABLE stg_past_weather_observation DROP COLUMN id;

CREATE TABLE stg_present_weather_observation LIKE present_weather_observation;
ALTER TABLE stg_present_weather_observation DROP COLUMN id;

CREATE TABLE stg_sky_condition_observation LIKE sky_condition_observation;
CREATE TABLE stg_marine_observation LIKE marine_observation;

-- ids deleted in the source (soft delete in silver)
CREATE TABLE stg_deleted_observation (
    id                  VARCHAR(24) PRIMARY KEY,
    source_ts           DATETIME(3),
    bronze_ingested_at  DATETIME(3)
) ENGINE=InnoDB;

CREATE TABLE stg_rejected_record (
    source_collection  VARCHAR(100) NOT NULL,
    record_key         VARCHAR(100),
    reason             VARCHAR(50)  NOT NULL,
    details            VARCHAR(500),
    raw_payload        MEDIUMTEXT
) ENGINE=InnoDB;
