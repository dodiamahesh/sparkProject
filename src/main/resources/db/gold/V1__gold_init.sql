-- =====================================================================
-- de_gold : aggregates built from de_silver
-- Created/updated automatically by Flyway at application startup.
-- Sentinel values (999.9 / 9999.9 = "not measured") and deleted rows are excluded.
-- =====================================================================

-- One row per station per day
CREATE TABLE daily_station_weather (
    station_code     VARCHAR(20)  NOT NULL,
    obs_date         DATE         NOT NULL,
    obs_count        INT          NOT NULL,
    avg_air_temp     DOUBLE,
    min_air_temp     DOUBLE,
    max_air_temp     DOUBLE,
    avg_dew_point    DOUBLE,
    avg_pressure     DOUBLE,
    max_wind_speed   DOUBLE,
    gold_loaded_at   DATETIME(3)  NOT NULL,
    batch_id         VARCHAR(36)  NOT NULL,
    PRIMARY KEY (station_code, obs_date)
) ENGINE=InnoDB;

-- One row per region per month (region from dim_station; UNKNOWN when the station is not in the master data)
CREATE TABLE monthly_region_weather (
    region           VARCHAR(50)  NOT NULL,
    obs_month        CHAR(7)      NOT NULL,     -- 'YYYY-MM'
    station_count    INT          NOT NULL,
    obs_count        INT          NOT NULL,
    avg_air_temp     DOUBLE,
    min_air_temp     DOUBLE,
    max_air_temp     DOUBLE,
    avg_pressure     DOUBLE,
    gold_loaded_at   DATETIME(3)  NOT NULL,
    batch_id         VARCHAR(36)  NOT NULL,
    PRIMARY KEY (region, obs_month)
) ENGINE=InnoDB;

-- Data completeness per station per day
CREATE TABLE station_data_quality (
    station_code        VARCHAR(20)  NOT NULL,
    obs_date            DATE         NOT NULL,
    total_obs           INT          NOT NULL,
    missing_air_temp    INT          NOT NULL,
    missing_dew_point   INT          NOT NULL,
    missing_pressure    INT          NOT NULL,
    missing_wind_speed  INT          NOT NULL,
    complete_obs_pct    DOUBLE       NOT NULL,  -- % of observations with all 4 values present
    gold_loaded_at      DATETIME(3)  NOT NULL,
    batch_id            VARCHAR(36)  NOT NULL,
    PRIMARY KEY (station_code, obs_date)
) ENGINE=InnoDB;

-- ---------- Staging ----------
CREATE TABLE stg_daily_station_weather LIKE daily_station_weather;
ALTER TABLE stg_daily_station_weather DROP COLUMN gold_loaded_at, DROP COLUMN batch_id;

CREATE TABLE stg_monthly_region_weather LIKE monthly_region_weather;
ALTER TABLE stg_monthly_region_weather DROP COLUMN gold_loaded_at, DROP COLUMN batch_id;

CREATE TABLE stg_station_data_quality LIKE station_data_quality;
ALTER TABLE stg_station_data_quality DROP COLUMN gold_loaded_at, DROP COLUMN batch_id;

-- keys (groups) that must be recomputed in this run
CREATE TABLE stg_keys_daily (
    station_code  VARCHAR(20) NOT NULL,
    obs_date      DATE        NOT NULL,
    PRIMARY KEY (station_code, obs_date)
) ENGINE=InnoDB;

CREATE TABLE stg_keys_monthly (
    obs_month  CHAR(7) NOT NULL PRIMARY KEY
) ENGINE=InnoDB;
