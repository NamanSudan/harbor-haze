-- Star schema for hourly air quality near the Port of Los Angeles / Long Beach.
-- Rebuilt from scratch on every load (drop + create), so edit freely.
-- Grain of fact_air_quality: one row per monitoring site x pollutant x UTC hour.

DROP VIEW  IF EXISTS v_pm25_model;
DROP TABLE IF EXISTS fact_air_quality, dim_bay_activity, dim_vessel_activity, dim_weather, dim_smoke,
                     dim_time, dim_station, dim_parameter CASCADE;

CREATE TABLE dim_parameter (
    parameter_key  SMALLINT PRIMARY KEY,
    param          TEXT NOT NULL UNIQUE,      -- pm25 | pm25_nonref | no2
    description    TEXT
);

CREATE TABLE dim_station (
    station_key    INTEGER PRIMARY KEY,
    site_id        TEXT NOT NULL UNIQUE,      -- EPA format, e.g. 06-037-4009 (same as AirNow)
    county         TEXT,
    lat            DOUBLE PRECISION,
    lon            DOUBLE PRECISION,
    distance_km    DOUBLE PRECISION,          -- distance from the port reference point
    wx_station     TEXT                       -- nearest weather station: LGB | LAX
);

CREATE TABLE dim_time (
    time_key          BIGINT PRIMARY KEY,     -- YYYYMMDDHH in UTC
    ts_utc            TIMESTAMPTZ NOT NULL UNIQUE,
    date_utc          DATE,
    year              SMALLINT,
    month             SMALLINT,
    day               SMALLINT,
    hour_utc          SMALLINT,
    ts_local          TIMESTAMP,              -- America/Los_Angeles wall-clock time
    date_local        DATE,
    hour_local        SMALLINT,
    dow_local         SMALLINT,               -- Monday = 0
    is_weekend_local  BOOLEAN
);

-- One row per calendar day (local). A day with no HMS file has NULL density = unknown.
CREATE TABLE dim_smoke (
    date_key        INTEGER PRIMARY KEY,      -- YYYYMMDD
    date_local      DATE NOT NULL,
    smoke_density   SMALLINT,                 -- 0 none .. 3 heavy
    smoke_polygons  INTEGER,
    file_found      BOOLEAN NOT NULL
);

-- Hourly weather per station, specials averaged into the hour.
CREATE TABLE dim_weather (
    weather_key             INTEGER PRIMARY KEY,
    wx_station              TEXT NOT NULL,
    time_key                BIGINT NOT NULL REFERENCES dim_time (time_key),
    temperature             DOUBLE PRECISION,
    dew_point_temperature   DOUBLE PRECISION,
    relative_humidity       DOUBLE PRECISION,
    wind_speed              DOUBLE PRECISION,
    wind_gust               DOUBLE PRECISION,
    wind_direction          DOUBLE PRECISION, -- vector-averaged degrees
    wind_u                  DOUBLE PRECISION, -- east component (blowing towards)
    wind_v                  DOUBLE PRECISION, -- north component (blowing towards)
    station_level_pressure  DOUBLE PRECISION,
    precipitation           DOUBLE PRECISION,
    visibility              DOUBLE PRECISION,
    n_obs                   SMALLINT,
    UNIQUE (wx_station, time_key)
);

-- Vessel activity within radius_km of a monitor, per hour. Derived from the 0.02 degree
-- grid cells in features/vessel_hourly. n_vessels is a sum over cells (activity index).
CREATE TABLE dim_vessel_activity (
    vessel_key         INTEGER PRIMARY KEY,
    station_key        INTEGER NOT NULL REFERENCES dim_station (station_key),
    time_key           BIGINT  NOT NULL REFERENCES dim_time (time_key),
    radius_km          DOUBLE PRECISION NOT NULL,
    n_cells            SMALLINT,
    n_pings            INTEGER,
    n_vessels          INTEGER,
    n_stationary       INTEGER,
    n_underway         INTEGER,
    n_cargo            INTEGER,
    n_tanker           INTEGER,
    n_passenger        INTEGER,
    n_tug              INTEGER,
    n_cargo_tanker             INTEGER,
    n_cargo_tanker_stationary  INTEGER,
    n_cargo_tanker_anchored    INTEGER,    -- status 1 only (at anchor); moored (5) not included
    mean_sog           DOUBLE PRECISION,
    mean_sog_underway  DOUBLE PRECISION,
    UNIQUE (station_key, time_key)
);

-- Whole-bay activity per hour. Each vessel counted once per hour (real headcount, approximate).
-- low_coverage = too few AIS pings that hour (an outage, not empty water).
CREATE TABLE dim_bay_activity (
    time_key           BIGINT PRIMARY KEY REFERENCES dim_time (time_key),
    n_pings            INTEGER,
    n_vessels          INTEGER,
    n_stationary       INTEGER,
    n_underway         INTEGER,
    n_cargo            INTEGER,
    n_tanker           INTEGER,
    n_passenger        INTEGER,
    n_tug              INTEGER,
    n_cargo_tanker             INTEGER,
    n_cargo_tanker_stationary  INTEGER,
    n_cargo_tanker_anchored    INTEGER,
    mean_sog           DOUBLE PRECISION,
    mean_sog_underway  DOUBLE PRECISION,
    low_coverage       BOOLEAN
);

CREATE TABLE fact_air_quality (
    fact_key       BIGSERIAL PRIMARY KEY,
    time_key       BIGINT   NOT NULL REFERENCES dim_time (time_key),
    station_key    INTEGER  NOT NULL REFERENCES dim_station (station_key),
    parameter_key  SMALLINT NOT NULL REFERENCES dim_parameter (parameter_key),
    weather_key    INTEGER  REFERENCES dim_weather (weather_key),
    smoke_key      INTEGER  REFERENCES dim_smoke (date_key),
    vessel_key     INTEGER  REFERENCES dim_vessel_activity (vessel_key),
    value          DOUBLE PRECISION,          -- PM2.5 in ug/m3, NO2 in ppb (see units)
    units          TEXT,
    n_poc          SMALLINT,                  -- instruments averaged into this value
    UNIQUE (time_key, station_key, parameter_key)
);

CREATE INDEX ON fact_air_quality (station_key, time_key);
CREATE INDEX ON fact_air_quality (parameter_key);
CREATE INDEX ON dim_vessel_activity (time_key);
CREATE INDEX ON dim_weather (time_key);
