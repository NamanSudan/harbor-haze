-- One flat row per PM2.5 reading with everything the model needs. Luca's SparkML work can read
-- this view (or its export) instead of re-joining the star schema.
-- Vessel counts are filled with 0 when no pings were recorded nearby in that hour (could be no
-- ships or no AIS coverage); has_vessel_row tells the two cases apart if that matters.
-- bay_* columns are whole-bay hourly headcounts; filter bay_low_coverage = false to drop AIS outage hours.
CREATE OR REPLACE VIEW v_pm25_model AS
SELECT
    t.ts_utc,
    t.ts_local,
    t.hour_local,
    t.dow_local,
    t.is_weekend_local,
    s.site_id,
    f.value                                   AS pm25,
    f.n_poc,
    w.temperature,
    w.relative_humidity,
    w.wind_speed,
    w.wind_direction,
    w.wind_u,
    w.wind_v,
    w.precipitation,
    sm.smoke_density,
    (v.vessel_key IS NOT NULL)                AS has_vessel_row,
    COALESCE(v.n_pings, 0)                    AS n_pings,
    COALESCE(v.n_vessels, 0)                  AS n_vessels,
    COALESCE(v.n_stationary, 0)               AS n_stationary,
    COALESCE(v.n_underway, 0)                 AS n_underway,
    COALESCE(v.n_cargo, 0)                    AS n_cargo,
    COALESCE(v.n_tanker, 0)                   AS n_tanker,
    COALESCE(v.n_passenger, 0)                AS n_passenger,
    COALESCE(v.n_tug, 0)                      AS n_tug,
    COALESCE(v.n_cargo_tanker, 0)             AS n_cargo_tanker,
    COALESCE(v.n_cargo_tanker_stationary, 0)  AS n_cargo_tanker_stationary,
    COALESCE(v.n_cargo_tanker_anchored, 0)    AS n_cargo_tanker_anchored,
    v.mean_sog,
    v.mean_sog_underway,
    b.n_vessels                               AS bay_n_vessels,
    b.n_cargo_tanker                          AS bay_n_cargo_tanker,
    b.n_cargo_tanker_stationary               AS bay_n_cargo_tanker_stationary,
    b.n_cargo_tanker_anchored                 AS bay_n_cargo_tanker_anchored,
    b.low_coverage                            AS bay_low_coverage
FROM fact_air_quality f
JOIN dim_parameter p            ON p.parameter_key = f.parameter_key AND p.param = 'pm25'
JOIN dim_time t                 ON t.time_key = f.time_key
JOIN dim_station s              ON s.station_key = f.station_key
LEFT JOIN dim_weather w         ON w.weather_key = f.weather_key
LEFT JOIN dim_smoke sm          ON sm.date_key = f.smoke_key
LEFT JOIN dim_vessel_activity v ON v.vessel_key = f.vessel_key
LEFT JOIN dim_bay_activity b    ON b.time_key = f.time_key;
