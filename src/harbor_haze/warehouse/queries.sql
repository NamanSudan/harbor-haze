-- Example queries for the report and the demo. Run with:
--   docker compose exec warehouse psql -U harbor -d harbor_haze -f /path/to/queries.sql
-- or paste them into psql one at a time.

-- 1. Coverage: how much PM2.5 data do we have per monitor and year?
SELECT s.site_id, t.year, COUNT(*) AS hourly_rows,
       ROUND(AVG(f.value)::numeric, 2) AS mean_pm25
FROM fact_air_quality f
JOIN dim_parameter p ON p.parameter_key = f.parameter_key AND p.param = 'pm25'
JOIN dim_station s   ON s.station_key = f.station_key
JOIN dim_time t      ON t.time_key = f.time_key
GROUP BY s.site_id, t.year
ORDER BY s.site_id, t.year;

-- 2. Daily pattern: average PM2.5 by local hour of day.
SELECT t.hour_local, ROUND(AVG(f.value)::numeric, 2) AS mean_pm25, COUNT(*) AS n
FROM fact_air_quality f
JOIN dim_parameter p ON p.parameter_key = f.parameter_key AND p.param = 'pm25'
JOIN dim_time t      ON t.time_key = f.time_key
GROUP BY t.hour_local
ORDER BY t.hour_local;

-- 3. Smoke effect: PM2.5 on smoke-density days (NULL = no HMS file, i.e. unknown).
SELECT sm.smoke_density, COUNT(*) AS n, ROUND(AVG(f.value)::numeric, 2) AS mean_pm25
FROM fact_air_quality f
JOIN dim_parameter p ON p.parameter_key = f.parameter_key AND p.param = 'pm25'
LEFT JOIN dim_smoke sm ON sm.date_key = f.smoke_key
GROUP BY sm.smoke_density
ORDER BY sm.smoke_density NULLS LAST;

-- 4. Ship signal: PM2.5 by number of cargo + tanker pings near the monitor.
SELECT CASE WHEN v.vessel_key IS NULL THEN '0 (no pings)'
            WHEN v.n_cargo + v.n_tanker = 0 THEN '0 cargo/tanker'
            WHEN v.n_cargo + v.n_tanker < 50 THEN '1-49'
            WHEN v.n_cargo + v.n_tanker < 200 THEN '50-199'
            ELSE '200+' END AS cargo_tanker_pings,
       COUNT(*) AS n, ROUND(AVG(f.value)::numeric, 2) AS mean_pm25
FROM fact_air_quality f
JOIN dim_parameter p ON p.parameter_key = f.parameter_key AND p.param = 'pm25'
LEFT JOIN dim_vessel_activity v ON v.vessel_key = f.vessel_key
GROUP BY 1
ORDER BY MIN(COALESCE(v.n_cargo + v.n_tanker, 0));

-- 5. The two windows side by side (anchorage queue in 2021 vs after it ended in 2024).
SELECT t.year,
       ROUND(AVG(f.value)::numeric, 2)                          AS mean_pm25,
       ROUND(AVG(COALESCE(v.n_cargo + v.n_tanker, 0))::numeric, 1) AS mean_cargo_tanker_pings,
       COUNT(*)                                                  AS n
FROM fact_air_quality f
JOIN dim_parameter p ON p.parameter_key = f.parameter_key AND p.param = 'pm25'
JOIN dim_time t      ON t.time_key = f.time_key
LEFT JOIN dim_vessel_activity v ON v.vessel_key = f.vessel_key
GROUP BY t.year
ORDER BY t.year;

-- 6. Data quality: how many fact rows have a weather / vessel / smoke link?
SELECT COUNT(*)                                        AS fact_rows,
       ROUND(100.0 * COUNT(weather_key) / COUNT(*), 1) AS pct_with_weather,
       ROUND(100.0 * COUNT(vessel_key)  / COUNT(*), 1) AS pct_with_vessel_row,
       ROUND(100.0 * COUNT(smoke_key)   / COUNT(*), 1) AS pct_with_smoke_day
FROM fact_air_quality;

-- 7. Data quality: negative or implausible readings.
SELECT p.param, COUNT(*) FILTER (WHERE f.value < 0)   AS negative,
                COUNT(*) FILTER (WHERE f.value > 500) AS above_500,
                COUNT(*) FILTER (WHERE f.value IS NULL) AS null_value
FROM fact_air_quality f
JOIN dim_parameter p ON p.parameter_key = f.parameter_key
GROUP BY p.param;
