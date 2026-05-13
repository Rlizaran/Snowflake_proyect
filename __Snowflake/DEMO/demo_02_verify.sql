-- Demo verify: chequeos en bronze, snapshot, silver y gold tras correr dbt.

USE ROLE ROLE_NYCBIKE;
USE WAREHOUSE WH_NYCBIKE_DEV;

-- ============================================================================
-- 1) Bronze: trips demo y valor NOAA corregido
-- ============================================================================
SELECT 'NY DEMO' AS src, COUNT(*) AS n
FROM PRO_CITYBIKE_BRONZE.CITYBIKE.CITYBIKE_TRIPS_NY
WHERE ride_id LIKE 'DEMO_NY_%'
UNION ALL
SELECT 'JC DEMO', COUNT(*)
FROM PRO_CITYBIKE_BRONZE.CITYBIKE.CITYBIKE_TRIPS_JC
WHERE ride_id LIKE 'DEMO_JC_%';

SELECT station_id, observation_date, element, data_value, q_flag, load_ts
FROM PRO_CITYBIKE_BRONZE.NOAA.NOAA_RAW_YEAR
WHERE station_id='USW00094728' AND observation_date='20240101' AND element='TMAX';

-- ============================================================================
-- 2) Snapshot SCD2: dos versiones para Manhattan-2024-01-01-TMAX
-- ============================================================================
SELECT scd_key, data_value, q_flag, q_flag_category,
       dbt_valid_from, dbt_valid_to,
       CASE WHEN dbt_valid_to IS NULL THEN 'CURRENT' ELSE 'SUPERSEDED' END AS estado
FROM PRO_CITYBIKE_SILVER.snapshots.snp_NOAA__noaa_raw_year
WHERE scd_key = 'USW00094728|20240101|TMAX'
ORDER BY dbt_valid_from;

-- ============================================================================
-- 3) Silver slv_trip: los 10 nuevos viajes
-- ============================================================================
SELECT ride_id, trip_date, trip_duration_min, city_id, rideable_type_code, user_type_code
FROM PRO_CITYBIKE_SILVER.CITYBIKE.slv_trip
WHERE ride_id LIKE 'DEMO_%'
ORDER BY ride_id;

-- ============================================================================
-- 4) Silver slv_weather_observation: refleja el nuevo data_value
-- ============================================================================
SELECT station_id, observation_date, element_code, data_value, q_flag, q_flag_category
FROM PRO_CITYBIKE_SILVER.NOAA.slv_weather_observation
WHERE station_id='USW00094728' AND observation_date='2024-01-01' AND element_code='TMAX';

-- ============================================================================
-- 5) Gold fct_trips_daily: aggregados para 2026-01-15
-- ============================================================================
SELECT trip_date, city_id, rideable_type_code, user_type_code, series_key, n_trips, avg_duration_min
FROM PRO_CITYBIKE_GOLD.MARTS.fct_trips_daily
WHERE trip_date = '2026-01-15'
ORDER BY series_key;

-- ============================================================================
-- 6) Gold fct_trips_weather: rides + clima 2026-01-15
-- ============================================================================
SELECT trip_date, city_name, station_id,
       n_trips, n_trips_member, n_trips_casual, n_trips_classic, n_trips_electric,
       temp_max_c, temp_min_c, precipitation_mm, weather_category
FROM PRO_CITYBIKE_GOLD.MARTS.fct_trips_weather
WHERE trip_date = '2026-01-15'
ORDER BY city_name;

-- ============================================================================
-- 7) Gold fct_noaa_corrections: la fila SCD2 corregida (2 versiones)
-- ============================================================================
SELECT scd_key, observation_date, element_code, data_value, q_flag, q_flag_category,
       is_current, is_superseded, is_problematic,
       dbt_valid_from, dbt_valid_to
FROM PRO_CITYBIKE_GOLD.MARTS.fct_noaa_corrections
WHERE scd_key = 'USW00094728|20240101|TMAX'
ORDER BY dbt_valid_from;

-- ============================================================================
-- 8) Resumen visible en Power BI (sanity check)
-- ============================================================================
SELECT
    'Total trips 2026-01-15'  AS metrica,
    COUNT(*)::varchar         AS valor
FROM PRO_CITYBIKE_GOLD.MARTS.fct_trips
WHERE trip_date = '2026-01-15'
UNION ALL
SELECT
    'Correcciones NOAA totales',
    COUNT(*)::varchar
FROM PRO_CITYBIKE_GOLD.MARTS.fct_noaa_corrections
WHERE is_superseded = TRUE;
