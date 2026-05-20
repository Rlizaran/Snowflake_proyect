-- Demo verify: chequeos en bronze, snapshot, silver y gold tras correr dbt.

USE ROLE ROLE_NYCBIKE;
USE WAREHOUSE WH_NYCBIKE_DEV;

-- ============================================================================
-- 1) Bronze: trips demo y valor NOAA corregido
-- ============================================================================
SELECT 'NY DEMO' AS src, COUNT(*) AS n
FROM DEV_CITYBIKE_BRONZE.CITYBIKE.CITYBIKE_TRIPS_NY
WHERE ride_id LIKE 'DEMO_NY_%'
UNION ALL
SELECT 'JC DEMO', COUNT(*)
FROM DEV_CITYBIKE_BRONZE.CITYBIKE.CITYBIKE_TRIPS_JC
WHERE ride_id LIKE 'DEMO_JC_%';

SELECT station_id, observation_date, element, data_value, q_flag, load_ts
FROM DEV_CITYBIKE_BRONZE.NOAA.NOAA_RAW_YEAR
WHERE station_id='USW00094728' AND observation_date='20240101' AND element='TMAX';

-- ============================================================================
-- 2) Snapshot SCD2: dos versiones para Manhattan-2024-01-01-TMAX
-- ============================================================================
SELECT scd_key, data_value, q_flag, q_flag_category,
       dbt_valid_from, dbt_valid_to,
       CASE WHEN dbt_valid_to IS NULL THEN 'CURRENT' ELSE 'SUPERSEDED' END AS estado
FROM DEV_CITYBIKE_SILVER.SNAPSHOTS.SNP_NOAA__NOAA_RAW_YEAR
WHERE scd_key = 'USW00094728|20240101|TMAX'
ORDER BY dbt_valid_from;

-- ============================================================================
-- 3) Silver slv_trip: los 10 nuevos viajes
-- ============================================================================
SELECT ride_id, trip_date, trip_duration_min, city_id, rideable_type_code, user_type_code
FROM DEV_CITYBIKE_SILVER.DBT_RLIZARAN_CITYBIKE.SLV_TRIP
WHERE ride_id LIKE 'DEMO_%'
ORDER BY ride_id;

-- ============================================================================
-- 4) Silver slv_weather_observation: refleja el nuevo data_value
-- ============================================================================
SELECT wo.station_id, wo.observation_date, wo.element_code, wo.data_value, wo.q_flag, qf.q_flag_category
FROM DEV_CITYBIKE_SILVER.DBT_RLIZARAN_NOAA.SLV_WEATHER_OBSERVATION wo
LEFT JOIN DEV_CITYBIKE_SILVER.DBT_RLIZARAN_NOAA.SLV_QUALITY_FLAG qf
ON wo.q_flag = qf.q_flag 
WHERE wo.station_id='USW00094728' AND wo.observation_date='2024-01-01' AND wo.element_code='TMAX';

-- ============================================================================
-- 5) Gold fct_trips_daily: aggregados para 2026-01-15
-- ============================================================================
SELECT trip_date, city_id, rideable_type_code, user_type_code, series_key, n_trips, avg_duration_min
FROM DEV_CITYBIKE_GOLD.DBT_RLIZARAN_MOBILITY.FCT_TRIPS_DAILY
WHERE trip_date = '2026-01-15'
ORDER BY series_key;

-- ============================================================================
-- 6) Gold fct_trips_weather: rides + clima 2026-01-15
-- ============================================================================
SELECT trip_date, CITY_ID, STATION_WEATHER_ID,
       n_trips, n_trips_member, n_trips_casual, n_trips_classic, n_trips_electric,
       temp_max_c, temp_min_c, precipitation_mm, weather_category
FROM DEV_CITYBIKE_GOLD.DBT_RLIZARAN_ANALYTICS.FCT_TRIPS_WEATHER
WHERE trip_date = '2026-01-15'
ORDER BY CITY_ID;

-- ============================================================================
-- 7) Gold fct_noaa_corrections: la fila SCD2 corregida (2 versiones)
-- ============================================================================
SELECT scd_key, observation_date, element_code, data_value, q_flag,
       is_current, is_superseded
       dbt_valid_from, dbt_valid_to
FROM DEV_CITYBIKE_GOLD.DBT_RLIZARAN_CLIMA.FCT_NOAA_CORRECTIONS
WHERE scd_key = 'USW00094728|20240101|TMAX'
ORDER BY dbt_valid_from;

-- ============================================================================
-- 8) Resumen visible en Power BI (sanity check)
-- ============================================================================
SELECT
    'Total trips 2026-01-15'  AS metrica,
    COUNT(*)::varchar         AS valor
FROM DEV_CITYBIKE_GOLD.DBT_RLIZARAN_MOBILITY.FCT_TRIPS
WHERE trip_date = '2026-01-15'
UNION ALL
SELECT
    'Correcciones NOAA totales',
    COUNT(*)::varchar
FROM DEV_CITYBIKE_GOLD.DBT_RLIZARAN_CLIMA.FCT_NOAA_CORRECTIONS
WHERE is_superseded = TRUE;




-- Revertir NOAA (poner el data_value original; sustituir <VALOR_ORIGINAL>)
-- DELETE FROM DEV_CITYBIKE_BRONZE.CITYBIKE.CITYBIKE_TRIPS_NY WHERE ride_id LIKE 'DEMO_NY_%';
-- DELETE FROM DEV_CITYBIKE_BRONZE.CITYBIKE.CITYBIKE_TRIPS_JC WHERE ride_id LIKE 'DEMO_JC_%';
-- UPDATE DEV_CITYBIKE_BRONZE.NOAA.NOAA_RAW_YEAR
-- SET data_value= '83'
-- WHERE station_id='USW00094728' AND observation_date='20240101' AND element='TMAX';
-- DROP TABLE DEV_CITYBIKE_SILVER.SNAPSHOTS.SNP_NOAA__NOAA_RAW_YEAR;

select * from DEV_CITYBIKE_SILVER.DBT_RLIZARAN_CITYBIKE.SLV_STATION where station_id ilike '%DEM%';

select * from DEV_CITYBIKE_SILVER.DBT_RLIZARAN_CITYBIKE.SLV_TRIP where ride_id ilike 'DEMO_%';

select * from DEV_CITYBIKE_GOLD.DBT_RLIZARAN_MOBILITY.FCT_TRIPS where ride_id ilike 'DEMO_%';

select dt.trip_date, dt.n_trips, us.member_casual, 'DEV' as tabla from DEV_CITYBIKE_GOLD.DBT_RLIZARAN_MOBILITY.FCT_TRIPS_DAILY dt 
join DEV_CITYBIKE_GOLD.DBT_RLIZARAN_MOBILITY.DIM_USER_TYPE us on dt.user_type_code = us.user_type_code
where trip_date = '2026-04-28'
union all
select dtp.trip_date, dtp.n_trips, usp.member_casual, 'PRO' as tabla from PRO_CITYBIKE_GOLD.MOBILITY.FCT_TRIPS_DAILY dtp
join PRO_CITYBIKE_GOLD.MOBILITY.DIM_USER_TYPE usp on dtp.user_type_code = usp.user_type_code
where trip_date = '2026-04-28' order by n_trips;

select * from DEV_CITYBIKE_GOLD.DBT_RLIZARAN_ANALYTICS.FCT_TRIPS_WEATHER where trip_date ='2026-04-28'
union all
select * from PRO_CITYBIKE_GOLD.ANALYTICS.FCT_TRIPS_WEATHER where trip_date ='2026-04-28';

select 'PRO' as tabla, * from PRO_CITYBIKE_GOLD.ANALYTICS.FCT_TRIPS_WEATHER
where trip_date ='2026-04-28'
union all
select 'DEV' as tabla, * from DEV_CITYBIKE_GOLD.DBT_RLIZARAN_ANALYTICS.FCT_TRIPS_WEATHER
where trip_date ='2026-04-28'
order by n_trips;


