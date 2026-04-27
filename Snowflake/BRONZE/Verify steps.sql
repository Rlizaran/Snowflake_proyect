-- Conectar usuario, warehouse y database
USE ROLE ROLE_NYCBIKE;
USE WAREHOUSE WH_NYCBIKE_DEV;
USE DATABASE WH_NYCBIKE;
-- Usar Bronze SCHEMA
USE WH_NYCBIKE.BRONZE;

-- CityBike NY filas por archivo
SELECT  source_file, COUNT(*) AS num_rows
FROM    bronze.citibike_trips_ny
GROUP BY source_file
ORDER BY source_file;


-- CityBike NY rango de fechas crudas y variedad de tipos
SELECT  DISTINCT
        MIN(STARTED_AT) AS min_start_raw,
        MAX(started_at) AS max_start_raw,
        rideable_type AS distinct_bike_types,
        member_casual AS distinct_user_types
FROM    bronze.citibike_trips_ny
GROUP BY MEMBER_CASUAL, RIDEABLE_TYPE;

-- CityBike NY distribucion por tipo de bicicleta
SELECT  
    rideable_type, 
    COUNT(*) AS n
FROM    bronze.citibike_trips_ny
GROUP BY 1
ORDER BY 2 DESC;

-- CityBike JC filas por archivo
SELECT  
source_file, 
COUNT(*) AS num_rows
FROM    bronze.citibike_trips_jc
GROUP BY source_file
ORDER BY source_file;

-- CityBike JC distribucion por tipo de bicicleta
SELECT  
rideable_type, 
COUNT(*) AS n
FROM    bronze.citibike_trips_jc
GROUP BY 1
ORDER BY 2 DESC;

/*
-- NOAA: observaciones por estacion y año
SELECT  
    station_id,
    LEFT(observation_date, 4)   AS yyyy,
    COUNT(*)                    AS num_obs
FROM WH_NYCBIKE.BRONZE.NOAA_RAW_STATION
WHERE LEFT(observation_date, 4) IN ('2024', '2025', '2026')
GROUP BY 1, 2
UNION ALL
SELECT  
    station_id,
    LEFT(observation_date, 4)   AS yyyy,
    COUNT(*)                    AS num_obs
FROM WH_NYCBIKE.BRONZE.NOAA_RAW_YEAR
WHERE LEFT(observation_date, 4) BETWEEN '2024' AND '2026'
  AND station_id IN ('USW00094728', 'USW00094789', 'USW00014734')
GROUP BY 1, 2
ORDER BY 1, 2;
*/

-- NOAA elementos meteorologicos disponibles (PRCP, TMAX, TMIN, SNOW...)
SELECT 
element, 
COUNT(*) AS num_obs,
FROM WH_NYCBIKE.BRONZE.NOAA_RAW_YEAR
WHERE station_id IN ('USW00094728', 'USW00094789', 'USW00014734')
GROUP BY 1
ORDER BY 2 DESC;

-- NOAA: rango de fechas por estacion
SELECT  
station_id, 
MIN(observation_date) AS min_date, 
MAX(observation_date) AS max_date,
ROUND(COUNT(DISTINCT observation_date)/365,1) AS num_years
FROM WH_NYCBIKE.BRONZE.NOAA_RAW_YEAR
WHERE station_id IN ('USW00094728', 'USW00094789', 'USW00014734')
GROUP BY 1;

-- Errores de COPY INTO CityBike NY en los ultimos 7 dias
SELECT  
table_name, 
file_name, 
status, 
row_count, 
row_parsed,
error_count,
first_error_message, 
last_load_time
FROM TABLE(WH_NYCBIKE.INFORMATION_SCHEMA.COPY_HISTORY(
            TABLE_NAME => 'BRONZE.CITIBIKE_TRIPS_NY',
            START_TIME => DATEADD(day, -7, CURRENT_TIMESTAMP())))
ORDER BY last_load_time DESC;

-- Errores de COPY INTO CityBike JC en los ultimos 7 dias
SELECT  
table_name, 
file_name, 
status, 
row_count, 
error_count,
first_error_message, 
last_load_time
FROM TABLE(WH_NYCBIKE.INFORMATION_SCHEMA.COPY_HISTORY(
            TABLE_NAME => 'BRONZE.CITIBIKE_TRIPS_JC',
            START_TIME => DATEADD(day, -7, CURRENT_TIMESTAMP())))
ORDER BY last_load_time DESC;

-- Errores de COPY INTO NOAA en los ultimos 7 dias
SELECT  
table_name, 
file_name, 
status, 
row_count, 
error_count,
first_error_message, 
last_load_time
FROM TABLE(WH_NYCBIKE.INFORMATION_SCHEMA.COPY_HISTORY(
            TABLE_NAME => 'BRONZE.NOAA_RAW_YEAR',
            START_TIME => DATEADD(day, -7, CURRENT_TIMESTAMP())))
ORDER BY last_load_time DESC;

-- Sanity check global: filas totales por tabla Bronze
SELECT 'citibike_trips_ny' AS tabla, COUNT(*) AS filas FROM bronze.citibike_trips_ny
UNION ALL
SELECT 'citibike_trips_jc' AS tabla, COUNT(*) AS filas FROM bronze.citibike_trips_jc
UNION ALL
SELECT 'NOAA_RAW_YEAR'  AS tabla, COUNT(*) AS filas FROM NOAA_RAW_YEAR;


-- Log interno: ultimas ejecuciones de procedures / tasks
SELECT * FROM bronze.load_log ORDER BY run_ts DESC LIMIT 20;





-- Verificar cuántas filas hay por mes y por ciudad
SELECT 
    COALESCE(C.yyyymm, Y.yyyymm) AS yyyymm,
    C.filas AS JC,
    Y.filas AS MANHATTAN,
    (C.filas + Y.filas) AS total
FROM (
    SELECT SUBSTR(source_file, 4, 6) AS yyyymm, 
    COUNT(*) AS filas
    FROM WH_NYCBIKE.BRONZE.CITIBIKE_TRIPS_JC
    GROUP BY yyyymm
) C
FULL JOIN (
    SELECT SUBSTR(source_file, 1, 6) AS yyyymm, 
    COUNT(*) AS filas
    FROM WH_NYCBIKE.BRONZE.CITIBIKE_TRIPS_NY
    GROUP BY yyyymm
) Y
ON C.yyyymm = Y.yyyymm
ORDER BY yyyymm;

-- Conteo total de filas
WITH counts AS (
    SELECT 'citibike_trips_ny' AS tabla, COUNT(*) AS filas 
    FROM bronze.citibike_trips_ny
    UNION ALL
    SELECT 'citibike_trips_jc' AS tabla, COUNT(*) AS filas 
    FROM bronze.citibike_trips_jc
    UNION ALL
    SELECT 'NOAA_RAW_YEAR' AS tabla, COUNT(*) AS filas
    FROM bronze.noaa_raw_year
)
SELECT tabla, filas FROM counts
UNION ALL
SELECT 'Total' AS tabla, SUM(filas) FROM counts;