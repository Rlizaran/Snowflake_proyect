-- Checks de ingesta Bronze: filas por archivo, rangos, distribuciones y log
USE ROLE ROLE_NYCBIKE;
USE WAREHOUSE WH_NYCBIKE_DEV;
USE DATABASE DB_CITYBIKE_BRONZE;

-- Validar que los datos estan en los stages externos
LS @DB_CITYBIKE_BRONZE.CITYBIKE.CITYBIKE_S3_STAGE;
LS @DB_CITYBIKE_BRONZE.NOAA.NOAA_S3_STAGE_STATION PATTERN = '.*(USW00094728|USW00014734)\\.csv\\.gz';

-- Refrescar el repo de GitHub y validar el landing stage interno
ALTER GIT REPOSITORY DB_CITYBIKE_BRONZE.CITYBIKE.CITIBIKE_REPO FETCH;
LS @DB_CITYBIKE_BRONZE.CITYBIKE.CITYBIKE_LANDING_STAGE;



-- CityBike NY: filas por archivo
SELECT  
source_file,
COUNT(*) AS num_rows
FROM CITYBIKE.CITYBIKE_TRIPS_NY
GROUP BY source_file
ORDER BY source_file;

-- CityBike NY: rango de fechas crudas y variedad de tipos
SELECT DISTINCT
MIN(started_at) AS min_start_raw,
MAX(started_at) AS max_start_raw,
rideable_type AS distinct_bike_types,
member_casual AS distinct_user_types
FROM CITYBIKE.CITYBIKE_TRIPS_NY
GROUP BY member_casual, rideable_type;

-- CityBike NY: distribucion por tipo de bicicleta
SELECT  
rideable_type,
COUNT(*) AS n
FROM CITYBIKE.CITYBIKE_TRIPS_NY
GROUP BY 1
ORDER BY 2 DESC;

-- CityBike JC: filas por archivo
SELECT  
source_file,
COUNT(*) AS num_rows
FROM CITYBIKE.CITYBIKE_TRIPS_JC
GROUP BY source_file
ORDER BY source_file;

-- CityBike JC: distribucion por tipo de bicicleta
SELECT  
rideable_type,
COUNT(*) AS n
FROM CITYBIKE.CITYBIKE_TRIPS_JC
GROUP BY 1
ORDER BY 2 DESC;

-- NOAA: elementos meteorologicos disponibles (PRCP, TMAX, TMIN, SNOW...)
SELECT 
element,
COUNT(*) AS num_obs
FROM NOAA.NOAA_RAW_YEAR
WHERE station_id IN ('USW00094728', 'USW00014734')
GROUP BY 1
ORDER BY 2 DESC;

-- NOAA: rango de fechas por estacion
SELECT  
station_id,
MIN(observation_date) AS min_date,
MAX(observation_date) AS max_date,
ROUND(COUNT(DISTINCT observation_date)/365, 1) AS num_years
FROM NOAA.NOAA_RAW_YEAR
WHERE station_id IN ('USW00094728', 'USW00014734')
GROUP BY 1;

-- Errores de COPY INTO CityBike NY (ultimos 7 dias)
SELECT 
table_name,
file_name,
status,
row_count,
error_count, 
first_error_message, 
last_load_time
FROM TABLE(DB_CITYBIKE_BRONZE.INFORMATION_SCHEMA.COPY_HISTORY(
            TABLE_NAME => 'CITYBIKE.CITYBIKE_TRIPS_NY',
            START_TIME => DATEADD(day, -7, CURRENT_TIMESTAMP())))
ORDER BY last_load_time DESC;

-- Errores de COPY INTO CityBike JC (ultimos 7 dias)
SELECT  
table_name,
file_name,
status,
row_count,
error_count, 
first_error_message, 
last_load_time
FROM TABLE(DB_CITYBIKE_BRONZE.INFORMATION_SCHEMA.COPY_HISTORY(
            TABLE_NAME => 'CITYBIKE.CITYBIKE_TRIPS_JC',
            START_TIME => DATEADD(day, -7, CURRENT_TIMESTAMP())))
ORDER BY last_load_time DESC;

-- Errores de COPY INTO NOAA (ultimos 7 dias)
SELECT
table_name,
file_name,
status,
row_count,
error_count, 
first_error_message, 
last_load_time
FROM TABLE(DB_CITYBIKE_BRONZE.INFORMATION_SCHEMA.COPY_HISTORY(
            TABLE_NAME => 'NOAA.NOAA_RAW_YEAR',
            START_TIME => DATEADD(day, -7, CURRENT_TIMESTAMP())))
ORDER BY last_load_time DESC;

-- Sanity check global: filas totales por tabla Bronze
SELECT 'citybike_trips_ny' AS tabla, 
COUNT(*) AS filas 
FROM CITYBIKE.CITYBIKE_TRIPS_NY
UNION ALL
SELECT 'citybike_trips_jc' AS tabla,
COUNT(*) AS filas 
FROM CITYBIKE.CITYBIKE_TRIPS_JC
UNION ALL
SELECT 'noaa_raw_year' AS tabla,
COUNT(*) AS filas 
FROM NOAA.NOAA_RAW_YEAR;

-- Estado actual de los streams (data pendiente de consumir)
SELECT 'STM_CITYBIKE_NY' AS stream, 
SYSTEM$STREAM_HAS_DATA('DB_CITYBIKE_BRONZE.LOGS.STM_CITYBIKE_NY') AS has_data
UNION ALL
SELECT 'STM_CITYBIKE_JC' AS stream, 
SYSTEM$STREAM_HAS_DATA('DB_CITYBIKE_BRONZE.LOGS.STM_CITYBIKE_JC') AS has_data
UNION ALL
SELECT 'STM_CITYBIKE_JC_STAGE' AS stream, SYSTEM$STREAM_HAS_DATA('DB_CITYBIKE_BRONZE.LOGS.STM_CITYBIKE_JC_STAGE') AS has_data
UNION ALL
SELECT 'STM_NOAA_YEAR' AS stream, 
SYSTEM$STREAM_HAS_DATA('DB_CITYBIKE_BRONZE.LOGS.STM_NOAA_YEAR') AS has_data;

-- Log interno: ultimas ejecuciones de procedures / tasks
SELECT * 
FROM LOGS.LOAD_LOG 
ORDER BY run_ts DESC 
LIMIT 20;

-- Verificar cuantas filas hay por mes y por ciudad
SELECT
COALESCE(C.yyyymm, Y.yyyymm) AS yyyymm,
C.filas AS jc,
Y.filas AS manhattan,
(COALESCE(C.filas,0) + COALESCE(Y.filas,0)) AS total
FROM(
    SELECT 
    SUBSTR(source_file, 4, 6) AS yyyymm,
    COUNT(*) AS filas
    FROM CITYBIKE.CITYBIKE_TRIPS_JC
    GROUP BY yyyymm
    ) C
FULL JOIN (
    SELECT 
        SUBSTR(source_file, 1, 6) AS yyyymm,
        COUNT(*) AS filas
    FROM CITYBIKE.CITYBIKE_TRIPS_NY
    GROUP BY yyyymm
) Y
ON C.yyyymm = Y.yyyymm
ORDER BY yyyymm;

-- Conteo total de filas
WITH counts AS (
    SELECT 
        'citybike_trips_ny' AS tabla, 
        COUNT(*) AS filas
    FROM CITYBIKE.CITYBIKE_TRIPS_NY
    UNION ALL
    SELECT 
        'citybike_trips_jc' AS tabla,
        COUNT(*) AS filas 
    FROM CITYBIKE.CITYBIKE_TRIPS_JC
    UNION ALL
    SELECT 
        'noaa_raw_year' AS tabla,
        COUNT(*) AS filas 
    FROM NOAA.NOAA_RAW_YEAR
)
SELECT 
tabla, 
filas 
FROM counts
UNION ALL
SELECT 
'Total' AS tabla,
SUM(filas) 
FROM counts;


CALL DB_CITYBIKE_BRONZE.CITYBIKE.LOAD_CITYBIKE_NY();
CALL DB_CITYBIKE_BRONZE.CITYBIKE.LOAD_CITYBIKE_JC();