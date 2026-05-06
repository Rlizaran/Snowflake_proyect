-- Verificacion completa de la capa Bronze: existencia, datos, errores, streams y log
-- Organizado en 5 secciones 
-- corre cada bloque por separado segun lo que quieras revisar

USE ROLE ROLE_NYCBIKE;
USE WAREHOUSE WH_NYCBIKE_DEV;
USE DATABASE DB_CITYBIKE_BRONZE;

-- 1. EXISTENCIA: objetos creados en cada schema

-- Listar tablas, stages, file formats por schema
SHOW TABLES IN DATABASE DB_CITYBIKE_BRONZE;
SHOW STAGES IN DATABASE DB_CITYBIKE_BRONZE;
SHOW FILE FORMATS IN DATABASE DB_CITYBIKE_BRONZE;
SHOW PROCEDURES IN SCHEMA DB_CITYBIKE_BRONZE.CITYBIKE;
SHOW PROCEDURES IN SCHEMA DB_CITYBIKE_BRONZE.NOAA;
SHOW STREAMS IN SCHEMA DB_CITYBIKE_LOGS.LOGS;
SHOW TASKS IN SCHEMA DB_CITYBIKE_LOGS.LOGS;

-- Stages externos: deben listar archivos
LS @CITYBIKE.CITYBIKE_S3_STAGE;
LS @NOAA.NOAA_S3_STAGE_YEAR PATTERN = '.*202[4-6]\\.csv\\.gz';
LS @CITYBIKE.CITYBIKE_LANDING_STAGE;


-- 2. CONTEO Y CALIDAD: filas por tabla, archivo y dimensiones

-- Total por tabla
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

-- CityBike NY: filas por archivo
SELECT 
source_file,
COUNT(*) AS num_rows
FROM CITYBIKE.CITYBIKE_TRIPS_NY
GROUP BY source_file 
ORDER BY source_file;

-- CityBike JC: filas por archivo
SELECT 
source_file, 
COUNT(*) AS num_rows
FROM CITYBIKE.CITYBIKE_TRIPS_JC
GROUP BY source_file 
ORDER BY source_file;

-- Filas por mes y por ciudad (cruce NY vs JC)
SELECT 
COALESCE(C.yyyymm, Y.yyyymm) AS yyyymm,
Y.filas AS manhattan, C.filas AS jc,
(COALESCE(C.filas,0) + COALESCE(Y.filas,0)) AS total
FROM (
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

-- CityBike NY: distribucion por tipo de bici y tipo de usuario
SELECT 
rideable_type, 
COUNT(*) AS n
FROM CITYBIKE.CITYBIKE_TRIPS_NY
GROUP BY 1, 2 
ORDER BY 3 DESC;

WITH bronze_counts AS (
    SELECT 
        COUNT(*) AS n_bronze
    FROM CITYBIKE.CITYBIKE_TRIPS_JC
),
silver_counts AS (
    SELECT  
        COUNT(*) AS n_silver
    FROM DB_CITYBIKE_SILVER.DBT_RLIZARAN_CITYBIKE.STG_CITYBIKE__CITYBIKE_TRIPS_JC
)
SELECT 
    b.n_bronze,
    s.n_silver,
    (COALESCE(b.n_bronze, 0) - COALESCE(s.n_silver, 0)) AS diferencia_n
FROM bronze_counts b
FULL OUTER JOIN silver_counts s
ORDER BY ABS(diferencia_n) DESC;

-- NOAA: elementos meteorologicos por estacion (Manhattan + Newark/JC)
SELECT 
station_id, 
element,
COUNT(*) AS num_obs
FROM NOAA.NOAA_RAW_YEAR
WHERE station_id IN ('USW00094728', 'USW00014734')
GROUP BY 1, 2 
ORDER BY 1, 3 DESC;

-- NOAA: rango de fechas por estacion
SELECT 
station_id,
MIN(observation_date) AS min_date,
MAX(observation_date) AS max_date,
ROUND(COUNT(DISTINCT observation_date)/365, 1) AS num_years
FROM NOAA.NOAA_RAW_YEAR
WHERE station_id IN ('USW00094728', 'USW00014734')
GROUP BY 1;


-- 3. ERRORES DE COPY (ultimos 7 dias)

-- COPY history NY
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

-- COPY history JC
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

-- COPY history NOAA
SELECT table_name,
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

-- 4. STREAMS: data pendiente y contenido

-- Estado de los 4 streams (TRUE = data pendiente de consumir)
SELECT 'STM_CITYBIKE_NY' AS stream, 
SYSTEM$STREAM_HAS_DATA('DB_CITYBIKE_LOGS.LOGS.STM_CITYBIKE_NY') AS has_data
UNION ALL
SELECT 'STM_CITYBIKE_JC' , 
SYSTEM$STREAM_HAS_DATA('DB_CITYBIKE_LOGS.LOGS.STM_CITYBIKE_JC')
UNION ALL
SELECT 'STM_CITYBIKE_JC_STAGE', 
SYSTEM$STREAM_HAS_DATA('DB_CITYBIKE_LOGS.LOGS.STM_CITYBIKE_JC_STAGE')
UNION ALL
SELECT 'STM_NOAA_YEAR',
SYSTEM$STREAM_HAS_DATA('DB_CITYBIKE_LOGS.LOGS.STM_NOAA_YEAR');

-- Contenido del stream NY (acciones pendientes)
SELECT 
METADATA$ACTION,
METADATA$ISUPDATE,
COUNT(*) AS rows_pendientes
FROM DB_CITYBIKE_LOGS.LOGS.STM_CITYBIKE_NY
GROUP BY 1, 2;

-- Contenido del stream JC (tabla)
SELECT 
METADATA$ACTION,
METADATA$ISUPDATE,
COUNT(*) AS rows_pendientes
FROM DB_CITYBIKE_LOGS.LOGS.STM_CITYBIKE_JC
GROUP BY 1, 2;

-- Contenido del stage stream JC (archivos detectados)
SELECT 
METADATA$ACTION,
RELATIVE_PATH, 
SIZE
FROM DB_CITYBIKE_LOGS.LOGS.STM_CITYBIKE_JC_STAGE
ORDER BY METADATA$ACTION, RELATIVE_PATH;

-- Contenido del stream NOAA
SELECT 
METADATA$ACTION,
METADATA$ISUPDATE,
COUNT(*) AS rows_pendientes
FROM DB_CITYBIKE_LOGS.LOGS.STM_NOAA_YEAR
GROUP BY 1, 2;


-- 5. TASKS: estado actual e historial

-- Estado de los tasks (state = started si estan RESUME)
SHOW TASKS IN SCHEMA DB_CITYBIKE_LOGS.LOGS;

-- Historial de ejecuciones (ultimos 7 dias)
SELECT 
name, 
state, 
scheduled_time, 
completed_time, 
return_value, 
error_message
FROM TABLE(DB_CITYBIKE_LOGS.INFORMATION_SCHEMA.TASK_HISTORY(
            SCHEDULED_TIME_RANGE_START => DATEADD(day, -7, CURRENT_TIMESTAMP())))
WHERE database_name = 'DB_CITYBIKE_LOGS'
ORDER BY scheduled_time DESC
LIMIT 30;

-- Tasks que fallaron en los ultimos 7 dias
SELECT 
name, 
scheduled_time,
error_message
FROM TABLE(DB_CITYBIKE_LOGS.INFORMATION_SCHEMA.TASK_HISTORY(
            SCHEDULED_TIME_RANGE_START => DATEADD(day, -7, CURRENT_TIMESTAMP())))
WHERE state = 'FAILED'
ORDER BY scheduled_time DESC;

-- Log interno propio
SELECT * 
FROM DB_CITYBIKE_LOGS.LOGS.LOAD_LOG 
ORDER BY run_ts 
DESC LIMIT 30;

-- Solo errores en el log
SELECT * 
FROM DB_CITYBIKE_LOGS.LOGS.LOAD_LOG 
WHERE outcome = 'ERROR' 
ORDER BY run_ts 
DESC LIMIT 20;
