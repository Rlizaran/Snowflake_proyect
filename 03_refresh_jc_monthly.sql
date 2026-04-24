-- Rutina mensual para traer nuevos archivos JC al Bronze.
-- Pasos: FETCH del repo -> correr Python localmente -> COPY INTO incremental.

USE ROLE ROLE_NYCBIKE;
USE WAREHOUSE WH_NYCBIKE_DEV;
USE DATABASE WH_NYCBIKE;
USE SCHEMA    bronze;


-- Refrescar el repo
ALTER GIT REPOSITORY WH_NYCBIKE.bronze.citibike_repo FETCH;

-- Ver qué JC hay actualmente en el stage
LS @WH_NYCBIKE.bronze.citibike_landing_stage;

-- Ejecutar el script de Python Eso sube los JC-YYYYMM nuevos al stage CITIBIKE_LANDING_STAGE.

-- COPY INTO incremental
COPY INTO bronze.citibike_trips_jc (
    ride_id, rideable_type, started_at, ended_at,
    start_station_name, start_station_id,
    end_station_name, end_station_id,
    start_lat, start_lng, end_lat, end_lng,
    member_casual, city,
    source_file
)
FROM (
    SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14
           SPLIT_PART(METADATA$FILENAME, '/', -1)
    FROM @WH_NYCBIKE.bronze.citibike_landing_stage
)
FILE_FORMAT = (FORMAT_NAME = 'WH_NYCBIKE.bronze.citibike_jc_csv')
PATTERN = '.*JC-202[4-9][0-9]{2}-citibike-tripdata\\.csv\\.gz'
ON_ERROR = 'CONTINUE'
FORCE = FALSE;

-- Verificar cuántas filas hay por mes
SELECT SUBSTR(source_file, 4, 6) AS yyyymm, COUNT(*) AS filas
FROM bronze.citibike_trips_jc
GROUP BY 1
ORDER BY 1;
