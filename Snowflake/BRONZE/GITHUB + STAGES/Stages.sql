/*
Creacion de FILE FORMATS y STAGES para poder cargar los datos en nuestras tablas
*/

USE ROLE ROLE_NYCBIKE;
USE WAREHOUSE WH_NYCBIKE_DEV;
USE DATABASE WH_NYCBIKE;
USE SCHEMA    bronze;

-- Crear File Format para evadir error de UTF8
CREATE OR REPLACE FILE FORMAT bronze.citibike_ny_csv
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 3
  REPLACE_INVALID_CHARACTERS = TRUE
  NULL_IF = ('NULL', '\\N', '')
  COMPRESSION = AUTO;

CREATE OR REPLACE FILE FORMAT bronze.citibike_jc_csv
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  REPLACE_INVALID_CHARACTERS = TRUE
  NULL_IF = ('NULL', '\\N', '')
  COMPRESSION = AUTO;

-- Stage externo apuntando al bucket publico de CityBike NYC
CREATE OR REPLACE STAGE WH_NYCBIKE.BRONZE.CITIBIKE_S3_STAGE
    URL = 's3://tripdata'
    FILE_FORMAT = bronze.citibike_ny_csv
    COMMENT = 'Bucket publico de CityBike NYC';

-- Ejecutar el script de Python Eso sube los JC-YYYYMM nuevos al stage interno CITIBIKE_LANDING_STAGE.
CREATE OR REPLACE STAGE WH_NYCBIKE.BRONZE.CITIBIKE_LANDING_STAGE
    FILE_FORMAT = bronze.citibike_jc_csv
    COMMENT = 'Landing stage interno para Github';

-- STAGE externo al bucket publico de NOAA by station
CREATE OR REPLACE STAGE bronze.noaa_s3_stage_station
    URL = 's3://noaa-ghcn-pds/csv.gz/by_station/'
    FILE_FORMAT = bronze.citibike_jc_csv
    COMMENT = 'Bucket publico NOAA by station';

-- STAGE externo al bucket publico de NOAA by year
CREATE OR REPLACE STAGE bronze.noaa_s3_stage_year
    URL = 's3://noaa-ghcn-pds/csv.gz/by_year/'
    FILE_FORMAT = bronze.citibike_jc_csv
    COMMENT = 'Bucket publico NOAA by year';





--Comprobar que stage de NOAA es el menos pesado para cargar en nuestra tabla y guardar dichos datos
CREATE OR REPLACE TEMPORARY TABLE stage_size(
    name_stage            VARCHAR(256),
    size                  VARCHAR(256),
    md5_                  VARCHAR(256),
    last_modify           VARCHAR(256)
);
LS @WH_NYCBIKE.BRONZE.NOAA_S3_STAGE_YEAR PATTERN = '.*(2024|2025|2026).csv.gz';
INSERT INTO stage_size
SELECT         
    $1,$2,$3,$4
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
LS @WH_NYCBIKE.BRONZE.NOAA_S3_STAGE_STATION PATTERN = '.*(USW00094728|USW00014734)\\.csv\\.gz';
INSERT INTO stage_size
SELECT         
    $1,$2,$3,$4
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

SELECT     
    CASE 
        WHEN name_stage LIKE '%by_year%'    THEN 'BY_YEAR'
        WHEN name_stage LIKE '%by_station%' THEN 'BY_STATION'
    END                                         AS stage,
    ROUND(SUM(size)) AS total_mb
FROM stage_size
GROUP BY stage
ORDER BY total_mb DESC;
