-- Creacion de FILE FORMATS y STAGES en DEV_CITYBIKE_BRONZE.BRONZE para cargar los datos crudos

USE ROLE ROLE_NYCBIKE;
USE WAREHOUSE WH_NYCBIKE_DEV;
USE DATABASE DEV_CITYBIKE_BRONZE;
USE SCHEMA   CITYBIKE;

-- File Format CSV para CityBike NY (header de 3 lineas, evita error UTF8)
CREATE OR REPLACE FILE FORMAT CITYBIKE.CITYBIKE_NY_CSV
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 3
  REPLACE_INVALID_CHARACTERS = TRUE
  NULL_IF = ('NULL', '\\N', '')
  COMPRESSION = AUTO;

-- Stage interno (landing) para JC: el script Python sube aqui los CSV mensuales
CREATE OR REPLACE STAGE DEV_CITYBIKE_BRONZE.CITYBIKE.CITYBIKE_LANDING_STAGE_NY
    FILE_FORMAT = CITYBIKE.CITYBIKE_JC_CSV
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Landing stage interno para <amhattan a partir del 202604 (Python PUT)'
    
-- File Format CSV para CityBike Jersey City (header simple)
CREATE OR REPLACE FILE FORMAT CITYBIKE.CITYBIKE_JC_CSV
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  REPLACE_INVALID_CHARACTERS = TRUE
  NULL_IF = ('NULL', '\\N', '')
  COMPRESSION = AUTO;

-- File Format CSV para NOAA (sin header)
CREATE OR REPLACE FILE FORMAT NOAA.NOAA_CSV
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 0
  REPLACE_INVALID_CHARACTERS = TRUE
  NULL_IF = ('NULL', '\\N', '')
  COMPRESSION = AUTO;

-- Stage externo apuntando al bucket publico de CityBike NYC
CREATE OR REPLACE STAGE DEV_CITYBIKE_BRONZE.CITYBIKE.CITYBIKE_S3_STAGE
    URL = 's3://tripdata'
    FILE_FORMAT = CITYBIKE.CITYBIKE_NY_CSV
    COMMENT = 'Bucket publico de CityBike NYC';

-- Stage interno (landing) para JC: el script Python sube aqui los CSV mensuales
CREATE OR REPLACE STAGE DEV_CITYBIKE_BRONZE.CITYBIKE.CITYBIKE_LANDING_STAGE
    FILE_FORMAT = CITYBIKE.CITYBIKE_JC_CSV
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Landing stage interno para Jersey City (Python PUT)';

-- Stage externo apuntando al bucket publico NOAA by station
CREATE OR REPLACE STAGE DEV_CITYBIKE_BRONZE.NOAA.NOAA_S3_STAGE_STATION
    URL = 's3://noaa-ghcn-pds/csv.gz/by_station/'
    FILE_FORMAT = NOAA.NOAA_CSV
    COMMENT = 'Bucket publico NOAA by station';

-- Stage externo apuntando al bucket publico NOAA by year
CREATE OR REPLACE STAGE DEV_CITYBIKE_BRONZE.NOAA.NOAA_S3_STAGE_YEAR
    URL = 's3://noaa-ghcn-pds/csv.gz/by_year/'
    FILE_FORMAT = NOAA.NOAA_CSV
    COMMENT = 'Bucket publico NOAA by year';


/*
NOAA_station solo tiene hasta la mitad de 2025, asi que solo se usara el NOAA_year

-- Comprobar que stage de NOAA es el menos pesado y cuantos anios tiene guardados
CREATE OR REPLACE TEMPORARY TABLE stage_size(
    name_stage    VARCHAR(256),
    size          VARCHAR(256),
    md5_          VARCHAR(256),
    last_modify   VARCHAR(256)
);
LS @DEV_CITYBIKE_BRONZE.BRONZE.NOAA_S3_STAGE_YEAR PATTERN = '.*(2024|2025|2026).csv.gz';
INSERT INTO stage_size
SELECT $1,$2,$3,$4
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
LS @DEV_CITYBIKE_BRONZE.BRONZE.NOAA_S3_STAGE_STATION PATTERN = '.*(USW00094728|USW00014734)\\.csv\\.gz';
INSERT INTO stage_size
SELECT $1,$2,$3,$4
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

SELECT
    CASE
        WHEN name_stage LIKE '%by_year%'    THEN 'BY_YEAR'
        WHEN name_stage LIKE '%by_station%' THEN 'BY_STATION'
    END AS stage,
    ROUND(SUM(size)) AS total_mb
FROM stage_size
GROUP BY stage
ORDER BY total_mb DESC;
*/
