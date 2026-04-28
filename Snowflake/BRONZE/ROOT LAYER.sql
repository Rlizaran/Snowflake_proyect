-- Conectar usuario, warehouse y database
USE ROLE ROLE_NYCBIKE;
USE WAREHOUSE WH_NYCBIKE_DEV;
USE DATABASE WH_NYCBIKE;

-- Usar Bronze SCHEMA
USE WH_NYCBIKE.BRONZE;

-- Validar que los datos estan en el stage
LS @WH_NYCBIKE.BRONZE.CITIBIKE_S3_STAGE;
LS @WH_NYCBIKE.BRONZE.NOAA_S3_STAGE_STATION PATTERN = '.*(USW00094728|USW00014734)\\.csv\\.gz';
-- Refrescar el repo y validar stage
ALTER GIT REPOSITORY WH_NYCBIKE.bronze.citibike_repo FETCH;
LS @WH_NYCBIKE.BRONZE.CITIBIKE_LANDING_STAGE;

-- Log de ejecuciones de procedures y tasks
CREATE OR REPLACE TABLE bronze.load_log (
    run_ts     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    task_name  VARCHAR(128),
    outcome    VARCHAR(32),
    details    VARCHAR(1024)
);

-- Tabla de NYC desde 2024 hasta 2026
CREATE OR REPLACE TABLE bronze.citibike_trips_ny (
    ride_id               VARCHAR(256),
    rideable_type         VARCHAR(256),
    started_at            VARCHAR(256),
    ended_at              VARCHAR(256),
    start_station_name    VARCHAR(256),
    start_station_id      VARCHAR(256),
    end_station_name      VARCHAR(256),
    end_station_id        VARCHAR(256),
    start_lat             VARCHAR(256),
    start_lng             VARCHAR(256),
    end_lat               VARCHAR(256),
    end_lng               VARCHAR(256),
    member_casual         VARCHAR(256),
    source_file           VARCHAR(256),
    load_ts               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Tabla de NYC desde 2024 hasta 2026
CREATE OR REPLACE TABLE bronze.citibike_trips_jc (
    ride_id               VARCHAR(256),
    rideable_type         VARCHAR(256),
    started_at            VARCHAR(256),
    ended_at              VARCHAR(256),
    start_station_name    VARCHAR(256),
    start_station_id      VARCHAR(256),
    end_station_name      VARCHAR(256),
    end_station_id        VARCHAR(256),
    start_lat             VARCHAR(256),
    start_lng             VARCHAR(256),
    end_lat               VARCHAR(256),
    end_lng               VARCHAR(256),
    member_casual         VARCHAR(256),
    source_file           VARCHAR(256),
    load_ts               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Table de NOAA BY YEAR desde 2024 hasta el 2026
CREATE OR REPLACE TABLE WH_NYCBIKE.BRONZE.NOAA_RAW_YEAR (
    station_id          VARCHAR(256),        
    observation_date    VARCHAR(256),         
    element             VARCHAR(256),        
    data_value          VARCHAR(256),        
    m_flag              VARCHAR(256),         
    q_flag              VARCHAR(256),        
    s_flag              VARCHAR(256),        
    obs_time            VARCHAR(256),         
    source_file         VARCHAR(256),
    load_ts             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Procedure carga incremental de CityBike NYC
-- Copiamos los datos desde 202401 hasta el 202603 de Manhattan.
CREATE OR REPLACE PROCEDURE BRONZE.LOAD_CITYBIKE_NY()
RETURNS STRING
LANGUAGE SQL
AS
BEGIN
    COPY INTO WH_NYCBIKE.BRONZE.CITIBIKE_TRIPS_NY (
        ride_id, 
        rideable_type, 
        started_at, 
        ended_at,
        start_station_name, 
        start_station_id,
        end_station_name, 
        end_station_id,
        start_lat, 
        start_lng, 
        end_lat, 
        end_lng,
        member_casual, 
        source_file,
        load_ts
    )
    FROM (
        SELECT 
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,
            SPLIT_PART(METADATA$FILENAME, '/', -1),
            CURRENT_TIMESTAMP()
        FROM @bronze.citibike_s3_stage
    )
    PATTERN = '2024[0-9]{2}-citibike-tripdata\\.zip|202[5-9][0-9]{2}-citibike-tripdata\\.zip'
    ON_ERROR = 'CONTINUE';
        -- Registrar resultado en el log interno
    INSERT INTO bronze.load_log (task_name, outcome, details)
    SELECT 'LOAD_CITYBIKE_NY', 'OK',
           'rows=' || COALESCE(SUM($1),0) || ' files=' || COUNT(*)
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID(-1)));

    RETURN 'Carga de datos a citybike_NY exitosa';

EXCEPTION
    -- Captura cualquier error y lo deja en el log
    WHEN OTHER THEN
        INSERT INTO bronze.load_log (task_name, outcome, details)
        VALUES ('LOAD_CITYBIKE_NY', 'ERROR', :SQLERRM);
        RAISE;
END;

-- Procedure carga incremental de CityBike JC
-- Copiamos los datos desde JC-202401 hasta el JC-202603 de Jersey City.
CREATE OR REPLACE PROCEDURE BRONZE.LOAD_CITYBIKE_JC()
RETURNS STRING
LANGUAGE SQL
AS
BEGIN
    COPY INTO bronze.citibike_trips_jc (
        ride_id, 
        rideable_type, 
        started_at, 
        ended_at,
        start_station_name, 
        start_station_id,
        end_station_name, 
        end_station_id,
        start_lat, 
        start_lng, 
        end_lat, 
        end_lng,
        member_casual,
        source_file,
        load_ts
    )
    FROM (
        SELECT 
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,
            SPLIT_PART(METADATA$FILENAME, '/', -1),
            CURRENT_TIMESTAMP()
        FROM @BRONZE.CITIBIKE_LANDING_STAGE
    )
    PATTERN = '.*JC-202[4-9][0-9]{2}-citibike-tripdata\\.csv\\.gz'
    ON_ERROR = 'CONTINUE';
        -- Registrar resultado en el log interno
    INSERT INTO bronze.load_log (task_name, outcome, details)
    SELECT 'LOAD_CITYBIKE_JC', 'OK',
           'rows=' || COALESCE(SUM($1),0) || ' files=' || COUNT(*)
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID(-1)));

    RETURN 'Carga de datos a citybike_JC exitosa';

EXCEPTION
    -- Captura cualquier error y lo deja en el log
    WHEN OTHER THEN
        INSERT INTO bronze.load_log (task_name, outcome, details)
        VALUES ('LOAD_CITYBIKE_JC', 'ERROR', :SQLERRM);
        RAISE;
END;

-- Procedure carga incremental de NOAA
-- Cargamos los 3 años completos, luego filtramos por station en la capa silver para la estacion de NY y JC
CREATE OR REPLACE PROCEDURE BRONZE.LOAD_NOAA_YEAR()
RETURNS STRING
LANGUAGE SQL
AS
BEGIN
    COPY INTO WH_NYCBIKE.BRONZE.NOAA_RAW_YEAR (
        station_id,        
        observation_date,         
        element,        
        data_value,        
        m_flag,         
        q_flag,        
        s_flag,        
        obs_time,       
        source_file,
        load_ts
    )
    FROM(
        SELECT
            $1,$2,$3,$4,$5,$6,$7,$8,
            SPLIT_PART(METADATA$FILENAME, '/', -1),
            CURRENT_TIMESTAMP()
        FROM @WH_NYCBIKE.BRONZE.NOAA_S3_STAGE_YEAR
    )
    PATTERN = '.*202[4-6]\\.csv\\.gz'
    ON_ERROR = 'CONTINUE';
       -- Registrar resultado en el log interno
    INSERT INTO bronze.load_log (task_name, outcome, details)
    SELECT 'LOAD_NOAA_YEAR', 'OK',
           'rows=' || COALESCE(SUM($1),0) || ' files=' || COUNT(*)
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID(-1)));

    RETURN 'Carga de datos a NOAA YEAR exitosa';

EXCEPTION
    -- Captura cualquier error y lo deja en el log
    WHEN OTHER THEN
        INSERT INTO bronze.load_log (task_name, outcome, details)
        VALUES ('LOAD_NOAA_YEAR', 'ERROR', :SQLERRM);
        RAISE;
END;


-- Refrescar la directory table del stage interno (run antes del task de JC)
CREATE OR REPLACE PROCEDURE BRONZE.REFRESH_JC_STAGE()
RETURNS STRING
LANGUAGE SQL
AS
BEGIN
    -- Refresco para que el stream detecte archivos nuevos del script Python
    ALTER STAGE bronze.citibike_landing_stage REFRESH;
    INSERT INTO bronze.load_log (task_name, outcome, details)
    SELECT 'REFRESH CITYBIKE_JC STAGE', 'OK',
           'rows=' || COALESCE(SUM($1),0) || ' files=' || COUNT(*)
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID(-1)));
    RETURN 'JC landing stage refrescado';
END;
