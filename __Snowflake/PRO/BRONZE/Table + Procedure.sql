USE ROLE ROLE_NYCBIKE;
USE WAREHOUSE WH_NYCBIKE_DEV;
USE DATABASE PRO_CITYBIKE_BRONZE;


CREATE OR REPLACE TABLE DB_CITYBIKE_LOGS.PRO.LOAD_LOG (
    run_ts     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    task_name  VARCHAR(128),
    outcome    VARCHAR(32),
    details    VARCHAR(1024)
);

-- Tabla raw de viajes CityBike NYC (todo VARCHAR para preservar el dato original)
CREATE OR REPLACE TABLE PRO_CITYBIKE_BRONZE.CITYBIKE.CITYBIKE_TRIPS_NY (
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

-- Tabla raw de viajes CityBike Jersey City
CREATE OR REPLACE TABLE PRO_CITYBIKE_BRONZE.CITYBIKE.CITYBIKE_TRIPS_JC(
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

-- Tabla raw NOAA by year (3 anios completos, se filtra por estacion en Silver)
CREATE OR REPLACE TABLE PRO_CITYBIKE_BRONZE.NOAA.NOAA_RAW_YEAR (
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

-- Procedure: carga incremental de CityBike NYC desde el bucket publico (2024 -> 2026+)
CREATE OR REPLACE PROCEDURE PRO_CITYBIKE_BRONZE.CITYBIKE.LOAD_CITYBIKE_NY()
RETURNS STRING
LANGUAGE SQL
AS
DECLARE
    v_rows  NUMBER := 0;
    v_files NUMBER := 0;
    v_qid   VARCHAR;
    v_zero  NUMBER := 0;
BEGIN
    
    COPY INTO PRO_CITYBIKE_BRONZE.CITYBIKE.CITYBIKE_TRIPS_NY (
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
        FROM @PRO_CITYBIKE_BRONZE.CITYBIKE.CITYBIKE_S3_STAGE
    )
    PATTERN = '202[4-9][0-9]{2}-citibike-tripdata\\.zip'
    ON_ERROR = 'CONTINUE';

    -- Captura el query id del COPY para no perderlo con queries siguientes
    v_qid := LAST_QUERY_ID(-1);

    -- Detecta caso "0 files processed" donde RESULT_SCAN solo tiene columna status ($1)
    SELECT COUNT(*) INTO :v_zero
    FROM   TABLE(RESULT_SCAN(:v_qid))
    WHERE  $1 LIKE 'Copy executed with 0 files%';

    -- Solo lee $3 (rows_parsed) si hubo archivos cargados, evita "invalid identifier"
    IF (v_zero = 0) THEN
        SELECT COALESCE(SUM(TRY_CAST($3 AS NUMBER)), 0), COUNT(*)
        INTO   :v_rows, :v_files
        FROM   TABLE(RESULT_SCAN(:v_qid))
        WHERE  TRY_CAST($3 AS NUMBER) IS NOT NULL;
    END IF;

    INSERT INTO DB_CITYBIKE_LOGS.PRO.LOAD_LOG (task_name, outcome, details)
    VALUES ('PRO_LOAD_CITYBIKE_NY', 'OK', 'rows=' || :v_rows || ' files=' || :v_files);

    RETURN 'Carga de datos a citybike_NY exitosa';

EXCEPTION
    WHEN OTHER THEN
        INSERT INTO DB_CITYBIKE_LOGS.PRO.LOAD_LOG (task_name, outcome, details)
        VALUES ('PRO_LOAD_CITYBIKE_NY', 'ERROR', :SQLERRM);
        RAISE;
END;

-- Procedure: carga incremental de CityBike Jersey City desde el landing stage interno
CREATE OR REPLACE PROCEDURE PRO_CITYBIKE_BRONZE.CITYBIKE.LOAD_CITYBIKE_JC()
RETURNS STRING
LANGUAGE SQL
AS
DECLARE
    v_rows  NUMBER := 0;
    v_files NUMBER := 0;
    v_qid   VARCHAR;
    v_zero  NUMBER := 0;
BEGIN
    COPY INTO PRO_CITYBIKE_BRONZE.CITYBIKE.CITYBIKE_TRIPS_JC (
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
        FROM @PRO_CITYBIKE_BRONZE.CITYBIKE.CITYBIKE_LANDING_STAGE
    )
    PATTERN = '.*JC-202[4-9][0-9]{2}-citibike-tripdata\\.csv\\.gz'
    ON_ERROR = 'CONTINUE';

    -- Captura el query id del COPY para no perderlo con queries siguientes
    v_qid := LAST_QUERY_ID(-1);

    -- Detecta caso "0 files processed" donde RESULT_SCAN solo tiene columna status ($1)
    SELECT COUNT(*) INTO :v_zero
    FROM   TABLE(RESULT_SCAN(:v_qid))
    WHERE  $1 LIKE 'Copy executed with 0 files%';

    -- Solo lee $3 (rows_parsed) si hubo archivos cargados, evita "invalid identifier"
    IF (v_zero = 0) THEN
        SELECT COALESCE(SUM(TRY_CAST($3 AS NUMBER)), 0), COUNT(*)
        INTO   :v_rows, :v_files
        FROM   TABLE(RESULT_SCAN(:v_qid))
        WHERE  TRY_CAST($3 AS NUMBER) IS NOT NULL;
    END IF;

    INSERT INTO DB_CITYBIKE_LOGS.PRO.LOAD_LOG (task_name, outcome, details)
    VALUES ('LOAD_CITYBIKE_JC', 'OK', 'rows=' || :v_rows || ' files=' || :v_files);

    RETURN 'Carga de datos a citybike_JC exitosa';

EXCEPTION
    WHEN OTHER THEN
        INSERT INTO DB_CITYBIKE_LOGS.PRO.LOAD_LOG (task_name, outcome, details)
        VALUES ('LOAD_CITYBIKE_JC', 'ERROR', :SQLERRM);
        RAISE;
END;


-- Procedure: carga incremental de NOAA by year (3 anios completos)
CREATE OR REPLACE PROCEDURE PRO_CITYBIKE_BRONZE.NOAA.LOAD_NOAA_YEAR()
RETURNS STRING
LANGUAGE SQL
AS
DECLARE
    v_rows  NUMBER := 0;
    v_files NUMBER := 0;
    v_qid   VARCHAR;
    v_zero  NUMBER := 0;
BEGIN
    COPY INTO PRO_CITYBIKE_BRONZE.NOAA.NOAA_RAW_YEAR (
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
    FROM (
        SELECT
            $1,$2,$3,$4,$5,$6,$7,$8,
            SPLIT_PART(METADATA$FILENAME, '/', -1),
            CURRENT_TIMESTAMP()
        FROM @PRO_CITYBIKE_BRONZE.NOAA.NOAA_S3_STAGE_YEAR
    )
    PATTERN = '.*202[4-6]\\.csv\\.gz'
    ON_ERROR = 'CONTINUE';

    -- Captura el query id del COPY para no perderlo con queries siguientes
    v_qid := LAST_QUERY_ID(-1);

    -- Detecta caso "0 files processed" donde RESULT_SCAN solo tiene columna status ($1)
    SELECT COUNT(*) INTO :v_zero
    FROM   TABLE(RESULT_SCAN(:v_qid))
    WHERE  $1 LIKE 'Copy executed with 0 files%';

    -- Solo lee $3 (rows_parsed) si hubo archivos cargados, evita "invalid identifier"
    IF (v_zero = 0) THEN
        SELECT COALESCE(SUM(TRY_CAST($3 AS NUMBER)), 0), COUNT(*)
        INTO   :v_rows, :v_files
        FROM   TABLE(RESULT_SCAN(:v_qid))
        WHERE  TRY_CAST($3 AS NUMBER) IS NOT NULL;
    END IF;

    INSERT INTO DB_CITYBIKE_LOGS.PRO.LOAD_LOG (task_name, outcome, details)
    VALUES ('LOAD_NOAA_RAW_YEAR()', 'OK', 'rows=' || :v_rows || ' files=' || :v_files);

    RETURN 'Carga de datos a NOAA_RAW_YEAR exitosa';

EXCEPTION
    WHEN OTHER THEN
        INSERT INTO DB_CITYBIKE_LOGS.PRO.LOAD_LOG (task_name, outcome, details)
        VALUES ('NOAA_RAW_YEAR', 'ERROR', :SQLERRM);
        RAISE;
END;

-- Procedure: refresca la directory table del stage interno antes del task de JC
CREATE OR REPLACE PROCEDURE DB_CITYBIKE_LOGS.PRO.REFRESH_JC_STAGE()
RETURNS STRING
LANGUAGE SQL
AS
DECLARE
    v_rows  NUMBER := 0;
    v_files NUMBER := 0;
BEGIN
    ALTER STAGE PRO_CITYBIKE_BRONZE.CITYBIKE.CITYBIKE_LANDING_STAGE REFRESH;
    -- ALTER STAGE no genera RESULT_SCAN utilizable; se loguea directamente
    INSERT INTO DB_CITYBIKE_LOGS.PRO.LOAD_LOG (task_name, outcome, details)
    VALUES ('REFRESH CITYBIKE_JC STAGE', 'OK', 'Stage refrescado correctamente');

    RETURN 'JC landing stage refrescado';

EXCEPTION
    WHEN OTHER THEN
        INSERT INTO DB_CITYBIKE_LOGS.PRO.LOAD_LOG (task_name, outcome, details)
        VALUES ('REFRESH CITYBIKE_JC STAGE', 'ERROR', :SQLERRM);
        RAISE;
END;







