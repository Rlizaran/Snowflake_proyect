-- Streams + Tasks que orquestan la ingesta Bronze
-- Todos los streams y Task dentro del schema LOGS para que el DAG funcione correctamente
-- Conectar usuario, warehouse y database
USE ROLE ROLE_NYCBIKE;
USE WAREHOUSE WH_NYCBIKE_DEV;
USE DATABASE DB_CITYBIKE_LOGS;
USE SCHEMA LOGS;

-- Stream append-only sobre CityBike NYC
CREATE OR REPLACE STREAM STM_CITYBIKE_NY
    ON TABLE DB_CITYBIKE_BRONZE.CITYBIKE.CITYBIKE_TRIPS_NY APPEND_ONLY = TRUE;

-- Stream append-only sobre CityBike Jersey City (insert manual en demo)
CREATE OR REPLACE STREAM STM_CITYBIKE_JC
    ON TABLE DB_CITYBIKE_BRONZE.CITYBIKE.CITYBIKE_TRIPS_JC APPEND_ONLY = TRUE;

-- Stream sobre el stage interno de Jersey City (detecta archivos nuevos)
CREATE OR REPLACE STREAM STM_CITYBIKE_JC_STAGE
    ON STAGE DB_CITYBIKE_BRONZE.CITYBIKE.CITYBIKE_LANDING_STAGE;

-- Stream append-only sobre NOAA (consumo incremental por Silver)
CREATE OR REPLACE STREAM STM_NOAA_YEAR
    ON TABLE DB_CITYBIKE_BRONZE.NOAA.NOAA_RAW_YEAR APPEND_ONLY = TRUE;

/*
                CHAIN 1
*/
-- Task padre: refresh semanal de CityBike NY (domingos 03:00 NY)
CREATE OR REPLACE TASK TSK_BRONZE_CITYBIKE
    WAREHOUSE = WH_NYCBIKE_DEV
    SCHEDULE = 'USING CRON 0 3 * * 0 America/New_York'
AS CALL DB_CITYBIKE_BRONZE.CITYBIKE.LOAD_CITYBIKE_NY();

-- Task hijo: NOAA solo si los streams de CityBike traen filas nuevas
CREATE OR REPLACE TASK TSK_BRONZE_NOAA
    WAREHOUSE = WH_NYCBIKE_DEV
    AFTER TSK_BRONZE_CITYBIKE
    WHEN SYSTEM$STREAM_HAS_DATA('DB_CITYBIKE_LOGS.LOGS.STM_CITYBIKE_NY')
        OR SYSTEM$STREAM_HAS_DATA('DB_CITYBIKE_LOGS.LOGS.STM_CITYBIKE_JC')
AS CALL DB_CITYBIKE_BRONZE.NOAA.LOAD_NOAA_YEAR();

/*
                CHAIN 2
*/
-- Task padre : refresca la directory table para que el stream vea archivos nuevos
CREATE OR REPLACE TASK TSK_BRONZE_JC_REFRESH
    WAREHOUSE = WH_NYCBIKE_DEV
    SCHEDULE = 'USING CRON 0 17 1 * * America/New_York'
AS CALL DB_CITYBIKE_BRONZE.CITYBIKE.REFRESH_JC_STAGE();

-- Task hijo: corre solo si el stream detecto archivos nuevos tras el refresh
CREATE OR REPLACE TASK TSK_BRONZE_JC_ONFILES
    WAREHOUSE = WH_NYCBIKE_DEV
    AFTER TSK_BRONZE_JC_REFRESH
    WHEN SYSTEM$STREAM_HAS_DATA('DB_CITYBIKE_LOGS.LOGS.STM_CITYBIKE_JC_STAGE')
AS CALL DB_CITYBIKE_BRONZE.CITYBIKE.LOAD_CITYBIKE_JC();

-- Procedure: drena el stream de stage de JC para que el WHEN del proximo refresh evalue limpio
CREATE OR REPLACE PROCEDURE DRAIN_JC_STAGE_STREAM()
RETURNS STRING
LANGUAGE SQL
AS
BEGIN
    INSERT INTO LOAD_LOG (task_name, outcome, details)
    SELECT 'STM_CITYBIKE_JC_STAGE', 'DRAIN', 'archivos_consumidos=' || COUNT(*)
    FROM STM_CITYBIKE_JC_STAGE;

    RETURN 'Stage stream JC drenado';

EXCEPTION
    WHEN OTHER THEN
        INSERT INTO LOAD_LOG (task_name, outcome, details)
        VALUES ('DRAIN_JC_STAGE_STREAM', 'ERROR', :SQLERRM);
        RAISE;
END;

-- Task nieto: drena el stream tras el COPY (sin esto el WHEN seguiria TRUE)
CREATE OR REPLACE TASK TSK_BRONZE_JC_DRAIN
    WAREHOUSE = WH_NYCBIKE_DEV
    AFTER TSK_BRONZE_JC_ONFILES
AS CALL DRAIN_JC_STAGE_STREAM();
