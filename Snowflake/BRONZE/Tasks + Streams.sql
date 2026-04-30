-- Streams + Tasks que orquestan la ingesta Bronze (DB_CITYBIKE_BRONZE.BRONZE)

-- Conectar usuario, warehouse y database
USE ROLE ROLE_NYCBIKE;
USE WAREHOUSE WH_NYCBIKE_DEV;
USE DATABASE DB_CITYBIKE_BRONZE;
USE SCHEMA   BRONZE;

-- Stream append-only sobre CityBike NYC (consumido por Silver dbt)
CREATE OR REPLACE STREAM BRONZE.STM_CITIBIKE_NY
    ON TABLE BRONZE.CITIBIKE_TRIPS_NY APPEND_ONLY = TRUE;

-- Stream sobre el stage interno de Jersey City (detecta archivos nuevos)
CREATE OR REPLACE STREAM BRONZE.STM_CITIBIKE_JC
    ON STAGE BRONZE.CITIBIKE_LANDING_STAGE;

-- Stream append-only sobre NOAA (consumo incremental por Silver)
CREATE OR REPLACE STREAM BRONZE.STM_NOAA_YEAR
    ON TABLE BRONZE.NOAA_RAW_YEAR APPEND_ONLY = TRUE;

-- Task padre: refresh semanal de CityBike NY (domingos 03:00 NY)
CREATE OR REPLACE TASK BRONZE.TSK_BRONZE_CITYBIKE
    WAREHOUSE = WH_NYCBIKE_DEV
    SCHEDULE = 'USING CRON 0 3 * * 0 America/New_York'
AS
BEGIN
    CALL BRONZE.LOAD_CITYBIKE_NY();
END;

-- Task hijo: NOAA solo si los streams de CityBike traen filas nuevas
CREATE OR REPLACE TASK BRONZE.TSK_BRONZE_NOAA
    WAREHOUSE = WH_NYCBIKE_DEV
    AFTER BRONZE.TSK_BRONZE_CITYBIKE
    WHEN SYSTEM$STREAM_HAS_DATA('DB_CITYBIKE_BRONZE.BRONZE.STM_CITIBIKE_NY')
        OR SYSTEM$STREAM_HAS_DATA('DB_CITYBIKE_BRONZE.BRONZE.STM_CITIBIKE_JC')
AS CALL BRONZE.LOAD_NOAA_YEAR();

-- Task padre Chain 2: refresca la directory table para que el stream vea archivos nuevos
CREATE OR REPLACE TASK BRONZE.TSK_BRONZE_JC_REFRESH
    WAREHOUSE = WH_NYCBIKE_DEV
    SCHEDULE = 'USING CRON 0 17 1 * * America/New_York'
AS CALL BRONZE.REFRESH_JC_STAGE();

-- Task hijo: corre solo si el stream detecto archivos nuevos tras el refresh
CREATE OR REPLACE TASK BRONZE.TSK_BRONZE_JC_ONFILES
    WAREHOUSE = WH_NYCBIKE_DEV
    AFTER BRONZE.TSK_BRONZE_JC_REFRESH
    WHEN SYSTEM$STREAM_HAS_DATA('DB_CITYBIKE_BRONZE.BRONZE.STM_CITIBIKE_JC')
AS CALL BRONZE.LOAD_CITYBIKE_JC();

-- Procedure: drena el stream de stage de JC para que el WHEN del proximo refresh evalue limpio
CREATE OR REPLACE PROCEDURE BRONZE.DRAIN_JC_STAGE_STREAM()
RETURNS STRING
LANGUAGE SQL
AS
BEGIN
    -- INSERT...SELECT con el stream en el FROM avanza el offset
    INSERT INTO BRONZE.LOAD_LOG (task_name, outcome, details)
    SELECT 'STM_CITIBIKE_JC', 'DRAIN', 'archivos_consumidos=' || COUNT(*)
    FROM BRONZE.STM_CITIBIKE_JC;

    RETURN 'Stage stream JC drenado';

EXCEPTION
    -- Captura cualquier error y lo deja en el log
    WHEN OTHER THEN
        INSERT INTO BRONZE.LOAD_LOG (task_name, outcome, details)
        VALUES ('DRAIN_JC_STAGE_STREAM', 'ERROR', :SQLERRM);
        RAISE;
END;

-- Task nieto Chain 2: drena el stream tras el COPY (sin esto el WHEN seguiria TRUE)
CREATE OR REPLACE TASK BRONZE.TSK_BRONZE_JC_DRAIN
    WAREHOUSE = WH_NYCBIKE_DEV
    AFTER BRONZE.TSK_BRONZE_JC_ONFILES
AS CALL BRONZE.DRAIN_JC_STAGE_STREAM();
