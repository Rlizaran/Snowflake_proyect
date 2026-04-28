-- Conectar usuario, warehouse y database
USE ROLE ROLE_NYCBIKE;
USE WAREHOUSE WH_NYCBIKE_DEV;
USE DATABASE WH_NYCBIKE;

-- Usar Bronze SCHEMA
USE WH_NYCBIKE.BRONZE;

-- Stream append-only sobre CityBike NYC
CREATE OR REPLACE STREAM bronze.stm_citibike_ny
    ON TABLE bronze.citibike_trips_ny APPEND_ONLY = TRUE;

-- Stream sobre el stage  interno de JC
CREATE OR REPLACE STREAM BRONZE.STM_CITIBIKE_JC
    ON STAGE bronze.citibike_landing_stage;

-- Stream append-only sobre NOAA para que Silver consuma incrementalmente
CREATE OR REPLACE STREAM bronze.stm_noaa_year
    ON TABLE bronze.noaa_raw_year APPEND_ONLY = TRUE;

-- Task padre -> refresh semanal de CityBike los domingos a las 3am America/New York
CREATE OR REPLACE TASK BRONZE.TSK_BRONZE_CITYBIKE
    WAREHOUSE = WH_NYCBIKE_DEV
    SCHEDULE = 'USING CRON 0 3 * * 0 America/New_York'
AS
BEGIN
    CALL BRONZE.LOAD_CITYBIKE_NY();
END;

-- Task hijo -> NOAA solo si los streams de CityBike traen filas nuevas
CREATE OR REPLACE TASK BRONZE.TSK_BRONZE_NOAA
    WAREHOUSE = WH_NYCBIKE_DEV
    AFTER BRONZE.TSK_BRONZE_CITYBIKE
    WHEN SYSTEM$STREAM_HAS_DATA('BRONZE.STM_CITIBIKE_NY')
        OR SYSTEM$STREAM_HAS_DATA('BRONZE.STM_CITIBIKE_JC')
AS CALL BRONZE.LOAD_NOAA_YEAR();

-- Task padre: refresca la directory table para que el stream vea archivos nuevos
CREATE OR REPLACE TASK BRONZE.TSK_BRONZE_JC_REFRESH
    WAREHOUSE = WH_NYCBIKE_DEV
    SCHEDULE = 'USING CRON 0 17 1 * * America/New_York'
AS CALL BRONZE.REFRESH_JC_STAGE();

-- Task hijo: corre solo si el stream detecto archivos nuevos tras el refresh
CREATE OR REPLACE TASK BRONZE.TSK_BRONZE_JC_ONFILES
    WAREHOUSE = WH_NYCBIKE_DEV
    AFTER BRONZE.TSK_BRONZE_JC_REFRESH
    WHEN SYSTEM$STREAM_HAS_DATA('BRONZE.STM_CITIBIKE_JC')
AS CALL BRONZE.LOAD_CITYBIKE_JC();

-- Procedure que drena el stream de stage de JC para que el WHEN del proximo refresh evalue limpio
CREATE OR REPLACE PROCEDURE BRONZE.DRAIN_JC_STAGE_STREAM()
RETURNS STRING
LANGUAGE SQL
AS
BEGIN
    -- INSERT...SELECT con el stream en el FROM avanza el offset
    INSERT INTO bronze.load_log (task_name, outcome, details)
    SELECT 'STM_CITIBIKE_JC', 'DRAIN', 'archivos_consumidos=' || COUNT(*)
    FROM bronze.stm_citibike_jc;

    RETURN 'Stage stream JC drenado';

EXCEPTION
    -- Captura cualquier error y lo deja en el log
    WHEN OTHER THEN
        INSERT INTO bronze.load_log (task_name, outcome, details)
        VALUES ('DRAIN_JC_STAGE_STREAM', 'ERROR', :SQLERRM);
        RAISE;
END;

-- Chain 2 nieto -> drena el stream de stage tras el COPY (sin esto el WHEN seguiria TRUE indefinidamente)
CREATE OR REPLACE TASK BRONZE.TSK_BRONZE_JC_DRAIN
    WAREHOUSE = WH_NYCBIKE_DEV
    AFTER BRONZE.TSK_BRONZE_JC_ONFILES
AS CALL BRONZE.DRAIN_JC_STAGE_STREAM();
