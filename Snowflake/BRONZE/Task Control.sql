-- Conectar usuario, warehouse y database
USE ROLE ROLE_NYCBIKE;
USE WAREHOUSE WH_NYCBIKE_DEV;
USE DATABASE WH_NYCBIKE;

-- Usar Bronze SCHEMA
USE WH_NYCBIKE.BRONZE;


-- Activar hijo primero, despues padre
ALTER TASK BRONZE.TSK_BRONZE_NOAA RESUME;
ALTER TASK BRONZE.TSK_BRONZE_CITYBIKE RESUME;

-- Suspender todo para ahorrar creditos (descomentar)
-- ALTER TASK BRONZE.TSK_BRONZE_CITYBIKE SUSPEND;
-- ALTER TASK BRONZE.TSK_BRONZE_NOAA SUSPEND;

-- Ejecturar el task padre manyalmente sin esperar el cron
-- EXECUTE TASK BRONZE.TSK_BRONZE_CITYBIKE;
-- EXECUTE TASK BRONZE.TSK_BRONZE_NOAA;

-- Verificacion de tasks y streams actuales
SHOW TASKS IN SCHEMA BRONZE;
SHOW STREAMS IN SCHEMA BRONZE;

-- Verificacion historial de ejecuciones de tasks (ultimos 7 dias)
SELECT  *
FROM    TABLE(WH_NYCBIKE.INFORMATION_SCHEMA.TASK_HISTORY())
WHERE NAME ILIKE '%tsk%'
ORDER BY scheduled_time DESC
LIMIT 20;

-- Verificacion: ultimos 20 eventos del log propio
SELECT * FROM bronze.load_log ORDER BY run_ts DESC LIMIT 20;