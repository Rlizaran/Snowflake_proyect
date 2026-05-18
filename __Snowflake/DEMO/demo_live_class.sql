-- Demo en vivo para clase: insertar datos y ver streams + tasks reaccionar.
-- Correr cada PASO por separado en Snowsight para ver el efecto en cada uno.
-- Entorno: DEV. Cambiar DEV_ por PRO_ si se ejecuta en PRO.

USE ROLE ROLE_NYCBIKE;
USE WAREHOUSE WH_NYCBIKE_DEV;
USE DATABASE DEV_CITYBIKE_BRONZE;

-- =====================================================================
-- PASO 1: INSERT MANUAL EN BRONZE (CityBike NY + JC + NOAA)
-- =====================================================================

-- Insertar 2 viajes de demo (Manhattan)
INSERT INTO CITYBIKE.CITYBIKE_TRIPS_NY
    (ride_id, rideable_type, started_at, ended_at,
     start_station_name, start_station_id, end_station_name, end_station_id,
     start_lat, start_lng, end_lat, end_lng, member_casual, source_file)
VALUES
    ('DEMO-NY-001','classic_bike','2026-05-02 10:00:00','2026-05-02 10:15:00',
     'Central Park S','6140.05','Times Sq','6450.05','40.766','-73.979','40.758','-73.985','member','demo_class.csv'),
    ('DEMO-NY-002','electric_bike','2026-05-02 11:00:00','2026-05-02 11:08:00',
     'W 21 St & 6 Ave','6140.06','Union Sq','6450.06','40.741','-73.994','40.735','-73.991','casual','demo_class.csv');

-- Insertar 2 viajes de demo (Jersey City)
INSERT INTO CITYBIKE.CITYBIKE_TRIPS_JC
    (ride_id, rideable_type, started_at, ended_at,
     start_station_name, start_station_id, end_station_name, end_station_id,
     start_lat, start_lng, end_lat, end_lng, member_casual, source_file)
VALUES
    ('DEMO-JC-001','classic_bike','2026-05-02 10:30:00','2026-05-02 10:45:00',
     'Grove St','JC013','Exchange Pl','JC020','40.719','-74.043','40.716','-74.033','member','demo_class.csv'),
    ('DEMO-JC-002','electric_bike','2026-05-02 12:00:00','2026-05-02 12:10:00',
     'Hoboken Terminal','HB101','Newport','JC005','40.735','-74.027','40.726','-74.033','casual','demo_class.csv');

-- Insertar 2 observaciones de demo (Manhattan + Newark/JC)
INSERT INTO NOAA.NOAA_RAW_YEAR
    (station_id, observation_date, element, data_value, m_flag, q_flag, s_flag, obs_time, source_file)
VALUES
    ('USW00094728','20260502','TMAX','220',NULL,NULL,'X',NULL,'demo_class.csv'),
    ('USW00014734','20260502','PRCP','15', NULL,NULL,'X',NULL,'demo_class.csv');

-- Confirmar las filas insertadas
SELECT * FROM CITYBIKE.CITYBIKE_TRIPS_NY WHERE ride_id LIKE 'DEMO-NY-%'
UNION ALL
SELECT * FROM CITYBIKE.CITYBIKE_TRIPS_JC WHERE ride_id LIKE 'DEMO-JC-%';

SELECT * FROM NOAA.NOAA_RAW_YEAR WHERE source_file = 'demo_class.csv';


-- =====================================================================
-- PASO 2: DISPARAR EL DAG DE TASKS A MANO
-- =====================================================================

-- Forzar Chain completo sin esperar el cron del dia 28
EXECUTE TASK DB_CITYBIKE_LOGS.LOGS.TSK_BRONZE_MASTER;

-- Esperar ~30 segundos y revisar el historial
SELECT name, state, scheduled_time, completed_time, return_value, error_message
FROM   TABLE(DB_CITYBIKE_LOGS.INFORMATION_SCHEMA.TASK_HISTORY(
            SCHEDULED_TIME_RANGE_START => DATEADD(minute, -10, CURRENT_TIMESTAMP())))
WHERE  database_name = 'DB_CITYBIKE_LOGS'
ORDER BY scheduled_time DESC;
-- Esperado:
-- TSK_BRONZE_MASTER, *_NY, *_JC_REFRESH, *_JC_DRAIN, *_NY_INT_*, NOAA, STREAMS_DRAIN  = SUCCEEDED


-- =====================================================================
-- PASO 3: dbt build + verificacion silver/gold
-- =====================================================================
-- Ejecutar 'dbt build' en la terminal antes de continuar.

SELECT * FROM DEV_CITYBIKE_GOLD.DBT_RLIZARAN_MOBILITY.FCT_TRIPS
WHERE ride_id IN ('DEMO-JC-001','DEMO-JC-002','DEMO-NY-001','DEMO-NY-002');

SELECT * FROM DEV_CITYBIKE_GOLD.DBT_RLIZARAN_CLIMA.FCT_NOAA_CORRECTIONS
WHERE DBT_VALID_TO IS NOT NULL;

SELECT * FROM DEV_CITYBIKE_SILVER.SNAPSHOTS.SNP_NOAA__NOAA_RAW_YEAR
WHERE scd_key IN ('USW00014734|20260502|PRCP', 'USW00094728|20260502|TMAX');


-- =====================================================================
-- PASO 4: Limpieza de bronze post-demo
-- =====================================================================

DELETE FROM CITYBIKE.CITYBIKE_TRIPS_NY WHERE source_file = 'demo_class.csv';
DELETE FROM CITYBIKE.CITYBIKE_TRIPS_JC WHERE source_file = 'demo_class.csv';
DELETE FROM NOAA.NOAA_RAW_YEAR        WHERE source_file = 'demo_class.csv';

SELECT COUNT(*) AS demo_rows_left FROM CITYBIKE.CITYBIKE_TRIPS_NY WHERE source_file = 'demo_class.csv';
SELECT COUNT(*) AS demo_rows_left FROM CITYBIKE.CITYBIKE_TRIPS_JC WHERE source_file = 'demo_class.csv';
SELECT COUNT(*) AS demo_rows_left FROM NOAA.NOAA_RAW_YEAR        WHERE source_file = 'demo_class.csv';


-- =====================================================================
-- PASO 5: Reset completo de schemas dbt + rebuild
-- =====================================================================
-- DEV (incluye prefijos dbt_<usuario>; ajustar al sandbox correspondiente)
DROP SCHEMA IF EXISTS DEV_CITYBIKE_GOLD.DBT_RLIZARAN_CORE;
DROP SCHEMA IF EXISTS DEV_CITYBIKE_GOLD.DBT_RLIZARAN_MOBILITY;
DROP SCHEMA IF EXISTS DEV_CITYBIKE_GOLD.DBT_RLIZARAN_CLIMA;
DROP SCHEMA IF EXISTS DEV_CITYBIKE_GOLD.DBT_RLIZARAN_ANALYTICS;
DROP SCHEMA IF EXISTS DEV_CITYBIKE_SILVER.DBT_RLIZARAN_CITYBIKE;
DROP SCHEMA IF EXISTS DEV_CITYBIKE_SILVER.DBT_RLIZARAN_INTERMEDIATE;
DROP SCHEMA IF EXISTS DEV_CITYBIKE_SILVER.DBT_RLIZARAN_NOAA;
DROP SCHEMA IF EXISTS DEV_CITYBIKE_SILVER.SNAPSHOTS;

-- PRO (sin prefijo)
DROP SCHEMA IF EXISTS PRO_CITYBIKE_GOLD.CORE;
DROP SCHEMA IF EXISTS PRO_CITYBIKE_GOLD.MOBILITY;
DROP SCHEMA IF EXISTS PRO_CITYBIKE_GOLD.CLIMA;
DROP SCHEMA IF EXISTS PRO_CITYBIKE_GOLD.ANALYTICS;
DROP SCHEMA IF EXISTS PRO_CITYBIKE_SILVER.CITYBIKE;
DROP SCHEMA IF EXISTS PRO_CITYBIKE_SILVER.INTERMEDIATE;
DROP SCHEMA IF EXISTS PRO_CITYBIKE_SILVER.NOAA;
DROP SCHEMA IF EXISTS PRO_CITYBIKE_SILVER.SNAPSHOTS;

-- Despues correr en terminal: dbt build --full-refresh
