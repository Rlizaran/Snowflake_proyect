-- Demo en vivo para clase: insertar datos y ver streams + tasks reaccionar
-- Correr cada PASO por separado en Snowsight para ver el efecto en cada uno

USE ROLE ROLE_NYCBIKE;
USE WAREHOUSE WH_NYCBIKE_DEV;
USE DATABASE DEV_CITYBIKE_BRONZE;

-- =====================================================================
-- PASO 0: ESTADO INICIAL (antes de tocar nada)
-- =====================================================================

-- Conteo actual por tabla
SELECT 'citybike_trips_ny' AS tabla, COUNT(*) AS filas FROM CITYBIKE.CITYBIKE_TRIPS_NY
UNION ALL
SELECT 'citybike_trips_jc'        , COUNT(*)         FROM CITYBIKE.CITYBIKE_TRIPS_JC
UNION ALL
SELECT 'noaa_raw_year'            , COUNT(*)         FROM NOAA.NOAA_RAW_YEAR;

-- Estado de los streams ANTES (todos deberian estar FALSE si Silver/dbt ya consumio)
SELECT 'STM_CITYBIKE_NY' AS stream, SYSTEM$STREAM_HAS_DATA('DB_CITYBIKE_LOGS.LOGS.STM_CITYBIKE_NY') AS has_data_before
UNION ALL
SELECT 'STM_CITYBIKE_JC'         , SYSTEM$STREAM_HAS_DATA('DB_CITYBIKE_LOGS.LOGS.STM_CITYBIKE_JC')
UNION ALL
SELECT 'STM_NOAA_YEAR'           , SYSTEM$STREAM_HAS_DATA('DB_CITYBIKE_LOGS.LOGS.STM_NOAA_YEAR');


-- =====================================================================
-- PASO 1: INSERT MANUAL EN CITYBIKE_TRIPS_NY
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

-- Confirmar las 2 filas insertadas
SELECT * FROM CITYBIKE.CITYBIKE_TRIPS_NY WHERE ride_id LIKE 'DEMO-NY-%';

-- El stream debe haber capturado las 2 filas
SELECT SYSTEM$STREAM_HAS_DATA('DB_CITYBIKE_LOGS.LOGS.STM_CITYBIKE_NY') AS ny_pending;

-- Ver el delta capturado por el stream (no consume todavia, solo mira)
SELECT METADATA$ACTION, METADATA$ISUPDATE, ride_id, rideable_type, member_casual
FROM   DB_CITYBIKE_LOGS.LOGS.STM_CITYBIKE_NY
WHERE  ride_id LIKE 'DEMO-NY-%';


-- =====================================================================
-- PASO 2: INSERT MANUAL EN CITYBIKE_TRIPS_JC
-- =====================================================================

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

-- Confirmar las filas
SELECT * FROM CITYBIKE.CITYBIKE_TRIPS_JC WHERE ride_id LIKE 'DEMO-JC-%';

-- Stream JC debe estar TRUE
SELECT SYSTEM$STREAM_HAS_DATA('DB_CITYBIKE_LOGS.LOGS.STM_CITYBIKE_JC') AS jc_pending;

-- Ver el delta del stream JC
SELECT METADATA$ACTION, METADATA$ISUPDATE, ride_id, rideable_type, member_casual
FROM   DB_CITYBIKE_LOGS.LOGS.STM_CITYBIKE_JC
WHERE  ride_id LIKE 'DEMO-JC-%';


-- =====================================================================
-- PASO 3: INSERT MANUAL EN NOAA_RAW_YEAR
-- =====================================================================

-- Insertar 2 observaciones de demo (Manhattan + Newark)
INSERT INTO NOAA.NOAA_RAW_YEAR
    (station_id, observation_date, element, data_value, m_flag, q_flag, s_flag, obs_time, source_file)
VALUES
    ('USW00094728','20260502','TMAX','220',NULL,NULL,'X',NULL,'demo_class.csv'),
    ('USW00014734','20260502','PRCP','15', NULL,NULL,'X',NULL,'demo_class.csv');

-- Confirmar las observaciones
SELECT * FROM NOAA.NOAA_RAW_YEAR WHERE source_file = 'demo_class.csv';

-- Stream NOAA debe estar TRUE
SELECT SYSTEM$STREAM_HAS_DATA('DB_CITYBIKE_LOGS.LOGS.STM_NOAA_YEAR') AS noaa_pending;


-- =====================================================================
-- PASO 4: ESTADO DESPUES DE INSERTS
-- =====================================================================

-- Conteo nuevo (debe haber +2 NY, +2 JC, +2 NOAA)
SELECT 'citybike_trips_ny' AS tabla, COUNT(*) AS filas FROM CITYBIKE.CITYBIKE_TRIPS_NY
UNION ALL
SELECT 'citybike_trips_jc'        , COUNT(*)         FROM CITYBIKE.CITYBIKE_TRIPS_JC
UNION ALL
SELECT 'noaa_raw_year'            , COUNT(*)         FROM NOAA.NOAA_RAW_YEAR;

-- Estado de streams (los 3 de tabla deben estar TRUE)
SELECT 'STM_CITYBIKE_NY' AS stream, SYSTEM$STREAM_HAS_DATA('DB_CITYBIKE_LOGS.LOGS.STM_CITYBIKE_NY') AS has_data
UNION ALL
SELECT 'STM_CITYBIKE_JC'         , SYSTEM$STREAM_HAS_DATA('DB_CITYBIKE_LOGS.LOGS.STM_CITYBIKE_JC')
UNION ALL
SELECT 'STM_NOAA_YEAR'           , SYSTEM$STREAM_HAS_DATA('DB_CITYBIKE_LOGS.LOGS.STM_NOAA_YEAR');


-- =====================================================================
-- PASO 5: DISPARAR EL DAG DE TASKS A MANO
-- =====================================================================

-- Forzar Chain 1 sin esperar el cron del domingo
EXECUTE TASK DB_CITYBIKE_LOGS.LOGS.TSK_BRONZE_CITYBIKE;

-- Esperar ~30 segundos y revisar el historial
SELECT name, state, scheduled_time, completed_time, return_value, error_message
FROM   TABLE(DB_CITYBIKE_LOGS.INFORMATION_SCHEMA.TASK_HISTORY(
            SCHEDULED_TIME_RANGE_START => DATEADD(minute, -10, CURRENT_TIMESTAMP())))
WHERE  database_name = 'DB_CITYBIKE_LOGS'
ORDER BY scheduled_time DESC;
-- Esperado:
-- TSK_BRONZE_CITYBIKE = SUCCEEDED (corre LOAD_CITYBIKE_NY que hace COPY desde S3)
-- TSK_BRONZE_NOAA     = SUCCEEDED (porque WHEN se evalua TRUE: stream NY tiene data)


-- =====================================================================
-- PASO 6: REVISAR EL LOG INTERNO
-- =====================================================================

-- Las procedures dejan rastro en LOAD_LOG
SELECT * FROM DB_CITYBIKE_LOGS.LOGS.LOAD_LOG ORDER BY run_ts DESC LIMIT 10;
-- Esperado: filas LOAD_CITYBIKE_NY OK, LOAD_NOAA_YEAR OK, posiblemente DRAIN si JC corrio


-- =====================================================================
-- PASO 7: LIMPIAR LOS DATOS DE DEMO (rollback)
-- =====================================================================

-- Borrar las filas de demo para no contaminar la tabla
DELETE FROM CITYBIKE.CITYBIKE_TRIPS_NY WHERE source_file = 'demo_class.csv';
DELETE FROM CITYBIKE.CITYBIKE_TRIPS_JC WHERE source_file = 'demo_class.csv';
DELETE FROM NOAA.NOAA_RAW_YEAR        WHERE source_file = 'demo_class.csv';

-- Confirmar que ya no estan
SELECT COUNT(*) AS demo_rows_left FROM CITYBIKE.CITYBIKE_TRIPS_NY WHERE source_file = 'demo_class.csv';
SELECT COUNT(*) AS demo_rows_left FROM CITYBIKE.CITYBIKE_TRIPS_JC WHERE source_file = 'demo_class.csv';
SELECT COUNT(*) AS demo_rows_left FROM NOAA.NOAA_RAW_YEAR        WHERE source_file = 'demo_class.csv';

-- Nota: los DELETE en streams APPEND_ONLY no aparecen como cambios en el stream;
-- los streams solo trackean inserts. La tabla queda limpia, los streams siguen
-- con sus offsets como estaban.
