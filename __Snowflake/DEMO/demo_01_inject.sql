-- Demo inject: 5 trips NY + 5 trips JC + UPDATE de un valor NOAA para disparar SCD2.

USE ROLE ROLE_NYCBIKE;
USE WAREHOUSE WH_NYCBIKE_DEV;
USE DATABASE DEV_CITYBIKE_BRONZE;


-- ============================================================================
-- 1) 5 ride_ids NY (New York), 2026-04-28
-- ============================================================================
INSERT INTO DEV_CITYBIKE_BRONZE.CITYBIKE.CITYBIKE_TRIPS_NY
    (ride_id, rideable_type, started_at, ended_at,
     start_station_name, start_station_id, end_station_name, end_station_id,
     start_lat, start_lng, end_lat, end_lng,
     member_casual, source_file, load_ts)
VALUES
    ('DEMO_NY_2026_01','classic_bike', '2026-04-28 08:05:00','2026-04-28 08:18:30',
     'Demo Start A','DEMO_NY_ST_A','Demo End A','DEMO_NY_ST_B',
     '40.7589','-73.9851','40.7505','-73.9934',
     'member','DEMO_INJECT.csv', CURRENT_TIMESTAMP()),

    ('DEMO_NY_2026_02','electric_bike','2026-04-28 09:30:00','2026-04-28 09:42:10',
     'Demo Start B','DEMO_NY_ST_B','Demo End A','DEMO_NY_ST_A',
     '40.7505','-73.9934','40.7589','-73.9851',
     'casual','DEMO_INJECT.csv', CURRENT_TIMESTAMP()),

    ('DEMO_NY_2026_03','classic_bike', '2026-04-28 12:15:00','2026-04-28 12:35:45',
     'Demo Start C','DEMO_NY_ST_C','Demo End D','DEMO_NY_ST_D',
     '40.7282','-74.0060','40.7411','-74.0018',
     'member','DEMO_INJECT.csv', CURRENT_TIMESTAMP()),

    ('DEMO_NY_2026_04','electric_bike','2026-04-28 17:00:00','2026-04-28 17:20:00',
     'Demo Start A','DEMO_NY_ST_A','Demo End D','DEMO_NY_ST_D',
     '40.7589','-73.9851','40.7411','-74.0018',
     'casual','DEMO_INJECT.csv', CURRENT_TIMESTAMP()),

    ('DEMO_NY_2026_05','classic_bike', '2026-04-28 19:45:00','2026-04-28 20:02:00',
     'Demo Start D','DEMO_NY_ST_D','Demo End C','DEMO_NY_ST_C',
     '40.7411','-74.0018','40.7282','-74.0060',
     'member','DEMO_INJECT.csv', CURRENT_TIMESTAMP());

-- ============================================================================
-- 2) 5 ride_ids JC (Jersey City), 2026-04-28
-- ============================================================================
INSERT INTO DEV_CITYBIKE_BRONZE.CITYBIKE.CITYBIKE_TRIPS_JC
    (ride_id, rideable_type, started_at, ended_at,
     start_station_name, start_station_id, end_station_name, end_station_id,
     start_lat, start_lng, end_lat, end_lng,
     member_casual, source_file, load_ts)
VALUES
    ('DEMO_JC_2026_01','classic_bike', '2026-04-28 07:50:00','2026-04-28 08:05:00',
     'Demo JC Start A','DEMO_JC_ST_A','Demo JC End A','DEMO_JC_ST_B',
     '40.7178','-74.0431','40.7282','-74.0476',
     'member','DEMO_INJECT.csv', CURRENT_TIMESTAMP()),

    ('DEMO_JC_2026_02','electric_bike','2026-04-28 10:10:00','2026-04-28 10:25:00',
     'Demo JC Start B','DEMO_JC_ST_B','Demo JC End A','DEMO_JC_ST_A',
     '40.7282','-74.0476','40.7178','-74.0431',
     'casual','DEMO_INJECT.csv', CURRENT_TIMESTAMP()),

    ('DEMO_JC_2026_03','classic_bike', '2026-04-28 13:00:00','2026-04-28 13:30:00',
     'Demo JC Start C','DEMO_JC_ST_C','Demo JC End D','DEMO_JC_ST_D',
     '40.7090','-74.0500','40.7250','-74.0600',
     'member','DEMO_INJECT.csv', CURRENT_TIMESTAMP()),

    ('DEMO_JC_2026_04','electric_bike','2026-04-28 16:20:00','2026-04-28 16:48:00',
     'Demo JC Start A','DEMO_JC_ST_A','Demo JC End D','DEMO_JC_ST_D',
     '40.7178','-74.0431','40.7250','-74.0600',
     'casual','DEMO_INJECT.csv', CURRENT_TIMESTAMP()),

    ('DEMO_JC_2026_05','classic_bike', '2026-04-28 18:30:00','2026-04-28 18:45:00',
     'Demo JC Start D','DEMO_JC_ST_D','Demo JC End C','DEMO_JC_ST_C',
     '40.7250','-74.0600','40.7090','-74.0500',
     'member','DEMO_INJECT.csv', CURRENT_TIMESTAMP());

-- ============================================================================
-- 3) Alteracion NOAA (simula correccion publicada): cambia data_value de TMAX
--    Manhattan 2024-01-01. Antes era el valor real; ahora '999' (99.9 C) para
--    que se vea claramente en BI que es una correccion sospechosa.
--    El snapshot detectara data_value distinto en check_cols y creara nueva
--    version SCD2; q_flag_category podria cambiar tambien si q_flag muta.
-- ============================================================================
UPDATE DEV_CITYBIKE_BRONZE.NOAA.NOAA_RAW_YEAR
SET data_value = '999',
    q_flag     = 'S',         -- SUSPECT, para que q_flag_category cambie a SUSPECT en snapshot
    load_ts    = CURRENT_TIMESTAMP()
WHERE station_id       = 'USW00094728'
  AND observation_date = '20260512'
  AND element          = 'TMAX';
