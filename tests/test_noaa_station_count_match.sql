-- Test: cuenta de OBSERVACIONES UNICAS por estacion (USW00094728 / USW00014734) en bronze
-- debe coincidir con la cuenta en slv_weather_observation. Detecta drops silenciosos de una
-- estacion entera (ej. NOAA reescribio el archivo y removio la otra).
-- FIX: anadido bronze_dedup_keys para que DEV duplicado no rompa el test.

{{ bronze_silver_count_diff(
    bronze_relation=source('NOAA', 'noaa_raw_year'),
    silver_relation=ref('slv_weather_observation'),
    bronze_group_expr='trim(station_id)',
    silver_group_expr='station_id',
    bronze_filter="
        station_id in ('USW00094728', 'USW00014734')
        and to_date(observation_date, 'YYYYMMDD') >= '2024-01-01'
        and element in ('TMAX','TMIN','PRCP','SNOW','AWND','SNWD','WSF2','WSF5')
        and obs_time is not null
    ",
    bronze_dedup_keys=['trim(station_id)', 'observation_date', 'trim(element)']
) }}
