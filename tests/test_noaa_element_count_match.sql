-- Test: cuenta de OBSERVACIONES UNICAS por elemento en bronze (filtrado a elementos validos)
-- debe coincidir con la cuenta en slv_weather_observation (vigente, deduplicada por snapshot SCD2).
-- FIX (ronda 6): removido 'station_id in (...)' del bronze_filter. El snapshot ya NO filtra
-- estaciones (decision: mantener todas las US para futuro fct_us_temperature). Si bronze
-- filtra a 2 estaciones y silver tiene N, las cuentas no cuadran -> test FAIL falso.
-- Ahora ambos lados ven todas las estaciones GHCN-Daily disponibles en bronze.

{{ bronze_silver_count_diff(
    bronze_relation=source('NOAA', 'noaa_raw_year'),
    silver_relation=ref('slv_weather_observation'),
    bronze_group_expr='trim(element)',
    silver_group_expr='element_code',
    bronze_filter="
        station_id is not null
        and to_date(observation_date, 'YYYYMMDD') >= '2024-01-01'
        and element in ('TMAX','TMIN','PRCP','SNOW','AWND','SNWD','WSF2','WSF5')
    ",
    bronze_dedup_keys=['trim(station_id)', 'observation_date', 'trim(element)']
) }}
