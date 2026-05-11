-- Test: cuenta de OBSERVACIONES UNICAS por estacion en bronze debe coincidir con silver.
-- Detecta drops silenciosos de estaciones (snapshot ahora deja entrar todas las GHCN-Daily;
-- silver debe reflejarlas todas).
-- FIX (ronda 6): removido el filtro de 2 estaciones del bronze_filter para alinear con el
-- snapshot que ya no filtra (mantenemos todas las US para futuro fct_us_temperature).

{{ bronze_silver_count_diff(
    bronze_relation=ref('stg_NOAA__noaa_raw_year'),
    silver_relation=ref('slv_weather_observation'),
    bronze_group_expr='trim(station_id)',
    silver_group_expr='station_id',
    bronze_filter="
        station_id is not null
        and element in ('TMAX','TMIN','PRCP','SNOW','AWND','SNWD','WSF2','WSF5')
    ",
    bronze_dedup_keys=['trim(station_id)', 'observation_date', 'trim(element)']
) }}
