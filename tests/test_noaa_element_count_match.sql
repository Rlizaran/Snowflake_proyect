-- Test: cuenta de OBSERVACIONES UNICAS por elemento en bronze (filtrado a estaciones/elementos
-- validos) debe coincidir con la cuenta en slv_weather_observation (vigente, deduplicada por
-- snapshot SCD2). Si bronze tiene 69 obs unicas con SNOW, silver debe tener 69.
-- FIX 1: ahora pasa bronze_dedup_keys=[station_id, observation_date, element]. Antes contaba
-- filas raw de bronze; con DEV duplicado fallaba aunque silver estuviera correcto.
-- FIX 2: removido 'and obs_time is not null' del bronze_filter. El snapshot/stg hace
-- coalesce(..., 2400) y NO descarta rows con obs_time NULL en bronze, asi que el filtro de
-- antes era mas estricto que silver -> mismatch falso (visible en cuanto la demo inserta
-- rows con obs_time=NULL). Ahora ambos lados ven el mismo conjunto.

{{ bronze_silver_count_diff(
    bronze_relation=source('NOAA', 'noaa_raw_year'),
    silver_relation=ref('slv_weather_observation'),
    bronze_group_expr='trim(element)',
    silver_group_expr='element_code',
    bronze_filter="
        station_id in ('USW00094728', 'USW00014734')
        and to_date(observation_date, 'YYYYMMDD') >= '2024-01-01'
        and element in ('TMAX','TMIN','PRCP','SNOW','AWND','SNWD','WSF2','WSF5')
    ",
    bronze_dedup_keys=['trim(station_id)', 'observation_date', 'trim(element)']
) }}
