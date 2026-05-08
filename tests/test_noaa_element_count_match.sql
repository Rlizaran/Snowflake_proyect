-- Test: cuenta de observaciones por elemento (TMAX, TMIN, PRCP, SNOW, AWND, SNWD, WSF2, WSF5)
-- en bronze (filtrado a estaciones/elementos validos) debe coincidir con la cuenta en
-- slv_weather_observation. Si bronze tiene 69 filas con SNOW, silver debe tener 69 con SNOW.
-- Falla si los filtros de stg dropearon filas inesperadamente.

{{ bronze_silver_count_diff(
    bronze_relation=source('NOAA', 'noaa_raw_year'),
    silver_relation=ref('slv_weather_observation'),
    bronze_group_expr='trim(element)',
    silver_group_expr='element_code',
    bronze_filter="
        station_id in ('USW00094728', 'USW00014734')
        and to_date(observation_date, 'YYYYMMDD') >= '2024-01-01'
        and element in ('TMAX','TMIN','PRCP','SNOW','AWND','SNWD','WSF2','WSF5')
        and obs_time is not null
    "
) }}
