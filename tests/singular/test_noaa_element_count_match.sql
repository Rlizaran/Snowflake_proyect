-- Test: conteo de observaciones unicas por elemento debe coincidir entre bronze y silver.

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
