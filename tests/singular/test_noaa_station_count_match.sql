-- Test: conteo de observaciones unicas por estacion debe coincidir entre bronze y silver.

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
