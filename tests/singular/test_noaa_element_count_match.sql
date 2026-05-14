-- Test: conteo de observaciones unicas por elemento debe coincidir entre stg_NOAA (filtrado por seed) y silver.

{{ bronze_silver_count_diff(
    bronze_relation=ref('stg_NOAA__noaa_raw_year'),
    silver_relation=ref('slv_weather_observation'),
    bronze_group_expr='element',
    silver_group_expr='element_code',
    bronze_filter="
        station_id is not null
        and element in ('TMAX','TMIN','PRCP','SNOW','AWND','SNWD','WSF2','WSF5')
    ",
    bronze_dedup_keys=['station_id', 'observation_date', 'element']
) }}
