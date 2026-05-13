-- dim_station_weather: passthrough de slv_weather_station con columnas explicitas.

select
    station_weather_id,
    city_id,
    station_name,
    lat,
    lng,
    state,
    elevation_m
from {{ ref('slv_weather_station') }}
