-- slv_weather_station: dim de estaciones NOAA del proyecto (Manhattan + Newark/JC).

with raw_stations as (
    select * from {{ ref('weather_station_us') }}
)

select
    -- PK
    stations_id as station_weather_id,

    -- atributos
    station_name,
    {{ dbt_utils.generate_surrogate_key(['city']) }} as city_id,
    lat,
    lng,
    state,
    elevation_m
    
from raw_stations
