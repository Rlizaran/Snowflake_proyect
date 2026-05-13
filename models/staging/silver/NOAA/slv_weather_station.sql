-- slv_weather_station: dim de estaciones meteo derivada del seed weather_station_us.
-- Las 2 estaciones del proyecto (Central Park NY + Newark/JC) se renombran a las ciudades CityBike
-- para que el city_id surrogate matchee con slv_city en fct_trips_weather.

with raw_stations as (
    select * from {{ ref('weather_station_us') }}
),

mapped as (
    select
        station_id as station_weather_id,
        station_name,
        case station_id
            when 'USW00094728' then 'Manhattan'
            when 'USW00014734' then 'Jersey City'
            else city
        end as city,
        lat,
        lng,
        state,
        elevation_m
    from raw_stations
)

select
    -- PK
    station_weather_id,

    -- FK a slv_city via surrogate del city renombrado
    {{ dbt_utils.generate_surrogate_key(['city']) }} as city_id,

    -- atributos
    station_name,
    lat,
    lng,
    state,
    elevation_m
from mapped
