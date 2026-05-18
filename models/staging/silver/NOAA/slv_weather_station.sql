-- slv_weather_station: dim de estaciones meteo derivada del seed, filtrada a las que aparecen en NOAA.
-- city_id se asigna via lookup en seed city_weather_station_map. Stations no mapeadas quedan con city_id NULL.

with raw_stations as (
    select * from {{ ref('weather_station_us') }}
),

noaa_active as (
    select distinct station_id from {{ ref('stg_NOAA__noaa_raw_year') }}
),

city_map as (
    select project_city, station_weather_id from {{ ref('city_weather_station_map') }}
),

mapped as (
    select
        ws.station_id as station_weather_id,
        cm.project_city,
        ws.station_name,
        ws.lat,
        ws.lng,
        ws.state,
        ws.elevation_m
    from raw_stations ws
    inner join noaa_active n  on ws.station_id = n.station_id
    left  join city_map     cm on cm.station_weather_id = ws.station_id
)

select
    station_weather_id,
    case
        when project_city is null then null
        else {{ dbt_utils.generate_surrogate_key(['project_city']) }}
    end as city_id,
    station_name,
    lat,
    lng,
    state,
    elevation_m
from mapped
