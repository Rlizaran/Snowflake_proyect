-- slv_weather_station: dim de estaciones meteo derivada del seed, filtrada a las que aparecen en NOAA.
-- city_id solo se asigna a las 2 estaciones del proyecto (USW00094728=Manhattan, USW00014734=Jersey City).
-- Las demas estaciones tienen city_id = NULL (no participan en el join de fct_trips_weather).

with raw_stations as (
    select * from {{ ref('weather_station_us') }}
),

noaa_active as (
    select distinct station_id from {{ ref('stg_NOAA__noaa_raw_year') }}
),

mapped as (
    select
        ws.station_id as station_weather_id,
        case ws.station_id
            when 'USW00094728' then 'Manhattan'
            when 'USW00014734' then 'Jersey City'
            else null
        end as project_city,
        ws.station_name,
        ws.lat,
        ws.lng,
        ws.state,
        ws.elevation_m
    from raw_stations ws
    inner join noaa_active n on ws.station_id = n.station_id
)

select
    -- PK
    station_weather_id,

    -- FK a slv_city (NULL para no-project, evita colision en city_id surrogate)
    case
        when project_city is null then null
        else {{ dbt_utils.generate_surrogate_key(['project_city']) }}
    end as city_id,

    -- atributos
    station_name,
    lat,
    lng,
    state,
    elevation_m
from mapped
