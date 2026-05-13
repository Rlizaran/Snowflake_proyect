-- Test: slv_weather_station debe tener el mismo numero de estaciones que NOAA bronze (las que el snapshot dejo entrar).

with slv_count as (
    select count(*) as n
    from {{ ref('slv_weather_station') }}
),

noaa_count as (
    select count(distinct trim(noaa.station_id)) as n
    from {{ source('NOAA', 'noaa_raw_year') }} noaa
    inner join {{ ref('weather_station_us') }} ws
        on trim(noaa.station_id) = ws.station_id
)

select
    'slv_weather_station count != NOAA distinct stations' as issue,
    s.n as slv_n,
    n.n as noaa_n
from slv_count s, noaa_count n
where s.n != n.n
