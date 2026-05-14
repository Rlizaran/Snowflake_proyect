-- Test: slv_weather_station debe tener el mismo numero de estaciones que stg_NOAA__noaa_raw_year (current SCD2).

with slv_count as (
    select count(*) as n
    from {{ ref('slv_weather_station') }}
),

noaa_count as (
    select count(distinct station_id) as n
    from {{ ref('stg_NOAA__noaa_raw_year') }}
)

select
    'slv_weather_station count != stg_NOAA distinct stations' as issue,
    s.n as slv_n,
    n.n as noaa_n
from slv_count s, noaa_count n
where s.n != n.n
