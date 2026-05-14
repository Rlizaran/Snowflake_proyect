-- Test: slv_weather_station debe tener el mismo numero de estaciones que stg_NOAA__noaa_raw_year (current SCD2).
-- Falla si el snapshot trae stations no presentes en el seed (snapshot stale -> requiere --full-refresh).

with slv as (
    select count(*) as n from {{ ref('slv_weather_station') }}
),

noaa as (
    select count(distinct station_id) as n from {{ ref('stg_NOAA__noaa_raw_year') }}
)

select
    'count mismatch slv_weather_station vs stg_NOAA distinct stations' as issue,
    coalesce(slv.n, 0)  as slv_n,
    coalesce(noaa.n, 0) as noaa_n,
    coalesce(slv.n, 0) - coalesce(noaa.n, 0) as diff
from slv, noaa
where coalesce(slv.n, 0) != coalesce(noaa.n, 0)
