-- Test: slv_weather_station debe contener exactamente las stations presentes en stg_NOAA.
-- Falla si hay stations en stg_NOAA que no estan en el seed (snapshot stale -> dbt snapshot --full-refresh)
-- o stations en slv sin observaciones (caso poco probable dado el inner join con stg_NOAA).

with in_noaa_not_in_slv as (
    select 'NOAA_NOT_IN_SLV' as side, station_id as station_weather_id
    from (
        select distinct station_id from {{ ref('stg_NOAA__noaa_raw_year') }}
        minus
        select station_weather_id   from {{ ref('slv_weather_station') }}
    )
),

in_slv_not_in_noaa as (
    select 'SLV_NOT_IN_NOAA' as side, station_weather_id
    from (
        select station_weather_id   from {{ ref('slv_weather_station') }}
        minus
        select distinct station_id from {{ ref('stg_NOAA__noaa_raw_year') }}
    )
)

select * from in_noaa_not_in_slv
union all
select * from in_slv_not_in_noaa
