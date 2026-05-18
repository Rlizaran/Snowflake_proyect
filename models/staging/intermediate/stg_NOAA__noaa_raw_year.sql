-- stg NOAA: vista delgada sobre snp_NOAA__noaa_raw_year (solo version vigente). Filtra a estaciones del seed.

{{ config(materialized='view') }}

with current_version as (
    select noaa.*
    from {{ ref('snp_NOAA__noaa_raw_year') }} noaa
    inner join {{ ref('weather_station_us') }} ws
    on noaa.station_id = ws.station_id
    where noaa.dbt_valid_to is null
)

select
    station_id,
    observation_date,
    element,
    data_value,
    q_flag,
    obs_time,
    source_file,
    load_ts,
    dbt_valid_from,
    dbt_updated_at
from current_version
