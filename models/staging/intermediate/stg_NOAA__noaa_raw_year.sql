-- stg NOAA: vista delgada sobre snp_NOAA__noaa_raw_year (solo version vigente).

{{ config(materialized='view') }}

with current_version as (
    select noaa.*
    from {{ ref('snp_NOAA__noaa_raw_year') }} noaa
    inner join {{ ref('weather_station_us') }} ws
    on noaa.station_id = ws.station_id
    where noaa.dbt_valid_to is null
)

select
    -- PK natural
    station_id,
    observation_date,
    element,

    -- atributos
    data_value,
    m_flag,
    q_flag,
    q_flag_category,
    s_flag,
    obs_time,

    -- linaje
    source_file,
    load_ts,
    dbt_valid_from,
    dbt_updated_at
from current_version
