-- stg NOAA: vista delgada sobre snp_NOAA__noaa_raw_year (solo version vigente).
-- Cambio v8: drop de m_flag, s_flag y q_flag_category.
--   m_flag, s_flag: no se usan downstream.
--   q_flag_category: ahora vive en slv_quality_flag (lookup normalizado); el join lo hace BI.

{{ config(materialized='view') }}

-- CTE current_version: solo filas vigentes del SCD2, filtradas a estaciones del seed
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
    q_flag,
    obs_time,

    -- linaje
    source_file,
    load_ts,
    dbt_valid_from,
    dbt_updated_at
from current_version
