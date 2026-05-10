-- Stg NOAA: ahora vista delgada sobre el snapshot SCD2 (snp_NOAA__noaa_raw_year).
-- Materializado como VIEW: el snapshot ya esta materializado y clusterizado por year.
{{ config(materialized='view') }}

select
    station_id,
    observation_date,
    element,
    data_value,
    m_flag,
    q_flag,
    s_flag,
    obs_time,
    source_file,
    load_ts,
    -- Linaje SCD2 expuesto por si downstream quiere auditar la version vigente
    dbt_valid_from,
    dbt_updated_at
from {{ ref('snp_NOAA__noaa_raw_year') }}
where dbt_valid_to is null
