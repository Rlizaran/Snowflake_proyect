-- Stg NOAA: vista delgada sobre el snapshot SCD2 (snp_NOAA__noaa_raw_year). Solo version vigente.
-- FIX 1: error de sintaxis original (faltaba coma).
-- FIX 2: incremental delete+insert -> snapshot SCD2 (no perder versiones reemplazadas).
-- FIX 3: removido filtro muerto 'obs_time is not null'.
-- FIX 4: el escalado /10 ya lo hace el snapshot. Aqui solo se pasa data_value en unidad real.
-- FIX 5: expuesto q_flag_category desde el snapshot (lo consume slv_weather_observation).
-- Materializado como VIEW: el snapshot ya esta materializado y clusterizado.
{{ config(materialized='view') }}

-- Filtra a la version vigente del SCD2
with current_version as (
    select *
    from {{ ref('snp_NOAA__noaa_raw_year') }}
    where dbt_valid_to is null
)

select
    station_id,
    observation_date,
    element,
    data_value,
    m_flag,
    q_flag,
    q_flag_category,
    s_flag,
    obs_time,
    source_file,
    load_ts,
    dbt_valid_from,
    dbt_updated_at
from current_version
