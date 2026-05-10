-- Stg NOAA: ahora vista delgada sobre el snapshot SCD2 (snp_NOAA__noaa_raw_year).
-- FIX 1: el original tenia un error de sintaxis (faltaba coma entre 'as obs_time' y 'source_file').
-- FIX 2: incremental delete+insert -> snapshot SCD2. NOAA reescribe anios cuando llega una correccion;
-- delete+insert perdia el valor anterior. El snapshot guarda la historia (dbt_valid_from/to);
-- esta vista expone solo la version vigente para que slv_weather_observation / slv_weather_daily
-- sigan viendo "los datos actuales" sin cambios.
-- FIX 3: removido 'and obs_time is not null'. El snapshot ya hace coalesce(...,2400) -> obs_time
-- nunca es null aqui. Filtro muerto que ademas no se aplicaba en el bronze original (consistencia
-- con tests, ver test_noaa_*_count_match.sql).
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
