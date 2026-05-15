-- Silver SCD2 expuesto para BI: TODAS las versiones del snapshot NOAA (vigentes + reemplazadas).
-- Cambio v8: drop de m_flag, s_flag y q_flag_category. q_flag queda como FK a slv_quality_flag.
-- Materializado como VIEW (default proyecto): el snapshot ya esta materializado y clusterizado.
-- Filtros utiles en BI:
--   is_current = TRUE     -> dato vigente (lo que cuenta para metricas finales)
--   is_superseded = TRUE  -> version corregida por NOAA mas tarde
--   has_q_flag = TRUE     -> NOAA marco la observacion como sospechosa en su momento

with snap as (
    select * from {{ ref('snp_NOAA__noaa_raw_year') }}
)

select
    -- PK del row historico (incluye dbt_valid_from para diferenciar versiones del mismo scd_key)
    {{ dbt_utils.generate_surrogate_key(['scd_key', 'dbt_valid_from']) }} as observation_version_id,

    -- Clave SCD2 + atributos NOAA
    scd_key,
    station_id,
    observation_date,
    year(observation_date)    as observation_year,
    quarter(observation_date) as observation_quarter,
    month(observation_date)   as observation_month,
    element                   as element_code,
    q_flag,

    -- Valor ya escalado a unidad real desde el snapshot (Celsius / mm / m/s)
    data_value,
    obs_time,

    -- Linaje SCD2
    dbt_valid_from,
    dbt_valid_to,
    dbt_updated_at,

    -- Flags listos para BI (evita logica en DAX)
    case when dbt_valid_to is null then true else false end as is_current,
    case when dbt_valid_to is not null then true else false end as is_superseded,
    case when q_flag is not null and q_flag <> '' then true else false end as has_q_flag,

    -- Linaje fuente
    source_file,
    load_ts
from snap
