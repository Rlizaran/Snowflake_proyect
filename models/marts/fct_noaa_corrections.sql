-- Gold mart: historico SCD2 de observaciones NOAA expuesto para BI.
-- Movido desde silver/NOAA/slv_weather_observation_history.sql (patron Gold: BI-only consumer).
-- Materializado como TABLE (default de marts en dbt_project.yml).
-- q_flag_category viene ya derivada del snapshot (no se re-deriva aqui).

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

    -- Valor ya escalado a unidad real desde el snapshot (Celsius / mm / m/s)
    data_value,

    m_flag,
    q_flag,
    q_flag_category,           -- OK / SUSPECT / INVALID / PROCESSING / METADATA / UNKNOWN
    s_flag,
    obs_time,

    -- Linaje SCD2
    dbt_valid_from,
    dbt_valid_to,
    dbt_updated_at,

    -- Flags BI-ready (evita logica DAX)
    case when dbt_valid_to is null     then true else false end as is_current,
    case when dbt_valid_to is not null then true else false end as is_superseded,

    -- Slicer "datos problematicos" (mas informativo que has_q_flag binario)
    case when q_flag_category in ('SUSPECT','INVALID') then true else false end as is_problematic,

    -- Linaje fuente
    source_file,
    load_ts
from snap
