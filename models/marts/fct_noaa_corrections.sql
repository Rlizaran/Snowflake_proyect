-- Gold fact: historico SCD2 NOAA expuesto para BI (todas las versiones del snapshot).

with snap as (
    select * from {{ ref('snp_NOAA__noaa_raw_year') }}
)

select
    -- PK del row historico
    {{ dbt_utils.generate_surrogate_key(['scd_key', 'dbt_valid_from']) }} as observation_version_id,

    -- claves SCD2
    scd_key,
    station_id,
    observation_date,
    year(observation_date)    as observation_year,
    quarter(observation_date) as observation_quarter,
    month(observation_date)   as observation_month,
    element                   as element_code,

    -- metricas
    data_value,
    m_flag,
    q_flag,
    q_flag_category,
    s_flag,
    obs_time,

    -- linaje SCD2
    dbt_valid_from,
    dbt_valid_to,
    dbt_updated_at,

    -- flags BI
    case when dbt_valid_to is null     then true else false end as is_current,
    case when dbt_valid_to is not null then true else false end as is_superseded,
    case when q_flag_category in ('SUSPECT','INVALID') then true else false end as is_problematic,

    -- linaje fuente
    source_file,
    load_ts
from snap
