-- Gold fact: historico SCD2 NOAA expuesto para BI (todas las versiones del snapshot).
-- Cambio v8: drop de m_flag, s_flag y q_flag_category. q_flag se conserva como FK a dim_quality_flag.

-- CTE snap: lee snapshot completo (vigente + reemplazadas)
with snap as (
    select * from {{ ref('snp_NOAA__noaa_raw_year') }}
)

select
    -- PK del row historico
    {{ dbt_utils.generate_surrogate_key(['scd_key', 'dbt_valid_from']) }} as observation_version_id,

    -- claves SCD2
    scd_key,
    station_id                as station_weather_id,
    observation_date,
    year(observation_date)    as observation_year,
    quarter(observation_date) as observation_quarter,
    month(observation_date)   as observation_month,
    element                   as element_code,
    q_flag,

    -- metrica
    data_value,
    obs_time,

    -- linaje SCD2
    dbt_valid_from,
    dbt_valid_to,
    dbt_updated_at,

    -- flags BI (pre-calculados; q_flag_category se resuelve via join con dim_quality_flag)
    case when dbt_valid_to is null     then true else false end as is_current,
    case when dbt_valid_to is not null then true else false end as is_superseded
from snap
