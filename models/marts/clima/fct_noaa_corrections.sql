-- Gold fact: historico SCD2 NOAA expuesto para BI (todas las versiones del snapshot).
-- Materializado table (full refresh en cada dbt run) para reflejar siempre el snapshot al 100%.
-- Cluster by year(observation_date) ayuda a Power BI cuando filtra historia por anio/mes.

{{ config(
    materialized='table',
    cluster_by=['year(observation_date)']
) }}

with snap as (
    select * from {{ ref('snp_NOAA__noaa_raw_year') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['scd_key', 'dbt_valid_from']) }} as observation_version_id,
    scd_key,
    station_id                as station_weather_id,
    observation_date,
    year(observation_date)    as observation_year,
    quarter(observation_date) as observation_quarter,
    month(observation_date)   as observation_month,
    element                   as element_code,
    q_flag,
    data_value,
    obs_time,
    dbt_valid_from,
    dbt_valid_to,
    dbt_updated_at,
    case when dbt_valid_to is null     then true else false end as is_current,
    case when dbt_valid_to is not null then true else false end as is_superseded
from snap
