-- Snapshot SCD2 sobre noaa_raw_year (Bronze).
-- NOAA reescribe el archivo del anio cuando publica correcciones de q_flag / data_value.
-- Strategy 'check' sobre las cols que cambian (data_value, q_flag, m_flag, s_flag).
-- Cluster por year(observation_date): anios cerrados ya no mutan

{% snapshot snp_NOAA__noaa_raw_year %}

{{
    config(
        target_database=env_var('DBT_ENVIRONMENTS', 'FAIL') ~ '_CITYBIKE_SILVER',
        target_schema='snapshots',
        unique_key='scd_key',
        strategy='check',
        check_cols=['data_value', 'q_flag_category'],
        invalidate_hard_deletes=False,
        cluster_by=['year(observation_date)'],
        transient=False
    )
}}

with src as (
    select
        trim(station_id) || '|' || trim(observation_date) || '|' || trim(element) as scd_key,
        trim(station_id) as station_id,
        to_date(observation_date, 'YYYYMMDD') as observation_date,
        trim(element) as element,
        case
            when trim(element) in ('TMAX','TMIN','PRCP','AWND','WSF2','WSF5')
                then round(try_to_decimal(data_value, 18, 2) / 10, 2)
            else try_to_decimal(data_value, 18, 2)
        end as data_value,
        trim(m_flag) as m_flag,
        trim(q_flag) as q_flag,
        trim(s_flag) as s_flag,
        -- Categoria del q_flag segun codebook NOAA GHCN-Daily.
        -- solo cambios de categoria disparan version SCD2.
        case
            when trim(q_flag) in ('Z','G')             then 'OK'
            when trim(q_flag) = 'S'                    then 'SUSPECT'
            when trim(q_flag) in ('I','X')             then 'INVALID'
            when trim(q_flag) in ('M','R','D','T','N') then 'PROCESSING'
            when trim(q_flag) in ('L','O','K','W')     then 'METADATA'
            else 'UNKNOWN'
        end as q_flag_category,
        coalesce(try_cast(obs_time as int), 2400) as obs_time,
        source_file,
        load_ts
    from {{ source('NOAA', 'noaa_raw_year') }}
    where station_id is not null
      and to_date(observation_date, 'YYYYMMDD') >= TO_DATE('20240101', 'YYYYMMDD')
      and trim(element) in ('TMAX','TMIN','PRCP','SNOW','AWND','SNWD','WSF2','WSF5')
)

-- conserva la fila mas reciente por scd_key
select *
from src
qualify row_number() over (partition by scd_key order by load_ts desc) = 1

{% endsnapshot %}