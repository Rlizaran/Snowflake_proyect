-- Snapshot SCD2 sobre noaa_raw_year. Strategy 'check' sobre [data_value, q_flag_category].

{% snapshot snp_NOAA__noaa_raw_year %}

{{
    config(
        target_database=env_var('DBT_ENVIRONMENTS', 'FAIL') ~ '_CITYBIKE_SILVER',
        target_schema='snapshots',
        unique_key='scd_key',
        strategy='check',
        check_cols=['data_value', 'q_flag_category'],
        invalidate_hard_deletes=false,
        cluster_by=['year(observation_date)'],
        transient=false
    )
}}

with raw_src as (
    select
        -- PK
        upper(trim(noaa.station_id)) || '|' || trim(noaa.observation_date) || '|' || upper(trim(noaa.element)) as scd_key,

        -- atributos
        upper(trim(noaa.station_id))                                                                  as station_id,
        to_date(noaa.observation_date, 'YYYYMMDD')                                                    as observation_date,
        upper(trim(noaa.element))                                                                     as element,
        case
            when upper(trim(noaa.element)) in ('TMAX','TMIN','PRCP','AWND','WSF2','WSF5')
                then round(try_to_decimal(noaa.data_value, 18, 2) / 10, 2)
            else try_to_decimal(noaa.data_value, 18, 2)
        end::decimal(18,2)                                                                       as data_value,
        trim(noaa.m_flag)                                                                             as m_flag,
        trim(noaa.q_flag)                                                                             as q_flag,
        trim(noaa.s_flag)                                                                             as s_flag,
        case
            when coalesce(trim(noaa.q_flag), '') in ('Z','G', '') then 'OK'
            when trim(noaa.q_flag) = 'S'                          then 'SUSPECT'
            when trim(noaa.q_flag) in ('I','X')                   then 'INVALID'
            when trim(noaa.q_flag) in ('M','R','D','T','N')       then 'PROCESSING'
            when trim(noaa.q_flag) in ('L','O','K','W')           then 'METADATA'
            else 'UNKNOWN'
        end                                                                                      as q_flag_category,
        coalesce(try_cast(noaa.obs_time as int), 2400)                                                as obs_time,

        -- linaje
        noaa.source_file,
        noaa.load_ts
    from {{ source('NOAA', 'noaa_raw_year') }} noaa
    inner join {{ ref('weather_station_us') }} ws
    on noaa.station_id = ws.station_id
    where noaa.station_id is not null
      and to_date(noaa.observation_date, 'YYYYMMDD') >= TO_DATE('20240101', 'YYYYMMDD')
      and upper(trim(noaa.element)) in ('TMAX','TMIN','PRCP','SNOW','AWND','SNWD','WSF2','WSF5')
),

deduped as (
    select *
    from raw_src
    qualify row_number() over (partition by scd_key order by load_ts desc nulls last) = 1
)

select * from deduped

{% endsnapshot %}