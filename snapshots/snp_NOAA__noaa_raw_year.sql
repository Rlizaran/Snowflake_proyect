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
        transient=false
    )
}}

with raw_src as (
    select
        upper(trim(station_id)) || '|' || trim(observation_date) || '|' || upper(trim(element)) as scd_key,
        upper(trim(station_id))                                                                  as station_id,
        to_date(observation_date, 'YYYYMMDD')                                                    as observation_date,
        upper(trim(element))                                                                     as element,
        -- Elementos en decimas en bronze (NOAA GHCN-Daily): se escalan a unidad real (Celsius / mm / m/s).
        case
            when upper(trim(element)) in ('TMAX','TMIN','PRCP','AWND','WSF2','WSF5')
                then round(try_to_decimal(data_value, 18, 2) / 10, 2)
            else try_to_decimal(data_value, 18, 2)
        end::decimal(18,2)                                                                       as data_value,
        trim(q_flag)                                                                             as q_flag,
        -- q_flag_category vive aqui porque la SCD2 strategy 'check' la necesita en check_cols.
        case
            when coalesce(trim(q_flag), '') in ('Z','G', '') then 'OK'
            when trim(q_flag) = 'S'                          then 'SUSPECT'
            when trim(q_flag) in ('I','X')                   then 'INVALID'
            when trim(q_flag) in ('M','R','D','T','N')       then 'PROCESSING'
            when trim(q_flag) in ('L','O','K','W')           then 'METADATA'
            else 'UNKNOWN'
        end                                                                                      as q_flag_category,
        coalesce(try_cast(obs_time as int), 2400)                                                as obs_time,
        source_file,
        load_ts
    from {{ source('NOAA', 'noaa_raw_year') }}
    where station_id is not null
      and to_date(observation_date, 'YYYYMMDD') >= TO_DATE('20240101', 'YYYYMMDD')
      and upper(trim(element)) in ('TMAX','TMIN','PRCP','SNOW','AWND','SNWD','WSF2','WSF5')
),

deduped as (
    select *
    from raw_src
    qualify row_number() over (partition by scd_key order by load_ts desc nulls last) = 1
)

select * from deduped

{% endsnapshot %}
