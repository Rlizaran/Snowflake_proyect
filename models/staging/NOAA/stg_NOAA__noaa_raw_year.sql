{{
  config(
    snowflake_warehouse='WH_ANALISIS'
  )
}}

with 

source as (

    select * from {{ source('NOAA', 'noaa_raw_year') }}

),

renamed as (

    select
        trim(station_id) as station_id,
        to_date(observation_date, 'YYYYMMDD') as observation_date,
        trim(element) as element,
        try_to_decimal(data_value, 18, 2) as data_value,
        trim(m_flag) as m_flag,
        trim(q_flag) as q_flag,
        trim(s_flag) as s_flag,
        obs_time,
        source_file,
        load_ts

    from source

),

cleaned as (
    select
        *
    from renamed
    where station_id in ('USW00094728', 'USW00014734')
      and observation_date >= TO_DATE(20240101::VARCHAR, 'YYYYMMDD')
      and element in ('TMAX', 'TMIN', 'PRCP', 'SNOW', 'AWND', 'SNWD', 'WSF2', 'WSF5')
      and obs_time is not null
)

select * from cleaned