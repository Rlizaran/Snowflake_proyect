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
        try_to_time(obs_time, 'HH24MI') as obs_time,
        source_file,
        load_ts

    from source

)

select * from renamed