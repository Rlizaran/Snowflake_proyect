with 

source as (

    select * from {{ source('NOAA', 'noaa_raw_year') }}

),

renamed as (

    select
        station_id,
        observation_date,
        element,
        data_value,
        m_flag,
        q_flag,
        s_flag,
        obs_time,
        source_file,
        load_ts

    from source

)

select * from renamed