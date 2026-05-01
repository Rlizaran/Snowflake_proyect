with 

source as (

    select * from {{ source('CityBike', 'citybike_trips_ny') }}

),

renamed as (

    select
        ride_id,
        rideable_type,
        started_at,
        ended_at,
        start_station_name,
        start_station_id,
        end_station_name,
        end_station_id,
        start_lat,
        start_lng,
        end_lat,
        end_lng,
        member_casual,
        source_file,
        load_ts

    from source

)

select * from renamed