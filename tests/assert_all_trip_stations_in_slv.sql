with all_trip_stations as (

    select start_station_name as station_name from {{ ref('stg_CityBike__citybike_trips_jc') }}
    union
    select end_station_name from {{ ref('stg_CityBike__citybike_trips_jc') }}
    
    union

    select start_station_name from {{ ref('stg_CityBike__citybike_trips_ny') }}
    union
    select end_station_name from {{ ref('stg_CityBike__citybike_trips_ny') }}
),

missing_stations as (
    select station_name 
    from all_trip_stations
    where station_name is not null
    
    except
    
    select station_name 
    from {{ ref('slv_station') }}
)

select * from missing_stations