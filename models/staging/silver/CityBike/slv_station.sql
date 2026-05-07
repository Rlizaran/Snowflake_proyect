

with combined_trips as (
    select 
        start_station_id as station_id, 
        start_station_name as station_name, 
        start_lat as lat, 
        start_lng as lng, 
        city
    from {{ ref('stg_CityBike__citybike_trips_ny') }}
    union all 
    select 
        end_station_id, 
        end_station_name, 
        end_lat, 
        end_lng, 
        city
    from {{ ref('stg_CityBike__citybike_trips_ny') }}
    union all
    select 
        start_station_id, 
        start_station_name, 
        start_lat, 
        start_lng, 
        city
    from {{ ref('stg_CityBike__citybike_trips_jc') }}
    union all
    select 
        end_station_id, 
        end_station_name, 
        end_lat, 
        end_lng, 
        city
    from {{ ref('stg_CityBike__citybike_trips_jc') }}
)
select distinct
    station_id,
    station_name,
    lat,
    lng,
    city
from combined_trips
where station_id is not null

