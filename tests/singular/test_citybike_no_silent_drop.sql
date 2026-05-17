-- Test: ride_ids validos en bronze deben existir en slv_trip (detecta drops silenciosos).

with bronze_valid_ny as (
    select trim(ride_id) as ride_id
    from {{ source('CityBike', 'citybike_trips_ny') }}
    where ride_id is not null
      and try_to_timestamp_ntz(started_at) is not null
      and try_to_timestamp_ntz(started_at) >= '2024-01-01'::timestamp_ntz
      and try_to_timestamp_ntz(ended_at) is not null
      and try_to_timestamp_ntz(ended_at) > try_to_timestamp_ntz(started_at)
      and start_station_id is not null
      and start_station_id not ilike '%SYS%'
      and end_station_id is not null
      and end_station_id not ilike '%SYS%'
      and start_lat between 40.67 and 40.88
      and start_lng between -74.05 and -73.90
      and end_lat between 40.67 and 40.88
      and end_lng between -74.05 and -73.90
      and lower(trim(rideable_type)) in ('classic_bike','electric_bike')
      and lower(trim(member_casual)) in ('member','casual')
),

bronze_valid_jc as (
    select trim(ride_id) as ride_id
    from {{ source('CityBike', 'citybike_trips_jc') }}
    where ride_id is not null
      and try_to_timestamp_ntz(started_at) is not null
      and try_to_timestamp_ntz(started_at) >= '2024-01-01'::timestamp_ntz
      and try_to_timestamp_ntz(ended_at) is not null
      and try_to_timestamp_ntz(ended_at) > try_to_timestamp_ntz(started_at)
      and start_station_id is not null
      and start_station_id not ilike '%SYS%'
      and end_station_id is not null
      and end_station_id not ilike '%SYS%'
      and lower(trim(rideable_type)) in ('classic_bike','electric_bike')
      and lower(trim(member_casual)) in ('member','casual')
      and start_lat between 40.67 and 40.88
      and start_lng between -74.05 and -73.90
      and end_lat between 40.67 and 40.88
      and end_lng between -74.05 and -73.90
),

bronze_all as (
    select ride_id from bronze_valid_ny
    union all
    select ride_id from bronze_valid_jc
)

select bv.ride_id
from bronze_all bv
left join {{ ref('slv_trip') }} sv on bv.ride_id = sv.ride_id
where sv.ride_id is null
