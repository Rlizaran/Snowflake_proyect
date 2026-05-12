-- Gold fact: passthrough de slv_trip (grano = 1 row por viaje) con FKs listas para PBI.

select
    -- PK
    t.ride_id,

    -- FKs
    t.trip_date,
    t.city_id,
    t.rideable_type_code,
    t.user_type_code,
    t.start_station_id,
    t.end_station_id,

    -- atributos viaje
    t.started_at,
    t.ended_at,
    t.trip_duration_min,
    case
        when trip_duration_min < 5  then 'short'
        when trip_duration_min < 20 then 'medium'
        when trip_duration_min < 60 then 'long'
        else 'extra_long'
    end as duration_bucket,
    ST_DISTANCE(
            ST_MAKEPOINT(start_c.canonical_lng, start_c.canonical_lat), 
            ST_MAKEPOINT(end_c.canonical_lng, end_c.canonical_lat)) as distance_in_km

from {{ ref('slv_trip') }} t
left join {{ ref('slv_station') }} start_c
    on t.start_station_id = start_c.station_id
left join {{ ref('slv_station') }} end_c
    on t.end_station_id = end_c.station_id