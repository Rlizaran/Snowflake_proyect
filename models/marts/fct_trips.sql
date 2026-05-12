-- Gold fact: passthrough de slv_trip (grano = 1 row por viaje) con FKs listas para PBI.

select
    -- PK
    ride_id,

    -- FKs
    trip_date,
    city_id,
    rideable_type_code,
    user_type_code,
    start_station_id,
    end_station_id,

    -- atributos viaje
    started_at,
    ended_at,
    trip_duration_min,
    case
        when trip_duration_min < 5  then 'short'
        when trip_duration_min < 20 then 'medium'
        when trip_duration_min < 60 then 'long'
        else 'extra_long'
    end as duration_bucket,

    -- linaje
    source_file,
    load_ts
from {{ ref('slv_trip') }}
