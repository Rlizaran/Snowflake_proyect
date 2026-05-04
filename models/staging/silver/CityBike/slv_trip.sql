-- Silver fact normalizado: un row por viaje (NY+JC unidos), con FKs explicitas a estaciones, tipos y fecha

with

ny as (
    select * from {{ ref('stg_CityBike__citybike_trips_ny') }}
),

jc as (
    select * from {{ ref('stg_CityBike__citybike_trips_jc') }}
),

unioned as (
    select * from ny
    union all
    select * from jc
)

select
    -- PK natural (Citi Bike garantiza unicidad global del ride_id en sus dos sistemas)
    ride_id,

    -- FK a slv_date
    date(started_at) as trip_date,

    -- Atributos del viaje
    started_at,
    ended_at,
    trip_duration_min,
    trip_distance_km,

    -- FKs a lookups y dimensiones
    rideable_type as rideable_type_code,
    member_casual as user_type_code,
    start_station_id,
    end_station_id,

    -- Atributo de origen (no FK, solo categoria)
    city,

    -- Linaje
    source_file,
    load_ts
from unioned
