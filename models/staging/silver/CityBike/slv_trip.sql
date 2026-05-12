-- slv_trip: fact normalizado, un row por viaje (NY+JC unidos) con FKs a dims.

{{ config(materialized='table') }}

with trips as (
    select * from {{ ref('stg_CityBike__citybike_trips') }}
),

deduplicated as (
    select * from trips
    qualify row_number() over (
        partition by ride_id
        order by load_ts desc, started_at desc
    ) = 1
)

select
    -- PK
    ride_id,

    -- FK fecha
    date(started_at) as trip_date,

    -- atributos viaje
    started_at,
    ended_at,
    trip_duration_min,

    -- FKs dimensiones
    {{ dbt_utils.generate_surrogate_key(['rideable_type']) }} as rideable_type_code,
    {{ dbt_utils.generate_surrogate_key(['member_casual']) }} as user_type_code,
    start_station_id,
    end_station_id,
    {{ dbt_utils.generate_surrogate_key(['city']) }} as city_id,

    -- linaje
    source_file,
    load_ts
from deduplicated
