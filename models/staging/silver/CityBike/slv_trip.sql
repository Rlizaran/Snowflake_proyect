-- slv_trip: fact normalizado, un row por viaje (NY+JC unidos) con FKs a dims. Incremental MERGE por ride_id.
-- distance_in_km = NULL si start = end (round trip) o si excede 500 (outlier de coords corruptas).

{{ config(
    materialized='incremental',
    unique_key='ride_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
) }}

with trips as (
    select * from {{ ref('stg_CityBike__citybike_trips') }}
    {% if is_incremental() %}
        where load_ts > (select coalesce(max(load_ts), '1900-01-01'::timestamp_ntz) from {{ this }})
    {% endif %}
),

deduplicated as (
    select * from trips
    qualify row_number() over (
        partition by ride_id
        order by load_ts desc, started_at desc
    ) = 1
),

enriched as (
    select
        t.*,
        ST_DISTANCE(
            ST_MAKEPOINT(start_c.canonical_lng, start_c.canonical_lat),
            ST_MAKEPOINT(end_c.canonical_lng,   end_c.canonical_lat)
        ) as dist_raw
    from deduplicated t
    left join {{ ref('slv_station') }} start_c
        on t.start_station_id = start_c.station_id
    left join {{ ref('slv_station') }} end_c
        on t.end_station_id   = end_c.station_id
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

    -- distancia limpia (NULL si round-trip o outlier > 500)
    case
        when start_station_id = end_station_id then null
        when dist_raw > 500                    then null
        when dist_raw <= 0                     then null
        else dist_raw
    end as distance_in_km,

    -- FK ciudad
    {{ dbt_utils.generate_surrogate_key(['city']) }} as city_id,

    -- linaje
    source_file,
    load_ts
from enriched
