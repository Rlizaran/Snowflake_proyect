-- slv_trip: fact normalizado, un row por viaje (NY+JC unidos) con FKs a dims. Incremental MERGE por ride_id.
-- Enrichment: trip_duration_min (datediff) y distance_in_km (ST_DISTANCE sobre coords canonicas de slv_station).
-- distance_in_km = NULL si start = end (round trip) o si excede 500 (outlier de coords corruptas).

{{ config(
    materialized='incremental',
    unique_key='ride_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

with trips as (
    select * from {{ ref('stg_CityBike__citybike_trips') }}
    {% if is_incremental() %}
        where load_ts > (select max(load_ts) from {{ this }})
    {% endif %}
),

deduped as (
    select * from trips
    qualify row_number() over (
        partition by ride_id
        order by load_ts desc, started_at desc
    ) = 1
),

stations as (
    select
        station_id,
        canonical_lat,
        canonical_lng
    from {{ ref('slv_station') }}
),

enriched as (
    select
        t.ride_id,
        date(t.started_at)                              as trip_date,
        t.started_at,
        t.ended_at,
        datediff('minute', t.started_at, t.ended_at)    as trip_duration_min,
        t.rideable_type,
        t.member_casual,
        t.city,
        t.start_station_id,
        t.end_station_id,
        ST_DISTANCE(
            ST_MAKEPOINT(s_start.canonical_lng, s_start.canonical_lat),
            ST_MAKEPOINT(s_end.canonical_lng,   s_end.canonical_lat)
        ) / 1000                                        as dist_km_raw,
        t.source_file,
        t.load_ts
    from deduped t
    left join stations s_start on t.start_station_id = s_start.station_id
    left join stations s_end   on t.end_station_id   = s_end.station_id
)

select
    ride_id,
    trip_date,
    started_at,
    ended_at,
    trip_duration_min,
    {{ dbt_utils.generate_surrogate_key(['rideable_type']) }} as rideable_type_code,
    {{ dbt_utils.generate_surrogate_key(['member_casual']) }} as user_type_code,
    start_station_id,
    end_station_id,
    case
        when start_station_id = end_station_id then null
        when dist_km_raw > 500                 then null
        when dist_km_raw < 0                   then null
        else round(dist_km_raw, 2)
    end as distance_in_km,
    {{ dbt_utils.generate_surrogate_key(['city']) }} as city_id,
    source_file,
    load_ts
from enriched
