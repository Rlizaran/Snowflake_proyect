-- Gold fact: 1 row por viaje, FKs + distance_in_km (ST_DISTANCE) precalculado. Incremental MERGE por ride_id.

{{ config(
    materialized='incremental',
    unique_key='ride_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
) }}

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
    end                                                                  as duration_bucket,
    ST_DISTANCE(
        ST_MAKEPOINT(start_c.canonical_lng, start_c.canonical_lat),
        ST_MAKEPOINT(end_c.canonical_lng,   end_c.canonical_lat)
    )                                                                    as distance_in_km,

    -- linaje
    t.load_ts

from {{ ref('slv_trip') }} t
left join {{ ref('slv_station') }} start_c
    on t.start_station_id = start_c.station_id
left join {{ ref('slv_station') }} end_c
    on t.end_station_id   = end_c.station_id

{% if is_incremental() %}
where t.load_ts > (select coalesce(max(load_ts), '1900-01-01'::timestamp_ntz) from {{ this }})
{% endif %}