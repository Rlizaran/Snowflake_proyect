-- Gold fact: 1 row por viaje, FKs + distance_in_km (ST_DISTANCE) precalculado. Incremental MERGE por ride_id.

{{ config(
    materialized='incremental',
    unique_key='ride_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
) }}

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
    distance_in_km,

    -- linaje
    load_ts

{% if is_incremental() %}
where load_ts > (select coalesce(max(load_ts), '1900-01-01'::timestamp_ntz) from {{ this }})
{% endif %}