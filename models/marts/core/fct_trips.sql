-- Gold fact: 1 row por viaje, solo IDs + metricas numericas. Incremental MERGE por ride_id.

{{ config(
    materialized='incremental',
    unique_key='ride_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
) }}

select
    -- PK
    ride_id,

    -- FKs a dims
    trip_date,
    city_id,
    rideable_type_code,
    user_type_code,
    start_station_id,
    end_station_id,

    -- metricas
    trip_duration_min,
    distance_in_km

from {{ ref('slv_trip') }}

{% if is_incremental() %}
where trip_date >= (
    select coalesce(dateadd(day, -7, max(trip_date)), '1900-01-01'::date)
    from {{ this }}
)
{% endif %}
