-- Gold fact: 1 row por viaje, solo IDs + metricas numericas. Incremental MERGE por ride_id.
-- Cluster by year(trip_date): ~100M filas, Power BI filtra por fecha; el pruning por anio acelera.

{{ config(
    materialized='incremental',
    unique_key='ride_id',
    incremental_strategy='merge',
    on_schema_change='fail',
    cluster_by=['year(trip_date)']
) }}

select
    ride_id,
    trip_date,
    city_id,
    rideable_type_code,
    user_type_code,
    start_station_id,
    end_station_id,
    trip_duration_min,
    distance_in_km
from {{ ref('slv_trip') }}

{% if is_incremental() %}
where trip_date >= (
    select coalesce(dateadd(day, -7, max(trip_date)), '1900-01-01'::date)
    from {{ this }}
)
{% endif %}
