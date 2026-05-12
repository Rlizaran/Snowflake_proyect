-- Gold fact: viajes agregados por (trip_date x city x rideable_type x user_type). Incremental MERGE con ventana de 7 dias.

{{ config(
    materialized='incremental',
    unique_key='daily_trip_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
) }}

with trips as (
    select * from {{ ref('slv_trip') }}
    {% if is_incremental() %}
        where trip_date >= (
            select coalesce(dateadd(day, -7, max(trip_date)), '1900-01-01'::date)
            from {{ this }}
        )
    {% endif %}
)

select
    -- PK
    {{ dbt_utils.generate_surrogate_key([
        'trip_date',
        'city_id',
        'rideable_type_code',
        'user_type_code'
    ]) }} as daily_trip_id,

    -- FKs
    trip_date,
    city_id,
    rideable_type_code,
    user_type_code,

    -- series key (para ML.FORECAST: SERIES_COLNAME = 'series_key')
    city_id || '|' || rideable_type_code || '|' || user_type_code as series_key,

    -- metricas
    count(*)                                  as n_trips,
    avg(trip_duration_min)::decimal(10,2)     as avg_duration_min,
    sum(trip_duration_min)                    as total_duration_min,
    min(trip_duration_min)                    as min_duration_min,
    max(trip_duration_min)                    as max_duration_min,
    median(trip_duration_min)::decimal(10,2)  as median_duration_min
from trips
group by trip_date, city_id, rideable_type_code, user_type_code
