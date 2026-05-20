-- slv_station: estaciones unicas (NY + JC) con nombre y coords canonicos derivados de stg.
-- Materializado table: el union all + group by + row_number sobre stg (millones de filas) es caro de recomputar en cada query.

{{ config(
    materialized='incremental',
    unique_key='station_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
) }}

with trips as (
    select * from {{ ref('stg_CityBike__citybike_trips') }}
    {% if is_incremental() %}
        where load_ts > (select coalesce(max(last_seen), '1900-01-01'::timestamp_ntz) from {{ this }})
    {% endif %}
),

all_stations_raw as (
    select
        start_station_id   as station_id,
        start_station_name as station_name,
        round(start_lat, 4) as lat,
        round(start_lng, 4) as lng,
        started_at         as activity_at
    from trips
    union all
    select
        end_station_id,
        end_station_name,
        round(end_lat, 4),
        round(end_lng, 4),
        ended_at
    from trips
),

station_counts as (
    select
        station_id,
        station_name,
        lat,
        lng,
        count(*)         as frequency,
        max(activity_at) as last_seen
    from all_stations_raw
    group by 1, 2, 3, 4
),

ranked_stations as (
    select
        *,
        row_number() over (
            partition by station_id
            order by frequency desc, last_seen desc
        ) as rn
    from station_counts
)

select
    station_id,
    station_name as canonical_name,
    lat          as canonical_lat,
    lng          as canonical_lng,
    last_seen
from ranked_stations
where rn = 1
