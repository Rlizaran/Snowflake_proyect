-- Stg JC: cast + clean + enriquecimiento de viajes Jersey City, incremental MERGE.
-- Sin cambios de estrategia (ya era merge). Anadido 'qualify row_number()' como defensa
-- in-batch: si bronze trae varias versiones del mismo ride_id en una sola tanda, el MERGE
-- falla con error "duplicate key" (Snowflake exige 1 fila por unique_key en el origen).
{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='ride_id',
    merge_update_columns=['rideable_type','started_at','ended_at','trip_duration_min','start_station_name','start_station_id','end_station_name','end_station_id','start_lat','start_lng','end_lat','end_lng','trip_distance_km','member_casual','source_file','load_ts']
  )
}}

with source as (

    select * from {{ source('CityBike', 'citybike_trips_jc') }}

    {% if is_incremental() %}
        -- Filtra bronze por load_ts reciente para reducir el set a mergear
        where load_ts > (select coalesce(max(load_ts), '1900-01-01'::timestamp_ntz) from {{ this }})
    {% endif %}

),

casted as (

    select
        trim(ride_id) as ride_id,
        lower(trim(rideable_type)) as rideable_type,
        try_to_timestamp_ntz(started_at) as started_at,
        try_to_timestamp_ntz(ended_at) as ended_at,
        trim(start_station_name) as start_station_name,
        trim(start_station_id) as start_station_id,
        trim(end_station_name) as end_station_name,
        trim(end_station_id) as end_station_id,
        try_to_decimal(start_lat, 10, 6) as start_lat,
        try_to_decimal(start_lng, 10, 6) as start_lng,
        try_to_decimal(end_lat, 10, 6) as end_lat,
        try_to_decimal(end_lng, 10, 6) as end_lng,
        lower(trim(member_casual)) as member_casual,
        source_file,
        load_ts

    from source

),

cleaned as (
    select *
    from casted
    where ride_id is not null
      and started_at is not null
      and started_at >= '2024-01-01'::timestamp_ntz
      and ended_at is not null
      and ended_at > started_at
      and end_station_id is not null
      and rideable_type in ('classic_bike', 'electric_bike')
      and member_casual in ('member', 'casual')
),

-- Defensa: dedupe in-batch antes del MERGE (Snowflake exige 1 fila por unique_key en source)
deduped as (
    select * from cleaned
    qualify row_number() over (partition by ride_id order by load_ts desc) = 1
),

enriched as (
    select
        ride_id,
        rideable_type,
        started_at,
        ended_at,
        datediff('minute', started_at, ended_at) as trip_duration_min,
        start_station_name,
        start_station_id,
        end_station_name,
        end_station_id,
        start_lat,
        start_lng,
        end_lat,
        end_lng,
        round(st_distance(st_makepoint(start_lng, start_lat), st_makepoint(end_lng,end_lat))/1000.0, 3) as trip_distance_km,
        member_casual,
        'JC' as city,
        source_file,
        load_ts
    from deduped
)

select * from enriched
