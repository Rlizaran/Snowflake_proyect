-- Gold fact: cruce diario rides + clima por ciudad. Incremental MERGE con ventana de 7 dias.

{{ config(
    materialized='incremental',
    unique_key='trip_weather_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
) }}

with daily_trips as (
    select
        t.trip_date,
        c.city as city_name,
        count(*) as n_trips,
        sum(case when ut.member_casual = 'member'        then 1 else 0 end) as n_trips_member,
        sum(case when ut.member_casual = 'casual'        then 1 else 0 end) as n_trips_casual,
        sum(case when rb.rideable_type = 'classic_bike'  then 1 else 0 end) as n_trips_classic,
        sum(case when rb.rideable_type = 'electric_bike' then 1 else 0 end) as n_trips_electric,
        avg(t.trip_duration_min)::decimal(10,2)                             as avg_duration_min
    from {{ ref('slv_trip') }}          t
    join {{ ref('slv_city') }}          c  on t.city_id = c.city_id
    join {{ ref('slv_user_type') }}     ut on t.user_type_code = ut.user_type_code
    join {{ ref('slv_rideable_type') }} rb on t.rideable_type_code = rb.rideable_type_code
    {% if is_incremental() %}
    where t.trip_date >= (
        select coalesce(dateadd(day, -7, max(trip_date)), '1900-01-01'::date)
        from {{ this }}
    )
    {% endif %}
    group by t.trip_date, c.city
),

city_to_station as (
    select 'Manhattan' as city_name, 'USW00094728' as station_id
    union all
    select 'Jersey City' as city_name, 'USW00014734' as station_id
),

weather as (
    select * from {{ ref('fct_weather_daily') }}
)

select
    -- PK
    {{ dbt_utils.generate_surrogate_key(['dt.trip_date', 'dt.city_name']) }} as trip_weather_id,

    -- FKs
    dt.trip_date,
    dt.city_name,
    cs.station_id,

    -- metricas viajes
    dt.n_trips,
    dt.n_trips_member,
    dt.n_trips_casual,
    dt.n_trips_classic,
    dt.n_trips_electric,
    dt.avg_duration_min,

    -- metricas clima
    w.temp_max_c,
    w.temp_min_c,
    w.temp_avg_c,
    w.precipitation_mm,
    w.snowfall_mm,
    w.snow_depth_mm,
    w.weather_category
from daily_trips dt
join city_to_station cs 
    on dt.city_name = cs.city_name
left join weather  w  
    on w.station_id = cs.station_id
    and w.observation_date  = dt.trip_date
