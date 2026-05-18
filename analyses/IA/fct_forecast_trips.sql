{{ config(
    materialized='table',
    schema='analytics'
) }}

with raw_forecast as (
    select * from {{ source('snowflake_ia', 'pronostico_final') }}
),

formatted_forecast as (
    select 
        split_part(SERIES, '|', 1) as city_id,
        split_part(SERIES, '|', 2) as rideable_type_code,
        split_part(SERIES, '|', 3) as user_type_code,
        TS::date as trip_date,
        round(FORECAST, 0) as predicted_n_trips,
        round(LOWER_BOUND, 0) as min_expected,
        round(UPPER_BOUND, 0) as max_expected
    from raw_forecast
)

select 
    f.anio,
    f.nombre_mes,
    f.dia_semana,
    f.es_fin_semana,
    f.estacion,
    c.city,
    b.rideable_type,
    u.member_casual,
    ai.predicted_n_trips,
    ai.min_expected,
    ai.max_expected
from formatted_forecast ai
left join {{ ref('dim_city') }} c 
    on c.city_id = ai.city_id
left join {{ ref('dim_rideable_bike') }} b 
    on b.rideable_type_code = ai.rideable_type_code
left join {{ ref('dim_user_type') }} u 
    on u.user_type_code = ai.user_type_code
inner join {{ ref('dim_fecha') }} f 
    on f.fecha_id::date = ai.trip_date
where ai.trip_date < '2027-01-01'