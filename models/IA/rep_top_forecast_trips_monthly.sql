-- rep_top_forecast_trips_monthly: pico mensual (top-1) de viajes predichos por ciudad. Override a view.

{{ config(materialized='view') }}

with forecast_base as (
    select * from {{ ref('fct_forecast_trips') }}
),

ranked_forecast as (
    select 
        *,
        row_number() over (
            partition by city, nombre_mes 
            order by predicted_n_trips desc
        ) as ranking
    from forecast_base
    where city in ('Manhattan', 'Jersey City')
)

select 
    anio,
    nombre_mes,
    dia_semana,
    es_fin_semana,
    estacion,
    city,
    rideable_type,
    member_casual,
    predicted_n_trips,
    max_expected,
    min_expected
from ranked_forecast
where ranking = 1
order by city asc, nombre_mes asc