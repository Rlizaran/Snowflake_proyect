-- Gold fact: clima diario por estacion (pivot wide de elementos + categorizacion).

with
obs as (
    select * from {{ ref('slv_weather_observation') }}
),

stations as (
    select * from {{ ref('slv_weather_station') }}
),

pivoted as (
    select
        o.station_id,
        o.observation_date,
        s.city,
        max(case when o.element_code = 'TMAX' then o.data_value end) as temp_max_c,
        max(case when o.element_code = 'TMIN' then o.data_value end) as temp_min_c,
        max(case when o.element_code = 'PRCP' then o.data_value end) as precipitation_mm,
        max(case when o.element_code = 'SNOW' then o.data_value end) as snowfall_mm,
        max(case when o.element_code = 'SNWD' then o.data_value end) as snow_depth_mm
    from obs o
    join stations s on o.station_id = s.station_id
    group by o.station_id, o.observation_date, s.city
)

select
    -- PK surrogate
    {{ dbt_utils.generate_surrogate_key(['station_id', 'observation_date']) }} as daily_id,

    -- FKs
    station_id,
    observation_date,
    city,

    -- metricas
    temp_max_c,
    temp_min_c,
    round((temp_max_c + temp_min_c) / 2, 2) as temp_avg_c,
    precipitation_mm,
    snowfall_mm,
    snow_depth_mm,

    -- categorizacion
    case
        when precipitation_mm > 5 then 'rainy'
        when snowfall_mm > 0      then 'snowy'
        when temp_max_c > 25      then 'hot'
        when temp_max_c < 5       then 'cold'
        else 'mild'
    end as weather_category
from pivoted
