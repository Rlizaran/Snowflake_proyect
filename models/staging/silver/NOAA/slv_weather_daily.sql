-- Silver wide: pivot pre-agregado de slv_weather_observation por (station_id, observation_date) - facilita joins por fecha
-- Surrogate key (daily_id) generado con dbt_utils para PK simple

with

obs as (
    select * from {{ ref('slv_weather_observation') }}
),

stations as (
    select * from {{ ref('slv_weather_station') }}
)

select
    -- Surrogate PK
    {{ dbt_utils.generate_surrogate_key(['o.station_id', 'o.observation_date']) }} as daily_id,

    -- FKs
    o.station_id,                                                                             -- -> slv_weather_station
    o.observation_date,                                                                       -- -> slv_date
    s.city,

    -- Metricas pivoteadas (todas en grados Celsius o mm, ya escaladas en stg)
    max(case when o.element_code = 'TMAX' then o.data_value end)                              as temp_max_c,
    max(case when o.element_code = 'TMIN' then o.data_value end)                              as temp_min_c,
    max(case when o.element_code = 'TAVG' then o.data_value end)                              as temp_avg_c,
    max(case when o.element_code = 'PRCP' then o.data_value end)                              as precipitation_mm,
    max(case when o.element_code = 'SNOW' then o.data_value end)                              as snowfall_mm,
    max(case when o.element_code = 'SNWD' then o.data_value end)                              as snow_depth_mm,

    -- Categorizacion derivada para reusar en Gold y filtros PBI
    case
        when max(case when o.element_code = 'PRCP' then o.data_value end) > 5  then 'rainy'
        when max(case when o.element_code = 'SNOW' then o.data_value end) > 0  then 'snowy'
        when max(case when o.element_code = 'TMAX' then o.data_value end) > 25 then 'hot'
        when max(case when o.element_code = 'TMAX' then o.data_value end) < 5  then 'cold'
        else                                                                        'mild'
    end                                                                                       as weather_category
from   obs o
join   stations s on o.station_id = s.station_id
group by o.station_id, o.observation_date, s.city
