-- Silver wide: pivot pre-agregado de slv_weather_observation por (station_id, observation_date) - facilita joins por fecha
-- FIX: temp_avg_c referenciaba alias del mismo SELECT (no permitido en Snowflake). Movido el pivot a un CTE 'pivoted' y temp_avg_c se calcula en el SELECT final.
-- Materializado como VIEW (default del proyecto): el pivot es 2 estaciones x ~365 dias x 3 anios
-- (~2200 filas), aceptable para recomputar al vuelo. Si crece la cantidad de estaciones
-- considerar override a 'table'.

with

obs as (
    select * from {{ ref('slv_weather_observation') }}
),

stations as (
    select * from {{ ref('slv_weather_station') }}
),

-- Pivot por (station, fecha) en su propio CTE para que los alias esten disponibles abajo
pivoted as (
    select
        o.station_id,
        o.observation_date,
        s.city,
        round(max(case when o.element_code = 'TMAX' then o.data_value end)/10,2) as temp_max_c,
        round(max(case when o.element_code = 'TMIN' then o.data_value end)/10,2) as temp_min_c,
        round(max(case when o.element_code = 'PRCP' then o.data_value end)/10,2) as precipitation_mm,
        max(case when o.element_code = 'SNOW' then o.data_value end) as snowfall_mm,
        max(case when o.element_code = 'SNWD' then o.data_value end) as snow_depth_mm
    from obs o
    join stations s on o.station_id = s.station_id
    group by o.station_id, o.observation_date, s.city
)

select
    -- Surrogate PK
    {{ dbt_utils.generate_surrogate_key(['station_id', 'observation_date']) }} as daily_id,

    -- FKs
    station_id,        -- -> slv_weather_station
    observation_date,  -- -> slv_date
    city,

    -- Metricas pivoteadas (todas en grados Celsius o mm, ya escaladas)
    temp_max_c,
    temp_min_c,
    round((temp_max_c + temp_min_c) / 2, 2) as temp_avg_c,
    precipitation_mm,
    snowfall_mm,
    snow_depth_mm,

    -- Categorizacion derivada para reusar en Gold y filtros PBI
    case
        when precipitation_mm > 5 then 'rainy'
        when snowfall_mm > 0     then 'snowy'
        when temp_max_c > 25     then 'hot'
        when temp_max_c < 5      then 'cold'
        else 'mild'
    end as weather_category
from pivoted
