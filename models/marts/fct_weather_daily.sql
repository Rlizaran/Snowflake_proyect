-- Gold mart: fact diario de clima por estacion (pivot wide + denormalizacion + categorizacion).
-- Movido desde silver/NOAA/slv_weather_daily.sql porque hace agregacion (pivot por element),
-- denormalizacion (join con slv_weather_station para meter city) y reglas de negocio
-- (weather_category con umbrales del proyecto) -> patron Gold, no Silver.
-- Materializado como TABLE (default de marts en dbt_project.yml). Se consume desde PBI
-- en cada visual; precalculado evita recomputar la cadena entera por query.

with

obs as (
    select * from {{ ref('slv_weather_observation') }}
),

stations as (
    select * from {{ ref('slv_weather_station') }}
),

-- Pivot por (station, fecha) en su propio CTE para que los alias esten disponibles abajo.
-- INNER JOIN con stations filtra implicitamente a las 2 estaciones del proyecto (slv_weather_station
-- es subset). Si en el futuro se amplia la dim, este fact crece automaticamente.
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
    -- Surrogate PK
    {{ dbt_utils.generate_surrogate_key(['station_id', 'observation_date']) }} as daily_id,

    -- FKs
    station_id,        -- -> slv_weather_station
    observation_date,  -- -> slv_date / dim_date
    city,

    -- Metricas (todas en unidad real, ya escaladas en el snapshot)
    temp_max_c,
    temp_min_c,
    round((temp_max_c + temp_min_c) / 2, 2) as temp_avg_c,
    precipitation_mm,
    snowfall_mm,
    snow_depth_mm,

    -- Categorizacion derivada (regla de negocio del proyecto)
    case
        when precipitation_mm > 5 then 'rainy'
        when snowfall_mm > 0     then 'snowy'
        when temp_max_c > 25     then 'hot'
        when temp_max_c < 5      then 'cold'
        else 'mild'
    end as weather_category
from pivoted
