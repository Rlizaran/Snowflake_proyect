-- Gold fact: clima diario por estacion (pivot wide de elementos + categorizacion).
-- Incremental MERGE con ventana de 7 dias sobre observation_date (mismo patron que fct_trips_weather).

{{ config(
    materialized='incremental',
    unique_key='daily_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
) }}

with
-- CTE obs: filtra ventana incremental sobre slv_weather_observation
obs as (
    select * from {{ ref('slv_weather_observation') }}
    {% if is_incremental() %}
        where observation_date >= (
            select coalesce(dateadd(day, -7, max(observation_date)), '1900-01-01'::date)
            from {{ this }}
        )
    {% endif %}
),

stations as (
    select * from {{ ref('dim_station_weather') }}
),

-- CTE pivoted: pivot wide de elementos NOAA por (station, fecha)
pivoted as (
    select
        o.station_id as station_weather_id,
        o.observation_date,
        s.city_id,
        max(case when o.element_code = 'TMAX' then o.data_value end) as temp_max_c,
        max(case when o.element_code = 'TMIN' then o.data_value end) as temp_min_c,
        max(case when o.element_code = 'PRCP' then o.data_value end) as precipitation_mm,
        max(case when o.element_code = 'SNOW' then o.data_value end) as snowfall_mm,
        max(case when o.element_code = 'SNWD' then o.data_value end) as snow_depth_mm
    from obs o
    join stations s on o.station_id = s.station_weather_id
    group by o.station_id, o.observation_date, s.city_id
)

select
    -- PK surrogate
    {{ dbt_utils.generate_surrogate_key(['station_weather_id', 'observation_date']) }} as daily_id,

    -- FKs
    station_weather_id,
    observation_date,
    city_id,

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
