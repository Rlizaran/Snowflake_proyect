-- Silver fact NOAA en long format normalizado: un row por (station_id, observation_date, element_code)
-- Surrogate key (observation_id) generado con dbt_utils para simplificar joins downstream
with

obs as (
    select * from {{ ref('stg_NOAA__noaa_raw_year') }}
)

select
    -- Surrogate PK (hash MD5 sobre las 3 columnas que forman la PK natural compuesta)
    {{ dbt_utils.generate_surrogate_key(['station_id', 'observation_date', 'element']) }} as observation_id,

    -- FKs
    station_id, -- -> slv_weather_station
    observation_date, -- -> slv_date
    element as element_code,  -- -> slv_weather_element

    -- Atributos
    data_value,
    q_flag,

    -- Linaje
    source_file,
    load_ts
from obs
