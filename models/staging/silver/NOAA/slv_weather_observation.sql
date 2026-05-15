-- slv_weather_observation: fact NOAA long format (un row por station, date, element).
-- Cambio v8: drop de q_flag_category. q_flag se mantiene como FK a slv_quality_flag (lookup normalizado).

-- CTE obs: select sobre stg vigente del snapshot
with obs as (
    select * from {{ ref('stg_NOAA__noaa_raw_year') }}
)

select
    -- PK surrogate
    {{ dbt_utils.generate_surrogate_key(['station_id', 'observation_date', 'element']) }} as observation_id,

    -- FKs
    station_id,
    observation_date,
    element as element_code,
    q_flag,

    -- atributo
    data_value,

    -- linaje
    source_file,
    load_ts
from obs
