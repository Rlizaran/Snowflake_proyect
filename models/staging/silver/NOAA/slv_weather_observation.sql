-- slv_weather_observation: fact NOAA long format (un row por station, date, element).

with obs as (
    select * from {{ ref('stg_NOAA__noaa_raw_year') }} stg
    left join {{ ref('slv_weather_station') }} ws
    on stg.station_id = ws.station_weather_id
)

select
    -- PK surrogate
    {{ dbt_utils.generate_surrogate_key(['station_id', 'observation_date', 'element']) }} as observation_id,

    -- FKs
    station_id,
    observation_date,
    element as element_code,

    -- atributos
    data_value,
    q_flag,
    q_flag_category,

    -- linaje
    source_file,
    load_ts
from obs
