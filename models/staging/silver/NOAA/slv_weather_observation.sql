-- slv_weather_observation: fact NOAA long format (un row por station, date, element).
-- CTE obs: select sobre stg vigente del snapshot
{{ config(
    materialized='incremental',
    unique_key='observation_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
) }}

with obs as (
    select * from {{ ref('stg_NOAA__noaa_raw_year') }}

    {% if is_incremental() %}
        where load_ts > (select coalesce(max(load_ts), '1900-01-01'::timestamp_ntz) from {{ this }})
    {% endif %}
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
