-- Stg NOAA: cast + filtro de estaciones y elementos relevantes, incremental DELETE+INSERT
-- Estrategia: delete+insert sobre clave compuesta (station_id, observation_date, element)
-- Justificacion: NOAA reescribe archivos por anio cuando hay correcciones (q_flag,
-- valores ajustados). No hay PK simple, la PK natural es compuesta. Merge con
-- composite key funciona pero requiere surrogate key explicita; delete+insert
-- borra las filas con la misma combinacion y reinserta — mas legible y sin
-- necesidad de hash. Append duplicaria observaciones tras una correccion NOAA.
{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['station_id','observation_date','element'],
    snowflake_warehouse='WH_ANALISIS'
  )
}}

with

source as (

    select * from {{ source('NOAA', 'noaa_raw_year') }}

    {% if is_incremental() %}
        -- Filtra bronze por load_ts reciente para reducir el set a procesar
        where load_ts > (select coalesce(max(load_ts), '1900-01-01'::timestamp_ntz) from {{ this }})
    {% endif %}

),

renamed as (

    select
        trim(station_id) as station_id,
        to_date(observation_date, 'YYYYMMDD') as observation_date,
        trim(element) as element,
        try_to_decimal(data_value, 18, 2) as data_value,
        trim(m_flag) as m_flag,
        trim(q_flag) as q_flag,
        trim(s_flag) as s_flag,
        obs_time,
        source_file,
        load_ts

    from source

),

cleaned as (
    select
        *
    from renamed
    where station_id in ('USW00094728', 'USW00014734')
      and observation_date >= TO_DATE(20240101::VARCHAR, 'YYYYMMDD')
      and element in ('TMAX', 'TMIN', 'PRCP', 'SNOW', 'AWND', 'SNWD', 'WSF2', 'WSF5')
      and obs_time is not null
)

select * from cleaned