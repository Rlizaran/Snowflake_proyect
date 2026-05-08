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

with source as (
    select * from {{ source('NOAA', 'noaa_raw_year') }}

    {% if is_incremental() %}
        where load_ts > (select coalesce(max(load_ts), '1900-01-01'::timestamp_ntz) from {{ this }})
    {% endif %}
),

renamed as (
    select
        trim(station_id) as station_id,
        to_date(observation_date, 'YYYYMMDD') as observation_date,
        trim(element) as element,
        try_to_decimal(data_value, 18, 2) as data_value,
<<<<<<< HEAD
        COALESCE(TRY_CAST(obs_time AS INT), 2400) as obs_time,
=======
        trim(m_flag) as m_flag,
        trim(q_flag) as q_flag,
        trim(s_flag) as s_flag,
        coalesce(try_cast(obs_time as int), 2400) as obs_time
>>>>>>> 99475784127c1d11161f30646db6b6f2b504e490
        source_file,
        load_ts
    from source
),

cleaned as (
    select
        *
    from renamed
    where station_id is not null
      and observation_date >= TO_DATE(20240101::VARCHAR, 'YYYYMMDD')
      and element in ('TMAX', 'TMIN', 'PRCP', 'SNOW', 'AWND', 'SNWD', 'WSF2', 'WSF5')
)

select * from cleaned