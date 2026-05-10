-- Stg NOAA: vista delgada sobre el snapshot SCD2 (snp_NOAA__noaa_raw_year).
-- FIX 1: error de sintaxis original (faltaba coma).
-- FIX 2: incremental delete+insert -> snapshot SCD2 (no perder versiones reemplazadas).
-- FIX 3: removido filtro muerto 'obs_time is not null'.
-- FIX 4: anadida conversion de unidades NOAA (decimas) a unidades reales. Antes este escalado
-- vivia oculto en slv_weather_daily como '/10' magic number; cualquiera que consultara
-- slv_weather_observation directo malinterpretaba el valor. Ahora la conversion ocurre una
-- sola vez aqui:
--   - TMAX, TMIN, PRCP, AWND, WSF2, WSF5 -> /10  (tenths -> unidad real)
--   - SNOW, SNWD -> sin escalar (NOAA los publica directamente en mm)
-- Se conserva data_value_raw para auditoria contra bronze. data_value queda en unidad real.
-- Materializado como VIEW: el snapshot ya esta materializado y clusterizado.
{{ config(materialized='view') }}

with current_version as (
    select *
    from {{ ref('snp_NOAA__noaa_raw_year') }}
    where dbt_valid_to is null
)

select
    station_id,
    observation_date,
    element,

    -- Valor en unidad real (Celsius / mm / m/s segun el elemento)
    case
        when element in ('TMAX','TMIN','PRCP','AWND','WSF2','WSF5') then data_value / 10
        else data_value
    end as data_value,

    -- Valor original NOAA (decimas en TMAX/TMIN/PRCP/wind, mm directo en SNOW/SNWD).
    -- Conservado para que slv_weather_observation_history pueda auditar contra bronze.
    data_value as data_value_raw,

    m_flag,
    q_flag,
    s_flag,
    obs_time,
    source_file,
    load_ts,

    -- Linaje SCD2 expuesto por si downstream quiere auditar la version vigente
    dbt_valid_from,
    dbt_updated_at
from current_version
