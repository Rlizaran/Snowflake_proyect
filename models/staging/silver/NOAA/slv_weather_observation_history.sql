-- Silver SCD2 expuesto para BI: TODAS las versiones del snapshot NOAA (vigentes + reemplazadas).
-- A diferencia de slv_weather_observation (solo vigente), este expone la historia para que en
-- Power BI se puedan medir correcciones de NOAA por anio/estacion/elemento.
-- FIX: anadidas columnas data_value (escalada) y data_value_raw (NOAA original). Antes solo
-- exponia el valor crudo y BI tenia que aplicar /10 ad-hoc.
-- Materializado como VIEW (default proyecto): el snapshot ya esta materializado y clusterizado.
-- Filtros utiles en BI:
--   is_current = TRUE      -> dato vigente (lo que cuenta para metricas finales)
--   is_superseded = TRUE   -> version corregida por NOAA mas tarde (= "error" detectado)
--   has_q_flag = TRUE      -> NOAA marco la observacion como sospechosa en su momento

with snap as (
    select * from {{ ref('snp_NOAA__noaa_raw_year') }}
)

select
    -- PK del row historico (incluye dbt_valid_from para diferenciar versiones del mismo scd_key)
    {{ dbt_utils.generate_surrogate_key(['scd_key', 'dbt_valid_from']) }} as observation_version_id,

    -- Clave SCD2 + atributos NOAA
    scd_key,
    station_id,
    observation_date,
    year(observation_date)    as observation_year,
    quarter(observation_date) as observation_quarter,
    month(observation_date)   as observation_month,
    element                   as element_code,

    -- Valor escalado a unidad real (mismo tratamiento que stg)
    case
        when element in ('TMAX','TMIN','PRCP','AWND','WSF2','WSF5') then data_value / 10
        else data_value
    end as data_value,

    -- Valor original NOAA (para auditoria contra bronze / ver el delta entre versiones)
    data_value as data_value_raw,

    m_flag,
    q_flag,
    s_flag,
    obs_time,

    -- Linaje SCD2
    dbt_valid_from,
    dbt_valid_to,
    dbt_updated_at,

    -- Flags listos para BI (evita logica en DAX)
    case when dbt_valid_to is null then true else false end as is_current,
    case when dbt_valid_to is not null then true else false end as is_superseded,
    case when q_flag is not null and q_flag <> '' then true else false end as has_q_flag,

    -- Linaje fuente
    source_file,
    load_ts
from snap
