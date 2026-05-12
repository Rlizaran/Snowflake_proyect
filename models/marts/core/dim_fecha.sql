-- dim_fecha: spine diario de calendario anclado al rango min/max de observaciones NOAA.

{{ config(materialized='view') }}

{%- set min_max_query -%}
    select
        to_char(min(observation_date), 'YYYY-MM-DD') as min_date,
        to_char(max(observation_date), 'YYYY-MM-DD') as max_date
    from {{ ref('stg_NOAA__noaa_raw_year') }}
{%- endset -%}

{%- set min_date = '2024-01-01' -%}
{%- set max_date = '2026-12-31' -%}

{%- if execute -%}
    {%- set results = run_query(min_max_query) -%}
    {%- if results and results.rows | length > 0 and results.columns[0].values()[0] is not none -%}
        {%- set min_date = results.columns[0].values()[0] -%}
        {%- set max_date = results.columns[1].values()[0] -%}
    {%- endif -%}
{%- endif -%}

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('" ~ min_date ~ "' as date)",
        end_date="dateadd(day, 1, cast('" ~ max_date ~ "' as date))"
    ) }}
)

select
    -- PK
    date_day as fecha_id,

    -- atributos calendario
    year(date_day) as anio,
    quarter(date_day) as trimestre,
    month(date_day) as mes,
    decode(month(date_day),
        1,'Enero', 2,'Febrero', 3,'Marzo', 4,'Abril',
        5,'Mayo', 6,'Junio', 7,'Julio', 8,'Agosto',
        9,'Septiembre',10,'Octubre',11,'Noviembre',12,'Diciembre'
    ) as nombre_mes,
    to_char(date_day, 'YYYY-MM') as anio_mes,
    day(date_day) as dia_mes,
    dayofweekiso(date_day) as dia_semana,
    decode(dayofweekiso(date_day),
        1,'Lunes', 2,'Martes', 3,'Miercoles', 4,'Jueves',
        5,'Viernes', 6,'Sabado', 7,'Domingo'
    ) as nombre_dia,
    case when dayofweekiso(date_day) in (6,7) then true else false end as es_fin_semana,
    weekofyear(date_day) as semana_anio,
    dayofyear(date_day) as dia_anio,
    case
        when month(date_day) in (12,1,2) then 'Invierno'
        when month(date_day) in (3,4,5)  then 'Primavera'
        when month(date_day) in (6,7,8)  then 'Verano'
        else 'Otono'
    end as estacion
from date_spine
