-- Silver lookup: ciudades CityBike (NY, JC) con surrogate key
-- FIX: anadido comentario de cabecera (regla del proyecto). Sin cambios funcionales.
-- Materializado como VIEW (default del proyecto): dominio cerrado de 2 valores.

with city_table as (
    select distinct city
    from {{ ref('stg_CityBike__citybike_trips') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['city']) }} as city_id,
    city
from city_table
