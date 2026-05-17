-- slv_city: lookup de ciudades CityBike (Manhattan, Jersey City).
-- Materializado table: el select distinct sobre slv_trip recompone full scan en cada query.

{{ config(materialized='table') }}

with city_table as (
    select distinct city
    from {{ ref('stg_CityBike__citybike_trips') }}
)

select
    -- PK
    {{ dbt_utils.generate_surrogate_key(['city']) }} as city_id,

    -- atributo
    city
from city_table
