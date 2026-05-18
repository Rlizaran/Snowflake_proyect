-- slv_city: lookup de ciudades CityBike (Manhattan, Jersey City). Materializado table.

{{ config(materialized='table') }}

with city_table as (
    select distinct city
    from {{ ref('stg_CityBike__citybike_trips') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['city']) }} as city_id,
    city
from city_table
