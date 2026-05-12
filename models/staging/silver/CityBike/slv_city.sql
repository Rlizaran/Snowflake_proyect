-- slv_city: lookup de ciudades CityBike (Manhattan, Jersey City).

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
