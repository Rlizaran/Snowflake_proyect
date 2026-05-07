with city_table as(
    select distinct
        city
    from  {{ ref('stg_CityBike__citybike_trips_ny') }}
    union
    select distinct
        city
    from  {{ ref('stg_CityBike__citybike_trips_jc') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['city']) }} as city_id,
    city
from city_table