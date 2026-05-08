<<<<<<< HEAD
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
=======
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
>>>>>>> 99475784127c1d11161f30646db6b6f2b504e490
from city_table