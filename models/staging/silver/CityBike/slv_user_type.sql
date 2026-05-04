-- Silver lookup: tipos de usuario CityBike + descripcion (2 valores: member, casual)

with

distinct_types as (
    select distinct member_casual
    from {{ ref('stg_CityBike__citybike_trips_ny') }}
    where member_casual is not null
    union
    select distinct member_casual
    from {{ ref('stg_CityBike__citybike_trips_jc') }}
    where member_casual is not null
)

select
    member_casual as user_type_code,
    case member_casual
        when 'member' then 'Suscriptor anual / mensual'
        when 'casual' then 'Usuario ocasional (single ride o day pass)'
        else 'Desconocido'
    end as description,
    case member_casual
        when 'member' then true
        else false
    end as is_subscriber
from distinct_types
