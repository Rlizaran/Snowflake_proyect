-- Silver lookup: tipos de usuario CityBike + descripcion (valor fijo: member, casual)
-- Materializado como VIEW (default del proyecto): dominio cerrado de 2 valores,
-- no tiene sentido persistirlo ni hacerlo incremental.

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
    {{ dbt_utils.generate_surrogate_key(['member_casual']) }} as user_type_code,
    member_casual,
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