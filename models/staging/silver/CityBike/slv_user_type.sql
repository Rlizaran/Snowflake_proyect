-- Silver lookup: tipos de usuario CityBike + descripcion (valor fijo: member, casual)
-- FIX: removido 'where member_casual is not null' (stg ya filtra; era redundante).
-- Materializado como VIEW (default del proyecto): dominio cerrado de 2 valores.

with distinct_types as (
    select distinct member_casual
    from {{ ref('stg_CityBike__citybike_trips') }}
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
