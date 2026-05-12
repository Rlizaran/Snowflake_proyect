-- slv_user_type: lookup de tipos de usuario CityBike (member, casual).

with distinct_types as (
    select distinct member_casual
    from {{ ref('stg_CityBike__citybike_trips') }}
)

select
    -- PK
    {{ dbt_utils.generate_surrogate_key(['member_casual']) }} as user_type_code,

    -- atributos
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
