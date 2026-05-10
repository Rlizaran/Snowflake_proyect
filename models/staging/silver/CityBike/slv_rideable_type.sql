-- Silver lookup: tipos de bicicleta CityBike + descripcion y flags derivados (valor fijo: classic, electric)
-- FIX: removido 'where rideable_type is not null' (stg ya filtra; era redundante).
-- Materializado como VIEW (default del proyecto): solo 2-3 filas, computar al vuelo es trivial
-- y evita la sobrecarga de incremental sobre un dominio cerrado que no crece.

with distinct_types as (
    select distinct rideable_type
    from {{ ref('stg_CityBike__citybike_trips') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['rideable_type']) }} as rideable_type_code,
    rideable_type,
    case rideable_type
        when 'classic_bike' then 'Bicicleta clasica (mecanica)'
        when 'electric_bike' then 'Bicicleta electrica con asistencia'
        else 'Desconocido'
    end as description,
    case rideable_type
        when 'electric_bike' then true
        else false
    end as is_electric
from distinct_types
