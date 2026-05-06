-- Silver lookup: tipos de bicicleta CityBike + descripcion y flags derivados (valor fijo: classic, electric)
-- Materializado como VIEW (default del proyecto): solo 2-3 filas, computar al vuelo es trivial
-- y evita la sobrecarga de incremental sobre un dominio cerrado que no crece.

with

distinct_types as (
    select distinct rideable_type
    from {{ ref('stg_CityBike__citybike_trips_ny') }}
    where rideable_type is not null
    union
    select distinct rideable_type
    from {{ ref('stg_CityBike__citybike_trips_jc') }}
    where rideable_type is not null
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