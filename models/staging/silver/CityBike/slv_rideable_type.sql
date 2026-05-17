-- slv_rideable_type: lookup de tipos de bicicleta CityBike (classic, electric).
-- Materializado table: el select distinct sobre slv_trip recompone full scan en cada query.

{{ config(materialized='table') }}

with distinct_types as (
    select distinct rideable_type
    from {{ ref('stg_CityBike__citybike_trips') }}
)

select
    -- PK
    {{ dbt_utils.generate_surrogate_key(['rideable_type']) }} as rideable_type_code,

    -- atributos
    rideable_type,
    case rideable_type
        when 'classic_bike'  then 'Bicicleta clasica (mecanica)'
        when 'electric_bike' then 'Bicicleta electrica con asistencia'
        else 'Desconocido'
    end as description,
    case rideable_type
        when 'electric_bike' then true
        else false
    end as is_electric
from distinct_types
