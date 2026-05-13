-- dim_rideable_bike: passthrough de slv_rideable_type.

select
    rideable_type_code,
    rideable_type,
    description,
    is_electric
from {{ ref('slv_rideable_type') }}
