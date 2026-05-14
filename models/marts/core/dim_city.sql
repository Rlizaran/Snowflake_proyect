-- dim_city: passthrough de slv_city.

select
    city_id,
    city
from {{ ref('slv_city') }}
