-- dim_weather_element: passthrough de slv_weather_element.

select
    element_code,
    description,
    unit
from {{ ref('slv_weather_element') }}
