-- dim_station: passthrough de slv_station (estaciones CityBike NY + JC).

select
    station_id,
    canonical_name,
    canonical_lat,
    canonical_lng
from {{ ref('slv_station') }}
