-- dim_station: pass-through de slv_station (estaciones CityBike NY + JC) para consumo Gold.

select * from {{ ref('slv_station') }}
