-- dim_station_weather: pass-through de slv_weather_station (estaciones NOAA del proyecto).

select * from {{ ref('slv_weather_station') }}
