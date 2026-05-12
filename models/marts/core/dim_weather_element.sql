-- dim_weather_element: pass-through de slv_weather_element (TMAX, TMIN, PRCP, etc.).

select * from {{ ref('slv_weather_element') }}
