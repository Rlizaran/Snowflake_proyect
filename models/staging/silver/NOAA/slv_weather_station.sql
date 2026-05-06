-- Silver dimension: estaciones meteorologicas NOAA con metadata fija (Manhattan + Newark/JC)
-- Materializado como VIEW (default del proyecto): 2 filas hardcoded, dominio cerrado.

select
    station_id,
    station_name,
    city,
    lat,
    lng,
    state,
    elevation_m
from (
    values
        ('USW00094728', 'NEW YORK CENTRAL PARK', 'NY', 40.7794, -73.9692, 'NY', 39.6),
        ('USW00014734', 'NEWARK LIBERTY INTL AP', 'JC', 40.6825, -74.1694, 'NJ', 9.1)
) as t(station_id, station_name, city, lat, lng, state, elevation_m)
