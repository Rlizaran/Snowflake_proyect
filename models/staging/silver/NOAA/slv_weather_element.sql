-- slv_weather_element: lookup de elementos meteorologicos NOAA GHCN-Daily usados en el proyecto.

select
    element_code,
    description,
    unit
from (
    values
        ('TMAX', 'Temperatura maxima del dia', 'celsius'),
        ('TMIN', 'Temperatura minima del dia', 'celsius'),
        ('PRCP', 'Precipitacion total (lluvia)', 'mm'),
        ('SNOW', 'Caida de nieve', 'mm'),
        ('SNWD', 'Profundidad de nieve acumulada', 'mm'),
        ('AWND', 'Velocidad media del viento', 'm/s'),
        ('WSF2', 'Rafaga maxima de 2 minutos', 'm/s'),
        ('WSF5', 'Rafaga maxima de 5 segundos', 'm/s')
) as t(element_code, description, unit)
