-- Silver lookup: elementos meteorologicos NOAA GHCN-Daily (codigo + descripcion + unidad)
-- Materializado como VIEW (default del proyecto): 9 filas hardcoded (valores fijos NOAA),
-- sin necesidad de persistencia ni incremental.

select
    element_code,
    description,
    unit
from (
    values
        ('TMAX', 'Temperatura maxima del dia', 'celsius'),
        ('TMIN', 'Temperatura minima del dia', 'celsius'),
        ('TAVG', 'Temperatura promedio del dia', 'celsius'),
        ('PRCP', 'Precipitacion total (lluvia)', 'mm'),
        ('SNOW', 'Caida de nieve', 'mm'),
        ('SNWD', 'Profundidad de nieve acumulada', 'mm'),
        ('AWND', 'Velocidad media del viento', 'm/s'),
        ('WSF2', 'Rafaga maxima de 2 minutos', 'm/s'),
        ('WSF5', 'Rafaga maxima de 5 segundos', 'm/s')
) as t(element_code, description, unit)
