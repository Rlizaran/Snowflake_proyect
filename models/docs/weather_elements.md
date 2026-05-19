{% docs weather_elements %}

Elementos meteorologicos NOAA GHCN-Daily usados en el proyecto.
Bronze trae 50+ elementos; el snapshot filtra a estos 8 (los demas se descartan):

| Codigo | Descripcion                       | Unidad real | Factor bronze |
|--------|-----------------------------------|-------------|---------------|
| TMAX   | Temperatura maxima del dia        | celsius     | /10 (decimas) |
| TMIN   | Temperatura minima del dia        | celsius     | /10 (decimas) |
| PRCP   | Precipitacion total (lluvia)      | mm          | /10 (decimas) |
| SNOW   | Caida de nieve                    | mm          | nativa        |
| SNWD   | Profundidad de nieve acumulada    | mm          | nativa        |
| AWND   | Velocidad media del viento        | m/s         | /10 (decimas) |
| WSF2   | Rafaga maxima de 2 minutos        | m/s         | /10 (decimas) |
| WSF5   | Rafaga maxima de 5 segundos       | m/s         | /10 (decimas) |

El escalado /10 ocurre en `snp_NOAA__noaa_raw_year` para que todos los modelos downstream consuman ya en unidad real.

Fuente: lookup `slv_weather_element` (silver) y `dim_weather_element` (gold).

{% enddocs %}
