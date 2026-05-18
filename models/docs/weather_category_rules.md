{% docs weather_category_rules %}

Clasificacion categorica del clima del dia calculada en `fct_weather_daily` y propagada a `fct_trips_weather`.
El CASE WHEN evalua las condiciones en orden — **la primera condicion verdadera gana**:

| Prioridad | Categoria | Condicion                  | Notas                                          |
|-----------|-----------|----------------------------|------------------------------------------------|
| 1         | rainy     | `precipitation_mm > 5`     | Lluvia significativa (umbral arbitrario).      |
| 2         | snowy     | `snowfall_mm > 0`          | Cualquier nieve registrada (sin nieve previa). |
| 3         | hot       | `temp_max_c > 25`          | Solo si no llueve ni nieva.                    |
| 4         | cold      | `temp_max_c < 5`           | Solo si no llueve ni nieva.                    |
| 5         | mild      | (default)                  | Cualquier otro caso.                           |

Consecuencias del orden:
- Un dia con `prcp=10, snow=3` se clasifica como **rainy** (precip gana sobre snow).
- Un dia con `prcp=2, snow=3, tmax=30` se clasifica como **snowy** (snow gana sobre temperatura).
- Solo dias secos pueden ser hot, cold o mild.

Para cambiar la prioridad, ajustar el orden del CASE WHEN en `fct_weather_daily.sql`.

{% enddocs %}
