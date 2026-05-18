{% docs scd2_lifecycle %}

Las observaciones NOAA usan el patron SCD2 (Slowly Changing Dimension Type 2) para capturar correcciones historicas sin perder versiones anteriores.

**Lifecycle de una fila SCD2:**

1. **INSERT inicial** — Cuando una `scd_key` (station + date + element) aparece por primera vez:
   - `dbt_valid_from` = timestamp del run
   - `dbt_valid_to` = NULL (vigente)
   - `dbt_updated_at` = timestamp del run

2. **NOAA publica correccion** — Cambia `data_value` o `q_flag_category` para una `scd_key` existente. La strategy `check` detecta la diferencia:
   - La fila vieja: `dbt_valid_to` = timestamp del run (cerrada)
   - INSERT de la fila nueva: `dbt_valid_from` = timestamp del run, `dbt_valid_to` = NULL

3. **Resultado** — Una misma `scd_key` puede tener N filas en el snapshot, solo una con `dbt_valid_to IS NULL`.

**Flags BI** (pre-calculados en `fct_noaa_corrections`):

| Flag           | Logica                          | Significado                                       |
|----------------|---------------------------------|---------------------------------------------------|
| `is_current`   | `dbt_valid_to IS NULL`          | Esta es la version vigente del dato.              |
| `is_superseded`| `dbt_valid_to IS NOT NULL`      | Esta version fue reemplazada por una mas reciente.|

**Uso comun:**
- Para metricas finales en BI: filtrar `is_current = TRUE`.
- Para auditoria de correcciones: `is_superseded = TRUE` lista todas las versiones obsoletas.
- Para contar cuantas correcciones publico NOAA por estacion/anio: `count(*) where is_superseded` agrupado por `scd_key` o `station_weather_id, observation_year`.

**Estrategia del snapshot:** `check_cols = [data_value, q_flag_category]` — solo estos dos campos disparan una nueva version. Otros campos (e.g. `obs_time`) pueden cambiar sin crear historia nueva.

{% enddocs %}
