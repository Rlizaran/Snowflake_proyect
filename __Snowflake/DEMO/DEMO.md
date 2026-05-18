# Demo Live — End-to-End Pipeline

Inyectamos 5 ride_ids en NY + 5 en JC, alteramos un valor NOAA para disparar SCD2, y vemos cómo todo aterriza en Power BI vía dbt.

Entorno objetivo: **PRO** (cambiar `PRO_` por `DEV_` si demo en DEV).
Fecha elegida para los trips: **2026-01-15** (dentro del rango ya cargado de NOAA → dim_fecha tendrá la fecha → no rompe `relationships`).

---

## 0. Pre-check (estado actual)

```sql
USE ROLE ROLE_NYCBIKE;
USE WAREHOUSE WH_NYCBIKE_DEV;

-- Conteo bronze antes
SELECT 'NY'   AS src, COUNT(*) FROM PRO_CITYBIKE_BRONZE.CITYBIKE.CITYBIKE_TRIPS_NY
UNION ALL
SELECT 'JC',  COUNT(*)         FROM PRO_CITYBIKE_BRONZE.CITYBIKE.CITYBIKE_TRIPS_JC
UNION ALL
SELECT 'NOAA',COUNT(*)         FROM PRO_CITYBIKE_BRONZE.NOAA.NOAA_RAW_YEAR;

-- Valor NOAA que vamos a corregir (anotar el valor original)
SELECT station_id, observation_date, element, data_value, q_flag, load_ts
FROM PRO_CITYBIKE_BRONZE.NOAA.NOAA_RAW_YEAR
WHERE station_id='USW00094728' AND observation_date='20240101' AND element='TMAX';
```

---

## 1. Inject — bronze (NY + JC + NOAA correction)

Pegar `demo_01_inject.sql` y ejecutar. Hace tres cosas:

- `INSERT` de 5 ride_ids en `CITYBIKE_TRIPS_NY` (fechas 2026-01-15).
- `INSERT` de 5 ride_ids en `CITYBIKE_TRIPS_JC` (fechas 2026-01-15).
- `UPDATE` del `data_value` de la observación NOAA Manhattan 2024-01-01 TMAX (simula corrección NOAA → dispara nueva versión en SCD2).

---

## 2. Ejecutar pipeline dbt

Desde la terminal del proyecto dbt, con `DBT_ENVIRONMENTS=PRO`:

```bash
cd Snowflake_proyect
export DBT_ENVIRONMENTS=PRO
export SF_SCHEMA=PRO   # o lo que toque para no caer en sandbox

dbt snapshot   # captura la correccion NOAA -> nueva version SCD2
dbt run        # propaga trips nuevos + cambios NOAA por stg/silver/marts (incremental MERGE)
dbt test       # valida integridad (PKs, FKs, accepted_values, count_match macros)
```

Lo que pasa por dentro:
- `snp_NOAA__noaa_raw_year`: detecta `data_value` distinto en check_cols → cierra versión anterior (`dbt_valid_to=now`) e inserta nueva (`dbt_valid_from=now`).
- `stg_NOAA__noaa_raw_year`: vista filtra `dbt_valid_to is null` → ahora ve el nuevo valor.
- `stg_CityBike__citybike_trips`: MERGE por `ride_id` → trae los 10 nuevos.
- `slv_trip`: MERGE → 10 nuevos.
- `slv_weather_observation`: refleja el nuevo data_value.
- `fct_trips_daily`, `fct_trips_weather`: incremental con ventana 7 días → recalcula los aggregados de 2026-01-15.
- `fct_noaa_corrections`: muestra las 2 versiones del SCD2 (`is_current` y `is_superseded`).

---

## 3. Verify — silver + gold

Pegar `demo_02_verify.sql` y revisar resultados. Bloques:

1. **Bronze post-inject**: conteo aumentó (+5 NY, +5 JC), valor NOAA actualizado.
2. **Snapshot SCD2**: dos versiones para `USW00094728|20240101|TMAX`, una vigente, una cerrada.
3. **Silver `slv_trip`**: los 10 nuevos `ride_id` (filtrados por `like 'DEMO_%'`).
4. **Silver `slv_weather_observation`**: refleja el nuevo data_value de la fila corregida.
5. **Gold `fct_trips_daily`**: hay rows con `trip_date='2026-01-15'` para Manhattan y Jersey City.
6. **Gold `fct_trips_weather`**: el row de 2026-01-15 muestra el clima joineado.
7. **Gold `fct_noaa_corrections`**: 2 rows para esa scd_key, una con `is_current=true`, otra con `is_superseded=true`.

---

## 4. Power BI

1. Abrir el `.pbix` que conecta a `PRO_CITYBIKE_GOLD.MARTS` + `PRO_CITYBIKE_GOLD.CORE`.
2. **Refresh** del dataset (Home → Refresh).
3. Comprobar visuales:
   - Total viajes de 2026-01-15 = anterior + 10 (slicer dim_fecha por día).
   - Tarjeta "Última corrección NOAA" o tabla de `fct_noaa_corrections` filtrada por `is_superseded=true` → debería listar la fila corregida.
   - Scatter `n_trips vs temp_max_c` para 2026-01-15 → punto nuevo.

---

## 5. Rollback (post-demo, opcional)

```sql
-- Borrar trips demo de bronze
DELETE FROM PRO_CITYBIKE_BRONZE.CITYBIKE.CITYBIKE_TRIPS_NY WHERE ride_id LIKE 'DEMO_NY_%';
DELETE FROM PRO_CITYBIKE_BRONZE.CITYBIKE.CITYBIKE_TRIPS_JC WHERE ride_id LIKE 'DEMO_JC_%';

-- Revertir NOAA (poner el data_value original; sustituir <VALOR_ORIGINAL>)
UPDATE PRO_CITYBIKE_BRONZE.NOAA.NOAA_RAW_YEAR
SET data_value='<VALOR_ORIGINAL>'
WHERE station_id='USW00094728' AND observation_date='20240101' AND element='TMAX';

-- Volver a correr dbt para que silver/gold reflejen el rollback
-- (snapshot creara OTRA version, no borra historico — eso es SCD2 by design)
```

Si quieres limpiar el historial del SCD2 también: `dbt snapshot --full-refresh` (cuidado, recrea toda la tabla).

---

## Demo condensada para clase — `demo_live_class.sql`

Variante alternativa más corta pensada para demo en vivo (clase / entrevista):

- Solo 2 ride_ids por fuente (4 trips totales) + 2 observaciones NOAA — basta para que stream + WHEN se disparen.
- Ejecuta `TSK_BRONZE_MASTER` manualmente con `EXECUTE TASK` para no esperar al cron del día 28.
- Verifica el TASK_HISTORY y los modelos gold (`MOBILITY.FCT_TRIPS`, `CLIMA.FCT_NOAA_CORRECTIONS`, snapshot SCD2).
- Cleanup de bronze + drop de todos los schemas dbt al final → permite un `dbt build --full-refresh` limpio.

5 pasos secuenciales en un solo archivo: `demo_live_class.sql`. Cada paso ejecutable independiente en Snowsight.
