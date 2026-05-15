# Snowflake_proyect

Pipeline medallion (Bronze → Silver → Gold) sobre **Citi Bike NY + Jersey City + NOAA GHCN-Daily** en Snowflake, transformaciones con **dbt** (incluido snapshot SCD2 para correcciones NOAA), consumo final desde **Power BI** y soporte para forecasting con **Snowflake Cortex `ML.FORECAST`**.

## Diagramas

ERDs de Bronze, Silver y Gold.

| Capa   | PNG                                       | DBML                              |
|--------|-------------------------------------------|-----------------------------------|
| Bronze | `Snowflake_proyect/photos/Bronze.png`     | —                                 |
| Silver | `Snowflake_proyect/photos/Silver.png`     | `sql/silver.dbml`                 |
| Gold   | `Snowflake_proyect/photos/Gold.png`       | `sql/gold.dbml`                   |

Los `.dbml` se pegan tal cual en https://dbdiagram.io. El sufijo `(table)` o `(view)` del nombre indica la materializacion dbt — solo aparece en el diagrama.

## Stack

- **Snowflake** — almacenamiento, COPY INTO, Streams + Tasks para orquestación bronze.
- **Python** — descarga incremental de archivos Citi Bike desde S3 público y `PUT` a stages internos (JC siempre, NY desde 202604 por cambio de formato).
- **dbt 1.12** — staging, silver normalizado, snapshot SCD2 NOAA, marts gold star schema.
- **Power BI** — dashboards (consume Gold).
- **Cortex `ML.FORECAST`** — predicción de viajes futuros sobre `fct_trips_daily`.

## Arquitectura

```
BRONZE  ({DEV|PRO}_CITYBIKE_BRONZE)            SILVER  ({DEV|PRO}_CITYBIKE_SILVER)         GOLD  ({DEV|PRO}_CITYBIKE_GOLD)
─────────────────────────────────              ─────────────────────────────────           ──────────────────────────────
CITYBIKE.CITYBIKE_TRIPS_NY                     intermediate.stg_CityBike__citybike_trips   CORE.dim_fecha
   ↑ COPY S3 publico (2024-202603)                ↑ cast + union NY/JC + dedupe + filter   CORE.dim_city
   ↑ COPY stage interno (202604+)                                                          CORE.dim_user_type
CITYBIKE.CITYBIKE_TRIPS_JC                     CITYBIKE.slv_trip          (table, incr.)   CORE.dim_rideable_bike
   ↑ COPY stage interno (Python PUT)           CITYBIKE.slv_station       (view)           CORE.dim_station
                                               CITYBIKE.slv_city          (view)           CORE.dim_station_weather
NOAA.NOAA_RAW_YEAR                             CITYBIKE.slv_rideable_type (view)           CORE.dim_weather_element
   ↑ COPY S3 NOAA (by_year)                    CITYBIKE.slv_user_type     (view)           CORE.dim_quality_flag

                                               snapshots.snp_NOAA__noaa_raw_year (SCD2)    MARTS.fct_trips           (table, incr.)
                                                  ↑ check_cols=[data_value, q_flag_cat]    MARTS.fct_trips_daily     (table, incr.)
                                                                                           MARTS.fct_trips_weather   (table, incr.)
                                               intermediate.stg_NOAA__noaa_raw_year        MARTS.fct_weather_daily   (view)
                                                  ↑ view sobre version vigente del SCD2    MARTS.fct_noaa_corrections (view)
                                               NOAA.slv_weather_observation (view)
                                               NOAA.slv_weather_station    (view, 2 rows)
                                               NOAA.slv_weather_element    (view, 8 rows)
                                               NOAA.slv_quality_flag       (view, lookup)
```

## Bronze

Tablas raw en VARCHAR para preservar el dato original.

| Tabla | Origen | Cron de carga | Procedure |
|---|---|---|---|
| `CITYBIKE_TRIPS_NY` | S3 público `s3://tripdata` (2024-01..2026-03) + stage interno `CITYBIKE_LANDING_STAGE_NY` (2026-04+) | día 28 03:00 NY | `LOAD_CITYBIKE_NY()` + `LOAD_CITYBIKE_NY_INT()` |
| `CITYBIKE_TRIPS_JC` | Stage interno `CITYBIKE_LANDING_STAGE` (upload Python) | día 28 03:00 NY | `LOAD_CITYBIKE_JC()` |
| `NOAA_RAW_YEAR` | S3 público `s3://noaa-ghcn-pds/csv.gz/by_year/` | después de las chains de Citi Bike (AFTER + WHEN stream) | `LOAD_NOAA_YEAR()` |

**¿Por qué dos paths para NY?** Citi Bike cambió formato a partir de 202604 y los archivos del bucket público están parcialmente corruptos. El script Python descarga el zip, lo extrae y `PUT` al stage interno limpio. El task del bucket público sigue activo para 2024-202603 (datos cerrados, sin cambio de formato).

**¿Por qué día 28?** Citi Bike publica el mes M durante el mes M+1. El día 28 garantiza que el mes anterior ya cerró y no llegarán partes nuevas corruptas.

## Snapshot SCD2 — `snp_NOAA__noaa_raw_year`

NOAA reescribe archivos del año cuando publica correcciones de `q_flag` o `data_value`. El snapshot captura todas las versiones históricas con strategy `check` sobre `[data_value, q_flag_category]`. Cluster por `year(observation_date)` (años cerrados no mutan).

`scd_key` = `upper(trim(station_id)) || '|' || trim(observation_date) || '|' || upper(trim(element))`. Dedupe explícito en CTE intermedia (no qualify final) para que el primer run no inserte filas duplicadas.

El snapshot guarda solo `q_flag` y `q_flag_category` (este ultimo necesario para SCD2 `check_cols`). `m_flag` y `s_flag` se eliminaron en v8 — no aportaban a la historia ni al consumo BI.

`fct_noaa_corrections` (gold) expone TODAS las versiones para BI: `is_current`, `is_superseded`. La categoria del flag se resuelve via join con `dim_quality_flag` (lookup normalizado), no como columna inline.

## Silver

`stg_CityBike__citybike_trips` — incremental MERGE por `ride_id`. Union NY+JC, cast a tipos correctos, filter por timestamps válidos / station_id válido / rideable_type ∈ (classic, electric) / member_casual ∈ (member, casual), dedupe intra-batch. Bronze trae basura, silver sale limpio.

`stg_NOAA__noaa_raw_year` — view delgada sobre el snapshot, filtra `dbt_valid_to is null` (versión vigente).

`slv_trip` — fact normalizado, materializado **table** (override sobre `+materialized: view` del project) por el coste del `ST_DISTANCE`. MERGE por `ride_id` con ventana `load_ts > max(load_ts)`. FKs surrogate a las dims via `dbt_utils.generate_surrogate_key`.

`slv_weather_*` — dims hardcoded (estación + elementos), lookup normalizado de q_flag (`slv_quality_flag`) y fact long (`slv_weather_observation`).

`slv_quality_flag` — lookup nuevo (v8). Antes la categoria de `q_flag` vivia inline en el snapshot y se propagaba como columna duplicada. Ahora es una tabla independiente con (`q_flag`, `q_flag_category`, `description`). Cualquier modelo que necesite la categoria hace JOIN.

## Gold

### `marts/core/` (dimensiones, schema `CORE`)

| Dim | Source | Notas |
|---|---|---|
| `dim_fecha` | `dbt_utils.date_spine` + `run_query` sobre min/max de `stg_NOAA__noaa_raw_year` | PK = `fecha_id` (DATE). El rango se ancla a NOAA porque NOAA se actualiza por trigger de NY/JC. Columnas en español: anio, trimestre, mes, nombre_mes, anio_mes, dia_mes, dia_semana, nombre_dia, es_fin_semana, semana_anio, dia_anio, estacion. |
| `dim_city` | passthrough `slv_city` | Manhattan, Jersey City |
| `dim_user_type` | passthrough `slv_user_type` | member, casual |
| `dim_rideable_bike` | passthrough `slv_rideable_type` | classic, electric |
| `dim_station` | passthrough `slv_station` | ~2000 estaciones Citi Bike |
| `dim_station_weather` | passthrough `slv_weather_station` | 2 estaciones NOAA |
| `dim_weather_element` | passthrough `slv_weather_element` | TMAX, TMIN, PRCP, SNOW, SNWD, AWND, WSF2, WSF5 |
| `dim_quality_flag` | passthrough `slv_quality_flag` | Lookup q_flag NOAA → q_flag_category (nuevo en v8) |

### `marts/` (facts, schema `MARTS`)

| Fact | Grano | Material. | Notas |
|---|---|---|---|
| `fct_trips` | 1 row/viaje | table, incremental MERGE | Passthrough de `slv_trip`. FKs a dims. |
| `fct_trips_daily` | `trip_date × city × bike × user` | table, incremental MERGE | Métricas: `n_trips`, `avg/sum/min/max/median duration`. Incluye `series_key` listo para `ML.FORECAST`. |
| `fct_trips_weather` | `trip_date × city` | table, incremental MERGE | Cruza trips agregados (`n_trips_member/casual/classic/electric`) con clima (`temp_max/min/avg_c`, `precipitation_mm`, `snow_mm`, `weather_category`). Mapeo Manhattan→`USW00094728`, JC→`USW00014734`. |
| `fct_weather_daily` | `(station, observation_date)` | view | Pivot wide de elementos NOAA + `weather_category` (rainy/snowy/hot/cold/mild). |
| `fct_noaa_corrections` | 1 row/versión SCD2 | view | Toda la historia del snapshot. `is_current`, `is_superseded`. La categoria de `q_flag` se resuelve via FK a `dim_quality_flag`. |

Los aggregados (`fct_trips_daily`, `fct_trips_weather`) son `incremental` con ventana de 7 días sobre `trip_date` para absorber viajes que lleguen tarde. Para reprocesar correcciones NOAA en historia profunda: `dbt run --full-refresh --select fct_trips_weather`.

## Constraints y contratos dbt

**Regla del proyecto (v8):** las clausulas `constraints:` en yml viven **solo a nivel modelo en tablas materializadas**. Las views no llevan constraints (Snowflake solo enforce `NOT NULL` y siempre informativo en views).

Modelos con `contract: { enforced: true }` + `constraints:` a nivel tabla:

| Modelo | Capa | PK |
|---|---|---|
| `slv_trip` | Silver (table) | `ride_id` |
| `fct_trips` | Gold MARTS (incremental → table) | `ride_id` |
| `fct_trips_daily` | Gold MARTS (incremental → table) | `daily_trip_id` |
| `fct_trips_weather` | Gold MARTS (incremental → table) | `trip_weather_id` |

Las demas dims, lookups y facts-view (`fct_weather_daily`, `fct_noaa_corrections`) solo llevan `data_tests` (`unique`, `not_null`, `relationships`, `accepted_values`). Sin `constraints:`.

## Schema DEV/PRO

`macros/generate_schema_name.sql` decide el layout según `DBT_ENVIRONMENTS`:

- **DEV** → `{target.schema}_{custom_schema}` (sandbox personal: `dbt_rlizaran_intermediate`, `dbt_rlizaran_CORE`, etc.).
- **PRO** → `{custom_schema}` directo (`intermediate`, `CITYBIKE`, `NOAA`, `MARTS`, `CORE`).

`DBT_ENVIRONMENTS=DEV|PRO` también decide la database (`DEV_CITYBIKE_*` vs `PRO_CITYBIKE_*`) via `generate_database_name.sql` + env_var en `dbt_project.yml`.

Cada dev pone `SF_SCHEMA=dbt_<usuario>` para su sandbox.

## Orquestación Snowflake

Todos los streams y tasks viven en `DB_CITYBIKE_LOGS.{LOGS|PRO}`.

```
                                    TSK_BRONZE_MASTER (cron 0 3 28 * * NY)
                                              │
                ┌─────────────────────────────┼─────────────────────────────┐
                ▼                             ▼                             ▼
   TSK_BRONZE_CITYBIKE_NY      TSK_BRONZE_NY_INT_REFRESH       TSK_BRONZE_JC_REFRESH
   (S3 publico 2024-202603)              │                              │
                │                        ▼ WHEN STM_NY_STAGE             ▼ WHEN STM_JC_STAGE
                │             TSK_BRONZE_NY_INT_ONFILES      TSK_BRONZE_JC_ONFILES
                │                        │                              │
                │                        ▼                              ▼
                │             TSK_BRONZE_NY_INT_DRAIN        TSK_BRONZE_JC_DRAIN
                │                        │                              │
                └────────────────────────┴──────────────────────────────┘
                                              ▼ WHEN STM_NY or STM_JC tiene datos
                                       TSK_BRONZE_NOAA
```

En **DEV** y **PRO** el cron está consolidado en `TSK_BRONZE_MASTER` (root único, los demás son `AFTER`).

## Jobs dbt recomendados (PRO)

| Job | Comando | Schedule |
|---|---|---|
| **Build mensual** | `dbt build` | día 28, ~05:00 NY (tras NOAA del task chain) |
| **Full refresh trimestral** | `dbt build --full-refresh` | primer día 28 del trimestre, ~06:00 NY |
| **Source freshness diario** | `dbt source freshness` | diario ~09:00 NY |
| **Docs** (opcional) | `dbt docs generate` | cada vez que termina el **build mensual** o el **full refresh trimestral** |

`dbt build` engloba snapshot → models → tests en orden DAG. Corta al primer error.

## ML.FORECAST (futuro)

`fct_trips_daily` tiene la forma necesaria:
- TIMESTAMP = `trip_date`
- TARGET = `n_trips`
- SERIES = `series_key` (concat `city_id|rideable_type_code|user_type_code` → 8 series)

Ejemplo:
```sql
CREATE OR REPLACE SNOWFLAKE.ML.FORECAST forecast_trips_30d(
    INPUT_DATA       => TABLE(PRO_CITYBIKE_GOLD.MARTS.FCT_TRIPS_DAILY),
    SERIES_COLNAME   => 'series_key',
    TIMESTAMP_COLNAME=> 'trip_date',
    TARGET_COLNAME   => 'n_trips'
);
CALL forecast_trips_30d!FORECAST(FORECASTING_PERIODS => 30);
```

Si se necesita meter exógenas (temp, prcp) se pasa a un modelo Python en dbt (`materialized='python'`).

## Tests

- **Sources** — minimos: solo `source_file` y `load_ts` `not_null` (catch fallo de COPY). `ride_id not_null` como warn. La basura del CSV se filtra en stg.
- **Silver** — `unique`/`not_null` en PKs, `accepted_values` en categóricos, `relationships` entre FKs y dims, incluyendo el nuevo FK `q_flag → slv_quality_flag`.
- **Snapshot** — `unique(dbt_scd_id)`, `not_null(scd_key)`, `accepted_values(q_flag_category)`.
- **Singulares** — `test_citybike_no_silent_drop` (todos los ride_id válidos en bronze llegan a silver), `test_citybike_partial_month_detection` (alerta meses cerrados con conteo bajo), `test_noaa_element_count_match` + `test_noaa_station_count_match` (conteos únicos bronze vs silver vía macro `bronze_silver_count_diff`).

## Estructura del repo

```
Snowflake_proyect/
├── extract_jc_to_stage.py        # ingestor idempotente JC + NY (202604+) -> stages internos
├── dbt_project.yml               # config dbt (DBs por env, schemas por capa)
├── profiles.yml                  # perfil Snowflake (env vars)
├── packages.yml                  # dbt_utils, codegen, dbt_expectations, dbt_date
├── README.md
│
├── macros/
│   ├── generate_database_name.sql        # nombre EXACTO de DB segun env
│   ├── generate_schema_name.sql          # DEV={target}_{custom}; PRO={custom}
│   └── bronze_silver_count_diff.sql      # macro test bronze vs silver
│
├── snapshots/
│   ├── snp_NOAA__noaa_raw_year.sql       # SCD2 sobre NOAA (solo q_flag + q_flag_category)
│   └── _snapshot.yml
│
├── models/
│   ├── staging/
│   │   ├── intermediate/
│   │   │   ├── __stg_citybike__source.yml
│   │   │   ├── __stg_NOAA__source.yml
│   │   │   ├── stg_CityBike__citybike_trips.sql      # union NY+JC incr. MERGE
│   │   │   └── stg_NOAA__noaa_raw_year.sql           # view sobre snapshot vigente
│   │   └── silver/
│   │       ├── CityBike/
│   │       │   ├── _stg_CityBike__model.yml
│   │       │   ├── slv_trip.sql                       # table incremental MERGE
│   │       │   ├── slv_station.sql
│   │       │   ├── slv_city.sql
│   │       │   ├── slv_rideable_type.sql
│   │       │   └── slv_user_type.sql
│   │       └── NOAA/
│   │           ├── _stg_NOAA__model.yml
│   │           ├── slv_weather_observation.sql
│   │           ├── slv_weather_station.sql
│   │           ├── slv_weather_element.sql
│   │           └── slv_quality_flag.sql               # NUEVO v8: lookup q_flag
│   └── marts/
│       ├── _marts__model.yml
│       ├── fct_trips_daily.sql                        # table incr.
│       ├── fct_trips_weather.sql                      # table incr.
│       ├── fct_noaa_corrections.sql                   # view
│       └── core/
│           ├── _core__model.yml
│           ├── dim_fecha.sql                          # date_spine anclado a NOAA
│           ├── dim_city.sql
│           ├── dim_user_type.sql
│           ├── dim_rideable_bike.sql
│           ├── dim_station.sql
│           ├── dim_station_weather.sql
│           ├── dim_weather_element.sql
│           ├── dim_quality_flag.sql                   # NUEVO v8: passthrough slv_quality_flag
│           ├── fct_trips.sql                          # table incr.
│           └── fct_weather_daily.sql                  # view
│
├── tests/
│   └── singular/
│       ├── test_citybike_no_silent_drop.sql
│       ├── test_citybike_partial_month_detection.sql
│       ├── test_noaa_element_count_match.sql
│       └── test_noaa_station_count_match.sql
│
└── __Snowflake/                  # SQL aplicado en Snowflake (no toca dbt)
    ├── ROLS/Rol.sql
    ├── DEV/
    │   ├── SETUP/Set Up Inicial.sql
    │   └── BRONZE/
    │       ├── GITHUB + STAGES/ (Github Integration + Secret + Stages + FileFormat)
    │       ├── ROOT LAYER.sql            # tablas raw + procedures (load, refresh, drain)
    │       ├── Tasks + Streams.sql       # streams + chains + cron
    │       ├── Task Control.sql
    │       └── Verify steps.sql
    └── PRO/BRONZE/
        ├── Stages + FileFromat.sql
        ├── Table + Procedure.sql
        ├── Task + Streams in PRO.sql     # con TSK_BRONZE_MASTER consolidado
        └── Task Control.sql
```

## Setup rápido

```bash
# variables de entorno (snowflake + selector DEV/PRO)
cp .env.example .env  # editar credenciales + DBT_ENVIRONMENTS=DEV + SF_SCHEMA=dbt_<usuario>

# Python ingestor (sube JC + NY 202604+)
pip install -r requirements.txt
python extract_jc_to_stage.py

# dbt
dbt deps
dbt build
```

Orden SQL Snowflake (una sola vez por entorno):
`Set Up Inicial → Rol → Github Secret → Github Integration → Stages + FileFormat → ROOT LAYER → Tasks + Streams → Task Control`

## Idempotencia y resiliencia

- **Python** — compara contra `LS @stage` y solo sube meses faltantes; OVERWRITE=TRUE para reintento limpio.
- **COPY INTO** — load metadata evita reingesta (`FORCE=FALSE`).
- **`ON_ERROR='CONTINUE'`** — un archivo malo no rompe el batch.
- **Snapshot SCD2** — NOAA corrige histórico; el snapshot lo captura sin perder versiones.
- **Incremental MERGE en silver/marts** — solo procesa rows nuevos o de la ventana de backfill.
- **Stream drain JC/NY** — evita que el `WHEN` quede TRUE indefinido.
- **Logging** — todo procedure escribe en `DB_CITYBIKE_LOGS.{LOGS|PRO}.LOAD_LOG`.

## Jobs dbt en PRO

| Job | Comando | Schedule | Propósito |
|---|---|---|---|
| **Build mensual** | `dbt build && dbt docs generate` | día 28 ~05:00 NY | Pipeline normal post-bronze |
| **Source freshness diario** | `dbt source freshness` | diario ~09:00 NY | Alerta de bronze stale |
| **Full refresh periódico** | `dbt build --full-refresh && dbt docs generate` | día 28 cada 3-6 meses ~06:00 NY | Catch correcciones NOAA profundas |

### Razones

**Build mensual** — corre después del task chain de Snowflake (`TSK_BRONZE_MASTER` día 28 03:00 NY → NY S3 + NY interno + JC + NOAA → ~04:00 finaliza). Ejecuta snapshot → models incrementales → tests → docs en orden DAG. Es el único job que transforma datos en cadencia normal. Más frecuencia es desperdicio porque bronze solo se refresca día 28.

**Source freshness diario** — independiente del build, lee solo `MAX(load_ts)` de los sources contra `warn_after 30d / error_after 40d`. Ejecución de ~10 segundos, casi gratis. Te enteras enseguida si un task de Snowflake falló en silencio y bronze quedó stale, sin esperar al día 28.

**Full refresh trimestral o semestral** — los aggregates `fct_trips_daily` y `fct_trips_weather` usan ventana incremental de **7 días** sobre `trip_date`. Si NOAA publica correcciones SCD2 de datos meteorológicos antiguos (más allá de esa ventana), los aggregates no se actualizan automáticamente. El `--full-refresh` reconstruye los modelos incrementales desde cero, captura esas correcciones y limpia cualquier drift acumulado. El snapshot SCD2 no se borra (dbt lo protege del full-refresh por diseño) — solo se rebuildean los models downstream.

### Notas operativas

- Encadenar `dbt docs generate` al **success** del build (no como job separado). Si falla el build, no generas docs de un estado roto.
- En **dbt Cloud**: los 3 jobs apuntan al mismo deployment environment (`PRO`), diferentes comandos + cron.
- En **GitHub Actions / cron externo**: 3 workflows separados con la misma imagen dbt y env vars (`DBT_ENVIRONMENTS=PRO`, `SF_*`).

## Changelog v8

- **NOAA flags** — drop de `m_flag` y `s_flag` en snapshot, stg, silver y gold. `q_flag` se conserva (input del SCD2 y FK al lookup normalizado).
- **Normalizacion q_flag** — nuevo `slv_quality_flag` (silver) + `dim_quality_flag` (gold). La columna `q_flag_category` desaparece de los modelos downstream; se resuelve via JOIN.
- **Constraints solo en tablas** — yml refactorizado: bloque `constraints:` solo en `slv_trip`, `fct_trips`, `fct_trips_daily`, `fct_trips_weather`. Las views (dims, fct_weather_daily, fct_noaa_corrections) no llevan constraints — solo `data_tests`.
- **DBML** — `sql/silver.dbml` y `sql/gold.dbml` listos para dbdiagram.io. PK/FK basicos. Nombre de tabla incluye `(table)` o `(view)` segun materializacion.
