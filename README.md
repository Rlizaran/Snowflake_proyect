# Snowflake_proyect

Pipeline medallion (Bronze → Silver → Gold) sobre **Citi Bike NYC + Jersey City + NOAA** en Snowflake, con transformaciones en dbt y consumo final desde Power BI.

## Stack

- **Snowflake** — ingesta, almacenamiento, orquestación (Tasks + Streams).
- **Python** — descarga incremental de archivos JC desde S3 público y `PUT` al stage interno.
- **dbt** — transformaciones Silver normalizado y Gold star schema.
- **Power BI** — dashboards.

## Arquitectura

```
DEV_/PRO_CITYBIKE_BRONZE                    DEV_/PRO_CITYBIKE_SILVER                    DEV_/PRO_CITYBIKE_GOLD
  CITYBIKE                                    CITYBIKE                                    MARTS
   citybike_trips_ny       ──cast──▶  stg_*_ny ──┐                                         fct_trips_daily
   citybike_trips_jc       ──cast──▶  stg_*_jc ──┴──union──▶ slv_trip ────────┐            dim_station
                                                              slv_station        \           dim_date
                                                              slv_rideable_type   ▶ ── star ─┘
                                                              slv_user_type      /
                                                              slv_date          /
   NOAA                                       NOAA                              /
   noaa_raw_year           ──cast──▶  stg_NOAA ──▶ slv_weather_observation ──┘
                                                  slv_weather_daily
                                                  slv_weather_station
                                                  slv_weather_element

   LOGS
   load_log                streams + tasks
```

## Capas

### Bronze — `DEV_CITYBIKE_BRONZE` (ingesta + dbt sources; PRO_ disponible para futuro)
3 tablas raw, todas en VARCHAR para preservar el dato original. Una por fuente: NY trips, JC trips, NOAA observations.

### Silver — `DEV_CITYBIKE_SILVER` (9 tablas, materializado por dbt; PRO_ disponible para futuro)
Casteado, limpio, normalizado en 3NF. **NY+JC unidos** en `slv_trip` para que Gold y PBI tengan una sola tabla de viajes. NOAA queda en long format normalizado (`slv_weather_observation`) + un wide pre-pivoteado (`slv_weather_daily`) para joins por fecha.

| Tabla | Schema | PK | FKs | Notas |
|---|---|---|---|---|
| `slv_trip` | CITYBIKE | `ride_id` | `trip_date`→slv_date, `rideable_type_code`→slv_rideable_type, `user_type_code`→slv_user_type, `start_station_id`/`end_station_id`→slv_station | NY+JC unidos. Incluye `trip_duration_min` y `trip_distance_km` (great-circle WGS84) calculados en stg |
| `slv_station` | CITYBIKE | `station_id` | — | Dedup de start+end. ~2000 rows |
| `slv_rideable_type` | CITYBIKE | `rideable_type_code` | — | Lookup (3 rows: classic, electric, docked) |
| `slv_user_type` | CITYBIKE | `user_type_code` | — | Lookup (2 rows: member, casual) |
| `slv_date` | CITYBIKE | `date_id` | — | Spine 2024-01-01 → 2026-12-31 con anio, mes, dia, dia_semana, estacion, etc. |
| `slv_weather_observation` | NOAA | `observation_id` (MD5 surrogate) | `station_id`→slv_weather_station, `observation_date`→slv_date, `element_code`→slv_weather_element | Long format. Datos en Celsius / mm (escalados desde décimas de NOAA en stg) |
| `slv_weather_daily` | NOAA | `daily_id` (MD5 surrogate) | `station_id`→slv_weather_station, `observation_date`→slv_date | Wide pivot pre-calculado para joins por fecha |
| `slv_weather_station` | NOAA | `station_id` | — | 2 rows: USW00094728 (Manhattan / NY) + USW00014734 (Newark / JC) |
| `slv_weather_element` | NOAA | `element_code` | — | Lookup: TMAX, TMIN, TAVG, PRCP, SNOW, SNWD, AWND, WSF2, WSF5 |

**¿Por qué surrogate keys solo en `slv_weather_*`?** Tienen PK natural compuesta `(station, date, element)` o `(station, date)`. Hash MD5 con `dbt_utils.generate_surrogate_key()` lo convierte en una sola columna → joins downstream más simples y rápidos. Las demás tablas tienen PK natural de una sola columna (`ride_id`, `station_id`, etc.).

**¿Por qué `trip_distance_km` y `trip_duration_min` van en Silver y no en Gold?** Son atributos derivados a nivel fila, no KPIs. KPI = agregación (avg, sum, percentil). Atributo derivado = enriquecimiento del row. Si los pones en Gold, cada mart que los necesite los recalcularía. Una vez en Silver, Gold solo agrega.

### Gold — `DEV_CITYBIKE_GOLD.MARTS` (3 marts, materializado por dbt; PRO_ disponible para futuro)
Star schema desnormalizado para Power BI. Consume Silver (no Bronze).

- `fct_trips_daily` — fact agregado por (`trip_date`, `city`, `rideable_type`, `member_casual`) con métricas (`num_trips`, `avg_distance_km`, etc.) y clima joineado por fecha
- `dim_station` — vista delgada sobre `slv_station`
- `dim_date` — vista delgada sobre `slv_date`

## Orquestación de Tasks

Todos los tasks y streams viven en `DB_CITYBIKE_LOGS.LOGS` (DB dedicada para orquestacion).

```
Chain 1 — domingos 03:00 NY
  TSK_BRONZE_CITYBIKE  -->  LOAD_CITYBIKE_NY()
        |
        v (AFTER + WHEN streams_have_data)
  TSK_BRONZE_NOAA      -->  LOAD_NOAA_YEAR()

Chain 2 — dia 1 mes 17:00 NY
  TSK_BRONZE_JC_REFRESH       -->  REFRESH_JC_STAGE()
        |
        v (AFTER + WHEN stage stream)
  TSK_BRONZE_JC_ONFILES       -->  LOAD_CITYBIKE_JC()
        |
        v (AFTER)
  TSK_BRONZE_JC_DRAIN         -->  DRAIN_JC_STAGE_STREAM()
```

Antes de Chain 2 corre el script Python (Task Scheduler / GitHub Actions) que sube al landing los meses JC faltantes.

## Flujo end-to-end

1. **Setup** — `WH_NYCBIKE_DEV`, 3 DBs medallion (Bronze/Silver/Gold), schemas LOGS/CITYBIKE/NOAA en Bronze, CITYBIKE/NOAA en Silver, MARTS en Gold, rol `ROLE_NYCBIKE`, integración Git para versionar SQL.
2. **Stages** — externos a S3 público (NY, NOAA) e interno para JC.
3. **Ingesta NY** — semanal, COPY directo desde S3 con `PATTERN`.
4. **Ingesta JC** — Python idempotente sube los meses nuevos al landing → stream sobre stage detecta archivos → COPY → drain del stage stream.
5. **Ingesta NOAA** — condicional: solo si los streams de Citi Bike traen filas nuevas.
6. **Logging** — cada procedure escribe en `LOGS.LOAD_LOG` (rows, files, errores).
7. **Transformación staging** — `dbt run` sobre `models/staging/*` → cast VARCHAR a tipos correctos + filtrado de basura (~20 filas malas en NY).
8. **Transformación silver** — `dbt run` sobre `models/silver/*` → normalización en 9 tablas con PKs y FKs explícitas.
9. **Transformación marts** — `dbt run` sobre `models/marts/*` → star schema final.
10. **Consumo Power BI** — conecta a `DEV_CITYBIKE_GOLD.MARTS`, modela relaciones `dim_date[date_id] → fct_trips_daily[trip_date]` y `dim_station[station_id] → fct_trips_daily[start_station_id]`.

## Estructura del repo

```
Snowflake_proyect/
├── extract_jc_to_stage.py        # ingestor idempotente JC → landing
├── .env.example                   # variables de conexion Snowflake
├── requirements.txt
├── dbt_project.yml                # config dbt: staging, silver, marts
├── profiles.yml                   # perfil dbt (mover a ~/.dbt/ o usar DBT_PROFILES_DIR)
├── packages.yml                   # dependencias dbt (dbt_utils, codegen, dbt_expectations)
├── macros/
│   └── generate_database_name.sql
├── models/
│   ├── staging/
│   │   ├── CityBike/
│   │   │   ├── __stg_citybike__source.yml
│   │   │   ├── stg_CityBike__citybike_trips_ny.sql
│   │   │   └── stg_CityBike__citybike_trips_jc.sql
│   │   └── NOAA/
│   │       ├── __stg_NOAA__source.yml
│   │       └── stg_NOAA__noaa_raw_year.sql
│   ├── silver/
│   │   ├── __silver_models.yml
│   │   ├── CityBike/
│   │   │   ├── slv_trip.sql
│   │   │   ├── slv_station.sql
│   │   │   ├── slv_rideable_type.sql
│   │   │   ├── slv_user_type.sql
│   │   │   └── slv_date.sql
│   │   └── NOAA/
│   │       ├── slv_weather_observation.sql
│   │       ├── slv_weather_daily.sql
│   │       ├── slv_weather_station.sql
│   │       └── slv_weather_element.sql
│   └── marts/
│       ├── fct_trips_daily.sql
│       ├── dim_station.sql
│       └── dim_date.sql
└── Snowflake/
    ├── ROLS + SETUP/
    │   ├── Set Up Inicial.sql     # warehouses + 3 DBs + schemas (LOGS/CITYBIKE/NOAA + CITYBIKE/NOAA + MARTS)
    │   └── Rol.sql                # ROLE_NYCBIKE + grants
    └── BRONZE/
        ├── GITHUB + STAGES/
        │   ├── Github Integration.sql
        │   ├── Github Secret.sql
        │   └── Stages + FileFormat.sql
        ├── ROOT LAYER.sql         # tablas, procedures, log
        ├── Tasks + Streams.sql    # streams + tasks encadenados
        ├── Task Control.sql       # RESUME / SUSPEND / EXECUTE
        └── Verify steps.sql       # checks de ingesta
```

## Idempotencia y resiliencia

- **Python** — compara contra `LS @stage` y solo sube los meses faltantes.
- **COPY INTO** — load metadata evita reingesta del mismo archivo (FORCE=FALSE por defecto).
- **`ON_ERROR='CONTINUE'`** — un archivo malo no rompe el batch.
- **EXCEPTION handlers** — cualquier error queda registrado en `LOGS.LOAD_LOG`.
- **Drain del stage stream JC** — evita que el `WHEN` quede permanentemente TRUE.
- **dbt tests** — relationships entre slv_* validan integridad referencial; uniqueness en PKs garantiza no duplicados.

## Configuración rápida

```bash
cp .env.example .env
# editar credenciales
pip install -r requirements.txt
python extract_jc_to_stage.py
```

Orden de ejecución del SQL:
`Set Up Inicial → Rol → Github Secret → Github Integration → Stages + FileFormat → ROOT LAYER → Tasks + Streams → Task Control`

Después en dbt:
```bash
dbt deps                              # instala dbt_utils, codegen, dbt_expectations
dbt run --select staging.*            # cast + clean
dbt run --select silver.*             # normalización
dbt run --select marts.*              # star schema
dbt test                              # valida PKs, FKs, accepted_values
```
