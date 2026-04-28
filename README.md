# Snowflake_proyect

Pipeline medallion (Bronze → Silver → Gold) sobre **Citi Bike NYC + Jersey City + NOAA** en Snowflake, con transformaciones en dbt y consumo final desde Power BI.

## Stack

- **Snowflake** — ingesta, almacenamiento, orquestación (Tasks + Streams).
- **Python** — descarga incremental de archivos JC desde S3 público y `PUT` al stage interno.
- **dbt** — transformaciones Silver y Gold.
- **Power BI** — dashboards.

## Arquitectura

```mermaid
flowchart TD
    subgraph EXT[Externo]
        PY[extract_jc_to_stage.py<br/>idempotente: solo sube meses faltantes]
    end

    subgraph S3[S3 publico]
        S3NY[s3://tripdata<br/>YYYYMM-citibike-tripdata.zip]
        S3NOAA[s3://noaa-ghcn-pds<br/>by_year YYYY.csv.gz]
    end

    subgraph STAGES[Stages Snowflake]
        STG_NY[CITIBIKE_S3_STAGE]
        STG_JC[CITIBIKE_LANDING_STAGE<br/>interno]
        STG_NOAA[NOAA_S3_STAGE_YEAR]
    end

    subgraph BRONZE[BRONZE]
        T_NY[(citibike_trips_ny)]
        T_JC[(citibike_trips_jc)]
        T_NOAA[(noaa_raw_year)]
        LOG[(load_log)]
        STM_NY{{stm_citibike_ny<br/>append-only on table}}
        STM_JC{{stm_citibike_jc<br/>on stage}}
        STM_NOAA{{stm_noaa_year<br/>append-only on table}}
    end

    subgraph SILVER[SILVER - dbt]
        SLV[stg_* / slv_trips / slv_weather_daily / slv_stations / slv_calendar]
    end

    subgraph GOLD[GOLD - dbt]
        GLD[dim_station / dim_date / fct_trips / fct_trips_daily]
    end

    PBI[Power BI]

    S3NY --> STG_NY --> T_NY
    PY -- PUT --> STG_JC --> T_JC
    S3NOAA --> STG_NOAA --> T_NOAA

    T_NY --> STM_NY
    STG_JC --> STM_JC
    T_NOAA --> STM_NOAA

    STM_NY --> SLV
    STM_NOAA --> SLV
    T_JC --> SLV

    SLV --> GLD --> PBI

    T_NY -.log.-> LOG
    T_JC -.log.-> LOG
    T_NOAA -.log.-> LOG
```

## Orquestación de Tasks

Dos cadenas independientes con cadencias distintas:

```mermaid
flowchart LR
    subgraph C1[Chain 1 — domingos 03:00 NY]
        A1[TSK_BRONZE_CITYBIKE<br/>LOAD_CITYBIKE_NY] --> A2{TSK_BRONZE_NOAA<br/>WHEN stm_ny OR stm_jc}
        A2 --> A3[LOAD_NOAA_YEAR]
    end

    subgraph C2[Chain 2 — dia 1 mes 17:00 NY]
        B1[TSK_BRONZE_JC_REFRESH<br/>REFRESH_JC_STAGE] --> B2{TSK_BRONZE_JC_ONFILES<br/>WHEN stm_jc}
        B2 --> B3[LOAD_CITYBIKE_JC]
        B3 --> B4[TSK_BRONZE_JC_DRAIN<br/>DRAIN_JC_STAGE_STREAM]
    end
```

Antes de Chain 2 corre el script Python (Task Scheduler / GitHub Actions) que sube al landing los meses JC faltantes.

## Flujo end-to-end

1. **Setup** — `WH_NYCBIKE_DEV`, DB `WH_NYCBIKE`, schemas BRONZE/SILVER/GOLD, rol `ROLE_NYCBIKE`, integración Git para versionar SQL.
2. **Stages** — externos a S3 público (NY, NOAA) e interno para JC.
3. **Ingesta NY** — semanal, COPY directo desde S3 con `PATTERN`.
4. **Ingesta JC** — Python idempotente sube los meses nuevos al landing → stream sobre stage detecta archivos → COPY → drain del stream.
5. **Ingesta NOAA** — condicional: solo si los streams de Citi Bike traen filas nuevas.
6. **Logging** — cada procedure escribe en `bronze.load_log` (rows, files, errores).
7. **Transformación** — dbt consume los streams en modelos incrementales (Silver) y construye dimensiones/hechos (Gold).
8. **Consumo** — Power BI sobre la capa Gold.

## Estructura del repo

```
Snowflake_proyect/
├── extract_jc_to_stage.py        # ingestor idempotente JC → landing
├── .env.example                   # variables de conexion Snowflake
├── requirements.txt
└── Snowflake/
    ├── ROLS + SETUP/
    │   ├── Set Up Inicial.sql     # warehouses, db, schemas
    │   └── Rol.sql                # ROLE_NYCBIKE + grants
    └── BRONZE/
        ├── GITHUB + STAGES/
        │   ├── Github Integration.sql
        │   └── Stages.sql         # file formats + stages
        ├── ROOT LAYER.sql         # tablas, procedures, log
        ├── Tasks + Streams.sql    # streams + tasks encadenados
        ├── Task Control.sql       # RESUME / SUSPEND / EXECUTE
        └── Verify steps.sql       # checks de ingesta
```

## Idempotencia y resiliencia

- **Python** — compara contra `LS @stage` y solo sube los meses faltantes.
- **COPY INTO** — load metadata evita reingesta del mismo archivo (FORCE=FALSE por defecto).
- **`ON_ERROR='CONTINUE'`** — un archivo malo no rompe el batch.
- **EXCEPTION handlers** — cualquier error queda registrado en `bronze.load_log`.
- **Drain del stream de stage JC** — evita que el `WHEN` quede permanentemente TRUE.

## Configuración rápida

```bash
cp .env.example .env
# editar credenciales
pip install -r requirements.txt
python extract_jc_to_stage.py
```

Orden de ejecución del SQL: `Set Up Inicial → Rol → Github Integration → Stages → ROOT LAYER → Tasks + Streams → Task Control`.
