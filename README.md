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
        STG_NY[CITYBIKE_S3_STAGE]
        STG_JC[CITYBIKE_LANDING_STAGE<br/>interno]
        STG_NOAA[NOAA_S3_STAGE_YEAR]
    end

    subgraph BRONZE[DB_CITYBIKE_BRONZE]
        subgraph BRONZE_CB[CITYBIKE schema]
            T_NY[(citybike_trips_ny)]
            T_JC[(citybike_trips_jc)]
        end
        subgraph BRONZE_NOAA[NOAA schema]
            T_NOAA[(noaa_raw_year)]
        end
        subgraph BRONZE_LOGS[LOGS schema]
            LOG[(load_log)]
            STM_NY{{stm_citybike_ny<br/>append-only on table}}
            STM_JC_TBL{{stm_citybike_jc<br/>append-only on table}}
            STM_JC_STG{{stm_citybike_jc_stage<br/>on stage}}
            STM_NOAA{{stm_noaa_year<br/>append-only on table}}
        end
    end

    subgraph SILVER[DB_CITYBIKE_SILVER - dbt]
        SLV_CB[CITYBIKE: stg_trips_ny / stg_trips_jc]
        SLV_NOAA[NOAA: stg_weather_daily]
    end

    subgraph GOLD[DB_CITYBIKE_GOLD.MARTS - dbt]
        GLD[dim_station / dim_date / fct_trips / fct_trips_daily]
    end

    PBI[Power BI]

    S3NY --> STG_NY --> T_NY
    PY -- PUT --> STG_JC --> T_JC
    S3NOAA --> STG_NOAA --> T_NOAA

    T_NY --> STM_NY
    T_JC --> STM_JC_TBL
    STG_JC --> STM_JC_STG
    T_NOAA --> STM_NOAA

    STM_NY --> SLV_CB
    STM_JC_TBL --> SLV_CB
    STM_NOAA --> SLV_NOAA

    SLV_CB --> GLD
    SLV_NOAA --> GLD
    GLD --> PBI

    T_NY -.log.-> LOG
    T_JC -.log.-> LOG
    T_NOAA -.log.-> LOG
```

## Orquestación de Tasks

Todos los tasks viven en `DB_CITYBIKE_BRONZE.LOGS` (mismo schema requerido por el DAG de Snowflake).

```mermaid
flowchart LR
    subgraph C1[Chain 1 — domingos 03:00 NY]
        A1[TSK_BRONZE_CITYBIKE<br/>LOAD_CITYBIKE_NY] --> A2{TSK_BRONZE_NOAA<br/>WHEN stm_citybike_ny OR stm_citybike_jc}
        A2 --> A3[LOAD_NOAA_YEAR]
    end

    subgraph C2[Chain 2 — dia 1 mes 17:00 NY]
        B1[TSK_BRONZE_JC_REFRESH<br/>REFRESH_JC_STAGE] --> B2{TSK_BRONZE_JC_ONFILES<br/>WHEN stm_citybike_jc_stage}
        B2 --> B3[LOAD_CITYBIKE_JC]
        B3 --> B4[TSK_BRONZE_JC_DRAIN<br/>DRAIN_JC_STAGE_STREAM]
    end
```

Antes de Chain 2 corre el script Python (Task Scheduler / GitHub Actions) que sube al landing los meses JC faltantes.

## Flujo end-to-end

1. **Setup** — `WH_NYCBIKE_DEV`, 3 DBs medallion `DB_CITYBIKE_BRONZE` / `DB_CITYBIKE_SILVER` / `DB_CITYBIKE_GOLD`. Schemas: Bronze tiene `LOGS`, `CITYBIKE`, `NOAA`; Silver tiene `CITYBIKE`, `NOAA`; Gold tiene `MARTS`. Rol `ROLE_NYCBIKE`, integración Git para versionar SQL.
2. **Stages** — externos a S3 público (NY, NOAA) e interno para JC.
3. **Ingesta NY** — semanal, COPY directo desde S3 con `PATTERN`.
4. **Ingesta JC** — Python idempotente sube los meses nuevos al landing → stream sobre stage detecta archivos → COPY → drain del stage stream.
5. **Ingesta NOAA** — condicional: solo si los streams de Citi Bike traen filas nuevas.
6. **Logging** — cada procedure escribe en `LOGS.LOAD_LOG` (rows, files, errores).
7. **Transformación** — dbt consume las tablas Bronze como sources y materializa Silver (`DB_CITYBIKE_SILVER.{CITYBIKE,NOAA}`) y Gold (`DB_CITYBIKE_GOLD.MARTS`).
8. **Consumo** — Power BI sobre la capa Gold.

## Estructura del repo

```
Snowflake_proyect/
├── extract_jc_to_stage.py        # ingestor idempotente JC → landing
├── .env.example                   # variables de conexion Snowflake
├── requirements.txt
├── dbt_project.yml                # config dbt: staging→SILVER.{CITYBIKE,NOAA}, marts→GOLD.MARTS
├── profiles.yml                   # perfil dbt (mover a ~/.dbt/ o usar DBT_PROFILES_DIR)
├── packages.yml                   # dependencias dbt
├── models/                        # modelos dbt
│   ├── staging/                   # se materializan en DB_CITYBIKE_SILVER (citybike/, noaa/)
│   └── marts/                     # se materializan en DB_CITYBIKE_GOLD.MARTS
└── Snowflake/
    ├── ROLS + SETUP/
    │   ├── Set Up Inicial.sql     # warehouses + 3 DBs + schemas
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

## Configuración rápida

```bash
cp .env.example .env
# editar credenciales
pip install -r requirements.txt
python extract_jc_to_stage.py
```

Orden de ejecución del SQL: `Set Up Inicial → Rol → Github Secret → Github Integration → Stages + FileFormat → ROOT LAYER → Tasks + Streams → Task Control`.
