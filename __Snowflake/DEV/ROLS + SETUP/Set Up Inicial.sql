-- Set Up Inicial: warehouses + 3 databases medallion (Bronze / Silver / Gold)
-- Warehouse para ingesta y transformaciones
CREATE OR REPLACE WAREHOUSE WH_NYCBIKE_DEV
    WITH WAREHOUSE_SIZE = 'XSMALL'
         AUTO_SUSPEND = 60
         AUTO_RESUME = TRUE
         INITIALLY_SUSPENDED = TRUE
         SCALING_POLICY = 'ECONOMY'
         COMMENT = 'Warehouse para ingesta y transformaciones';

-- Warehouse para analisis en Notebooks
CREATE OR REPLACE WAREHOUSE WH_ANALISIS
    WITH WAREHOUSE_SIZE = 'SMALL'
         AUTO_SUSPEND = 60
         AUTO_RESUME = TRUE
         INITIALLY_SUSPENDED = TRUE
         SCALING_POLICY = 'ECONOMY'
         COMMENT = 'Warehouse para analisis en Notebooks';

-- Database Bronze DEV: landing para los datos crudos (entorno desarrollo, usado por __Snowflake)
CREATE OR REPLACE DATABASE DEV_CITYBIKE_BRONZE
    COMMENT = 'datos crudos tal cual llegan desde S3 / landing stages (DEV)';

-- Database Bronze PRO: landing produccion (lo consume dbt)
CREATE OR REPLACE DATABASE PRO_CITYBIKE_BRONZE
    COMMENT = 'datos crudos tal cual llegan desde S3 / landing stages (PRO, usado por dbt)';

-- Database Silver DEV: modelos limpios y tipados (entorno desarrollo)
CREATE OR REPLACE DATABASE DEV_CITYBIKE_SILVER
    COMMENT = 'datos limpios, tipados y conformes (DEV)';

-- Database Silver PRO: modelos limpios produccion (lo consume dbt)
CREATE OR REPLACE DATABASE PRO_CITYBIKE_SILVER
    COMMENT = 'datos limpios, tipados y conformes (PRO, usado por dbt)';

-- Database Gold DEV: datamarts para Power BI (entorno desarrollo)
CREATE OR REPLACE DATABASE DEV_CITYBIKE_GOLD
    COMMENT = 'datamarts analiticos para Power BI (DEV)';

-- Database Gold PRO: datamarts produccion (lo consume dbt)
CREATE OR REPLACE DATABASE PRO_CITYBIKE_GOLD
    COMMENT = 'datamarts analiticos para Power BI (PRO, usado por dbt)';

-- Database LOGS: database para alojar tablas, stages, streams, tasks
CREATE OR REPLACE DATABASE DB_CITYBIKE_LOGS
    COMMENT = 'database para alojar tablas, stages, streams, tasks';

-- Schema LOGS dentro de DB_CITYBIKE_LOGS para alojar tablas, stages, streams, tasks
CREATE OR REPLACE SCHEMA DB_CITYBIKE_LOGS.LOGS
    COMMENT = 'Tablas raw, stages, file formats, streams y tasks';

-- Schema CITYBIKE dentro de DEV_CITYBIKE_BRONZE para alojar tablas, stages
CREATE OR REPLACE SCHEMA DEV_CITYBIKE_BRONZE.CITYBIKE
    COMMENT = 'Tablas raw, stages, file formats, streams y tasks';

-- Schema NOAA dentro de DEV_CITYBIKE_BRONZE para alojar tablas, stages
CREATE OR REPLACE SCHEMA DEV_CITYBIKE_BRONZE.NOAA
    COMMENT = 'Tablas raw, stages, file formats, streams y tasks';

-- Schema CITYBIKE dentro de PRO_CITYBIKE_BRONZE (espejo PRO para dbt)
CREATE OR REPLACE SCHEMA PRO_CITYBIKE_BRONZE.CITYBIKE
    COMMENT = 'Tablas raw, stages, file formats (PRO)';

-- Schema NOAA dentro de PRO_CITYBIKE_BRONZE (espejo PRO para dbt)
CREATE OR REPLACE SCHEMA PRO_CITYBIKE_BRONZE.NOAA
    COMMENT = 'Tablas raw, stages, file formats (PRO)';

