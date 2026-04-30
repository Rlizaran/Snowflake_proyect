-- Set Up Inicial: warehouses + 3 databases medallion (Bronze / Silver / Gold)

-- Warehouse para ingesta y transformaciones
CREATE OR REPLACE WAREHOUSE WH_NYCBIKE_DEV
    WITH WAREHOUSE_SIZE = 'XSMALL'
         AUTO_SUSPEND = 60
         AUTO_RESUME = TRUE
         INITIALLY_SUSPENDED = TRUE
         SCALING_POLICY = 'ECONOMY'
         COMMENT = 'Warehouse para ingesta y transformaciones del proyecto NYC CityBike + NOAA';

-- Warehouse para analisis en Notebooks
CREATE OR REPLACE WAREHOUSE WH_ANALISIS
    WITH WAREHOUSE_SIZE = 'SMALL'
         AUTO_SUSPEND = 60
         AUTO_RESUME = TRUE
         INITIALLY_SUSPENDED = TRUE
         SCALING_POLICY = 'ECONOMY'
         COMMENT = 'Warehouse para analisis en Notebooks';

-- Database Bronze: landing para los datos crudos
CREATE OR REPLACE DATABASE DB_CITYBIKE_BRONZE
    COMMENT = 'Capa Bronze: datos crudos tal cual llegan desde S3 / landing stages';

-- Database Silver: dbt materializa aqui los modelos limpios y tipados
CREATE OR REPLACE DATABASE DB_CITYBIKE_SILVER
    COMMENT = 'Capa Silver: datos limpios, tipados y conformes (dbt)';

-- Database Gold: datamarts para Power BI
CREATE OR REPLACE DATABASE DB_CITYBIKE_GOLD
    COMMENT = 'Capa Gold: datamarts analiticos para Power BI (dbt)';

-- Schema BRONZE dentro de DB_CITYBIKE_BRONZE para alojar tablas, stages, streams, tasks
CREATE OR REPLACE SCHEMA DB_CITYBIKE_BRONZE.BRONZE
    COMMENT = 'Objetos crudos: tablas raw, stages, file formats, streams y tasks';

-- Schema SILVER dentro de DB_CITYBIKE_SILVER para los modelos de dbt
CREATE OR REPLACE SCHEMA DB_CITYBIKE_SILVER.SILVER
    COMMENT = 'Modelos dbt limpios y tipados (staging silver)';

-- Schema GOLD dentro de DB_CITYBIKE_GOLD para los datamarts de dbt
CREATE OR REPLACE SCHEMA DB_CITYBIKE_GOLD.GOLD
    COMMENT = 'Datamarts dbt (marts gold)';

-- Verificaciones
SHOW WAREHOUSES LIKE 'WH_NYCBIKE_DEV';
SHOW DATABASES   LIKE 'DB_CITYBIKE_%';
SHOW SCHEMAS IN DATABASE DB_CITYBIKE_BRONZE;
SHOW SCHEMAS IN DATABASE DB_CITYBIKE_SILVER;
SHOW SCHEMAS IN DATABASE DB_CITYBIKE_GOLD;
