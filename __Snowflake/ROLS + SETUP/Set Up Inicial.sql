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

-- Database Bronze: landing para los datos crudos
CREATE OR REPLACE DATABASE DB_CITYBIKE_BRONZE
    COMMENT = 'datos crudos tal cual llegan desde S3 / landing stages';

-- Database Silver: dbt materializa aqui los modelos limpios y tipados
CREATE OR REPLACE DATABASE DB_CITYBIKE_SILVER
    COMMENT = 'datos limpios, tipados y conformes';

-- Database Gold: datamarts para Power BI
CREATE OR REPLACE DATABASE DB_CITYBIKE_GOLD
    COMMENT = 'datamarts analiticos para Power BI';

-- Schema LOGS dentro de DB_CITYBIKE_BRONZE para alojar tablas, stages, streams, tasks
CREATE OR REPLACE SCHEMA DB_CITYBIKE_BRONZE.LOGS
    COMMENT = 'Table to keep track of the logs';

-- Schema CITYBIKE dentro de DB_CITYBIKE_BRONZE para alojar tablas, stages, streams, tasks
CREATE OR REPLACE SCHEMA DB_CITYBIKE_BRONZE.CITYBIKE
    COMMENT = 'Tablas raw, stages, file formats, streams y tasks';

-- Schema NOAA dentro de DB_CITYBIKE_BRONZE para alojar tablas, stages, streams, tasks
CREATE OR REPLACE SCHEMA DB_CITYBIKE_BRONZE.NOAA
    COMMENT = 'Tablas raw, stages, file formats, streams y tasks';

-- Schema CITYBIKE dentro de DB_CITYBIKE_SILVER para los modelos staging dbt de CityBike
CREATE OR REPLACE SCHEMA DB_CITYBIKE_SILVER.CITYBIKE
    COMMENT = 'Modelos dbt limpios y tipados (staging silver) - CityBike';

-- Schema NOAA dentro de DB_CITYBIKE_SILVER para los modelos staging dbt de NOAA
CREATE OR REPLACE SCHEMA DB_CITYBIKE_SILVER.NOAA
    COMMENT = 'Modelos dbt limpios y tipados (staging silver) - NOAA';

-- Schema GOLD dentro de DB_CITYBIKE_GOLD para los datamarts de dbt
CREATE OR REPLACE SCHEMA DB_CITYBIKE_GOLD.MARTS
    COMMENT = 'Datamarts';

-- Verificaciones
SHOW WAREHOUSES LIKE 'WH_NYCBIKE_DEV';
SHOW DATABASES   LIKE 'DB_CITYBIKE_%';
SHOW SCHEMAS IN DATABASE DB_CITYBIKE_BRONZE;
SHOW SCHEMAS IN DATABASE DB_CITYBIKE_SILVER;
SHOW SCHEMAS IN DATABASE DB_CITYBIKE_GOLD;

