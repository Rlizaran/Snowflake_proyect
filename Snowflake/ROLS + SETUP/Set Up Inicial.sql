CREATE OR REPLACE WAREHOUSE WH_NYCBIKE_DEV
    WITH WAREHOUSE_SIZE = 'XSMALL'
         AUTO_SUSPEND = 60
         AUTO_RESUME = TRUE
         INITIALLY_SUSPENDED = TRUE
         SCALING_POLICY = 'ECONOMY'
         COMMENT = 'Warehouse para ingesta y transformaciones del proyecto NYC CityBike + NOAA';

CREATE OR REPLACE WAREHOUSE WH_ANALISIS
    WITH WAREHOUSE_SIZE = 'SMALL'
         AUTO_SUSPEND = 60
         AUTO_RESUME = TRUE
         INITIALLY_SUSPENDED = TRUE
         SCALING_POLICY = 'ECONOMY'
         COMMENT = 'Warehouse para analisis en Notebooks';

CREATE OR REPLACE DATABASE WH_NYCBIKE
    COMMENT = 'Data Warehouse del proyecto: CityBike NYC + Jersey City + NOAA Weather';

CREATE OR REPLACE SCHEMA BRONZE
    COMMENT = 'Capa Bronze: datos crudos tal cual llegan desde S3 / landing stages';

CREATE OR REPLACE SCHEMA SILVER
    COMMENT = 'Capa Silver: datos limpios, tipados y conformes';

CREATE OR REPLACE SCHEMA GOLD
    COMMENT = 'Capa Gold: modelos analiticos para Power BI';

SHOW WAREHOUSES LIKE 'WH_NYCBIKE_DEV';
SHOW DATABASES   LIKE 'WH_NYCBIKE';
SHOW SCHEMAS IN DATABASE WH_NYCBIKE;