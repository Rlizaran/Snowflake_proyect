-- Secret con el Personal Access Token de GitHub usado por el API_INTEGRATION para el repo

USE ROLE ACCOUNTADMIN;
USE DATABASE DB_CITYBIKE_BRONZE;

-- Schema dedicado a credenciales del proyecto (aislado de BRONZE)
CREATE SCHEMA IF NOT EXISTS DB_CITYBIKE_BRONZE.CITYBIKE
    COMMENT = 'Schema para secrets y credenciales del proyecto CityBike';

-- Secret tipo PASSWORD: usuario de GitHub + PAT (reemplazar <github_pat> por el token real)
CREATE OR REPLACE SECRET DB_CITYBIKE_BRONZE.CITYBIKE.GITHUB_PAT
    TYPE = PASSWORD
    USERNAME = '******'
    PASSWORD = '*******'
    COMMENT = 'GitHub Personal Access Token para citibike_repo';

-- Verificar el secret creado
SHOW SECRETS IN SCHEMA DB_CITYBIKE_BRONZE.CITYBIKE;
