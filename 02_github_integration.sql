-- 02_github_integration.sql
-- Conecta Snowflake con el repo de GitHub Rlizaran/Snowflake_proyect.

USE ROLE ROLE_NYCBIKE;
USE WAREHOUSE WH_NYCBIKE_DEV;
USE DATABASE WH_NYCBIKE;
USE SCHEMA    bronze;

-- API_INTEGRATION hacia GitHub (solo referencia el SECRET, no el token)
CREATE OR REPLACE API INTEGRATION github_api_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES  = ('https://github.com/Rlizaran/')
  ALLOWED_AUTHENTICATION_SECRETS = (WH_NYCBIKE.bronze.github_pat)
  ENABLED = TRUE;

-- GIT REPOSITORY apuntando al repo de GitHub
CREATE OR REPLACE GIT REPOSITORY WH_NYCBIKE.bronze.citibike_repo
  API_INTEGRATION = github_api_integration
  GIT_CREDENTIALS = WH_NYCBIKE.bronze.github_pat
  ORIGIN = 'https://github.com/Rlizaran/Snowflake_proyect.git';

-- Traer la última versión del repo
ALTER GIT REPOSITORY WH_NYCBIKE.bronze.citibike_repo FETCH;

-- Verificaciones
SHOW GIT BRANCHES IN WH_NYCBIKE.bronze.citibike_repo;
LS @WH_NYCBIKE.bronze.citibike_repo/branches/main/;
