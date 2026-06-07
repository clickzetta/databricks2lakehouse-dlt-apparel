-- Setup: schemas and volume
-- Databricks equivalent: environment_setup.ipynb + dlt pipeline init

CREATE SCHEMA IF NOT EXISTS apparel_bronze;
CREATE SCHEMA IF NOT EXISTS apparel_silver;
CREATE SCHEMA IF NOT EXISTS apparel_gold;
CREATE VOLUME IF NOT EXISTS apparel_bronze.raw_data;
