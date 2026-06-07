-- Setup: schemas and volume
CREATE SCHEMA IF NOT EXISTS apparel_bronze;
CREATE SCHEMA IF NOT EXISTS apparel_silver;
CREATE SCHEMA IF NOT EXISTS apparel_gold;
CREATE VOLUME IF NOT EXISTS apparel_bronze.raw_data;
