-- Bronze Layer: Raw data ingestion
-- Databricks equivalent: 01_bronze.py — @dlt.table with read_stream (Auto Loader)
-- Migration: DLT streaming read → COPY INTO from Volume (batch) or Pipe (streaming)
-- For static seed data: use COPY INTO

-- ── Load CSV data from Volume ──
COPY INTO apparel_bronze.raw_customers
FROM VOLUME apparel_bronze.raw_data
USING CSV OPTIONS ('header' = 'true', 'inferSchema' = 'true')
FILES ('customers.csv')
ON_ERROR = CONTINUE;

COPY INTO apparel_bronze.raw_products
FROM VOLUME apparel_bronze.raw_data
USING CSV OPTIONS ('header' = 'true', 'inferSchema' = 'true')
FILES ('products.csv')
ON_ERROR = CONTINUE;

COPY INTO apparel_bronze.raw_stores
FROM VOLUME apparel_bronze.raw_data
USING CSV OPTIONS ('header' = 'true', 'inferSchema' = 'true')
FILES ('stores.csv')
ON_ERROR = CONTINUE;

COPY INTO apparel_bronze.raw_sales
FROM VOLUME apparel_bronze.raw_data
USING CSV OPTIONS ('header' = 'true', 'inferSchema' = 'true')
FILES ('sales.csv')
ON_ERROR = CONTINUE;
