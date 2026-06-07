-- Bronze Layer: Pipe for continuous S3 ingestion
-- DLT equivalent: Auto Loader (spark.readStream.format("cloudFiles")...)
-- Requires: external Volume connected to S3/OSS/COS
--
-- Verified on AWS Singapore: Pipe auto-ingested new files from S3 ✅
--
-- Setup:
-- 1. Create external Volume connected to S3
-- 2. Create target table with explicit schema (no inferSchema with external volume)
-- 3. Create Pipe pointing to the external Volume

-- ── Step 1: Create S3 connection (if not exists) ──
-- CREATE STORAGE CONNECTION s3_conn ...  (see connection setup docs)

-- ── Step 2: Create external Volume pointing to S3 ──
CREATE EXTERNAL VOLUME IF NOT EXISTS apparel_bronze.s3_sales_landing
    LOCATION 's3://your-bucket/apparel_landing/'
    USING CONNECTION s3_conn
    DIRECTORY = (enable=true, auto_refresh=true)
    RECURSIVE = true;

-- ── Step 3: Create target table with explicit schema ──
-- (External volumes don't support inferSchema — must define schema explicitly)
CREATE TABLE IF NOT EXISTS apparel_bronze.raw_sales_pipe (
    transaction_id   BIGINT,
    store_id         BIGINT,
    event_time       STRING,
    customer_id      BIGINT,
    product_id       BIGINT,
    quantity         BIGINT,
    unit_price       DOUBLE,
    total_amount     DOUBLE,
    payment_method   STRING,
    discount_applied DOUBLE,
    tax_amount       DOUBLE
);

-- ── Step 4: Create Pipe ──
-- DLT: spark.readStream.format("cloudFiles").option("cloudFiles.format","csv").load(PATH)
-- Pipe: continuously polls for new files in Volume, runs COPY INTO when new files arrive
CREATE OR REPLACE PIPE apparel_bronze.pipe_sales
    VIRTUAL_CLUSTER = 'DEFAULT'
    INGEST_MODE = 'LIST_PURGE'     -- polls for new files (like Auto Loader LIST mode)
AS COPY INTO apparel_bronze.raw_sales_pipe
FROM VOLUME apparel_bronze.s3_sales_landing
USING CSV OPTIONS ('header'='true')
PURGE = TRUE                       -- remove processed files from landing zone
ON_ERROR = CONTINUE;

-- ── Check Pipe status ──
SHOW PIPES IN apparel_bronze;

-- ── Manual trigger (for testing) ──
-- ALTER PIPE apparel_bronze.pipe_sales REFRESH;
