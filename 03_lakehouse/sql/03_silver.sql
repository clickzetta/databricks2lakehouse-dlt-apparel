-- Silver Layer: Cleansed & conformed data
-- Databricks equivalent: 02A_silver.py (@dlt.view + @dlt.expect_or_drop)
--                        02B_silver.py (dlt.create_auto_cdc_flow, SCD Type 2)
--                        02C/02D: sales + returns cleaning
--
-- Migration:
--   @dlt.view + @dlt.expect_or_drop → CREATE TABLE AS SELECT ... WHERE condition
--   dlt.create_auto_cdc_flow (SCD2) → window-based SCD2 with LEAD()
--   dlt.read_stream / dlt.read → FROM schema.table

-- ── Silver: Customers (SCD2) ──
-- DLT: dlt.create_auto_cdc_flow(target=silver_customers, keys=["customer_id"],
--        sequence_by=last_update_time, stored_as_scd_type=2)
CREATE OR REPLACE TABLE apparel_silver.silver_customers AS
WITH customers_cleaned AS (
    -- @dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
    SELECT *
    FROM apparel_bronze.raw_customers
    WHERE customer_id IS NOT NULL
      AND age >= 18 AND age <= 100        -- @dlt.expect("realistic_age", ...)
),
scd2 AS (
    SELECT
        customer_id, name, email, address,
        join_date, loyalty_points, phone_number, age, gender,
        last_update_time AS __start_at,
        LEAD(last_update_time) OVER (
            PARTITION BY customer_id ORDER BY last_update_time
        ) AS __end_at
    FROM customers_cleaned
)
SELECT
    customer_id, name, email, address,
    join_date, loyalty_points, phone_number, age, gender,
    __start_at,
    COALESCE(__end_at, TIMESTAMP'9999-12-31 23:59:59') AS __end_at,
    (__end_at IS NULL) AS __is_current
FROM scd2;

-- ── Silver: Products (SCD2) ──
CREATE OR REPLACE TABLE apparel_silver.silver_products AS
WITH products_cleaned AS (
    SELECT *
    FROM apparel_bronze.raw_products
    WHERE product_id IS NOT NULL
),
scd2 AS (
    SELECT
        product_id, name, category, brand, price, description, color, size,
        last_update_time AS __start_at,
        LEAD(last_update_time) OVER (
            PARTITION BY product_id ORDER BY last_update_time
        ) AS __end_at
    FROM products_cleaned
)
SELECT
    product_id, name, category, brand, price, description, color, size,
    __start_at,
    COALESCE(__end_at, TIMESTAMP'9999-12-31 23:59:59') AS __end_at,
    (__end_at IS NULL) AS __is_current
FROM scd2;

-- ── Silver: Stores (SCD2) ──
CREATE OR REPLACE TABLE apparel_silver.silver_stores AS
WITH stores_cleaned AS (
    SELECT *
    FROM apparel_bronze.raw_stores
    WHERE store_id IS NOT NULL
),
scd2 AS (
    SELECT
        store_id, name, city, state, country,
        last_update_time AS __start_at,
        LEAD(last_update_time) OVER (
            PARTITION BY store_id ORDER BY last_update_time
        ) AS __end_at
    FROM stores_cleaned
)
SELECT
    store_id, name, city, state, country,
    __start_at,
    COALESCE(__end_at, TIMESTAMP'9999-12-31 23:59:59') AS __end_at,
    (__end_at IS NULL) AS __is_current
FROM scd2;

-- ── Silver: Sales transactions (cleaned) ──
-- DLT: @dlt.view(SALES_CLEANED_STREAM) + @dlt.expect_or_drop
CREATE OR REPLACE TABLE apparel_silver.silver_sales_transactions AS
SELECT
    transaction_id,
    store_id,
    CAST(event_time AS TIMESTAMP) AS event_time,
    customer_id,
    product_id,
    quantity,
    unit_price,
    total_amount,
    payment_method,
    discount_applied,
    tax_amount
FROM apparel_bronze.raw_sales
WHERE transaction_id IS NOT NULL
  AND total_amount > 0                   -- @dlt.expect_or_drop("valid_amount", ...)
  AND quantity > 0;                      -- @dlt.expect_or_drop("valid_quantity", ...)

-- ── Silver: Current dimension views (latest snapshot) ──
-- DLT: silver_customers_current / silver_products_current / silver_stores_current
CREATE OR REPLACE VIEW apparel_silver.silver_customers_current AS
SELECT * FROM apparel_silver.silver_customers WHERE __is_current = TRUE;

CREATE OR REPLACE VIEW apparel_silver.silver_products_current AS
SELECT * FROM apparel_silver.silver_products WHERE __is_current = TRUE;

CREATE OR REPLACE VIEW apparel_silver.silver_stores_current AS
SELECT * FROM apparel_silver.silver_stores WHERE __is_current = TRUE;
