-- Silver Layer: Pure SQL alternative to ZettaPark Python
-- @dlt.expect_or_drop → WHERE; create_auto_cdc_flow(scd=2) → LEAD() OVER Window

CREATE OR REPLACE TABLE apparel_silver.silver_customers AS
WITH cleaned AS (
    SELECT * FROM apparel_bronze.raw_customers
    WHERE customer_id IS NOT NULL AND age >= 18 AND age <= 100
),
scd2 AS (
    SELECT *, last_update_time AS __start_at,
        LEAD(last_update_time) OVER (PARTITION BY customer_id ORDER BY last_update_time) AS __end_at
    FROM cleaned
)
SELECT *, COALESCE(__end_at, TIMESTAMP'9999-12-31 23:59:59') AS __end_at,
    (__end_at IS NULL) AS __is_current
FROM scd2;

CREATE OR REPLACE TABLE apparel_silver.silver_products AS
WITH cleaned AS (SELECT * FROM apparel_bronze.raw_products WHERE product_id IS NOT NULL),
scd2 AS (SELECT *, last_update_time AS __start_at,
    LEAD(last_update_time) OVER (PARTITION BY product_id ORDER BY last_update_time) AS __end_at FROM cleaned)
SELECT *, COALESCE(__end_at, TIMESTAMP'9999-12-31 23:59:59') AS __end_at, (__end_at IS NULL) AS __is_current FROM scd2;

CREATE OR REPLACE TABLE apparel_silver.silver_stores AS
WITH cleaned AS (SELECT * FROM apparel_bronze.raw_stores WHERE store_id IS NOT NULL),
scd2 AS (SELECT *, last_update_time AS __start_at,
    LEAD(last_update_time) OVER (PARTITION BY store_id ORDER BY last_update_time) AS __end_at FROM cleaned)
SELECT *, COALESCE(__end_at, TIMESTAMP'9999-12-31 23:59:59') AS __end_at, (__end_at IS NULL) AS __is_current FROM scd2;

CREATE OR REPLACE TABLE apparel_silver.silver_sales_transactions AS
SELECT transaction_id, store_id, CAST(event_time AS TIMESTAMP) AS event_time,
    customer_id, product_id, quantity, unit_price, total_amount,
    payment_method, discount_applied, tax_amount
FROM apparel_bronze.raw_sales WHERE transaction_id IS NOT NULL AND total_amount > 0 AND quantity > 0;

CREATE OR REPLACE VIEW apparel_silver.silver_customers_current AS SELECT * FROM apparel_silver.silver_customers WHERE __is_current = TRUE;
CREATE OR REPLACE VIEW apparel_silver.silver_products_current AS SELECT * FROM apparel_silver.silver_products WHERE __is_current = TRUE;
CREATE OR REPLACE VIEW apparel_silver.silver_stores_current AS SELECT * FROM apparel_silver.silver_stores WHERE __is_current = TRUE;
