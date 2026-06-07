-- Silver Layer: Dynamic Tables (GIC)
-- DLT equivalent: @dlt.table(name=SILVER_X) + create_auto_cdc_flow(scd_type=2)
-- Key difference from DLT: DT auto-refreshes on schedule; DLT auto-refreshes on new data
--
-- To run:
--   cz-cli sql --file 03_lakehouse/dynamic_tables/silver_dynamic_tables.sql --profile aws_singapore_prod --sync --write

-- ── DT: Customers (SCD Type 2) ──
-- DLT: create_auto_cdc_flow(keys=["customer_id"], sequence_by="last_update_time", stored_as_scd_type=2)
CREATE OR REPLACE DYNAMIC TABLE apparel_silver.dt_customers_current
REFRESH INTERVAL 10 MINUTE
VCLUSTER DEFAULT
AS
WITH cleaned AS (
    SELECT customer_id, name, email, address, join_date, loyalty_points,
           phone_number, age, gender, last_update_time
    FROM apparel_bronze.raw_customers
    WHERE customer_id IS NOT NULL AND age >= 18 AND age <= 100
),
scd2 AS (
    SELECT *,
           LEAD(last_update_time) OVER (PARTITION BY customer_id ORDER BY last_update_time) AS lead_ts
    FROM cleaned
)
SELECT customer_id, name, email, address, join_date, loyalty_points,
       phone_number, age, gender,
       last_update_time AS __start_at,
       COALESCE(lead_ts, TIMESTAMP'9999-12-31 23:59:59') AS __end_at,
       (lead_ts IS NULL) AS __is_current
FROM scd2;

-- ── Manual refresh (equivalent to DLT pipeline trigger) ──
REFRESH DYNAMIC TABLE apparel_silver.dt_customers_current;
