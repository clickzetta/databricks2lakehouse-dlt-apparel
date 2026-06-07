-- Gold Layer: Dynamic Tables (GIC)
-- DLT equivalent: @dlt.table(name=GOLD_X) aggregations
-- F.window("event_time","1 day") → CAST(event_time AS DATE) GROUP BY

-- ── DT: Daily sales by store ──
-- DLT: df.groupBy(F.window("event_time","1 day"), "store_id"...)
CREATE OR REPLACE DYNAMIC TABLE apparel_gold.dt_daily_sales_by_store
REFRESH INTERVAL 10 MINUTE
VCLUSTER DEFAULT
AS
SELECT CAST(event_time AS DATE) AS sale_date, store_id, store_name,
    ROUND(SUM(total_amount), 2)      AS total_revenue,
    COUNT(transaction_id)             AS total_transactions,
    SUM(quantity)                     AS total_items_sold,
    COUNT(DISTINCT customer_id)       AS unique_customers
FROM apparel_gold.denormalized_sales_facts
GROUP BY CAST(event_time AS DATE), store_id, store_name;

-- ── DT: Product performance ──
CREATE OR REPLACE DYNAMIC TABLE apparel_gold.dt_product_performance
REFRESH INTERVAL 10 MINUTE
VCLUSTER DEFAULT
AS
SELECT product_id, product_name,
    MAX(product_category)            AS product_category,
    ROUND(SUM(total_amount), 2)      AS total_revenue,
    ROUND(SUM(quantity), 2)          AS total_quantity_sold,
    COUNT(transaction_id)            AS total_orders
FROM apparel_gold.denormalized_sales_facts
GROUP BY product_id, product_name;

-- ── DT: Customer lifetime value ──
CREATE OR REPLACE DYNAMIC TABLE apparel_gold.dt_customer_lifetime_value
REFRESH INTERVAL 10 MINUTE
VCLUSTER DEFAULT
AS
SELECT customer_id, customer_name,
    ROUND(SUM(total_amount), 2)              AS total_spend,
    COUNT(DISTINCT transaction_id)            AS total_orders,
    MIN(CAST(event_time AS DATE))             AS first_purchase_date,
    MAX(CAST(event_time AS DATE))             AS last_purchase_date,
    ROUND(AVG(total_amount), 2)              AS avg_order_value
FROM apparel_gold.denormalized_sales_facts
GROUP BY customer_id, customer_name;

-- ── Refresh all ──
REFRESH DYNAMIC TABLE apparel_gold.dt_daily_sales_by_store;
REFRESH DYNAMIC TABLE apparel_gold.dt_product_performance;
REFRESH DYNAMIC TABLE apparel_gold.dt_customer_lifetime_value;
