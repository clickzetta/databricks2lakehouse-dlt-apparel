-- Gold Layer: Pure SQL alternative
-- F.window("event_time","1 day") → DATE_TRUNC('day', event_time)

CREATE OR REPLACE TABLE apparel_gold.denormalized_sales_facts AS
SELECT s.transaction_id, CAST(s.event_time AS TIMESTAMP) AS event_time,
    s.customer_id, c.name AS customer_name,
    s.product_id, p.name AS product_name, p.category,
    s.store_id, st.name AS store_name,
    s.quantity, s.unit_price, s.total_amount, s.payment_method, s.discount_applied, s.tax_amount
FROM apparel_silver.silver_sales_transactions s
LEFT JOIN apparel_silver.silver_customers_current c ON s.customer_id = c.customer_id
LEFT JOIN apparel_silver.silver_products_current p ON s.product_id = p.product_id
LEFT JOIN apparel_silver.silver_stores_current st ON s.store_id = st.store_id;

CREATE OR REPLACE TABLE apparel_gold.gold_daily_sales_by_store AS
SELECT DATE_TRUNC('day', event_time) AS sale_date, store_id, store_name,
    ROUND(SUM(total_amount),2) AS total_revenue,
    COUNT(transaction_id) AS total_transactions,
    SUM(quantity) AS total_items_sold,
    COUNT(DISTINCT customer_id) AS unique_customers
FROM apparel_gold.denormalized_sales_facts
GROUP BY DATE_TRUNC('day', event_time), store_id, store_name;

CREATE OR REPLACE TABLE apparel_gold.gold_product_performance AS
SELECT product_id, product_name, category,
    ROUND(SUM(total_amount),2) AS total_revenue,
    ROUND(SUM(quantity),2) AS total_quantity_sold,
    COUNT(transaction_id) AS total_orders
FROM apparel_gold.denormalized_sales_facts GROUP BY product_id, product_name, category;

CREATE OR REPLACE TABLE apparel_gold.gold_customer_lifetime_value AS
SELECT customer_id, customer_name,
    ROUND(SUM(total_amount),2) AS total_spend,
    COUNT(DISTINCT transaction_id) AS total_orders,
    MIN(event_time)::DATE AS first_purchase_date,
    MAX(event_time)::DATE AS last_purchase_date,
    ROUND(AVG(total_amount),2) AS avg_order_value
FROM apparel_gold.denormalized_sales_facts GROUP BY customer_id, customer_name;
