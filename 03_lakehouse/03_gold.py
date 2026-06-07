import os, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / "includes"))

from clickzetta.zettapark import Session
from clickzetta.zettapark import functions as F
from clickzetta.zettapark.window import Window
from clickzetta.zettapark.types import StringType, DoubleType, TimestampType

from configuration import BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA

session = Session.builder.configs({
    "instance": os.getenv("CZ_INSTANCE", "de1cbb4a"),
    "workspace": os.getenv("CZ_WORKSPACE", "quick_start"),
    "vcluster": os.getenv("CZ_VCLUSTER", "default"),
    "username": os.getenv("CZ_USERNAME", ""),
    "password": os.getenv("CZ_PASSWORD", ""),
    "service": os.getenv("CZ_SERVICE", "https://ap-southeast-1-aws.api.singdata.com"),
}).create()

# Migration from 03_gold.py:
# dlt.read_stream("LIVE.X") / dlt.read("LIVE.X") → session.table("X")
# F.window("event_time","1 day") → F.to_date(F.col("event_time"))  ← F.window not in ZettaPark
# @dlt.table(name=X) → df.write.mode("overwrite").saveAsTable(X)

# ── Denormalized sales facts ──
# DLT: join sales stream with current dimension snapshots
sales    = session.table(f"{SILVER_SCHEMA}.silver_sales_transactions")
customers = session.table(f"{SILVER_SCHEMA}.silver_customers_current")
products  = session.table(f"{SILVER_SCHEMA}.silver_products_current")
stores    = session.table(f"{SILVER_SCHEMA}.silver_stores_current")

facts = (sales
    .join(customers.select("customer_id", F.col("name").alias("customer_name")), "customer_id", "left")
    .join(products.select("product_id", F.col("name").alias("product_name"), "category"), "product_id", "left")
    .join(stores.select("store_id", F.col("name").alias("store_name")), "store_id", "left")
    .select("transaction_id","event_time","customer_id","customer_name",
            "product_id","product_name","category","store_id","store_name",
            "quantity","unit_price","total_amount","payment_method","discount_applied","tax_amount")
)
facts.write.mode("overwrite").saveAsTable(f"{GOLD_SCHEMA}.denormalized_sales_facts")
print(f"  denormalized_sales_facts: {facts.count():,}")

# ── Daily sales by store ──
# DLT: df.groupBy(F.window("event_time","1 day"),...) → F.to_date(F.col("event_time"))
daily = (facts
    .withColumn("sale_date", F.to_date(F.col("event_time")))  # F.window → F.to_date
    .groupBy("sale_date","store_id","store_name")
    .agg(
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.count("transaction_id").alias("total_transactions"),
        F.sum("quantity").alias("total_items_sold"),
        F.countDistinct("customer_id").alias("unique_customers")
    )
)
daily.write.mode("overwrite").saveAsTable(f"{GOLD_SCHEMA}.gold_daily_sales_by_store")
print(f"  gold_daily_sales_by_store: {daily.count():,}")

# ── Product performance ──
product_perf = (facts
    .groupBy("product_id","product_name","category")
    .agg(
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.round(F.sum("quantity"), 2).alias("total_quantity_sold"),
        F.count("transaction_id").alias("total_orders")
    )
)
product_perf.write.mode("overwrite").saveAsTable(f"{GOLD_SCHEMA}.gold_product_performance")
print(f"  gold_product_performance: {product_perf.count():,}")

# ── Customer lifetime value ──
clv = (facts
    .groupBy("customer_id","customer_name")
    .agg(
        F.round(F.sum("total_amount"), 2).alias("total_spend"),
        F.countDistinct("transaction_id").alias("total_orders"),
        F.min(F.to_date(F.col("event_time"))).alias("first_purchase_date"),
        F.max(F.to_date(F.col("event_time"))).alias("last_purchase_date"),
        F.round(F.avg("total_amount"), 2).alias("avg_order_value")
    )
)
clv.write.mode("overwrite").saveAsTable(f"{GOLD_SCHEMA}.gold_customer_lifetime_value")
print(f"  gold_customer_lifetime_value: {clv.count():,}")
