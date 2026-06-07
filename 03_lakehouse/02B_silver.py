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

# Migration from 02B_silver.py:
# dlt.create_streaming_table(X) + dlt.create_auto_cdc_flow(scd_type=2)
#   → Window LEAD() SCD2 + df.write.saveAsTable(X)
#
# SCD2 logic: LEAD(sequence_by) gives the end timestamp for each row.
# NULL end_at = current record (is_current=True).

def build_scd2(df, key_col, seq_col):
    """Equivalent to dlt.create_auto_cdc_flow(keys=[key_col], sequence_by=seq_col, stored_as_scd_type=2)"""
    w = Window.partitionBy(key_col).orderBy(seq_col)
    return (df
        .withColumn("__start_at", F.col(seq_col))
        .withColumn("__end_at",   F.lead(seq_col).over(w))
        .withColumn("__end_at",   F.coalesce(F.col("__end_at"),
                                             F.lit("9999-12-31 23:59:59").cast(TimestampType())))
        .withColumn("__is_current", F.lead(seq_col).over(w).isNull())
    )

# Drop old SQL-created tables to avoid schema conflicts
session.sql(f"DROP TABLE IF EXISTS {SILVER_SCHEMA}.silver_customers").collect()
session.sql(f"DROP TABLE IF EXISTS {SILVER_SCHEMA}.silver_products").collect()
session.sql(f"DROP TABLE IF EXISTS {SILVER_SCHEMA}.silver_stores").collect()

# Customers SCD2
customers = session.table(f"{BRONZE_SCHEMA}.raw_customers").filter(
    F.col("customer_id").isNotNull() & (F.col("age") >= 18)
)
silver_customers = build_scd2(customers, "customer_id", "last_update_time")
silver_customers.write.mode("overwrite").saveAsTable(f"{SILVER_SCHEMA}.silver_customers")
cnt = session.sql(f"SELECT COUNT(*) FROM {SILVER_SCHEMA}.silver_customers").collect()[0][0]
curr = session.sql(f"SELECT COUNT(*) FROM {SILVER_SCHEMA}.silver_customers WHERE __is_current=TRUE").collect()[0][0]
print(f"  silver_customers: {cnt} total / {curr} current")

# Products SCD2
products = session.table(f"{BRONZE_SCHEMA}.raw_products").filter(F.col("product_id").isNotNull())
silver_products = build_scd2(products, "product_id", "last_update_time")
silver_products.write.mode("overwrite").saveAsTable(f"{SILVER_SCHEMA}.silver_products")
print(f"  silver_products: {silver_products.count()} total")

# Stores SCD2
stores = session.table(f"{BRONZE_SCHEMA}.raw_stores").filter(F.col("store_id").isNotNull())
silver_stores = build_scd2(stores, "store_id", "last_update_time")
silver_stores.write.mode("overwrite").saveAsTable(f"{SILVER_SCHEMA}.silver_stores")
print(f"  silver_stores: {silver_stores.count()} total")

# Current views
session.sql(f"CREATE OR REPLACE VIEW {SILVER_SCHEMA}.silver_customers_current AS SELECT * FROM {SILVER_SCHEMA}.silver_customers WHERE __is_current = TRUE").collect()
session.sql(f"CREATE OR REPLACE VIEW {SILVER_SCHEMA}.silver_products_current AS SELECT * FROM {SILVER_SCHEMA}.silver_products WHERE __is_current = TRUE").collect()
session.sql(f"CREATE OR REPLACE VIEW {SILVER_SCHEMA}.silver_stores_current AS SELECT * FROM {SILVER_SCHEMA}.silver_stores WHERE __is_current = TRUE").collect()
print("  current views: OK")
