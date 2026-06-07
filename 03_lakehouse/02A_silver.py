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

# Migration from 02A_silver.py:
# @dlt.view(name=X) → CREATE OR REPLACE VIEW (or just use df directly)
# @dlt.expect_or_drop("msg","cond") → df.filter(condition)   ← same semantics
# @dlt.expect("msg","cond")         → df.filter(condition)   ← warn promoted to drop
# dlt.read_stream("LIVE.X")         → session.table("X")

def customers_cleaned():
    # @dlt.expect_or_drop("valid_customer_id","customer_id IS NOT NULL")
    # @dlt.expect("realistic_age","age >= 18 AND age <= 100")
    return (session.table(f"{BRONZE_SCHEMA}.raw_customers")
            .filter(F.col("customer_id").isNotNull())
            .filter((F.col("age") >= 18) & (F.col("age") <= 100))
            .filter((F.col("loyalty_points").isNull()) | (F.col("loyalty_points") >= 0))
    )

def products_cleaned():
    return (session.table(f"{BRONZE_SCHEMA}.raw_products")
            .filter(F.col("product_id").isNotNull())
            .filter(F.col("price") > 0)
    )

def stores_cleaned():
    return (session.table(f"{BRONZE_SCHEMA}.raw_stores")
            .filter(F.col("store_id").isNotNull())
    )

def sales_cleaned():
    # @dlt.expect_or_drop("valid_amount","total_amount > 0")
    # @dlt.expect_or_drop("valid_quantity","quantity > 0")
    return (session.table(f"{BRONZE_SCHEMA}.raw_sales")
            .filter(F.col("transaction_id").isNotNull())
            .filter(F.col("total_amount") > 0)
            .filter(F.col("quantity") > 0)
    )

# Expose cleaned DataFrames as module-level for use by 02B+
_customers_cleaned = customers_cleaned()
_products_cleaned  = products_cleaned()
_stores_cleaned    = stores_cleaned()
_sales_cleaned     = sales_cleaned()

print(f"  customers_cleaned: {_customers_cleaned.count():,}")
print(f"  products_cleaned:  {_products_cleaned.count():,}")
print(f"  stores_cleaned:    {_stores_cleaned.count():,}")
print(f"  sales_cleaned:     {_sales_cleaned.count():,}")
