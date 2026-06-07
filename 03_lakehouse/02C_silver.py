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

# Migration from 02C_silver.py (sales transactions cleaning):
# @dlt.expect_or_drop → df.filter()
# dlt.read_stream("LIVE.bronze_sales") → session.table("BRONZE_SCHEMA.raw_sales")

session.sql(f"DROP TABLE IF EXISTS {SILVER_SCHEMA}.silver_sales_transactions").collect()
df = session.table(f"{BRONZE_SCHEMA}.raw_sales")

silver_sales = (df
    .filter(F.col("transaction_id").isNotNull())
    .filter(F.col("total_amount") > 0)
    .filter(F.col("quantity") > 0)
    .withColumn("event_time", F.col("event_time").cast(TimestampType()))
)

silver_sales.write.mode("overwrite").saveAsTable(f"{SILVER_SCHEMA}.silver_sales_transactions")
print(f"  silver_sales_transactions: {silver_sales.count():,} rows")
