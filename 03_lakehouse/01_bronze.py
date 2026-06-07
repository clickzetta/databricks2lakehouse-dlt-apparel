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

# Migration from 01_bronze.py:
# spark.readStream.format("cloudFiles").load(VOL_PATH) → session.read.csv("vol://...")
# @dlt.table(name=X) → df.write.mode("overwrite").saveAsTable(X)

VOL = f"vol://{BRONZE_SCHEMA}.raw_data"

table_files = [
    (f"{BRONZE_SCHEMA}.raw_customers", "customers.csv"),
    (f"{BRONZE_SCHEMA}.raw_products",  "products.csv"),
    (f"{BRONZE_SCHEMA}.raw_stores",    "stores.csv"),
    (f"{BRONZE_SCHEMA}.raw_sales",     "sales.csv"),
]

for table_name, csv_file in table_files:
    # spark.readStream.format("cloudFiles")... → session.read.csv("vol://...")
    df = session.read.option("header", "true").csv(f"{VOL}/{csv_file}")
    # @dlt.table writes are handled by → df.write.saveAsTable(name)
    df.write.mode("overwrite").saveAsTable(table_name)
    print(f"  {table_name}: {df.count():,} rows")
