#!/usr/bin/env python3
"""One-click setup: schemas, volume, upload data, run SQL pipeline."""

import os, sys, time, subprocess
from pathlib import Path
from clickzetta.zettapark import Session

PROJECT_ROOT = Path(__file__).parent
os.chdir(PROJECT_ROOT)

session = Session.builder.configs({
    "instance": os.getenv("CZ_INSTANCE", "de1cbb4a"),
    "workspace": os.getenv("CZ_WORKSPACE", "quick_start"),
    "schema": os.getenv("CZ_SCHEMA", "quick_start"),
    "vcluster": os.getenv("CZ_VCLUSTER", "default"),
    "username": os.getenv("CZ_USERNAME", ""),
    "password": os.getenv("CZ_PASSWORD", ""),
    "service": os.getenv("CZ_SERVICE", "https://ap-southeast-1-aws.api.singdata.com"),
}).create()
print("Session created OK")

PROFILE = "aws_singapore_prod"

def sql_file(path, write=False):
    """Execute a SQL file via cz-cli (handles multi-statement files)."""
    content = Path(path).read_text()
    stmts = [s.strip() for s in content.split(";") if s.strip() and not s.strip().startswith("--")]
    ok = 0
    for stmt in stmts:
        flag = ["--write"] if write else []
        r = subprocess.run(
            ["cz-cli", "sql", stmt, "--profile", PROFILE, "--sync"] + flag,
            capture_output=True, text=True, cwd="/tmp", timeout=60
        )
        if '"ok": true' in r.stdout or r.returncode == 0:
            ok += 1
        else:
            import json
            try:
                err = json.loads(r.stdout).get("error", {}).get("message", r.stdout[:100])
            except:
                err = r.stdout[:100]
            print(f"  WARN: {stmt[:60]} → {err}")
    print(f"  {Path(path).name}: {ok}/{len(stmts)} statements OK")

print("\n1. Setup schemas and volume...")
sql_file("03_lakehouse/sql/01_setup.sql", write=True)

print("\n2. Uploading data to volume...")
vol = "vol://apparel_bronze.raw_data/"
for csv_file in ["customers.csv","products.csv","stores.csv","sales.csv"]:
    local = PROJECT_ROOT / "data" / csv_file
    result = session.file.put(str(local), vol)
    print(f"  {csv_file}: {result[0].source_size:,}b uploaded")

print("\n3. Creating bronze tables (COPY INTO)...")
# Use ZettaPark to create tables from volume
table_files = [
    ("apparel_bronze.raw_customers", "customers.csv"),
    ("apparel_bronze.raw_products",  "products.csv"),
    ("apparel_bronze.raw_stores",    "stores.csv"),
    ("apparel_bronze.raw_sales",     "sales.csv"),
]
for table, csv in table_files:
    df = session.read.option("header","true").csv(f"vol://apparel_bronze.raw_data/{csv}")
    df.write.mode("overwrite").saveAsTable(table)
    print(f"  {table}: {df.count():,} rows")

print("\n4. Running Silver transforms...")
sql_file("03_lakehouse/sql/03_silver.sql", write=True)

print("\n5. Running Gold aggregations...")
sql_file("03_lakehouse/sql/04_gold.sql", write=True)

print("\nSetup complete! Run python3 e2e.py to verify.")
