#!/usr/bin/env python3
"""One-click setup: schemas, volume, upload data, run ZettaPark migration scripts."""

import os, sys, subprocess
from pathlib import Path
from clickzetta.zettapark import Session

PROJECT_ROOT = Path(__file__).parent
os.chdir(PROJECT_ROOT)

env = {**os.environ,
    "CZ_INSTANCE": os.getenv("CZ_INSTANCE", "de1cbb4a"),
    "CZ_WORKSPACE": os.getenv("CZ_WORKSPACE", "quick_start"),
    "CZ_VCLUSTER": os.getenv("CZ_VCLUSTER", "default"),
    "CZ_USERNAME": os.getenv("CZ_USERNAME", ""),
    "CZ_PASSWORD": os.getenv("CZ_PASSWORD", ""),
    "CZ_SERVICE":  os.getenv("CZ_SERVICE",  "https://ap-southeast-1-aws.api.singdata.com"),
}

# Init session for schema/volume setup
session = Session.builder.configs({k.replace("CZ_","").lower(): v for k,v in env.items() if k.startswith("CZ_")}).create()

print("1. Setup schemas and volume...")
for schema in ["apparel_bronze","apparel_silver","apparel_gold"]:
    session.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}").collect()
    print(f"  {schema}: OK")
session.sql("CREATE VOLUME IF NOT EXISTS apparel_bronze.raw_data").collect()
print("  Volume apparel_bronze.raw_data: OK")

print("\n2. Uploading data to volume...")
for csv in ["customers.csv","products.csv","stores.csv","sales.csv"]:
    result = session.file.put(str(PROJECT_ROOT / "data" / csv), "vol://apparel_bronze.raw_data/")
    print(f"  {csv}: {result[0].source_size:,}b")

def run(script):
    print(f"\n{script}...")
    r = subprocess.run(["python3", script], capture_output=False, env=env, cwd=str(PROJECT_ROOT))
    if r.returncode != 0:
        print(f"FAILED: {script}")
        sys.exit(1)

run("03_lakehouse/01_bronze.py")
run("03_lakehouse/02B_silver.py")
run("03_lakehouse/02C_silver.py")
run("03_lakehouse/03_gold.py")

print("\nSetup complete! Run python3 e2e.py to verify.")
