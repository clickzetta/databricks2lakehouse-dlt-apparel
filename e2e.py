#!/usr/bin/env python3
"""End-to-end validation for Databricks DLT → Lakehouse Dynamic Table migration."""

import subprocess, json, os, sys

PROFILE = os.getenv("CLICKZETTA_PROFILE", "aws_singapore_prod")

def sql(q):
    r = subprocess.run(["cz-cli","sql",q,"--profile",PROFILE,"--sync"],
                       capture_output=True,text=True,cwd="/tmp",timeout=30)
    return json.loads(r.stdout) if r.stdout.strip() else {}

def n(t): return sql(f"SELECT COUNT(*) FROM {t}").get("rows",[[-1]])[0][0]

# ── EXPECTED (verified on AWS Singapore) ──
EXPECTED = {
    # Bronze
    "apparel_bronze.raw_customers": 150,
    "apparel_bronze.raw_products": 50,
    "apparel_bronze.raw_stores": 5,
    "apparel_bronze.raw_sales": 500,
    # Silver
    "apparel_silver.silver_customers": 150,       # all rows including history
    "apparel_silver.silver_products": 50,
    "apparel_silver.silver_stores": 5,
    "apparel_silver.silver_sales_transactions": 500,
    "apparel_silver.silver_customers_current": 120, # current snapshot only
    "apparel_silver.silver_products_current": 50,
    "apparel_silver.silver_stores_current": 5,
    # Gold
    "apparel_gold.denormalized_sales_facts": 500,
    "apparel_gold.gold_product_performance": 50,
}

METRICS = [
    ("total_revenue",       "SELECT CAST(ROUND(SUM(total_amount)) AS BIGINT) FROM apparel_gold.denormalized_sales_facts", 281490),
    ("customers_with_sales","SELECT COUNT(DISTINCT customer_id) FROM apparel_gold.gold_customer_lifetime_value", 96),
    ("scd2_history_rows",   "SELECT COUNT(*) FROM apparel_silver.silver_customers WHERE __is_current = FALSE", 30),
]

passed = failed = 0

for table, exp in EXPECTED.items():
    act = n(table)
    ok = act == exp
    status = "\u2705" if ok else f"\u274C EXP={exp}"
    if ok: passed += 1
    else: failed += 1
    print(f"{status}  {table.split('.')[-1]}: {act:,}")

for label, q, exp in METRICS:
    r = sql(q)
    act = r.get("rows",[[-1]])[0][0]
    ok = act == exp
    status = "\u2705" if ok else f"\u274C EXP={exp}"
    if ok: passed += 1
    else: failed += 1
    print(f"{status}  {label}: {act:,}")

print(f"\n{passed}/{passed+failed} passed", "\u2705" if failed==0 else f"\u274C {failed} FAILED")
sys.exit(0 if failed == 0 else 1)
