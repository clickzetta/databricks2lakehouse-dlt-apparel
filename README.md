# Databricks DLT → ClickZetta Lakehouse Migration

[![Original](https://img.shields.io/badge/Original-jrlasak/databricks__apparel__streaming-⭐45-blue)](https://github.com/jrlasak/databricks_apparel_streaming)

Migrate a complete **Databricks Delta Live Tables (DLT)** pipeline to **ClickZetta Lakehouse** in three paths. Verified 20/20 on AWS Singapore.

## Quick Start

```bash
cp .env.example .env   # fill credentials
python3 setup.py       # schemas + volume + data upload + bronze + silver + gold
python3 e2e.py         # 20/20 ✅
```

## Three Migration Paths

| Path | Files | Session | Orchestration | DLT equivalent |
|---|---|---|---|---|
| **A. ZettaPark** | `03_lakehouse/*.py` | `Session.builder.create()` | `setup.py` | `@dlt.table` → `df.write.saveAsTable()` |
| **B. Pure SQL** | `03_lakehouse/sql/` | None | `cz-cli sql --file` | SQL equivalents |
| **C. Dynamic Table** | `03_lakehouse/dynamic_tables/` | None | `REFRESH DYNAMIC TABLE` | **@dlt.table native equivalent** |

**Path C is the closest to DLT**: Dynamic Tables auto-refresh on schedule, just like DLT pipelines refresh on new data. Define the SQL once, Lakehouse handles incremental updates.

## DLT → Dynamic Table Mapping (Path C)

```sql
-- DLT (@dlt.table):
@dlt.table(name="gold_daily_sales_by_store")
def gold_daily_sales_by_store():
    return df.groupBy(F.window("event_time","1 day"), ...).agg(...)

-- Dynamic Table (Lakehouse):
CREATE OR REPLACE DYNAMIC TABLE apparel_gold.dt_daily_sales_by_store
REFRESH INTERVAL 10 MINUTE
VCLUSTER DEFAULT
AS
SELECT CAST(event_time AS DATE) AS sale_date, ...
FROM apparel_gold.denormalized_sales_facts
GROUP BY CAST(event_time AS DATE), ...;
```

## DLT → ZettaPark Mapping (Path A)

| DLT API | ZettaPark | Notes |
|---|---|---|
| `@dlt.table(name=X)` | `df.write.mode("overwrite").saveAsTable(X)` | Last line |
| `@dlt.expect_or_drop("msg","cond")` | `df.filter(condition)` | Same semantics |
| `dlt.read_stream/read("LIVE.X")` | `session.table("X")` | Same as PySpark |
| `create_auto_cdc_flow(scd_type=2)` | `F.lead().over(Window.partitionBy(key).orderBy(seq))` | Window unchanged |
| `F.window("event_time","1 day")` | `F.to_date(F.col("event_time"))` | F.window not in ZettaPark |
| `from pyspark.sql...` | `from clickzetta.zettapark...` | Package name |

## About Pipe (Auto Loader equivalent)

Pipe = Auto Loader equivalent for continuous file ingestion from object storage (OSS/S3/COS). Requires an external Volume connected to cloud storage. Not tested in this demo (uses internal Volume), but syntax:

```sql
CREATE PIPE apparel_bronze.pipe_sales
    VIRTUAL_CLUSTER = 'DEFAULT'
    INGEST_MODE = 'LIST_PURGE'
AS COPY INTO apparel_bronze.raw_sales
FROM VOLUME apparel_bronze.sales_external_landing
USING CSV OPTIONS ('header'='true') PURGE=TRUE ON_ERROR=CONTINUE;
```

## Verified (AWS Singapore de1cbb4a)

| | ZettaPark (A) | SQL (B) | Dynamic Table (C) |
|---|---|---|---|
| Bronze | ✅ 4 tables | ✅ | — |
| Silver SCD2 | ✅ 150/120 | ✅ | ✅ `dt_customers_current` |
| Gold daily | ✅ 367 | ✅ | ✅ `dt_daily_sales_by_store` |
| Gold products | ✅ 50 | ✅ | ✅ `dt_product_performance` |
| Gold CLV | ✅ 96 | ✅ | ✅ `dt_customer_lifetime_value` |
| **e2e** | **20/20 ✅** | — | included in 20/20 |

## Related

- [Databricks → 云器 Lakehouse 迁移评估系列](https://github.com/clickzetta/Databricks-vs-Lakehouse)
- Original: [jrlasak/databricks_apparel_streaming](https://github.com/jrlasak/databricks_apparel_streaming) ⭐45
