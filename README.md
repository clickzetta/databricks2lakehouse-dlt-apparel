# Databricks DLT → ClickZetta Lakehouse Migration

[![Original](https://img.shields.io/badge/Original-jrlasak/databricks__apparel__streaming-⭐45-blue)](https://github.com/jrlasak/databricks_apparel_streaming)

Migrate a complete **Databricks Delta Live Tables (DLT)** pipeline (Bronze/Silver/Gold, SCD Type 2, data quality expectations) to **ClickZetta Lakehouse SQL** — no streaming engine, just SQL.

## Quick Start

```bash
cp .env.example .env   # fill credentials
python3 setup.py       # schemas + volume + data + bronze + silver + gold
python3 e2e.py         # 16/16 assertions ✅
```

## Project Structure

```
├── 01_source/dlt/         ← Original Databricks DLT Python files (preserved)
│   ├── 01_bronze.py       #   @dlt.table — Auto Loader streaming ingestion
│   ├── 02A_silver.py      #   @dlt.view + @dlt.expect_or_drop — cleansed streams
│   ├── 02B_silver.py      #   dlt.create_auto_cdc_flow — SCD Type 2
│   ├── 02C/02D_silver.py  #   sales + returns cleaning
│   ├── 03_gold.py         #   @dlt.table — denormalized facts + aggregates
│   ├── data_generator.py  #   synthetic data generator
│   └── variables.py       #   catalog/schema/path constants
├── 02_migration/          ← Migration notes & DLT → SQL mapping table
├── 03_lakehouse/sql/      ← Migrated SQL (4 files)
│   ├── 01_setup.sql       #   CREATE SCHEMA + CREATE VOLUME
│   ├── 02_bronze.sql      #   COPY INTO from Volume
│   ├── 03_silver.sql      #   SCD2 + cleaning + current views
│   └── 04_gold.sql        #   denormalized facts + aggregations
├── data/                  ← Seed CSV files (4 files)
├── setup.py               ← One-click: schemas + upload + bronze + silver + gold
└── e2e.py                 ← 16 automated checks
```

## DLT → SQL Mapping

| DLT (Databricks) | Lakehouse SQL | Notes |
|---|---|---|
| `@dlt.table(name=X)` | `CREATE OR REPLACE TABLE X AS SELECT ...` | Direct equivalent |
| `@dlt.view(name=X)` | `CREATE OR REPLACE VIEW X AS SELECT ...` | Direct equivalent |
| `@dlt.expect_or_drop("msg","cond")` | `WHERE condition` | Filter in SQL |
| `dlt.create_auto_cdc_flow(scd_type=2)` | `LEAD() OVER (PARTITION BY key ORDER BY seq)` | Manual SCD2 |
| `F.window("event_time","1 day")` | `DATE_TRUNC('day', event_time)` | Standard SQL |
| Auto Loader (cloudFiles) | `COPY INTO FROM VOLUME` | Batch |
| `/Volumes/catalog/schema/vol/` | `vol://schema.vol/` | Path format |

## Verified (AWS Singapore)

| Check | Result |
|---|---|
| Bronze (4 tables) | 150 customers / 50 products / 5 stores / 500 sales |
| Silver SCD2 | 150 all rows / 120 current (30 historical) |
| Gold facts | 500 rows, $281,490 total revenue |
| Gold daily | 367 day×store combinations |
| Gold products | 50 products ranked by revenue |
| Gold CLV | 96 customers with purchases |
| **e2e** | **16/16 passed ✅** |

## Related

- [Databricks → 云器 Lakehouse 迁移评估系列](https://github.com/clickzetta/Databricks-vs-Lakehouse)
- Original: [jrlasak/databricks_apparel_streaming](https://github.com/jrlasak/databricks_apparel_streaming) ⭐45
