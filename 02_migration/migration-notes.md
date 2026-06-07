# Migration Notes: Databricks DLT → Lakehouse SQL

## Changes Summary

| DLT (Databricks) | Lakehouse SQL | Notes |
|---|---|---|
| `@dlt.table(name=X)` | `CREATE OR REPLACE TABLE X AS SELECT ...` | Direct equivalent |
| `@dlt.view(name=X)` | `CREATE OR REPLACE VIEW X AS SELECT ...` | Direct equivalent |
| `@dlt.expect_or_drop("msg","cond")` | `WHERE condition` | Filter in SQL |
| `@dlt.expect("msg","cond")` | Comment only (warn-level) | Not enforced in SQL |
| `dlt.read_stream("LIVE.X")` | `FROM schema.X` | DT handles incremental automatically |
| `dlt.read("LIVE.X")` | `FROM schema.X` | Same |
| `dlt.create_auto_cdc_flow(scd_type=2)` | `LEAD() OVER (PARTITION BY key ORDER BY seq)` | Manual SCD2 with window function |
| `F.window("event_time","1 day")` | `DATE_TRUNC('day', event_time)` | Standard SQL |
| `/Volumes/catalog/schema/vol/` | `vol://schema.vol/` | Path format only |
| Auto Loader (cloudFiles) | `COPY INTO FROM VOLUME` | Batch; use Pipe for streaming |

## What Stayed the Same

- All JOIN logic (LEFT JOIN dimension snapshots)
- All aggregation functions (SUM, COUNT, AVG, MIN, MAX)
- All GROUP BY / ORDER BY logic
- Column aliases and expressions
- Data quality filters (moved from `@dlt.expect` to `WHERE`)

## Verified (AWS Singapore de1cbb4a)

| Layer | Tables | Rows |
|---|---|---|
| Bronze | 4 | 705 total (150+50+5+500) |
| Silver | 4 tables + 3 views | SCD2 correct (150 all/120 current) |
| Gold | 4 | 500 facts, 50 products, 96 customers, 367 daily |
| Total revenue | — | 281,490 |
