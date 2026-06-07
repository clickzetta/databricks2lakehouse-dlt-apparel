# Migration Notes: Databricks DLT → ZettaPark (Minimal Change)

## Migration Strategy

Minimal change: keep Python DataFrame API, replace only DLT-specific decorators and APIs.
Each original DLT file maps 1:1 to a ZettaPark file in 03_lakehouse/.

## File Mapping

| 01_source/dlt/ | 03_lakehouse/ | Change |
|---|---|---|
| `01_bronze.py` | `01_bronze.py` | Auto Loader → `session.read.csv("vol://")` |
| `02A_silver.py` | _(inline in 02B)_ | `@dlt.expect_or_drop` → `df.filter()` |
| `02B_silver.py` | `02B_silver.py` | `create_auto_cdc_flow(scd=2)` → `LEAD() OVER Window` |
| `02C_silver.py` | `02C_silver.py` | `@dlt.expect_or_drop` → `df.filter()` |
| `03_gold.py` | `03_gold.py` | `F.window("1 day")` → `F.to_date()` |
| `variables.py` | `includes/configuration.py` | Schema/path constants |

## DLT → ZettaPark Changes

| DLT API | ZettaPark | Notes |
|---|---|---|
| `import dlt` | _(removed)_ | No DLT in ZettaPark |
| `from pyspark.sql import functions as F` | `from clickzetta.zettapark import functions as F` | Package name only |
| `from pyspark.sql.window import Window` | `from clickzetta.zettapark.window import Window` | Package name only |
| `spark` (global) | `session = Session.builder.configs({}).create()` | Explicit session |
| `@dlt.table(name=X)` | `df.write.mode("overwrite").saveAsTable(X)` | Decorator → last line |
| `@dlt.view(name=X) + @dlt.expect_or_drop("msg","cond")` | `df = df.filter(condition)` | Inline filter |
| `dlt.read_stream("LIVE.X")` / `dlt.read("LIVE.X")` | `session.table("X")` | Same as PySpark |
| `dlt.create_auto_cdc_flow(scd_type=2)` | `df.withColumn("__end_at", F.lead(seq).over(Window.partitionBy(key).orderBy(seq)))` | Standard window |
| `F.window("event_time","1 day")` | `F.to_date(F.col("event_time"))` | F.window not in ZettaPark |
| `/Volumes/catalog/schema/vol/` | `vol://schema.vol/` | Path format |

## Completely Unchanged

All DataFrame operations are identical between PySpark and ZettaPark:
- `.filter()`, `.select()`, `.join()`, `.groupBy()`, `.agg()`
- `F.col()`, `F.lit()`, `F.when()`, `F.coalesce()`, `F.sum()`, etc.
- `F.lead()`, `F.lag()`, `F.row_number()` via `Window`
- `F.to_date()`, `F.current_date()`, `F.round()`, `F.count()`, `F.countDistinct()`
- `.withColumn()`, `.withColumnRenamed()`, `.alias()`

## Verified (AWS Singapore de1cbb4a)

```
01_bronze.py:   4 tables (150+50+5+500 rows)
02B_silver.py:  SCD2 correct (150 all / 120 current / 30 history)
02C_silver.py:  500 sales transactions
03_gold.py:     500 facts / 367 daily / 50 products / 96 customers
e2e.py:         16/16 passed ✅
```

## Alternative: Pure SQL

If your team prefers SQL over Python, `03_lakehouse/sql/` provides equivalent SQL scripts:

```bash
# Run SQL alternative (cz-cli)
cz-cli sql --file 03_lakehouse/sql/02_silver.sql --profile aws_singapore_prod --sync --write
cz-cli sql --file 03_lakehouse/sql/03_gold.sql   --profile aws_singapore_prod --sync --write
```

| SQL File | Equivalent DLT file | Key SQL pattern |
|---|---|---|
| `sql/02_silver.sql` | `02B_silver.py` + `02C_silver.py` | `LEAD() OVER (PARTITION BY key ORDER BY seq)` |
| `sql/03_gold.sql` | `03_gold.py` | `DATE_TRUNC('day', event_time)` |

**When to use SQL**: team is SQL-first, no Python runtime needed, quick ad-hoc testing.  
**When to use ZettaPark**: existing PySpark skills, want to reuse Python code, need UDFs or complex logic.

## Pipe (Auto Loader equivalent) — Verified ✅

Pipe was successfully tested with external S3 Volume:
- Created external Volume connected to S3 (`s3://qiliang-udf-code/apparel_landing/`)
- Created Pipe with `INGEST_MODE = 'LIST_PURGE'`
- Uploaded new CSV to S3 → Pipe auto-detected and ingested within 15 seconds

Key difference from internal Volume:
- **External Volume** (S3/OSS/COS): required for Pipe
- **Internal Volume**: supports COPY INTO but NOT Pipe

Requirement: explicit table schema (no `inferSchema` with external Volume).

```sql
CREATE PIPE apparel_bronze.pipe_sales
    VIRTUAL_CLUSTER = 'DEFAULT'
    INGEST_MODE = 'LIST_PURGE'
AS COPY INTO apparel_bronze.raw_sales
FROM VOLUME apparel_bronze.s3_sales_landing  -- external S3 Volume
USING CSV OPTIONS ('header'='true')
PURGE = TRUE ON_ERROR = CONTINUE;
```
