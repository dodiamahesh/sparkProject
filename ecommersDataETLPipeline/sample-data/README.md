# Sample data + table scripts (one folder per job)

Every folder = one pipeline (job), with 3 files:

| File | What it does |
|---|---|
| `01_mongo_full_load.js` | Drops + loads the job's MongoDB collections (good records + deliberately bad ones). Run before the **first** run of the job. |
| `02_mongo_incremental.js` | Changes after the first run: **UPDATE**, **INSERT**, **DELETE**. Every changed / new document gets `updatedAt = now`. Run before the **next** run. |
| `03_postgres_tables.sql` | **All** PostgreSQL tables of the job in one file: META tables (`meta.*`, `etl_staging`, `silver.dim_date`) + the job's target tables + the seed of `meta.meta_error_master` (every error this job can produce). Re-run safe (`IF NOT EXISTS` / `ON CONFLICT`). |

`03_postgres_tables.sql` is the **only** place where tables are defined: the application does not create
tables (no Flyway). Run the script of a job once before its first run. A new job = a new folder with its own `03` file.

The expected result of every run (read / valid / rejected, final status, and why) is written at the top of each script.

## How a run reads MongoDB (incremental by timestamp)

There is only ONE way to run a pipeline: `POST /api/etl/pipelines/{name}/run`. Every source reads

```
updatedAt > last watermark   AND   updatedAt <= start time of this run
```

- **First run:** no watermark yet → everything up to now is read.
- **After a successful run:** `meta.meta_watermark` = start time of that run. A failed run does not move it,
  so the next run reads the same window again (the UPSERTs make that safe).
- **MongoDB is only read.** The ETL never writes to it. The timestamp field (`updatedAt`, `updated_at` for sales)
  must be a BSON **Date**, and should be indexed (the scripts create the index).

## Meta tables (schema `meta`, the same in every 03 file)

| Table | One row per |
|---|---|
| `meta_pipeline_run` | pipeline run: status, counts, error. **status** = `SUCCESS` (nothing rejected), `SUCCESS_WITH_FAILED` (some rows rejected), `FAILED` (all rows rejected, an exception / reject gate, or the app stopped during the run). Empty (NULL) while the run is busy. |
| `meta_pipeline_step` | step of a run (`SUCCESS` / `FAILED`, NULL while busy) |
| `meta_error_master` | possible error: `error_id`, `error_type` (CORRUPT / VALIDATION / ORPHAN / DUPLICATE), `field_name`, `error_message`. Seeded by the 03 file; a rule added later in the code gets its row on its first reject. |
| `meta_rejected_record` | rejected record **+ failed rule**: `error_id` -> `meta_error_master`, `field_value` (the bad value, PII masked), `raw_record`. A record that broke 2 rules has 2 rows. |
| `meta_watermark` | pipeline + collection: read up to this timestamp |

Which errors are open, with their message:

```sql
SELECT r.pipeline_name, r.source_collection, r.business_key, e.error_type, e.error_message, r.field_value
  FROM meta.meta_rejected_record r JOIN meta.meta_error_master e USING (error_id)
 ORDER BY 1, 2, 3;
```

## Jobs

| Folder | Mongo DB | Collections | Target |
|---|---|---|---|
| `payment-pipeline` | `bankdb` | payments | `silver` star |
| `transaction-pipeline` | `bankdb` | customers, accounts, merchants, transactions | `silver` star |
| `ecommerce-order-pipeline` | `shopdb` | ecom_categories, ecom_products, ecom_customers, ecom_orders, ecom_order_items | `silver` snowflake |
| `ecommerce-fulfillment-pipeline` | `shopdb` | ecom_warehouses, ecom_carriers, ecom_shipments | `silver` star (needs `silver.fact_order`) |
| `sales-pipeline` (Job-2) | `shopdb` | products, customers, orders, payments | `curated` star |

**Why two Mongo databases?** `payments` and `customers` exist in both the banking and the sales examples with a
different shape. The app reads ONE database (`MONGO_DATABASE`, default `shopdb`). For the banking jobs start it with
`$env:MONGO_DATABASE="bankdb"`.

## Run order (PowerShell, local)

```powershell
# 0) PostgreSQL database (once)
& "C:\Program Files\PostgreSQL\18\bin\psql.exe" -h localhost -p 5433 -U postgres -c "CREATE DATABASE shop_dw"

# 1) Tables of the job (meta tables are included in every file; running several files is fine)
& "C:\Program Files\PostgreSQL\18\bin\psql.exe" -h localhost -p 5433 -U postgres -d shop_dw -f sample-data\sales-pipeline\03_postgres_tables.sql

# 2) First run (no watermark yet: reads everything)
mongosh "mongodb://localhost:27017" sample-data\sales-pipeline\01_mongo_full_load.js
curl.exe -X POST "http://localhost:8080/api/etl/pipelines/sales-pipeline/run"

# 3) Next run (reads only what changed after the first run)
mongosh "mongodb://localhost:27017" sample-data\sales-pipeline\02_mongo_incremental.js
curl.exe -X POST "http://localhost:8080/api/etl/pipelines/sales-pipeline/run"
```

Dependency: `ecommerce-fulfillment-pipeline` needs the orders of `ecommerce-order-pipeline`. Order:
order 01 → order run → fulfillment 01 → fulfillment run → order 02 → order run → fulfillment 02 → fulfillment run.

## Notes

- **Start again from zero:** the 01 scripts write the original (past) timestamps. Run them only on an empty target
  (or delete the job's rows from `meta.meta_watermark`), otherwise those timestamps are older than the watermark
  and are not read.
- **DELETE is not pushed to PostgreSQL.** The job reads only documents that exist, so a deleted document is never
  seen. The scripts delete only records that were never loaded (rejected / test ones); their row in
  `meta.meta_rejected_record` stays as history.
- **Fixing a rejected record:** correct it in MongoDB (the source sets a new `updatedAt`, e.g. `PAY-0013` in the 02
  script). The next run reads it; when it is valid now it is loaded. Its rows in `meta.meta_rejected_record` stay as
  history (the table keeps every record that was ever rejected).
- **Reject gate:** every source allows at most 30% bad rows (duplicates not counted, batches under 10 rows skipped).
  Above that the job FAILS, nothing of that collection is loaded and the watermark does not move.
- **Database from an older version** (schema `etl_audit`): the 03 files create the new `meta` schema next to it.
  The old schema is no longer used and can be dropped with `DROP SCHEMA etl_audit CASCADE;`. Without old watermarks
  the next run of every job reads everything again (safe: the loads are UPSERTs).
