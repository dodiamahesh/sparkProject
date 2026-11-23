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

A pipeline runs through the REST API (`POST /api/admin/dwh/pipelines/{name}/run`) or from Airflow
(`java -jar dmj-dwh-1.0.0.jar --run-pipeline={name}`, one DAG per pipeline: see `airflow/README.md`). Every source of the pipeline reads

```
updatedAt > meta_pipeline.last_run_time   AND   updatedAt <= start time of this run
```

- **First run:** `last_run_time` is NULL → everything up to now is read.
- **After a successful run:** `meta.meta_pipeline.last_run_time` = start time of that run (one value per pipeline,
  the same for all its sources). A failed run does not move it, so the next run reads the same window again
  (the UPSERTs make that safe).
- **MongoDB is only read.** The ETL never writes to it. The timestamp field (`updatedAt`, `updated_at` for sales)
  must be a BSON **Date**, and should be indexed (the scripts create the index).

## Meta tables (schema `meta`, the same in every 03 file)

| Table | One row per |
|---|---|
| `meta_pipeline` | pipeline (registered by the application at start-up): `pipeline_type`, description, **`last_run_time`** = incremental watermark, `active_flag`. |
| `meta_pipeline_run` | pipeline run: `pipeline_status`, `total_records` (read), `success_records` (valid + soft-deleted), `failed_records` (rejected), `error_code`, `error_message`, `last_run_time` (this run read up to here). **status** = `RUNNING` (busy), `SUCCESS` (nothing rejected), `SUCCESS_WITH_FAILED` (some rows rejected), `FAILED` (all rows rejected, an exception / reject gate, or the app stopped during the run). |
| `meta_asset` | lineage asset: a MongoDB collection (`COLLECTION`, layer `RAW`) or a PostgreSQL table (`TABLE`, layer `CURATED`). Filled by the application. |
| `meta_lineage` | table-level edge source asset -> target asset, per pipeline. Found **automatically** from every load of a successful run (Spark plan of the loaded data). |
| `meta_column_lineage` | selected field-to-field mapping (critical columns only), declared in the pipeline code (`columnLineage()`, e.g. VAT). |
| `meta_error_master` | possible error: `error_id`, `error_type` (CORRUPT / VALIDATION / ORPHAN / DUPLICATE), `field_name`, `error_message`. Seeded by the 03 file; a rule added later in the code gets its row on its first reject. |
| `meta_rejected_record` | rejected record **+ failed rule**: `error_id` -> `meta_error_master`, `field_value` (the bad value), `raw_record`. A record that broke 2 rules has 2 rows. |

Which errors are open, with their message:

```sql
SELECT r.pipeline_name, r.source_collection, r.business_key, e.error_type, e.error_message, r.field_value
  FROM meta.meta_rejected_record r JOIN meta.meta_error_master e USING (error_id)
 ORDER BY 1, 2, 3;
```

Table lineage (filled automatically by every successful run):

```sql
SELECT p.pipeline_name,
       COALESCE(s.schema_name, s.database_name) || '.' || s.asset_name AS source,
       t.schema_name || '.' || t.asset_name                             AS target
  FROM meta.meta_lineage l
  JOIN meta.meta_asset s    ON s.asset_id = l.source_asset_id
  JOIN meta.meta_asset t    ON t.asset_id = l.target_asset_id
  JOIN meta.meta_pipeline p ON p.pipeline_id = l.pipeline_id
 WHERE l.active_flag = 'Y'
 ORDER BY 1, 3, 2;
```

## Soft delete (every pipeline)

A record deleted in the source system arrives in MongoDB as a **soft delete**: `isDeleted: true` plus a new
`updatedAt` (every source definition has `.softDelete()`). The next run reads it like any change, then:

| Step | What happens |
|---|---|
| split | after sanitizing, the **latest** version of each business key decides: deleted -> delete list (not validated, never rejected) |
| apply | the pipeline marks the rows built from that source: `is_deleted = TRUE`, `deleted_at` = its `updatedAt`. Rows are **never removed**, so facts keep their dimension rows (no broken foreign keys) and the history stays |
| SCD2 | `curated.dim_customer`: the **current version is closed** (`is_current = N`, `valid_to = deleted_at`) and marked deleted |
| cascade | a deleted order also marks its lines (`fact_order_item` by `order_id`, `curated.fact_sales` by `order_no`) |
| restore | `isDeleted: false` again (+ new `updatedAt`): the upsert sets `is_deleted = FALSE`, `deleted_at = NULL`; SCD2 gets a new current version |
| count | `meta_pipeline_run.deleted_count`; a run with only deletes is `SUCCESS` |
| safety | a source that sends deletes which the pipeline does not apply FAILS the job (`last_run_time` does not move: nothing is lost) |

Reports / gold layer: `WHERE NOT is_deleted`. Both load modes are supported: staging (set-based UPDATE) and
direct (`vat-return-pipeline`, inside its all-or-nothing transaction). The `03` scripts add the columns with
`ALTER TABLE ... ADD COLUMN IF NOT EXISTS`, so running them again upgrades an existing database.

| Pipeline | Tables with soft delete (key) | 02 sample |
|---|---|---|
| payment | fact_payment (payment_id) | PAY-0005 |
| transaction | dim_customer, dim_account, dim_merchant, fact_transaction | MER-0006, TXN-000006 |
| ecommerce-order | dim_category, dim_product, dim_ecom_customer, fact_order, fact_order_item (+ cascade by order_id) | ORD-00004 (+ 2 lines) |
| ecommerce-fulfillment | dim_warehouse, dim_carrier, fact_shipment | SHP-00008 |
| sales | curated.dim_product, dim_customer (SCD2), fact_sales (all lines of the order), fact_payment | C020, O1019 (+ 2 lines) |
| vat-return | fact_vat_return (ack_no) | 120000000614 |

## Jobs

| Folder | Mongo DB | Collections | Target |
|---|---|---|---|
| `payment-pipeline` | `bankdb` | payments | `silver` star |
| `transaction-pipeline` | `bankdb` | customers, accounts, merchants, transactions | `silver` star |
| `ecommerce-order-pipeline` | `bronze_db` | ecom_categories, ecom_products, ecom_customers, ecom_orders, ecom_order_items | `silver` snowflake |
| `ecommerce-fulfillment-pipeline` | `bronze_db` | ecom_warehouses, ecom_carriers, ecom_shipments | `silver` star (needs `silver.fact_order`) |
| `sales-pipeline` (Job-2) | `bronze_db` | products, customers, orders, payments | `curated` star |
| `vat-return-pipeline` | `bronze_db` | vat_return_coll | `silver` star, loaded WITHOUT staging (design: `vat-return-pipeline/vat-return-pipeline.md`) |

**Why two Mongo databases?** `payments` and `customers` exist in both the banking and the sales examples with a
different shape. The app reads ONE database (`MONGO_DATABASE`, default `bronze_db`). For the banking jobs start it with
`$env:MONGO_DATABASE="bankdb"`.

## Run order (PowerShell, local)

```powershell
# 0) PostgreSQL: database silver on localhost:5433 (user postgres) already exists, nothing to create

# 1) Tables of the job (meta tables are included in every file; running several files is fine)
& "C:\Program Files\PostgreSQL\18\bin\psql.exe" -h localhost -p 5433 -U postgres -d silver -f sample-data\sales-pipeline\03_postgres_tables.sql

# 2) First run (last_run_time is NULL: reads everything)
mongosh "mongodb://localhost:27017" sample-data\sales-pipeline\01_mongo_full_load.js
curl.exe -X POST "http://localhost:8080/api/admin/dwh/pipelines/sales-pipeline/run"

# 3) Next run (reads only what changed after the first run)
mongosh "mongodb://localhost:27017" sample-data\sales-pipeline\02_mongo_incremental.js
curl.exe -X POST "http://localhost:8080/api/admin/dwh/pipelines/sales-pipeline/run"
```

Dependency: `ecommerce-fulfillment-pipeline` needs the orders of `ecommerce-order-pipeline`. Order:
order 01 → order run → fulfillment 01 → fulfillment run → order 02 → order run → fulfillment 02 → fulfillment run.

## Notes

- **Start again from zero:** the 01 scripts write the original (past) timestamps. Run them only on an empty target
  (or set the job's `meta.meta_pipeline.last_run_time` back to NULL), otherwise those timestamps are older than
  `last_run_time` and are not read.
- **HARD delete (document removed from MongoDB) is not pushed to PostgreSQL.** The job reads only documents that
  exist, so a removed document is never seen. The scripts hard-delete only records that were never loaded
  (rejected / test ones); their rows in `meta.meta_rejected_record` stay as history. Use SOFT delete instead (below).
- **Fixing a rejected record:** correct it in MongoDB (the source sets a new `updatedAt`, e.g. `PAY-0013` in the 02
  script). The next run reads it; when it is valid now it is loaded. Its rows in `meta.meta_rejected_record` stay as
  history (the table keeps every record that was ever rejected).
- **Reject gate:** every source allows at most 30% bad rows (duplicates not counted, batches under 10 rows skipped).
  Above that the job FAILS, nothing of that collection is loaded and `last_run_time` does not move.
- **Database from an older version** (schema `etl_audit`): the 03 files create the new `meta` schema next to it.
  The old schema is no longer used and can be dropped with `DROP SCHEMA etl_audit CASCADE;`. With an empty `meta_pipeline`
  the next run of every job reads everything again (safe: the loads are UPSERTs).
