# Sample data + table scripts (one folder per job)

Every folder = one pipeline (job), with 3 files:

| File | What it does |
|---|---|
| `01_mongo_full_load.js` | Drops + loads the job's MongoDB collections (good records + deliberately bad ones). Run before the **FULL** load. |
| `02_mongo_incremental.js` | Changes after the full load: **UPDATE**, **INSERT**, **DELETE** (sets `processedFlag = 'N'` on every changed / new document). Run before the **INCREMENTAL** load. |
| `03_postgres_tables.sql` | **All** PostgreSQL tables of the job in one file: META tables (`etl_audit.*`, `etl_staging`, `silver.dim_date`) + the job's target tables. Re-run safe (`IF NOT EXISTS`). |

`03_postgres_tables.sql` is the **only** place where tables are defined: the application does not create
tables (no Flyway). Run the script of a job once before its first run. A new job = a new folder with its own `03` file.

The expected result of every run (read / valid / rejected / warnings, and why) is written at the top of each script.

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

# 2) FULL load
mongosh "mongodb://localhost:27017" sample-data\sales-pipeline\01_mongo_full_load.js
curl.exe -X POST "http://localhost:8080/api/etl/pipelines/sales-pipeline/run?loadType=FULL"

# 3) INCREMENTAL load
mongosh "mongodb://localhost:27017" sample-data\sales-pipeline\02_mongo_incremental.js
curl.exe -X POST "http://localhost:8080/api/etl/pipelines/sales-pipeline/run?loadType=INCREMENTAL"
```

Dependency: `ecommerce-fulfillment-pipeline` needs the orders of `ecommerce-order-pipeline`. Order:
order 01 → order FULL → fulfillment 01 → fulfillment FULL → order 02 → order INCREMENTAL → fulfillment 02 → fulfillment INCREMENTAL.

## Notes

- **DELETE is not pushed to PostgreSQL.** The job reads only documents that exist (by `processedFlag`), so a
  deleted document is never seen. The scripts delete only records that were never loaded (rejected / test ones);
  their row in `etl_audit.etl_rejected_record` stays as history.
- **Fixing a rejected record:** correct it in MongoDB and set `processedFlag: "N"` (the incremental scripts do this,
  e.g. `PAY-0013`), or run `loadType=RETRY_REJECTED`. When it is valid now it is loaded and removed from the rejected table.
- **Reject gate:** every source allows at most 30% bad rows (duplicates not counted, batches under 10 rows skipped).
  Above that the job FAILS and nothing of that collection is loaded.
