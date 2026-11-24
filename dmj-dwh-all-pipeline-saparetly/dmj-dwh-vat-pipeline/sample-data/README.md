# Sample data + table scripts (one folder per job)

Every folder = one pipeline (job). Today there is one job: **`vat-return-pipeline/`**, with these files:

| File | What it does |
|---|---|
| `01_mongo_full_load.js` | Drops + loads `vat_return_coll` (good records + deliberately bad ones). Run before the **first** run. |
| `02_mongo_incremental.js` | Changes after the first run: **UPDATE**, **INSERT**, **DELETE**. Every changed / new document gets `updatedAt = now`. Run before the **next** run. |
| `03_postgres_tables.sql` | **All** PostgreSQL tables of the job in one file: META tables (`meta.*`, `etl_staging`, `silver.dim_date`) + the job's target tables + the seed of `meta.meta_error_master` (every error this job can produce). Re-run safe (`IF NOT EXISTS` / `ON CONFLICT`). |
| `vat-return-pipeline.md` | Design, rules, sample records and expected results. |

`03_postgres_tables.sql` is the **only** place where tables are defined: the application does not create
tables (no Flyway). Run it once before the first run. A new job = a new folder with its own `01` / `02` / `03`
files (see `docs/PIPELINE_GUIDE.md`).

## How a run reads MongoDB (incremental by timestamp)

A pipeline runs through the REST API (`POST /api/admin/dwh/pipelines/{name}/run`) or from Airflow
(`java -jar dmj-dwh-1.0.0.jar --run-pipeline={name}`, see `airflow/README.md`). Every source of the pipeline reads

```
updatedAt > meta_pipeline.last_run_time   AND   updatedAt <= start time of this run
```

- **First run:** `last_run_time` is NULL → everything up to now is read.
- **After a successful run:** `meta.meta_pipeline.last_run_time` = start time of that run (one value per pipeline,
  the same for all its sources). A failed run does not move it, so the next run reads the same window again
  (the UPSERTs make that safe).
- **MongoDB is only read.** The ETL never writes to it. The timestamp field (`updatedAt`) must be a BSON **Date**,
  and should be indexed (the scripts create the index).

## Meta tables (schema `meta`)

| Table | One row per |
|---|---|
| `meta_pipeline` | pipeline (registered by the application at start-up): `pipeline_type`, description, **`last_run_time`** = incremental watermark, `active_flag`. |
| `meta_pipeline_run` | pipeline run: `pipeline_status`, `total_records` (read), `success_records` (valid + soft-deleted), `failed_records` (rejected), `error_code`, `error_message`, `last_run_time` (this run read up to here). **status** = `RUNNING` (busy), `SUCCESS` (nothing rejected), `SUCCESS_WITH_FAILED` (some rows rejected), `FAILED` (all rows rejected, an exception / reject gate, or the app stopped during the run). |
| `meta_asset` | lineage asset: a MongoDB collection (`COLLECTION`, layer `RAW`) or a PostgreSQL table (`TABLE`, layer `CURATED`). Filled by the application. |
| `meta_lineage` | table-level edge source asset -> target asset, per pipeline. Found **automatically** from every load of a successful run (Spark plan of the loaded data). |
| `meta_column_lineage` | selected field-to-field mapping (critical columns only), declared in the pipeline code (`columnLineage()`). |
| `meta_error_master` | possible error: `error_id`, `error_type` (CORRUPT / VALIDATION / ORPHAN / DUPLICATE), `field_name`, `error_message`. Seeded by the 03 file; a rule added later in the code gets its row on its first reject. |
| `meta_rejected_record` | rejected record (**one row per record**): `error_ids` (array -> `meta_error_master`, every rule it broke), `error_messages` (comma separated, same order), `field_values` (JSON: bad value per field), `raw_record`, `attempt_count`. Rejected again -> the row is overwritten with its current errors. |

Which errors are open, with their message:

```sql
SELECT pipeline_name, source_collection, business_key, error_ids, error_messages, field_values
  FROM meta.meta_rejected_record
 ORDER BY 1, 2, 3;

-- records with error 12:  WHERE 12 = ANY (error_ids)
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

## Soft delete

A record deleted in the source system arrives in MongoDB as a **soft delete**: `isDeleted: true` plus a new
`updatedAt` (the source definition has `.softDelete()`). The next run reads it like any change, then:

| Step | What happens |
|---|---|
| split | after sanitizing, the **latest** version of each business key decides: deleted -> delete list (not validated, never rejected) |
| apply | the pipeline marks the rows built from that source: `is_deleted = TRUE`, `deleted_at` = its `updatedAt`. Rows are **never removed**, so facts keep their dimension rows (no broken foreign keys) and the history stays |
| restore | `isDeleted: false` again (+ new `updatedAt`): the upsert sets `is_deleted = FALSE`, `deleted_at = NULL` |
| count | `meta_pipeline_run.success_records` includes the deleted documents; a run with only deletes is `SUCCESS` |
| safety | a source that sends deletes which the pipeline does not apply FAILS the job (`last_run_time` does not move: nothing is lost) |

Reports / gold layer: `WHERE NOT is_deleted`. The `03` script adds the columns with
`ALTER TABLE ... ADD COLUMN IF NOT EXISTS`, so running it again upgrades an existing database.

| Pipeline | Tables with soft delete (key) | 02 sample |
|---|---|---|
| vat-return | fact_vat_return (ack_no) | 120000000614 |

## Jobs

| Folder | Mongo DB | Collections | Target |
|---|---|---|---|
| `vat-return-pipeline` | `bronze_db` | vat_return_coll | `silver` star, loaded WITHOUT staging (design: `vat-return-pipeline/vat-return-pipeline.md`) |

## Run order (PowerShell, local)

```powershell
# 0) PostgreSQL: database silver on localhost:5433 (user postgres) already exists, nothing to create

# 1) Tables of the job (meta tables are included in the file)
& "C:\Program Files\PostgreSQL\18\bin\psql.exe" -h localhost -p 5433 -U postgres -d silver -f sample-data\vat-return-pipeline\03_postgres_tables.sql

# 2) First run (last_run_time is NULL: reads everything)
mongosh "mongodb://localhost:27017" sample-data\vat-return-pipeline\01_mongo_full_load.js
curl.exe -X POST "http://localhost:8080/api/admin/dwh/pipelines/vat-return-pipeline/run"

# 3) Next run (reads only what changed after the first run)
mongosh "mongodb://localhost:27017" sample-data\vat-return-pipeline\02_mongo_incremental.js
curl.exe -X POST "http://localhost:8080/api/admin/dwh/pipelines/vat-return-pipeline/run"
```

## Notes

- **Start again from zero:** the 01 script writes the original (past) timestamps. Run it only on an empty target
  (or set `meta.meta_pipeline.last_run_time` of the pipeline back to NULL), otherwise those timestamps are older
  than `last_run_time` and are not read.
- **HARD delete (document removed from MongoDB) is not pushed to PostgreSQL.** The job reads only documents that
  exist, so a removed document is never seen. The scripts hard-delete only records that were never loaded
  (rejected / test ones); their rows in `meta.meta_rejected_record` stay as history. Use SOFT delete instead.
- **Fixing a rejected record:** correct it in MongoDB (the source sets a new `updatedAt`, e.g. ackNo
  `120000000619` in the 02 script). The next run reads it; when it is valid now it is loaded. Its row in
  `meta.meta_rejected_record` stays as history.
- **Reject gate:** the source allows at most 30% bad rows (duplicates not counted, batches under 10 rows skipped).
  Above that the job FAILS, nothing of that collection is loaded and `last_run_time` does not move.
