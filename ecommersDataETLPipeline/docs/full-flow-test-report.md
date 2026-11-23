# Full flow test report: dmj-dwh (2026-11-24)

Juna badha tables / collections kadhi nakhi ne, sav khali database thi aakho flow chalavyo:
DDL → Mongo sample data → pehli run (6 pipelines) → incremental data → biji run (6 pipelines) → DB ane API check.

**Parinam: badhu PASS.** Darek run na aankda sample-data scripts ma lakhela expected aankda saathe match thay che.
Data tamara DB ma **rakhyo che**, jethi tame jate joi shako (section 7).

---

## 1. Environment

| | |
|---|---|
| PostgreSQL | `localhost:5433`, database **`silver`** (user `postgres`) |
| MongoDB | `localhost:27017`: **`bronze_db`** (order, fulfillment, sales, vat-return), **`bankdb`** (payment, transaction) |
| Application | `target/dmj-dwh-1.0.0.jar`, profile `dev` |
| Run mode | **batch mode** (Airflow jevu): `java -jar dmj-dwh-1.0.0.jar --run-pipeline=<name>` |

## 2. Shu delete karyu (pehla backup)

**Backup:** `D:\ClaudeProjects\dataEngineeringProjects\db-backup-20261124\`

- `postgres_silver.dump`: aakho `silver` database (25 tables)
- `mongo\bronze_db\`, `mongo\sourceMongo_db\`

| Jagya | Delete karyu | Kyathi aavelu hatu |
|---|---|---|
| PostgreSQL `silver` | schema `etl_audit` (4 tables) | juni version na audit tables |
| | schema `meta` (`etl_run_audit`, `etl_watermark`, `meta_rejected`) | juno demo project |
| | schema `silver` (17 juna tables), `curated`, `etl_staging` | juni runs |
| | table `public.flyway_schema_history` | aa project ni juni Flyway version |
| MongoDB | `bronze_db` (customers, products) | CDC (Kafka) project no sink data |
| | `sourceMongo_db` (customers, orders, payments) | CDC (Kafka) demo project no source data |
| | `bankdb` | (te vakhte hatu j nahi) |

System databases (`postgres`, Mongo `admin` / `config` / `local`) ne aDya nathi.

**Restore** (joie to):

```powershell
& "C:\Program Files\PostgreSQL\18\bin\pg_restore.exe" -h localhost -p 5433 -U postgres -d silver "D:\ClaudeProjects\dataEngineeringProjects\db-backup-20261124\postgres_silver.dump"
& "C:\Program Files\MongoDB\Tools\100\bin\mongorestore.exe" --uri=mongodb://localhost:27017 "D:\ClaudeProjects\dataEngineeringProjects\db-backup-20261124\mongo"
```

> ⚠️ `sourceMongo_db` CDC (mongoKafkaConnectorDemo) project no source che. E project chalavvo hoy to upar ni
> `mongorestore` command thi pachhu lavo, athva e project ni potani sample-data script fari chalavo.

## 3. Setup

| Step | Parinam |
|---|---|
| 6 x `03_postgres_tables.sql` (`silver` DB par) | badha OK. Schemas: `meta` 7 tables, `silver` 24, `curated` 6 |
| `meta_error_master` seed | 267 error messages |
| 6 x `01_mongo_full_load.js` | badha collections ma expected jetla documents (script na `✔` / `expected` mujab) |

## 4. Pehli run (full load)

| Run | Pipeline | Status | Total | Success | Failed | Expected (01 script) | |
|---|---|---|---|---|---|---|---|
| 1 | ecommerce-order-pipeline | SUCCESS_WITH_FAILED | 51 | 39 | 12 | 51 / 39 / 12 | ✅ |
| 2 | ecommerce-fulfillment-pipeline | SUCCESS_WITH_FAILED | 26 | 19 | 7 | 26 / 19 / 7 | ✅ |
| 3 | sales-pipeline | SUCCESS_WITH_FAILED | 94 | 80 | 16 | 94 / 80 / 16 | ✅ |
| 4 | vat-return-pipeline | SUCCESS_WITH_FAILED | 21 | 14 | 7 | 21 / 14 / 7 | ✅ |
| 5 | payment-pipeline | SUCCESS_WITH_FAILED | 20 | 13 | 7 | 20 / 13 / 7 | ✅ |
| 6 | transaction-pipeline | SUCCESS_WITH_FAILED | 48 | 35 | 13 | 48 / 35 / 13 | ✅ |

Badhi runs no exit code `0` aavyo (Airflow ma task success).

## 5. Biji run (incremental, 02 scripts pachhi)

Kram: order 02 → order → fulfillment 02 → fulfillment → sales → vat-return → payment → transaction.

| Run | Pipeline | Status | Total | Success | Failed | Expected (02 script) | |
|---|---|---|---|---|---|---|---|
| 7 | ecommerce-order-pipeline | SUCCESS_WITH_FAILED | 12 | 11 | 1 | OIT-000018 rejected | ✅ |
| 8 | ecommerce-fulfillment-pipeline | SUCCESS | 6 | 6 | 0 | nothing rejected | ✅ |
| 9 | sales-pipeline | SUCCESS | 12 | 12 | 0 | nothing rejected | ✅ |
| 10 | vat-return-pipeline | SUCCESS_WITH_FAILED | 7 | 6 | 1 | read 7, valid 5, rejected 1, deleted 1 | ✅ |
| 11 | payment-pipeline | SUCCESS_WITH_FAILED | 6 | 5 | 1 | read 6, valid 4, rejected 1, deleted 1 | ✅ |
| 12 | transaction-pipeline | SUCCESS_WITH_FAILED | 13 | 12 | 1 | TXN-000019 rejected | ✅ |

`success` = valid + soft-deleted. Dakhla tarike payment: 4 valid + 1 deleted = 5.

## 6. Database checks

### 6.1 Meta tables

| Check | Parinam | |
|---|---|---|
| `meta_pipeline` | 6 pipelines, badhi `active_flag = Y`, `pipeline_type = RAW_TO_CURATED` | ✅ |
| `airflow_dag_id = pipeline_name` | 6 / 6 | ✅ |
| `last_run_time` | darek pipeline ni biji run no start time | ✅ |
| `meta_pipeline_run` | 12 runs, ek pan `RUNNING` nathi | ✅ |
| `etl_staging` | 0 tables (temporary tables kadhai gaya) | ✅ |
| ID gaps | `meta_asset` 48/48, `meta_lineage` 54/54, `meta_column_lineage` 11/11, `meta_rejected_record` 66/66, `meta_error_master` 267/267 | ✅ |

### 6.2 Rejected records (ek row = ek record)

| Pipeline | Rows | Distinct records |
|---|---|---|
| ecommerce-order-pipeline | 13 | 13 |
| ecommerce-fulfillment-pipeline | 7 | 7 |
| sales-pipeline | 16 | 16 |
| vat-return-pipeline | 8 | 8 |
| payment-pipeline | 8 | 8 |
| transaction-pipeline | 14 | 14 |

Kul 66 = pehli run na 62 + biji run na 4. Darek record ni **ek j row** che (`error_ids`, `error_messages`,
`field_values`). Sudharelo record (jem ke ECU-0008, SHP-00010, PAY-0013) load thayo, ane eni row history tarike rahi.

### 6.3 Soft delete

| Table | Deleted key | |
|---|---|---|
| `silver.fact_payment` | PAY-0005 | ✅ |
| `silver.fact_order` + `silver.fact_order_item` (2 lines, cascade) | ORD-00004 | ✅ |
| `silver.fact_shipment` | SHP-00008 | ✅ |
| `silver.dim_merchant` / `silver.fact_transaction` | MER-0006 / TXN-000006 | ✅ |
| `silver.fact_vat_return` | 120000000614 | ✅ |
| `curated.dim_customer` (SCD2: current version band) | C020 | ✅ |
| `curated.fact_sales` (2 lines) | O1019 | ✅ |

Rows delete **nathi** thati: `is_deleted = TRUE` ane `deleted_at` set thay che.

### 6.4 SCD2 (`curated.dim_customer`)

| customer_code | city | tier | is_current | valid_from | valid_to |
|---|---|---|---|---|---|
| C003 | Mumbai | Gold | N | 2025-03-05 | 2026-11-24 |
| C003 | Pune | Platinum | **Y** | 2026-11-24 | 9999-12-31 |

Sales 02 script ni expected values pan match thay che: `dim_customer` 22 rows, `fact_sales` 32, `fact_payment` 21. ✅

### 6.5 Rows per table

| Table | Rows | Soft-deleted |
|---|---|---|
| curated.dim_customer | 22 | 1 |
| curated.dim_product | 21 | 0 |
| curated.fact_payment | 21 | 0 |
| curated.fact_sales | 32 | 2 |
| silver.dim_account | 10 | 0 |
| silver.dim_carrier | 5 | 0 |
| silver.dim_category | 6 | 0 |
| silver.dim_customer | 10 | 0 |
| silver.dim_ecom_customer | 8 | 0 |
| silver.dim_merchant | 8 | 1 |
| silver.dim_product | 9 | 0 |
| silver.dim_warehouse | 5 | 0 |
| silver.fact_order | 9 | 1 |
| silver.fact_order_item | 14 | 2 |
| silver.fact_payment | 16 | 1 |
| silver.fact_shipment | 12 | 1 |
| silver.fact_transaction | 14 | 1 |
| silver.fact_vat_return | 17 | 1 |

### 6.6 Lineage

- **Table lineage:** `meta_lineage` ma 54 edges ane `meta_asset` ma 48 assets (Mongo collections + PostgreSQL tables), badha automatic.
- **Column lineage:** `meta_column_lineage` ma 11 VAT columns.

### 6.7 REST API (aa j data par, pachhi app bandh kari)

| Endpoint | Parinam | |
|---|---|---|
| `GET /api/admin/dwh/pipelines` | HTTP 200, 6 pipelines | ✅ |
| `GET /api/admin/dwh/pipeline-runs?limit=50` | HTTP 200, 12 runs | ✅ |
| `GET /api/admin/dwh/pipeline-runs/1` / `.../999` | HTTP 200 / HTTP 404 | ✅ |
| `GET /api/admin/dwh/rejected-records?limit=500` | HTTP 200, 66 records (`errorIds`, `errorMessages`, `fieldValues`) | ✅ |
| `GET /api/admin/dwh/rejected-records/summary` | HTTP 200 | ✅ |

## 7. Data jate jovo (pgAdmin / psql, database `silver`)

```sql
-- runs
SELECT r.pipeline_run_id, p.pipeline_name, r.pipeline_status, r.total_records, r.success_records, r.failed_records,
       r.start_time, r.end_time, r.error_code
  FROM meta.meta_pipeline_run r JOIN meta.meta_pipeline p USING (pipeline_id) ORDER BY 1;

-- pipelines + watermark
SELECT pipeline_name, airflow_dag_id, last_run_time FROM meta.meta_pipeline ORDER BY 1;

-- rejected records (ek row = ek record)
SELECT pipeline_name, source_collection, business_key, error_ids, error_messages, field_values, attempt_count
  FROM meta.meta_rejected_record ORDER BY 1, 2, 3;

-- soft-deleted rows
SELECT * FROM silver.fact_payment WHERE is_deleted;

-- SCD2 history
SELECT * FROM curated.dim_customer WHERE customer_code = 'C003' ORDER BY valid_from;

-- table lineage
SELECT p.pipeline_name, COALESCE(s.schema_name, s.database_name) || '.' || s.asset_name AS source,
       t.schema_name || '.' || t.asset_name AS target
  FROM meta.meta_lineage l
  JOIN meta.meta_asset s ON s.asset_id = l.source_asset_id
  JOIN meta.meta_asset t ON t.asset_id = l.target_asset_id
  JOIN meta.meta_pipeline p ON p.pipeline_id = l.pipeline_id
 WHERE l.active_flag = 'Y' ORDER BY 1, 3, 2;
```

MongoDB: `mongosh` → `use bronze_db` / `use bankdb` → `db.ecom_orders.find()` vagere.

## 8. Je aa test ma NATHI

- **Airflow pote:** aa machine par Airflow / Python nathi. DAG file (`airflow/dags/dmj_dwh_dags.py`) Airflow ma
  chalavi nathi. Jar no batch mode (Airflow je command chalave che e j) upar test thayo che.
- Fail run, kill karela process ni recovery ane ek j pipeline nu be var ek saathe chalvu, aa vakhte nahi pan pehla
  kamchalau DBs par test thaya hata (pass).
