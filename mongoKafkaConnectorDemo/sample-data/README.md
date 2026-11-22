# Sample data for the CDC test: sourceMongo_db → Kafka → bronze_db

`sourceMongo_db` is treated as the **transaction (OLTP) database**: the application inserts, updates and
**hard deletes** documents there. Nothing reads it directly; every change travels through CDC:

```
sourceMongo_db  --(MongoDB source connector "mongo-cdc-db-source")-->  Kafka topic "sourceMongo_db"
                --(mongo-cdc-subscriber)-------------------------->  bronze_db (same collection names)
```

> ⚠️ **Only the collections in the connector list are synced.** Now: `customers`, `products` (sales-pipeline), see
> `pipeline` in `../kafka-connect/mongo-cdc-db-source.json`. The other scripts still load `sourceMongo_db`, but those
> collections do NOT reach Kafka / bronze_db, and `99_verify_bronze.js` lists them as "Not synced". To sync more, add the
> collection to `pipeline` + `startup.mode.copy.existing.namespace.regex`, bump `offset.partition.name` (`-v3`, ...)
> so its existing data is copied, register the connector, and add it to `SYNCED` in `99_verify_bronze.js`.
> With `customers` + `products`, the sales 02 script gives 1 soft delete in bronze: `customers` C020.

| Change in `sourceMongo_db` | Result in `bronze_db` (same `_id`) |
|---|---|
| insert | document + `_op: "insert"`, `_deleted: false`, `_source_ts`, `_ingested_at` |
| update / replace | whole document replaced, `_op: "update"` / `"replace"` |
| **HARD delete** (`deleteOne` / `deleteMany`) | document **kept** with its last values, `_op: "delete"`, **`_deleted: true`** (soft delete) |

Every folder = one pipeline, with 2 MongoDB scripts (the PostgreSQL `03_*.sql` files were removed; the original
ETL copies stay in `../../ecommersDataETLPipeline/sample-data/`):

| File | What it does |
|---|---|
| `01_mongo_full_load.js` | **Empties** (`deleteMany`, not `drop`) and loads the pipeline's collections (good + deliberately bad records). |
| `02_mongo_incremental.js` | UPDATE, INSERT and **HARD DELETE** (rejected records **and** loaded / valid records). The header lists every delete and the expected counts. |
| `00_cdc_collection_mapping.sql` (this folder) | PostgreSQL table `meta.cdc_collection_mapping` (`source`, `destination`, `db_type`, `active`) + insert rows. The subscriber checks it before every write. |
| `00_create_bronze_collections.js` (this folder) | Creates the destination collections in `bronze_db` (the subscriber never creates one). |
| `99_verify_bronze.js` (this folder) | Compares `sourceMongo_db` with `bronze_db` by `_id` and lists every soft-deleted document. |

## Collections in sourceMongo_db

All pipelines now share ONE database, so the banking collections got the prefix `bank_`
(`payments` and `customers` exist in the sales job too, with another shape):

| Folder | Collections | Hard deletes in 02 (→ `_deleted: true` in bronze) |
|---|---|---|
| `payment-pipeline` | bank_payments | PAY-0005 (update → CANCELLED, then delete at once), PAY-0016, PAY-0018 |
| `transaction-pipeline` | bank_customers, bank_accounts, bank_merchants, bank_transactions | MER-0006, MER-0007, TXN-000006, TXN-000013 |
| `ecommerce-order-pipeline` | ecom_categories, ecom_products, ecom_customers, ecom_orders, ecom_order_items | CAT-06, PRD-0009, ORD-00010, OIT-000014, **ORD-00004 + OIT-000005/6 in one transaction** |
| `ecommerce-fulfillment-pipeline` | ecom_warehouses, ecom_carriers, ecom_shipments | SHP-00008, SHP-00011 |
| `sales-pipeline` | products, customers, orders, payments | C020, O1019, O1023, PAY22 |
| `vat-return-pipeline` | vat_return_coll | 120000000614, 120000000617, every `isTest: true` (deleteMany by condition) |

Total after all 02 scripts: **23 soft-deleted documents** in `bronze_db`.

Special cases that are tested:
- **delete of a loaded record** (not only rejected ones), e.g. PAY-0005, ORD-00004, C020, 120000000614.
- **update immediately followed by delete** (PAY-0005): the connector finds no document any more
  (`fullDocument: null`), the subscriber applies only the changed fields. Bronze must show `status: "CANCELLED"`.
- **multi-document transaction** (ORD-00004 + its 2 lines): 3 delete events after the commit.
- **deleteMany by a condition** (`isTest: true` in vat_return_coll): one delete event per document.
- **re-run of a 01 script**: `deleteMany({})` soft-deletes all old documents in bronze, the new ones arrive with new `_id`s.
  (`drop()` would only send a `drop` event, which the subscriber ignores with `apply.drop=false`, so old documents would stay live.)

## Before the first test (once)

1. **bronze_db must not contain documents from the old ETL sample scripts** (they loaded `bronze_db` directly,
   without `_op` / `_deleted`). `99_verify_bronze.js` shows them in the column `other`. Drop those collections first:
   ```javascript
   use bronze_db
   db.getCollectionNames().forEach(c => db.getCollection(c).drop())   // only if nothing else uses bronze_db!
   ```
2. **Collection mapping table** (PostgreSQL, port 5433). The subscriber writes a source collection only when it has an
   active row of its source type (`db_type`: NOSQL = MongoDB collection, SQL = SQL table) in `meta.cdc_collection_mapping` (`source`, `destination`, `db_type`, `active`); otherwise the
   message goes to the DLQ. The table is read for every batch, so changes need no restart.
   ```powershell
   $psql = "C:\Program Files\PostgreSQL\18\bin\psql.exe"
   & $psql -h localhost -p 5433 -U postgres -c "CREATE DATABASE shop_dw"      # once; "already exists" is fine
   & $psql -h localhost -p 5433 -U postgres -d shop_dw -f sample-data\00_cdc_collection_mapping.sql
   # ➡️ 5 rows: customers, products (NOSQL, t), orders (NOSQL, f), station, station_owner (SQL, f)
   ```
3. **Destination collections in bronze_db.** The subscriber NEVER creates a collection (missing destination -> DLQ):
   ```powershell
   mongosh "mongodb://localhost:27017/?replicaSet=rs0" sample-data\00_create_bronze_collections.js
   # ➡️ created bronze_db.customers, created bronze_db.products
   ```
   Other destination name (for example `products` -> `raw_products`): change the row in the mapping table, add the name
   to `DESTINATIONS` in `00_create_bronze_collections.js` (and to `SYNCED` in `99_verify_bronze.js`), run it again.
4. Topics (Kafka running, `C:\kafka`):
   ```powershell
   .\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --create --topic sourceMongo_db --partitions 3 --replication-factor 1
   .\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --create --topic cdc-dlq --partitions 1 --replication-factor 1
   ```
5. Connector (Kafka Connect running):
   ```powershell
   cd D:\ClaudeProjects\dataEngineeringProjects\sparkProject\mongoKafkaConnectorDemo\kafka-connect
   .\register-connectors.ps1
   # ➡️ mongo-cdc-db-source            connector=RUNNING  tasks=RUNNING
   ```
6. Subscriber (own terminal, keep it open). Rebuild after every code / properties change (the properties are inside the jar):
   ```powershell
   cd D:\ClaudeProjects\dataEngineeringProjects\sparkProject\mongoKafkaConnectorDemo\mongo-cdc-subscriber
   mvn -q clean package
   java -jar target\mongo-cdc-subscriber-1.0.0-jar-with-dependencies.jar
   # ➡️ Collection mapping from meta.cdc_collection_mapping (source.db.type=NOSQL): 2 active source(s)
   # ➡️   customers -> customers / products -> products
   # ➡️ Reading topic 'sourceMongo_db' and writing to database 'bronze_db'
   ```

## Test run (PowerShell, from the mongoKafkaConnectorDemo folder)

```powershell
$uri = "mongodb://localhost:27017/?replicaSet=rs0"

# 1) Full load (order matters only for ecommerce: order before fulfillment)
"payment","transaction","ecommerce-order","ecommerce-fulfillment","sales","vat-return" | ForEach-Object {
    mongosh $uri --quiet "sample-data\$_-pipeline\01_mongo_full_load.js" }

# 2) Wait until LAG = 0, then check: every collection ✔, 0 soft-deleted
C:\kafka\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --describe --group mongo-cdc-subscriber
mongosh $uri --quiet sample-data\99_verify_bronze.js

# 3) Changes with HARD deletes (ecommerce-order 02 before ecommerce-fulfillment 02: SHP-00013 needs ORD-00011)
"payment","transaction","ecommerce-order","ecommerce-fulfillment","sales","vat-return" | ForEach-Object {
    mongosh $uri --quiet "sample-data\$_-pipeline\02_mongo_incremental.js" }

# 4) Check again: every collection ✔, 23 soft-deleted documents listed
mongosh $uri --quiet sample-data\99_verify_bronze.js
```

Check one record by hand:
```javascript
db.getSiblingDB("sourceMongo_db").bank_payments.findOne({ paymentId: "PAY-0005" })   // null (hard deleted)
db.getSiblingDB("bronze_db").bank_payments.findOne({ paymentId: "PAY-0005" })
// { ..., status: "CANCELLED", _op: "delete", _deleted: true, _source_ts: <delete time>, _ingested_at: ... }
```

## Note for the ETL (ecommersDataETLPipeline) reading bronze_db

The ETL soft delete expects `isDeleted: true` **plus a new `updatedAt`** (`updated_at` for sales) and reads
`updatedAt > watermark`. A CDC soft delete only sets `_deleted: true` / `_op: "delete"` and keeps the old
`updatedAt`, so the ETL does **not** see these deletes yet. Options: the ETL reads bronze by `_ingested_at` and treats
`_deleted` as its soft-delete field, or the subscriber also writes `isDeleted` + the watermark field on a delete.
The `EXPECTED FIRST RUN` blocks of the 01 scripts are still valid for the ETL; the old ETL expectations of the 02
scripts were replaced by the CDC expectations.
