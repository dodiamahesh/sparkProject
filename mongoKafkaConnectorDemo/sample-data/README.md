# Sample data for the CDC test: sourceMongo_db → Kafka → bronze_db

`sourceMongo_db` is treated as the **transaction (OLTP) database**: the application inserts, updates and
**hard deletes** documents there. Nothing reads it directly; every change travels through CDC:

```
sourceMongo_db  --(MongoDB source connector "mongo-cdc-sourcemongo-source")-->  Kafka topic "sourceMongo_db"
                --(mongo-cdc-subscriber + subscriber-sourcemongo.properties)-->  bronze_db (same collection names)
```

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
2. Topics (Kafka running, `C:\kafka`):
   ```powershell
   .\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --create --topic sourceMongo_db --partitions 3 --replication-factor 1
   .\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --create --topic sourceMongo_db.dlq --partitions 1 --replication-factor 1
   ```
3. Connector (Kafka Connect running):
   ```powershell
   cd D:\ClaudeProjects\dataEngineeringProjects\sparkProject\mongoKafkaConnectorDemo\kafka-connect
   .\register-connectors.ps1 apply mongo-cdc-sourcemongo-source
   # ➡️ mongo-cdc-sourcemongo-source   connector=RUNNING  tasks=RUNNING
   ```
4. Subscriber for this flow (own terminal, keep it open; the `de_source_mongo` subscriber can run next to it):
   ```powershell
   cd D:\ClaudeProjects\dataEngineeringProjects\sparkProject\mongoKafkaConnectorDemo\mongo-cdc-subscriber
   java -jar target\mongo-cdc-subscriber-1.0.0-jar-with-dependencies.jar src\main\resources\subscriber-sourcemongo.properties
   # ➡️ Reading topic 'sourceMongo_db' and writing to database 'bronze_db'
   ```

## Test run (PowerShell, from the mongoKafkaConnectorDemo folder)

```powershell
$uri = "mongodb://localhost:27017/?replicaSet=rs0"

# 1) Full load (order matters only for ecommerce: order before fulfillment)
"payment","transaction","ecommerce-order","ecommerce-fulfillment","sales","vat-return" | ForEach-Object {
    mongosh $uri --quiet "sample-data\$_-pipeline\01_mongo_full_load.js" }

# 2) Wait until LAG = 0, then check: every collection ✔, 0 soft-deleted
C:\kafka\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --describe --group mongo-cdc-subscriber-sourcemongo
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
