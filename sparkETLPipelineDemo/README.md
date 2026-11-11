# Spark ETL Pipeline Framework (MongoDB → Spark → PostgreSQL star schema)

A small, standard ETL framework built with **Java 17, Spring Boot 3.5, Apache Spark 3.5, MongoDB and PostgreSQL**.

* **Bronze** = MongoDB (`bronze_db`): raw source data, 10 banking collections
* **Silver** = PostgreSQL (`silver_db`): only clean, valid data, in star-schema form (dimensions + facts)
* Every run is tracked (status, counts, time, steps, failure reason)
* Bad records go to a rejected table with the reason, and are removed from it automatically when they become valid
* Incremental load is driven by a **flag field** in MongoDB (not by timestamps)
* New pipelines are added by writing **one class** (+ a SQL migration)

---

## 1. Quick start

**Prerequisites:** Java 17, Maven 3.9+, MongoDB on `localhost:27017`, PostgreSQL with a database `silver_db`.

PostgreSQL settings (dev) come from environment variables with defaults (see `application-dev.yml`):

| Variable | Default |
|---|---|
| `POSTGRES_URL` | `jdbc:postgresql://localhost:5433/silver_db` |
| `POSTGRES_USER` | `postgres` |
| `POSTGRES_PASSWORD` | `postgres` → set your real password |

Using `docker compose` instead? Its PostgreSQL listens on 5432, so set
`POSTGRES_URL=jdbc:postgresql://localhost:5432/silver_db`.

```bash
# no MongoDB/PostgreSQL installed? start both with Docker:
docker compose up -d

# otherwise create the database once:   CREATE DATABASE silver_db;

mvn clean package
java -jar target/spark-etl-pipeline-1.0.0.jar          # dev profile is the default
# or:  mvn spring-boot:run
```

On startup (dev profile) the app:
1. creates all PostgreSQL tables with **Flyway** (`src/main/resources/db/migration`)
2. loads the 18 sample JSON files into MongoDB (only into empty collections)
3. starts one shared SparkSession (`local[*]`)

Then trigger a pipeline:

```bash
curl -X POST "http://localhost:8080/api/etl/pipelines/payment-pipeline/run?loadType=INCREMENTAL"
curl -X POST "http://localhost:8080/api/etl/pipelines/transaction-pipeline/run?loadType=INCREMENTAL"
```

All requests are in [api-requests.http](api-requests.http).
Step-by-step flow of every end point (in Gujarati): [docs/API_FLOW_GUJARATI.md](docs/API_FLOW_GUJARATI.md).
Low level design (components, flows, data model, failure handling, production gaps): [docs/LOW_LEVEL_DESIGN.md](docs/LOW_LEVEL_DESIGN.md).

> **Running from the IDE:** Spark needs extra JVM flags on Java 17, otherwise startup fails with
> `IllegalAccessError: ... cannot access class sun.nio.ch.DirectBuffer`. `java -jar` and
> `mvn spring-boot:run` add them automatically. **VS Code:** use the run configuration
> `SparkEtlApplication (dev)` from `.vscode/launch.json` (also sets `POSTGRES_PASSWORD`).
> **IntelliJ / Eclipse:** copy the `--add-opens ...` lines from the `spark.jvm.add-opens` property in
> `pom.xml` into the run configuration's **VM options**.

---

## 2. Architecture

```
                 REST API (Spring Boot controllers)
                               |
                         EtlJobService  ----- lock, job-run row, async thread pool
                               |
                 +-------------+-------------+
                 |                           |
          PipelineRegistry             SourceCommitService
     (finds every @Component           (after a successful run: save rejects,
      that extends AbstractEtlPipeline)  delete fixed rejects, update Mongo flags)
                 |
     +-----------+------------+
     |                        |
 PaymentPipeline       TransactionPipeline      <- business code (one class per pipeline)
     |                        |
     +---- uses the reusable framework building blocks -----+
           |                 |                |             |
   MongoSourceReader   DataSanitizer     PostgresWriter   StepTracker
   SourceProcessor     DataQualityRules  PostgresReader   JobRun/Step/Rejected repositories
                       DataQualityEngine DateDimension
```

### What happens in ONE job run

```
POST /api/etl/pipelines/{name}/run?loadType=...
  1. lock pipeline (409 if it is already running)
  2. INSERT etl_job_run (status RUNNING)
  3. FULL only: TRUNCATE the pipeline's fact tables
  4. pipeline.run():
       for each source collection:
         EXTRACT   read MongoDB (filter on processedFlag), all fields as String, cache it
         SANITIZE  trim, blank -> null, upper/lower/proper case, remove commas, normalise phone
         VALIDATE  CORRUPT / VALIDATION / ORPHAN rules -> every failed rule is collected
         DEDUPE    same business key twice -> keep latest updatedAt, others = DUPLICATE
       TRANSFORM   rename + cast to real types, build dimension and fact rows
       LOAD        dimensions first, then facts (surrogate key lookup), UPSERT via staging table
  5. COMMIT (only if step 4 worked):
       save rejected rows          -> etl_audit.etl_rejected_record
       delete rows that are valid now from the rejected table
       update MongoDB flags         valid = Y, rejected = E, duplicate = D
  6. UPDATE etl_job_run -> SUCCESS / SUCCESS_WITH_REJECTS / FAILED + counts + error
  7. always: free Spark cache, release lock
```

**Why flags are updated last:** if the job fails half-way, MongoDB is untouched, so the next run reads
the same documents again. The loads are UPSERTs, so running twice is safe (idempotent).

---

## 3. Project structure

```
src/main/java/com/demo/etl
├── SparkEtlApplication.java              entry point
├── config/                               Spring configuration
│   ├── EtlProperties.java                all "etl.*" settings (typed)
│   ├── SparkConfig.java                  the shared SparkSession bean
│   └── AsyncConfig.java                  thread pool for async=true jobs
│
├── framework/                            ★ REUSABLE - no business logic in here
│   ├── pipeline/                         contract + base class
│   │   ├── EtlPipeline.java              interface every pipeline implements
│   │   ├── AbstractEtlPipeline.java      building blocks: extractAndValidate, load, addSurrogateKey ...
│   │   ├── PipelineContext.java          one job run: id, load type, metrics, cached data
│   │   ├── PipelineRegistry.java         auto-discovers pipelines
│   │   ├── PipelineToolkit.java          bundles the framework services
│   │   ├── LoadType.java                 FULL / INCREMENTAL / RETRY_REJECTED
│   │   └── JobMetrics.java               counters
│   ├── source/                           EXTRACT (MongoDB)
│   │   ├── SourceDefinition.java         collection + fields + business key
│   │   ├── MongoSourceReader.java        flag-based filter per load type
│   │   ├── SourceProcessor.java          extract -> sanitize -> validate -> dedupe
│   │   ├── SourceBatch.java              valid + rejected result of one collection
│   │   ├── MongoFlagUpdater.java         writes processedFlag back (bulk, per partition)
│   │   └── SourceFlag.java               N / Y / E / D
│   ├── quality/                          DATA QUALITY
│   │   ├── DataSanitizer.java            cleaning recipe (fluent builder)
│   │   ├── DataQualityRules.java         validation rules (fluent builder)
│   │   ├── DataQualityEngine.java        applies rules, splits valid / rejected
│   │   ├── ErrorType.java                CORRUPT / VALIDATION / ORPHAN / DUPLICATE
│   │   └── ValidationRule, ReferenceRule, QualityResult, QualityColumns
│   ├── target/                           LOAD (PostgreSQL)
│   │   ├── PostgresWriter.java           UPSERT through a staging table
│   │   ├── PostgresReader.java           read tables, surrogate key lookup
│   │   └── DateDimension.java            shared dim_date builder
│   ├── audit/                            job / step / rejected tables (JdbcTemplate)
│   │   ├── StepTracker.java              wraps work as a tracked step
│   │   └── JobRunRepository, JobStepRepository, RejectedRecordRepository + records
│   ├── job/                              ORCHESTRATION
│   │   ├── EtlJobService.java            runs a job start -> end, error handling
│   │   ├── SourceCommitService.java      commit phase (rejects + flags)
│   │   └── JobLockManager.java           no double runs of the same pipeline
│   ├── common/                           SparkTypes, SparkUtils, ErrorMessages
│   └── exception/                        EtlException + specific exceptions
│
├── pipelines/                            ★ BUSINESS CODE - one package per pipeline
│   ├── payment/                          EXAMPLE 1: one collection -> star schema
│   │   ├── PaymentPipeline.java          the flow (what to do, in which order)
│   │   ├── PaymentSource.java            source definition + cleaning + rules
│   │   └── PaymentStarSchemaMapper.java  DataFrame -> dimension / fact rows
│   ├── transaction/                      EXAMPLE 2: 4 collections -> star schema
│   │   ├── TransactionPipeline.java
│   │   ├── TransactionSources.java
│   │   └── TransactionStarSchemaMapper.java
│   └── ecommerce/
│       ├── order/                        EXAMPLE 3: 5 collections -> SNOWFLAKE schema
│       │   ├── EcommerceOrderPipeline.java
│       │   ├── EcommerceOrderSources.java
│       │   └── EcommerceOrderSchemaMapper.java
│       └── fulfillment/                  EXAMPLE 4: 3 collections -> star schema (depends on EXAMPLE 3)
│           ├── FulfillmentPipeline.java
│           ├── FulfillmentSources.java
│           └── FulfillmentStarSchemaMapper.java
│
├── api/                                  REST controllers + GlobalExceptionHandler
├── ingestion/                            data ingestion API: sample-data JSON files -> MongoDB
└── devdata/                              dev profile only: sample data loader + reset endpoint

src/main/resources
├── application.yml / application-dev.yml / application-prod.yml
├── db/migration/V1..V7__*.sql            Flyway: audit + warning tables, star / snowflake schemas
└── sample-data/*.json                    10 banking collections x 100 documents + 8 ecom_* collections

scripts/SampleDataGenerator.java          regenerates the banking sample data + docs/SAMPLE_DATA_REPORT.md
scripts/generate-ecommerce-sample-data.js regenerates the 8 ecom_*.json files (node)
docs/SAMPLE_DATA_REPORT.md                expected counts + list of every planted bad record
```

**Rule of thumb:** `framework` never knows about payments or customers. `pipelines` never talks to
JDBC, MongoDB or the audit tables directly. That separation is what keeps it easy to maintain.

---

## 4. The four examples

### Example 1 – single collection: `payment-pipeline`

```
MongoDB payments ──► dim_payer           (payer_id)
                 ├─► dim_payment_method  (payment_method + payment_provider)
                 ├─► dim_biller          (biller_name)
                 ├─► dim_date            (shared)
                 └─► fact_payment        (payment_id) with the 4 surrogate keys
```

### Example 2 – multiple collections: `transaction-pipeline`

```
customers    ──► dim_customer
accounts     ──► dim_account      ORPHAN check: customerId must exist in dim_customer
merchants    ──► dim_merchant
transactions ──► fact_transaction ORPHAN check: customer, account AND merchant must exist
             └─► dim_date
```

Dimensions are loaded first; the fact then looks up their surrogate keys. Lookups read the
PostgreSQL dimension tables, so they include rows loaded in earlier runs too.

### Example 3 – five collections → SNOWFLAKE schema: `ecommerce-order-pipeline`

```
ecom_categories  ──► dim_category
ecom_products    ──► dim_product        ORPHAN: categoryId must exist in dim_category
                                         dim_product stores category_key  ◄── SNOWFLAKE (dim → dim)
ecom_customers   ──► dim_ecom_customer
ecom_orders      ──► fact_order         grain = one order       ORPHAN: customer must exist  (+ dim_date)
ecom_order_items ──► fact_order_item    grain = one order line  ORPHAN: order AND product must exist
                                         customer_key + date_key are inherited from fact_order
                                         measures: gross_amount = qty × unit_price, net_amount = gross − discount
```

Snowflake vs star: in a star schema `dim_product` would repeat `category_name` and `department`;
here they live once in `dim_category`, so a sales-by-department query joins
`fact_order_item → dim_product → dim_category`.

### Example 4 – three collections → STAR schema, depends on Example 3: `ecommerce-fulfillment-pipeline`

```
ecom_warehouses ──► dim_warehouse
ecom_carriers   ──► dim_carrier
ecom_shipments  ──► fact_shipment   ORPHAN: order (silver.fact_order!), warehouse AND carrier must exist
                                     customer_key taken from the order   ◄── conformed dim_ecom_customer
                                     shipped_date_key + delivered_date_key ◄── role-playing dim_date
                                     measures: delivery_days, is_on_time
```

**Dependency:** run `ecommerce-order-pipeline` first. Run the fulfillment pipeline first and every
shipment fails the ORPHAN check ("orderId not found in fact_order"); 100 % rejected is above the
30 % **reject gate**, so the job `FAILED` with that reason before anything of `ecom_shipments` is loaded
and without changing MongoDB flags. Run the order pipeline, then the fulfillment pipeline again.

---

## 5. Load types (flag based)

Every MongoDB document has `processedFlag`:

| Flag | Meaning | Set by |
|---|---|---|
| `N` or missing | new / changed, must be processed | source application |
| `Y` | loaded into silver | ETL |
| `E` | rejected (in the rejected table) | ETL |
| `D` | duplicate (kept in the rejected table for audit, never retried) | ETL |

| loadType | Reads | Target behaviour |
|---|---|---|
| `INCREMENTAL` (default) | `processedFlag` = `N` or missing | upsert |
| `FULL` | every document | truncate the pipeline's fact tables, upsert dimensions |
| `RETRY_REJECTED` | `processedFlag` = `E` | upsert; records that pass now leave the rejected table |

**Contract for the source system:** when it changes a document, it sets `processedFlag` back to `N`.

---

## 6. Data quality

| Check | Error type | Example rule |
|---|---|---|
| Wrong data type | `CORRUPT` | `mustBeDecimal("amount")`, `mustBeTimestamp(...)`, `mustBeDate(...)`, `mustBeInteger(...)` |
| Business rule | `VALIDATION` | `required(...)`, `mustBePositive`, `mustBeOneOf`, `mustMatch(regex)`, `mustBeValidEmail`, `mustNotBeInFuture`, `mustBeBetween`, `custom(...)` |
| Referential integrity | `ORPHAN` | `mustExistIn("customerId", dimCustomer, "customer_id", "dim_customer")` |
| Same business key twice | `DUPLICATE` | automatic; latest `updatedAt` wins |

Sanitizing happens **before** validation, so messy but correct values (`" inr "`, `"1,25,000.50"`,
`"+91 98765 43210"`) are fixed instead of rejected. All source fields are read as strings, so a corrupt
value is reported with its real content instead of silently becoming `NULL`.

### Warnings (record is loaded, problem is reported)

```java
DataQualityRules.create()
        .required("customerId", "email")              // error   -> rejected
        .warnings(w -> w
                .required("phone")                    // warning -> loaded + etl_audit.etl_data_warning
                .mustNotBeMoreThan("amount", 60_000));
```
ORPHAN checks cannot be warnings (the fact would have no surrogate key).

### Data contract per source (`SourceDefinition`)

Every source of all 4 pipelines declares these (see `PaymentSource`, `TransactionSources`,
`EcommerceOrderSources`, `FulfillmentSources`):

| Option | What happens | When |
|---|---|---|
| `.maxRejectPercent(30)` | **Reject gate**: more bad rows (duplicates not counted) → step `Reject gate x` fails → job `FAILED` before the collection is loaded, flags unchanged; error names the reasons and the owner | INCREMENTAL + FULL, batches ≥ `etl.quality.reject-gate-min-records` (10) |
| *(automatic)* schema check | Step `Schema check x`: fields in MongoDB vs. `fields(...)` (sample of `etl.quality.schema-sample-size` docs). New / missing field → `SCHEMA_DRIFT` warning; with `.failOnSchemaDrift()` the job fails. `.ignoreFields(...)` for fields not read on purpose | every run |
| `.expectedRows(min, max)` | Fewer / more rows read → `VOLUME` warning | INCREMENTAL |
| `.maskEmail(..)` / `.maskPhone(..)` | `r****@gmail.com` / `XXXXXX3210` in silver, in `raw_record` and in error / warning messages. Rules still validate the real value | every run |
| `.excludeTestRecords()` / `.sourceFilter(json)` | Extra MongoDB `$match` (inside MongoDB): `{"isTest": true}` documents are never read | every run |
| `.description(..)`, `.owner(..)`, `.sla(..)` | Shown in `GET /api/etl/pipelines` (`sources`), owner is added to gate / volume messages | - |

---

## 7. Monitoring and re-processing

| What | API | Table |
|---|---|---|
| Job status, counts, times, failure reason | `GET /api/etl/jobs`, `GET /api/etl/jobs/{id}` | `etl_audit.etl_job_run` |
| Each step (where it failed, how long, how many rows) | `GET /api/etl/jobs/{id}` | `etl_audit.etl_job_step` |
| Bad records with all reasons + original JSON | `GET /api/etl/rejected-records` | `etl_audit.etl_rejected_record` |
| Counts per pipeline / collection / error type | `GET /api/etl/rejected-records/summary` | |
| Warnings of a run (`RULE`, `SCHEMA_DRIFT`, `VOLUME`) | `GET /api/etl/jobs/{id}/warnings` | `etl_audit.etl_data_warning` (+ `warning_count` in `etl_job_run`) |
| Data contract of every source (owner, SLA, checks, PII) | `GET /api/etl/pipelines` → `sources` | |

**Fix-and-retry flow:**

```js
// 1. fix the document in MongoDB (it still has processedFlag "E")
db.payments.updateOne({paymentId: "PAY-0078"}, {$set: {amount: 499.00}})
```
```bash
# 2. re-process only rejected documents
curl -X POST "http://localhost:8080/api/etl/pipelines/payment-pipeline/run?loadType=RETRY_REJECTED"
# -> PAY-0078 is loaded, flag becomes Y, its row is DELETED from etl_rejected_record (resolvedCount = 1)
# -> records that are still bad stay, with attempt_count + 1
```

Job statuses: `RUNNING`, `SUCCESS`, `SUCCESS_WITH_REJECTS`, `FAILED`. A job interrupted by an
application crash is marked `FAILED` at the next startup.

---

## 8. Add a new pipeline (example: `loan-pipeline` from the `loans` collection)

1. **SQL** – `src/main/resources/db/migration/V8__create_loan_star_schema.sql` (V1–V7 already exist)
   (dimension tables with `BIGSERIAL` key + `UNIQUE` business key, a fact table, and the
   `job_run_id BIGINT, loaded_at TIMESTAMP` columns on every table).
2. **Package** `com.demo.etl.pipelines.loan` with three classes, copied from `payment`:
   * `LoanSource` – `SourceDefinition` + `DataSanitizer` + `DataQualityRules`
   * `LoanStarSchemaMapper` – `toTypedLoans`, `toDimLoanType`, `toFactLoan` ...
   * `LoanPipeline` – `@Component`, extends `AbstractEtlPipeline`, implements `run()`:

```java
@Component
public class LoanPipeline extends AbstractEtlPipeline {
    public LoanPipeline(PipelineToolkit toolkit) { super(toolkit); }

    @Override public String name() { return "loan-pipeline"; }
    @Override public String description() { return "loans -> dim_loan_type, fact_loan"; }
    @Override public List<SourceDefinition> sources() { return List.of(LoanSource.LOANS); }
    @Override public List<String> targetTables() { return List.of("silver.dim_loan_type", "silver.fact_loan"); }
    @Override public List<String> fullLoadTruncateTables() { return List.of("silver.fact_loan"); }

    @Override
    public void run(PipelineContext context) {
        SourceBatch loans = extractAndValidate(context, LoanSource.LOANS, LoanSource.sanitizer(), LoanSource.rules());
        Dataset<Row> typed = LoanStarSchemaMapper.toTypedLoans(loans.valid());
        loadDateDimension(context, typed, "disbursement_date");
        load(context, LoanStarSchemaMapper.toDimLoanType(typed), "silver.dim_loan_type", "loan_type");
        Dataset<Row> facts = addSurrogateKey(typed, "silver.dim_loan_type", "loan_type_key", "loan_type");
        load(context, LoanStarSchemaMapper.toFactLoan(facts), "silver.fact_loan", "loan_id");
    }
}
```

3. Restart → `POST /api/etl/pipelines/loan-pipeline/run`. Job tracking, rejects, retries, flags,
   locking and error handling all work without extra code.

The rules for `loans`, `cards`, `branches`, `fund_transfers` and `refunds` are already described in
`docs/SAMPLE_DATA_REPORT.md`, so they are good practice exercises.

---

## 9. Sample data

`docs/SAMPLE_DATA_REPORT.md` lists the expected counts and **every** planted bad record with its reason.
First-run results (verified):

| Pipeline | source | valid | rejected | duplicates |
|---|---:|---:|---:|---:|
| payment-pipeline | 100 | 82 | 18 | 3 |
| transaction-pipeline | 400 | 332 | 68 | 10 |
| ecommerce-order-pipeline | 369 | 333 | 36 | 5 |
| ecommerce-fulfillment-pipeline (after the order pipeline) | 75 | 66 | 9 | 1 |

The e-commerce files (`ecom_*.json`) contain planted CORRUPT / VALIDATION / ORPHAN / DUPLICATE records
too; each one is commented in `scripts/generate-ecommerce-sample-data.js`.

Regenerate the data: `java scripts/SampleDataGenerator.java` (banking + payments) and
`node scripts/generate-ecommerce-sample-data.js` (e-commerce). Both use a fixed seed → same output.
Start a test from zero (dev only): `POST /api/dev/reset?cleanPostgres=true`.

---

## 10. Data ingestion API (sample-data → MongoDB)

Package `com.demo.etl.ingestion` (controller + service only). It loads every file from
`src/main/resources/sample-data/` into the collection with the same name (`payments.json` → `payments`).

| Request | What it does |
|---|---|
| `GET  /api/ingestion/sample-data/files` | list the files that can be ingested |
| `POST /api/ingestion/sample-data?mode=REPLACE` | ingest ALL files |
| `POST /api/ingestion/sample-data/{fileName}?mode=REPLACE` | ingest ONE file (`payments` or `payments.json`) |

`mode`: `REPLACE` (default, drop + create + insert), `APPEND` (add to existing documents),
`SKIP_IF_EXISTS` (leave collections that already have data alone).

Workflow, run for each file one by one:

```
1. RESOLVE   payments.json -> collection "payments"
2. READ      parse the JSON array (file must be [ {...}, {...} ])
3. PREPARE   create the collection if missing (REPLACE drops it first, SKIP_IF_EXISTS may stop here)
4. INSERT    insertMany in batches of 500
5. VERIFY    count documents in the collection = expected count
```

A bad file is reported as `FAILED` with the reason and the next file is still processed.
HTTP status: `200` all files OK, `207` some failed, `500` all failed. The response lists every file
with `status`, `documentsInFile`, `documentsInserted`, `documentsInCollection`, `collectionCreated`, `durationMs`.

---

## 11. Design decisions and limits

* **JdbcTemplate instead of JPA:** Hibernate 6 and Spark 3.5 need incompatible ANTLR versions.
* **Spark UI is disabled:** Spark 3.5's UI uses `javax.servlet`, Spring Boot 3 uses `jakarta.servlet`.
* **Pinned Janino 3.1.9** in `pom.xml`: Spark's code generator breaks with the newer version Spring Boot manages.
* **Job lock is in memory:** fine for one instance. For several instances use a DB lock (e.g. ShedLock).
* **Flag updates are not conditional:** if the source changes a document *during* a run, the change
  can be marked `Y`. For strict CDC, add a version field and update only when it is unchanged.
* **Dimensions are SCD type 1** (overwrite). Add history columns if you need SCD type 2.
