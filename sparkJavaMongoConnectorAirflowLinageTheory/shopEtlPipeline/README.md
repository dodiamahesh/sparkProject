# shop-etl-pipeline – Job-2 (J2_SALES)

MongoDB (raw) → Spark (Java) → PostgreSQL / EPAS (silver = `curated` schema)

- **Design:** [ETL_Jobs_Data_Design_Guide.md](../sparkJavaMongoConnectorAirflowLinageTheory/ETL_Jobs_Data_Design_Guide.md) (Section 6 = Job-2)
- **Sample data ane SQL:** [sample_data/](../sparkJavaMongoConnectorAirflowLinageTheory/sample_data/)
- **Stack:** Java 17, Spring Boot 3.5, Spark 3.5.9, Mongo Spark Connector 10.7, PostgreSQL 18 (EPAS compatible SQL)

---

## 1. Project shu kare che?

Job-2 ek j run ma 3 collections ne star schema ma lai jay che:

```
 customers ──────────────────────────────► dim_customer        (SCD2)
 orders ───── items[] explode ───────────► fact_sales          (1 row = 1 order line)
 payments ─── order lookup ──────────────► fact_payment
              status / method ───────────► dim_order_status, dim_payment_method (seed)
 kharab data (corrupt / duplicate / rule fail / FK) ─► meta.meta_rejected
 darek run no hisab ─────────────────────► meta.etl_run_audit
 "kya sudhi vanchyu" ────────────────────► meta.etl_watermark
```

Staging table **nathi**: darek table par unique business key che, ane direct upsert thay che (design doc Section 12).

---

## 2. Folder structure

```
src/main/java/com/shop/etl/
├── ShopEtlApplication.java
├── common/                         ← BADHA jobs mate reusable (Job 1–5)
│   ├── config/    EtlProperties (yml), SparkConfig (1 SparkSession), ExecutorConfig (API thread pool)
│   ├── io/        MongoSourceReader, MongoProjection, PostgresReader, JdbcBatchWriter (+ partition writer, binder)
│   ├── quality/   Rule, RuleEngine, Deduplicator, Clean (sanitize functions), RejectWriter, QualityGate
│   ├── pipeline/  SourcePipeline  ← ek collection no 12-step template
│   ├── job/       EtlJob, JobLauncher (badha triggers no entry), JobRunner, JobLock, JobContext
│   ├── audit/     AuditService, SourceStats
│   ├── watermark/ WatermarkService, LoadWindow
│   └── trigger/   JobController (API), JobScheduler (cron), CliJobRunner (Airflow CLI)
├── dataset/                        ← "dataset classes": column naam + schema
│   ├── raw/       RawCustomer, RawOrder, RawPayment          (Mongo)
│   ├── silver/    DimCustomer, DimProduct, ..., FactSales     (curated)
│   └── gold/      Customer360Summary                          (Job-5 mate taiyar)
└── jobs/sales/                     ← JOB-2 fakt aatlu
    ├── SalesJob.java               customers → orders → payments (order jaruri)
    ├── shared/    SalesReferenceData (status/method), SalesLookups (dim reads)
    ├── customer/  CustomerSanitizer, CustomerValidator, CustomerLoader, CustomerPipeline
    ├── order/     OrderSanitizer,    OrderValidator,    OrderLoader,    OrderPipeline
    └── payment/   PaymentSanitizer,  PaymentValidator,  PaymentLoader,  PaymentPipeline
airflow/dags/      shop_etl_j2_sales_api.py, shop_etl_j2_sales_cli.py
api-requests.http  API examples
```

**Niyam:** darek collection mate 3 alag service: **Sanitizer** (sudharo), **Validator** (rules), **Loader** (lakho).
`XxxPipeline` fakt emne jode che. Read, persist, count, rejects, quality gate, audit common `SourcePipeline` kare che.

**Rule badalvo hoy to?** Fakt e Validator ni list ma ek line. Example:
```java
Rule.of("J2-V02", RejectType.VALIDATION, "Email format khoto",
        not(col("email").rlike(EMAIL_REGEX)), "email", col("email"))
```

---

## 3. Ek run no flow

```
 Trigger (API / Airflow / Scheduler / CLI)
     │
 JobLauncher ── job shodho → window end → LOCK (pg advisory) → watermark windows
     │           window khali? → SKIPPED
     │           run id J2_20261021_0400, audit JOB = RUNNING
 JobRunner  ── meta_rejected ma aa run_id na juna rejects delete (re-run safe)
     │
 SalesJob   ── seed status/method dims
     │         ├─ CustomerPipeline ─┐
     │         ├─ OrderPipeline    ─┤  darek: read → sanitize → validate → PERSIST → count
     │         └─ PaymentPipeline  ─┘         → rejects → gate → load → audit source row
     │
 SUCCESS: EK transaction ma { watermark = window end, audit JOB = SUCCESS }
 FAILED : audit FAILED + error, watermark AAGAL NATHI vadhto (re-run e j window thi)
     │
 LOCK chhuto
```

---

## 4. Kevi rite chalavvu (local)

```powershell
# 1) Postgres DB (ek vaar)
#    CREATE DATABASE shop_dw;
#    03_postgres_create_tables.sql, pachhi 04_postgres_seed_job1_output.sql (PART A)
#    (04 = Job-1 nu output. Job-1 code ready thay pachhi jarur nahi)

# 2) Mongo full load data
mongosh "mongodb://localhost:27017" ..\sparkJavaMongoConnectorAirflowLinageTheory\sample_data\01_mongo_full_load_data.js

# 3) Build
mvn clean package -DskipTests

# 4) FULL LOAD (design doc mujab window end = 2026-10-21 00:00 IST)
$env:POSTGRES_PASSWORD="root"
java -jar target\shop-etl-pipeline-1.0.0-exec.jar --spring.main.web-application-type=none `
     --etl.cli.job=J2_SALES --etl.cli.window-end=2026-10-21T00:00

# 5) INCREMENTAL: 02 Mongo script + 04 SQL na PART B (P024) pachhi
java -jar target\shop-etl-pipeline-1.0.0-exec.jar --spring.main.web-application-type=none `
     --etl.cli.job=J2_SALES --etl.cli.window-end=2026-10-21T04:00

# ke server mode (API):
java -jar target\shop-etl-pipeline-1.0.0-exec.jar
```

> **IDE thi chalavo to:** Spark ne Java 17 par `--add-opens` flags joie (pom.xml na `spark.jvm.add-opens`).
> `java -jar` ane `mvn spring-boot:run` ma e automatic che.

### Tested result (aa machine par, temporary DB ma chalavi ne)

| Run | customers | orders | payments | Audit JOB |
|---|---|---|---|---|
| Full 00:00 | read 23, reject 3, insert 20 | read 24, reject 4, partial 2, insert 30 lines | read 23, reject 3, insert 20 | 70 / 60 / 10, SUCCESS |
| Incr 04:00 | read 3: insert 2, update 2 (C003 SCD2, C002 SCD1, C023) | read 3: insert 2, update 3 | read 2: insert 1, update 1 | 8 / 8 / 0, SUCCESS |
| Same 04:00 farithi | – | – | – | SKIPPED |

- `meta_rejected`: 12 rows. Badha rule code ane bad_value design doc Section 10.1 sathe match.
- `fact_sales` total 249,500.
- C003: key 5 (Mumbai) bandh, key 21 (Pune/Platinum) navi. O1003 juni key sathe j rahyo.

⚠️ Surrogate key na number (customer_key 1, 2, …) doc karta alag hoi shake. Spark parallel lakhe che, etle order fix nathi. Aa normal che. Test hammesha business key (`customer_code`, `order_no`) thi karo.

---

## 5. Job ne 3 rite trigger karvo

Tranne rasta chhelle **ek j** `JobLauncher.launch(...)` ne call kare che. Etle lock, audit ane watermark no niyam badhe same che.

### 5.1 API

```
POST /api/jobs/J2_SALES/runs?windowEnd=2026-10-21T04:00     → 202 {runId, status: STARTED}
POST /api/jobs/J2_SALES/runs?mode=sync                       → 200 SUCCESS / 500 FAILED
GET  /api/jobs/runs/J2_20261021_0400                         → status + source wise counts
GET  /api/jobs                                               → jobs + watermarks
```

| HTTP | Matlab |
|---|---|
| 202 | Background ma shuru (async default) |
| 200 | SUCCESS (sync) ke SKIPPED (window pehla j load) |
| 409 | Same job chalu che (lock) |
| 404 | Job naam khotu |
| 400 | windowEnd format khoto |
| 503 | Queue bharai gai |

### 5.2 Airflow (2 options)

| | `shop_etl_j2_sales_api.py` (recommended) | `shop_etl_j2_sales_cli.py` |
|---|---|---|
| Kevi rite | HttpOperator POST, pachhi HttpSensor status poll | BashOperator `java -jar ... --etl.cli.job=J2_SALES` |
| App | Hammesha chalu (server / pod) | Run vakhte j chale, pachhi bandh |
| Startup kharch | Nathi (SparkSession ready) | Darek run ma 10–20 s |
| Fail | Sensor FAILED status joi ne task fail | Exit code 1 → task fail |
| Kyare | Dedicated ETL server che | Server nathi, Airflow worker / K8s pod |

Banne ma `windowEnd = data_interval_end`. Etle run id ane window **deterministic** che: backfill ke "clear task" thi farithi chalavo to same window aave.
**Ek j DAG ON rakho.**

### 5.3 In-app Scheduler

```yaml
etl:
  scheduler:
    enabled: true                     # master switch
  jobs:
    "[J2_SALES]":
      schedule-enabled: true
      schedule-cron: "0 0 0/4 * * *"  # sec min hour day month weekday, IST
```

Airflow na hoy evu nanu setup ke dev mate. **Prod ma bandh** (`application-prod.yml`), kem ke tya Airflow chalave che.

### 5.4 Banne ek sathe aave to?

PostgreSQL **advisory lock** (`pg_try_advisory_lock`): bijo trigger turant 409 / "already running" le che.
App na 2 instance hoy to pan kaam kare (lock DB ma che). App crash thay to connection bandh thata lock pote chhuti jay.

---

## 6. Audit table (`meta.etl_run_audit`)

Darek run ni darek source ni ek row, ane ek `JOB` summary row:

| Column | Matlab |
|---|---|
| `total_record` | Window ma vanchela source documents |
| `processed_record` | Load thaya (partial pan aama) |
| `rejected_record` | Document level reject |
| `partial_record` | Load thaya, pan ek line reject thai (O1008, O1012) |
| `inserted_record` / `updated_record` | Target table ma navi / kharekhar badlayeli rows |
| `triggered_by` | API / AIRFLOW / SCHEDULER / CLI |
| `status`, `error_message` | RUNNING → SUCCESS / FAILED |

Niyam: `SUCCESS` hoy to `total = processed + rejected` (DB CHECK constraint).
`meta_rejected` ma line rejects alag che, etle Job-2 ma audit rejected 10 che pan `meta_rejected` ma 12 rows. E sachu che.

`inserted` / `updated` kevi rite? Silver table na `etl_inserted_at = etl_updated_at` hoy to navi, ane trigger e `etl_updated_at` badlyu hoy to update. Value same hoy to upsert update j nathi kartu, etle "updated" = kharekhar badlayeli rows.

---

## 7. Performance: kai technique, ane biji rit karta kem saari

| # | Technique (code ma kya) | Biji rit | Kem saari |
|---|---|---|---|
| 1 | **Filter pushdown**: watermark `$match` aggregation na pehla stage ma (`MongoSourceReader`) | Aakhi collection vanchi Spark ma filter | Mongo `updated_at` index vapre. Fakt window na docs network par aave |
| 2 | **Projection pushdown**: `$project` (`MongoProjection`) | Aakho document | Fakt jaruri fields. Ochhi network, ochhi memory |
| 3 | **Explicit schema** (`RawXxx.SCHEMA`) | Connector schema infer kare | Sampling query nathi. Type fix, ane mixed type thi crash nathi |
| 4 | **Parallel read**: SamplePartitioner `_id` range | Ek connection thi badhu | Darek executor potano tukdo vanche. Driver par data nathi aavto |
| 5 | **`_reject` column + persist** (`RuleEngine`, `SourcePipeline`) | Valid ane reject mate alag filter/queries | Mongo EK j vaar vanchay. Count, rejects ane load same cached data thi |
| 6 | **Column expressions, UDF nahi** (`Rule`, `Clean`) | Java UDF / `map()` lambda | Catalyst optimize kare ane code generate kare. Badha rules ek pass ma |
| 7 | **`Dataset<Row>` + dataset classes** (column constants) | Java bean encoder (`Dataset<Customer>`) | Row → object conversion nathi. Typo safety constants thi male che |
| 8 | **Broadcast join** nani dims (`SalesLookups`) | Shuffle join | Moto orders data hale j nahi. Nani table darek executor ne copy |
| 9 | **DB ma filter + GROUP BY** (`orderCustomers` lookback) | Aakhi fact_sales Spark ma | 90 divas j, ane Postgres index vapre |
| 10 | **Partition diith 1 connection + batch** (`JdbcPartitionWriter`) | Row diith connection / statement | 1000 rows = 1 round-trip |
| 11 | **`reWriteBatchedInserts=true`** (JDBC url) | Normal batch | pgjdbc multi-row INSERT banave. 5–10x fast |
| 12 | **Chunk diith commit** | Aakhi partition ek transaction | Locks tunka, ane DB par moti transaction nathi |
| 13 | **`IS DISTINCT FROM` upsert** | Hammesha UPDATE | Same value hoy to write nahi. Ochha dead tuples ane ochhu vacuum |
| 14 | **`coalesce(writeParallelism)`** | Badha partitions thi sidhu lakhvu | DB connections control ma. `repartition` jevo shuffle nathi |
| 15 | **Ek SparkSession** (`SparkConfig`) | Darek run ma navu | Darek run ma 5–10 s bachat |
| 16 | **AQE + Kryo** (application.yml) | Default | Runtime par partitions bhega, skew handle, fast serialization |
| 17 | **Staging vagar direct upsert** | Staging + MERGE | Nana/madhyam volume ma ek write ochhu. Moto scale par staging (design doc Section 12.2) |

**Je NATHI karyu (big data ma avoid):** `collect()`, `toPandas()`, `coalesce(1)`, JDBC `mode("overwrite")`, driver par loop.

---

## 8. Navo job (Job-1 / 3 / 4) kevi rite umervo

1. `dataset/raw/RawXxx` (schema + projection + `MongoSource`)
2. `dataset/silver/...` table classes
3. `jobs/<naam>/xxx/` ma Sanitizer, Validator, Loader, ane `Pipeline extends SourcePipeline`
4. `XxxJob implements EtlJob` + `@Component`
5. `application.yml` ma `etl.jobs."[J1_...]"` settings

API, scheduler, CLI, lock, audit ane watermark ma **kai badlavu nathi padtu**.

---

## 9. Haju baki / dhyan

- **Job-1 nathi**, etle `dim_product` 04 SQL thi bharvo pade che.
- **Cluster mode** (`spark://...`): plain jar (`shop-etl-pipeline-1.0.0.jar`) ane Mongo connector `spark.jars` thi executors ne moklo (`application-prod.yml` ma comment).
- **Spark UI bandh che** (Spark 3.5 javax vs Spring Boot 3 jakarta). Monitoring mate audit table ane logs.
- **Airflow DAG files** Airflow 2.x + `apache-airflow-providers-http` mate lakhi che. Ahiya Airflow nathi, etle chalavi ne test nathi kari.
