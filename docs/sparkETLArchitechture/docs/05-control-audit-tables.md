# 05 — Control, Audit and Staging Tables (the "Control Room" of the Platform)

> Builds on [01](01-architecture-and-structure.md), [02](02-ingestion-bronze.md), [03](03-bronze-to-silver.md) and [04](04-silver-to-gold.md).
> Written in **simple language**. Every section starts with an **"In simple words"** box, and every abbreviation is written out in full (table below).

---

## 0. Abbreviations Used in This Document

| Short form | Full form | Meaning in simple words |
|---|---|---|
| **ETL** | Extract, Transform, Load | Take data out, clean/reshape it, put it somewhere else |
| **DB** | Database | Where data is stored |
| **EPAS** | EDB Postgres Advanced Server (EDB = EnterpriseDB) | The PostgreSQL-based database we use for Silver, Gold and control tables |
| **DDL** | Data Definition Language | SQL that **creates** tables (`CREATE TABLE ...`) |
| **SQL** | Structured Query Language | The language to talk to relational databases |
| **ER diagram** | Entity–Relationship diagram | A picture of tables and how they are connected |
| **PK** | Primary Key | The column(s) that uniquely identify one row |
| **FK** | Foreign Key | A column that points to the PK of another table |
| **DQ** | Data Quality | Checks that data is correct (not null, valid range, ...) |
| **DLQ** | Dead Letter Queue | A "problem box" Kafka topic for messages that could not be processed |
| **CDC** | Change Data Capture | Capturing every insert/update/delete from the source DB |
| **SK** | Surrogate Key | A number created by us (in Gold) to identify a dimension row |
| **SCD** | Slowly Changing Dimension | Rules for keeping history when a dimension attribute changes |
| **B→S / B2S** | Bronze to Silver | Jobs that move data from Bronze to Silver |
| **S→G / S2G** | Silver to Gold | Jobs that move data from Silver to Gold |
| **UUID** | Universally Unique Identifier | A random 128-bit ID that is practically never repeated (`3f2a…`) |
| **JSON** | JavaScript Object Notation | A text format for structured data `{"key": "value"}` |
| **JSONB** | JSON Binary | PostgreSQL's fast, indexable binary JSON column type |
| **WAL** | Write-Ahead Log | PostgreSQL's crash-safety log; every normal write is recorded there first |
| **API** | Application Programming Interface | A way for programs to call each other (e.g. a REST URL) |
| **REST** | Representational State Transfer | The common style of web APIs (`GET /jobs/123`) |
| **UI** | User Interface | Screens people click on |
| **BI** | Business Intelligence | Reports and dashboards (Power BI, Tableau, ...) |
| **CI/CD** | Continuous Integration / Continuous Delivery | Automatic build, test and deploy pipeline |
| **YAML** | YAML Ain't Markup Language | The config file format we use for jobs (`key: value`) |
| **DAG** | Directed Acyclic Graph | An Airflow workflow: tasks with arrows, no loops |
| **PII** | Personally Identifiable Information | Data that identifies a person (name, email, phone) |
| **GDPR** | General Data Protection Regulation | EU privacy law |
| **DPDP** | Digital Personal Data Protection (Act, 2023) | India's privacy law |
| **TTL** | Time To Live | How long something is kept before it is deleted automatically |
| **UTC** | Coordinated Universal Time | One global time zone, so all timestamps agree |
| **SLA** | Service Level Agreement | A promise like "data ready by 8 AM" |
| **RBAC** | Role-Based Access Control | Permissions given by role (admin, operator, viewer) |
| **FY** | Financial Year | e.g. April–March in India |
| **GST** | Goods and Services Tax | Indian indirect tax (has legal record-keeping rules) |

---

## 1. Why Do We Need Control Tables at All?

> **In simple words**
> Think of the data platform as a **factory**. Bronze, Silver and Gold are the **production lines**. The control tables are the **control room**:
> - a **register of all machines** (which jobs exist, who owns them, when they run) → `job_registry`
> - a **logbook** of every shift (which job ran, when, how many items, success or not) → `job_run_audit`
> - a **"do not start, machine in use" board** → `job_lock`
> - a **bookmark** of how far each machine has read → `watermark`
> - a **rejected-items shelf** where bad items wait for repair → `quarantine_record`
> - a **quality inspection report** → `dq_results`
> - a **stock tally sheet** (did all items that went in come out?) → `reconciliation`
> - a **dictionary of problem codes** → `error_code_catalog`
>
> Without these, when something goes wrong at 3 AM, nobody can answer "what ran, what failed, why, and what do I re-run?".

---

## 2. Where Do These Tables Live?

> **In simple words**
> All control tables live in **EPAS**, in a schema (a "folder" inside the database) called **`etl_ctl`**, in the **same database** as Silver and Gold.

### 2.1 Options compared

| Option | Verdict | Reason |
|---|---|---|
| **EPAS, schema `etl_ctl`, same database as Silver and Gold** | ✅ **Chosen** | See the key reason below |
| EPAS, a separate database (e.g. `etl_control`) | ❌ | PostgreSQL cannot run **one transaction across two databases** |
| MongoDB | ❌ | Weak for joins and reporting; locks and status changes need strong transactions |
| Airflow's own metadata database | ❌ | That database belongs to Airflow (upgrades, cleanups); mixing our data in it couples us to Airflow internals |

### 2.2 The key reason: "same database, same transaction"

> **In simple words:** some updates must happen **together or not at all**. Example from doc 04: when a Gold fact job commits, it must, in the **same** transaction:
> 1. write the fact rows (schema `gold_sales`),
> 2. update the Silver summary status (schema `silver_order`),
> 3. write the failure list (`etl_ctl.gold_record_status`),
> 4. move the bookmark (`etl_ctl.watermark`).
>
> If step 4 happened in another database and failed after step 1 succeeded, the bookmark would be wrong and data could be processed twice or skipped. One database = one transaction = always consistent.

So the physical layout is:

```
EPAS cluster
└── database: dataplatform
    ├── schema silver_<domain>, silver_ref, silver_mdm     (Silver data)
    ├── schema stg_silver_<domain>                          (Silver staging)
    ├── schema gold_core, gold_<area>                       (Gold data)
    ├── schema stg_gold_<area>                              (Gold staging)
    └── schema etl_ctl                                      (control, audit, quarantine)  ← this document

MongoDB
└── bronze_<service> databases                           (Bronze, doc 02)

Airflow metadata DB                                      (separate, owned by Airflow)
```

Bronze is in MongoDB, so a Bronze write and an `etl_ctl` write can never be in one transaction. That is why the Bronze → Silver job uses the order "commit Silver + control first, then write back to Bronze" (doc 03 §9.1).

### 2.3 Database users (roles)

| Role | Can do | Used by |
|---|---|---|
| `etl_job` | read/write `etl_ctl`, Silver, Gold, staging | Spark jobs, `etl-ingestion` |
| `etl_ops` | read `etl_ctl`; update quarantine status; insert manual run requests | `etl-ops-api` (the operations service) |
| `etl_monitor` | read-only `etl_ctl` | Grafana dashboards, alerts |
| `etl_migrate` | DDL (create/alter tables) | Flyway (database migration tool) in CI/CD only |
| `bi_reader` | read-only Gold `v_*` views | BI tools. **No** access to `etl_ctl` or staging |

---

## 3. Staging Tables

### 3.1 Are staging tables needed for every job?

> **In simple words**
> A staging table is a **waiting room**. Spark first puts all the new rows in the waiting room. Then the database moves them into the real table **in one go**, inside one transaction. If anything goes wrong, nothing in the real table changes.
>
> We need this waiting room only when Spark writes into **EPAS with merge logic**, because Spark's built-in database writer can only **insert**, not "update if exists, else insert".

| Job / write | Staging needed? | Why |
|---|---|---|
| `etl-ingestion` → Bronze (MongoDB) | ❌ No | MongoDB bulk `updateOne(upsert)` does the merge directly (doc 02) |
| Bronze → Silver (Spark → EPAS) | ✅ **Yes** | Merge + orphan handling + one transaction for parent and child tables (doc 03) |
| Silver → Gold dimension / fact (Spark → EPAS) | ✅ **Yes** | SCD merge, surrogate key lookups, inferred members, one transaction (doc 04) |
| Gold aggregate (SQL inside EPAS) | ❌ No | The data never leaves the database; a temporary table inside the transaction is enough |
| Gold periodic snapshot (SQL inside EPAS) | ❌ No | Delete + insert for one date, inside the database |
| Write-back to Bronze (Spark → MongoDB) | ❌ No | Direct bulk conditional updates (doc 03 §9) |

### 3.2 Why not merge directly from Spark (without staging)?

You *could* let every Spark partition run its own `INSERT ... ON CONFLICT` batches. We don't, because:

| Problem with direct merge from Spark | Effect |
|---|---|
| Each Spark partition has **its own** database connection and transaction | There is no single "commit everything" moment. If partition 7 fails, partitions 1–6 are already written |
| Parent and child tables are written by different partitions at different times | Users can see an order without its items for a while |
| The circuit breaker (doc 03 §7.3) decides **after** the checks | With direct writes, the data would already be in the table |
| Orphan handling, SCD and surrogate key logic are set-based | They need all rows of the run in one place |

### 3.3 Why not PostgreSQL TEMP tables? ➕

> **In simple words:** a `TEMP` table is visible **only to the one connection** that created it. Spark writes from **many executors, each with its own connection**, so each executor would see a different (empty) temp table. That is why our staging tables are normal (but `UNLOGGED`) tables.

### 3.4 Staging table design

| Choice | Decision |
|---|---|
| Naming | `stg_<target schema>.<target table>` (e.g. `stg_silver_order.sales_order`) |
| One per | **target table** (not per run). The job lock guarantees one run per job at a time, and each target table has only one writer job |
| Type | `UNLOGGED` = not written to the WAL (Write-Ahead Log). Much faster. Safe to lose on a crash, because the run is simply repeated |
| Columns | Same as the target table + `_etl_run_id` + helper columns (`_bronze_id`, `_action` for SCD, `_src_key`) |
| Created by | Flyway migration (generated from the target DDL by a template), never by hand |
| Indexes | None (a bulk load is faster without indexes); `ANALYZE` after loading so the merge query gets a good plan |
| Access | Same permissions as the target (it may contain PII); **no** BI access |

```sql
-- Generated by the framework's DDL template for each target table
CREATE SCHEMA IF NOT EXISTS stg_silver_order;
CREATE UNLOGGED TABLE IF NOT EXISTS stg_silver_order.sales_order
    (LIKE silver_order.sales_order INCLUDING DEFAULTS);          -- same columns as the target
ALTER TABLE stg_silver_order.sales_order
    ADD COLUMN IF NOT EXISTS _bronze_id   varchar(64),
    ADD COLUMN IF NOT EXISTS _read_version bigint;
-- (_etl_run_id already exists in every target table)
```

### 3.5 Staging lifecycle

```
 ┌──────────────┐   ┌──────────────┐   ┌──────────────┐   ┌──────────────┐   ┌──────────────────────┐
 │ 1. TRUNCATE  │──►│ 2. LOAD      │──►│ 3. ANALYZE   │──►│ 4. MERGE     │──►│ 5. KEEP until the    │
 │ at run start │   │ COPY from    │   │ refresh      │   │ in ONE       │   │    next run (for     │
 │ (empty the   │   │ Spark        │   │ statistics   │   │ transaction  │   │    debugging)        │
 │  room)       │   │ executors    │   │              │   │ into target  │   │                      │
 └──────────────┘   └──────────────┘   └──────────────┘   └──────────────┘   └──────────┬───────────┘
                                                                                         │
                                                       ┌─────────────────────────────────▼───────────┐
                                                       │ 6. CLEANUP (maintenance DAG, nightly):       │
                                                       │    TRUNCATE staging of jobs idle > 7 days ➕ │
                                                       │    (frees disk; data may contain PII)        │
                                                       └─────────────────────────────────────────────┘
```

| Step | Detail |
|---|---|
| 1. Truncate | At the **start** of the run (not at the end), so after a failed run the staging data is still there for investigation |
| 2. Load | PostgreSQL `COPY` per Spark partition (fast), tagged with `_etl_run_id` |
| 3. Analyze | `ANALYZE stg_x.t` so the database knows how many rows there are |
| 4. Merge | The SQL from docs 03/04, all targets of the job in **one** transaction |
| 5. Keep | Last run's rows stay until the next run truncates them |
| 6. Cleanup | Nightly maintenance job truncates staging tables of jobs that have not run for 7 days |

**Swap (only for full fact rebuilds, doc 04 §9):**

```
 create gold_sales.fact_sales_line__rebuild  →  load all  →  validate (reconciliation)
   →  BEGIN; rename live → __old; rename __rebuild → live; COMMIT;   (a few milliseconds)
   →  drop __old after 7 days
```

The swap tables are tracked in `job_run_audit` (step `SWAP`) so it is always clear which table version is live.

---

## 4. The Full Picture: ER Diagram

> **In simple words:** the boxes are tables. A line `1 ──< *` means "one row on the left has many rows on the right". For example, one job (in `job_registry`) has many runs (in `job_run_audit`).

```
                                 ┌──────────────────────────┐
                                 │   error_code_catalog     │  (dictionary of problem codes)
                                 │   PK error_code          │
                                 └────────────┬─────────────┘
                     referenced by: job_run_audit, job_step_audit, record_error_log,
                                    quarantine_record, gold_record_status, dq_results,
                                    ingestion_dlq_index
                                              ┆
 ┌───────────────────────┐ 1   * ┌────────────┴──────────┐ 1   * ┌───────────────────────┐
 │ job_config_version    │>──────│     job_registry      │──────<│    job_dependency     │
 │ PK (job_name,         │       │     PK job_name       │──────<│ PK (job_name,         │
 │     config_hash)      │       │ type, layer, domain,  │   *   │     depends_on_job)   │
 └───────────────────────┘       │ owner, schedule,      │       └───────────────────────┘
                                 │ thresholds, enabled   │ 1   * ┌───────────────────────┐
 ┌───────────────────────┐ 1   1 │                       │──────<│      job_source       │
 │       job_lock        │───────│                       │       │ PK (job_name,         │
 │   PK job_name         │       │                       │       │     source_table)     │
 └───────────────────────┘       │                       │ 1   * ├───────────────────────┤
                                 │                       │──────<│      job_target       │
 ┌───────────────────────┐ *   1 │                       │       │ PK (job_name,         │
 │       watermark       │>──────│                       │       │     target_table)     │
 │ PK (job_name,         │       └───────────┬───────────┘       │ UNIQUE target_table ➕│
 │     source_table)     │                   │ 1                 └───────────────────────┘
 └──────────┬────────────┘                   │
            │ 1                              │ *
            │ *            ┌─────────────────▼─────────────────┐  parent_run_id / root_run_id
 ┌──────────▼────────────┐ │          job_run_audit            │──┐ (self reference: a rerun
 │  watermark_history ➕ │ │          PK run_id                │◄─┘  points to the run it fixes)
 └───────────────────────┘ │ run_mode, trigger_type, status,   │
                           │ counts, params, spark_app_id ...  │
                           └──┬───────┬────────┬────────┬──────┘
                        1   * │     * │      * │      * │
            ┌─────────────────┘       │        │        └───────────────────────────┐
            ▼                         ▼        ▼                                    ▼
 ┌─────────────────────┐ ┌──────────────────┐ ┌───────────────────────┐ ┌──────────────────────────┐
 │   job_step_audit    │ │   dq_results     │ │    reconciliation     │ │    record_error_log      │
 │ PK (run_id,step_no) │ │ PK (run_id,      │ │ PK (run_id, target,   │ │ (append-only, partitioned│
 └─────────────────────┘ │  target, rule_id)│ │     check_name)       │ │  by day, no FKs)         │
                         └──────────────────┘ └───────────────────────┘ └──────────────────────────┘
                                                     job_run_audit 1 ──< * (created_run_id / reprocess_run_id)
                                                                  │
                           ┌──────────────────────────────────────▼──────┐ 1   * ┌──────────────────────────────┐
                           │            quarantine_record                │──────<│ quarantine_status_history ➕ │
                           │            PK quarantine_id (UUID)          │       │ every status change, who,    │
                           │ layer, job, source pointer, errors, status  │       │ when, note                   │
                           └─────────────────────────────────────────────┘       └──────────────────────────────┘

 Gold sync helpers (doc 04)                Ingestion helpers (doc 02)               Privacy (doc 02 §10)
 ┌──────────────────────────┐              ┌──────────────────────────┐            ┌──────────────────────┐
 │   gold_record_status     │              │   ingestion_dlq_index    │            │   purge_request ➕   │
 │ PK (job_name,            │              │ PK (dlq_partition,       │            │ PK purge_id          │
 │     source_table,        │              │     dlq_offset)          │            └──────────────────────┘
 │     row_key)             │              ├──────────────────────────┤
 ├──────────────────────────┤              │  ingestion_replay_audit  │            ┌──────────────────────┐
 │  gold_touched_partition  │              │  PK replay_id            │            │ retention_policy ➕  │
 │ PK (target_table,        │              └──────────────────────────┘            │ PK object_name       │
 │     partition_value,     │                                                      └──────────────────────┘
 │     run_id)              │
 └──────────────────────────┘
```

### 4.1 Table list at a glance

| # | Table | One row = | Who writes it | Who reads it |
|---|---|---|---|---|
| 1 | `job_registry` | one job | Registry sync (from Git YAML) | Airflow DAG factory, ops API, jobs |
| 2 | `job_config_version` ➕ | one version of a job's config | Registry sync | Audits ("which config did run X use?") |
| 3 | `job_source` | one table/collection a job reads | Registry sync | Gold sync summary (doc 04 §5.4), lineage |
| 4 | `job_target` | one table a job writes | Registry sync | Single-writer check, lineage |
| 5 | `job_dependency` | one "A depends on B" link | Registry sync | Airflow, cascade reruns |
| 6 | `job_lock` | one currently running job | Jobs | Jobs, ops UI |
| 7 | `job_run_audit` | one run of a job | Jobs, ops API (manual requests) | Everyone |
| 8 | `job_step_audit` | one step of one run | Jobs | Debugging, performance tuning |
| 9 | `watermark` | one bookmark (job × source table) | Jobs | Jobs |
| 10 | `watermark_history` ➕ | one old bookmark value | Jobs | Replay / reset |
| 11 | `record_error_log` | one error of one record in one run | Jobs | Analysis, ops UI |
| 12 | `quarantine_record` | one record waiting for a fix | Jobs, ops API | Ops UI, reprocess jobs |
| 13 | `quarantine_status_history` ➕ | one status change of a quarantine record | Ops API, jobs | Audit trail |
| 14 | `dq_results` | one DQ rule result per run and table | Jobs | DQ dashboards, OpenMetadata |
| 15 | `reconciliation` | one tally check per run | Jobs | Dashboards, alerts |
| 16 | `error_code_catalog` | one error code | Flyway seed (from Git) | Jobs (retryable flag), ops UI |
| 17 | `gold_record_status` | one non-OK Silver row per Gold job | Gold jobs | Gold jobs, Silver summary |
| 18 | `gold_touched_partition` | one "dirty" date of a fact | Fact jobs | Aggregate jobs |
| 19 | `ingestion_dlq_index` | one DLQ message | `etl-ingestion` | Ops UI, DLQ replay |
| 20 | `ingestion_replay_audit` | one replay run | Replay job | Audit |
| 21 | `purge_request` ➕ | one privacy delete request | Ops API | Purge job |
| 22 | `retention_policy` ➕ | one retention rule | Flyway seed | Maintenance job |
| 23 | `run_request` (doc 07) | one manual run request (with approval) | Ops API | Ops API, approvers |
| 24 | `pii_access_log` (doc 11) | one PII reveal / export | Ops API | Privacy audits |

---

## 5. General Design Rules for All Control Tables

> **In simple words:** a few house rules, so that all 22 tables look and behave the same way.

| Rule | Decision | Why |
|---|---|---|
| Time | Always `timestamptz`, stored in UTC (Coordinated Universal Time) | No time-zone confusion |
| Status / type columns | `varchar` + `CHECK (... IN (...))` constraint, **not** PostgreSQL `ENUM` types ➕ | Adding a value to a CHECK is one simple migration; changing ENUM types is awkward |
| IDs | `run_id` = readable text `b2s_order-20261030T091500Z-01` (job + start time + counter). Quarantine/purge IDs = UUID (Universally Unique Identifier) | Run IDs appear in logs and Slack alerts, so readability helps; UUIDs are safe to create anywhere |
| Foreign keys | Enforced on small tables (to `job_registry`, `error_code_catalog`, `job_run_audit`); **not** on the high-volume `record_error_log` | Integrity where it is cheap, speed where volume is high |
| Flexible extra data | `jsonb` (JSON Binary) columns for parameters, details and snapshots | New fields without schema changes |
| Who changed it | `created_at`, `updated_at` (+ `created_by`/`updated_by` where people act) | Audit trail |
| Source of truth for configs | **Git YAML.** The DB tables are a synchronized copy for querying | No "someone changed prod config by hand" surprises |
| Migrations | All DDL in `db/control/V<n>__*.sql` (Flyway) | Same structure in every environment |

---

## 6. DDL for Every Table

All files go to `db/control/`. The examples assume `CREATE SCHEMA etl_ctl;` already ran (`V1__schema.sql`).

### 6.1 `error_code_catalog`: the dictionary of problem codes

> **In simple words:** every error in the platform gets a **short code** (like `DQ_NOT_NULL`) instead of free text. The catalog explains each code and, most importantly, says whether **retrying can help** (`retryable`). Jobs read this flag to decide between FAILED (retry later) and QUARANTINED (needs a fix).

```sql
-- db/control/V2__error_code_catalog.sql
CREATE TABLE etl_ctl.error_code_catalog (
    error_code          varchar(50)   PRIMARY KEY,
    category            varchar(20)   NOT NULL CHECK (category IN
                          ('INGESTION','DQ','REFERENTIAL','TRANSFORM','MODEL','INFRA','CONFIG',
                           'RECONCILIATION','CONTROL','SECURITY')),
    layer               varchar(20)   NOT NULL CHECK (layer IN ('INGESTION','B2S','S2G','ANY')),
    description         varchar(500)  NOT NULL,
    retryable           boolean       NOT NULL,          -- can a retry (without any fix) succeed?
    default_severity    varchar(12)   NOT NULL CHECK (default_severity IN ('WARN','ERROR','QUARANTINE','FATAL')),
    recommended_action  varchar(500)  NOT NULL,          -- shown in the ops UI and in alerts
    owner_team          varchar(100),
    is_active           boolean       NOT NULL DEFAULT true,
    created_at          timestamptz   NOT NULL DEFAULT now(),
    updated_at          timestamptz   NOT NULL DEFAULT now()
);
```

**Seed data** (`db/control/R__error_code_catalog_seed.sql`, a Flyway "repeatable" migration that re-runs whenever the file changes). These are all the codes used in docs 02–04:

| error_code | category | retryable | severity | Meaning / action |
|---|---|---|---|---|
| `PARSE_ERROR` | INGESTION | no | QUARANTINE | Kafka message is not valid JSON → check the connector config |
| `UNMAPPED_COLLECTION` | CONFIG | no | ERROR | Event for a collection with no route → add the route, then replay the DLQ |
| `MISSING_VERSION` | INGESTION | no | QUARANTINE | Event without `clusterTime` → connector/version issue |
| `HANDLER_ERROR` | INGESTION | no | QUARANTINE | Custom handler crashed → fix the code, replay the DLQ |
| `BRONZE_WRITE_REJECTED` | INGESTION | no | QUARANTINE | MongoDB refused the document (too large, bad field name) |
| `POISON_MESSAGE` | INGESTION | no | QUARANTINE | Message crashes the consumer every time |
| `DQ_NOT_NULL` | DQ | no | QUARANTINE | Mandatory field is empty → fix at source |
| `DQ_TYPE_CAST` | DQ | no | QUARANTINE | Value cannot be converted to the target type |
| `DQ_RANGE` | DQ | no | QUARANTINE | Value outside the allowed range |
| `DQ_ALLOWED_VALUES` | DQ | no | QUARANTINE | Value not in the allowed list |
| `DQ_REGEX` | DQ | no | QUARANTINE | Value does not match the expected pattern |
| `DQ_LENGTH` | DQ | no | QUARANTINE | Value too long |
| `DQ_EXPRESSION` | DQ | no | QUARANTINE | A business rule expression failed |
| `DQ_FUTURE_DATE` | DQ | no | WARN | Date is in the future |
| `DUPLICATE_BUSINESS_KEY` | DQ | no | ERROR | Two source records with the same business key (loser is SKIPPED) |
| `FILTERED` | TRANSFORM | no | WARN | Record intentionally filtered out (SKIPPED) |
| `REF_PARENT_MISSING` | REFERENTIAL | **yes** | ERROR | Referenced record has not arrived yet → retried automatically |
| `REF_MAPPING_MISSING` | REFERENTIAL | no | QUARANTINE | Source code not in a `silver_ref` mapping table → add the mapping |
| `RETRIES_EXHAUSTED` | CONTROL | no | QUARANTINE | Retryable error kept failing until `maxRetries` |
| `MODEL_AMOUNT_MISMATCH` | MODEL | no | QUARANTINE | Gold amounts don't add up (gross − discount ≠ net) |
| `SCD_INTEGRITY_VIOLATION` | MODEL | no | FATAL | Two current versions / overlapping SCD2 ranges (framework bug) |
| `UNKNOWN_SK_RATIO_HIGH` | MODEL | no | WARN | Too many facts point to "Unknown" (-1) |
| `THRESHOLD_BREACHED` | CONTROL | no | FATAL | Too many failures in one run → circuit breaker rolled back |
| `RECONCILIATION_MISMATCH` | RECONCILIATION | no | FATAL | Counts don't tally → framework bug, investigate |
| `WRITEBACK_INCOMPLETE` | CONTROL | **yes** | WARN | Status write-back to the previous layer partly failed; next run heals it |
| `LOCK_NOT_ACQUIRED` | CONTROL | **yes** | WARN | Same job is already running → run skipped |
| `DEPENDENCY_FAILED` | CONTROL | **yes** | WARN | Upstream job failed → run skipped/waited (per policy) |
| `CONFIG_INVALID` | CONFIG | no | FATAL | Job YAML failed validation |
| `INFRA_DB_UNAVAILABLE` | INFRA | **yes** | FATAL | EPAS not reachable |
| `INFRA_MONGO_UNAVAILABLE` | INFRA | **yes** | FATAL | MongoDB not reachable |
| `INFRA_SPARK_FAILURE` | INFRA | **yes** | FATAL | Spark executor/driver failure |
| `INFRA_TIMEOUT` | INFRA | **yes** | FATAL | An operation took too long |
| `INFRA_UNKNOWN` | INFRA | **yes** | FATAL | Unclassified exception (to be classified later) |

```sql
-- excerpt of R__error_code_catalog_seed.sql: idempotent upsert of the whole list
INSERT INTO etl_ctl.error_code_catalog (error_code, category, layer, description, retryable, default_severity, recommended_action, owner_team)
VALUES
 ('DQ_NOT_NULL',        'DQ',          'ANY', 'Mandatory field is empty',               false, 'QUARANTINE', 'Fix the value in the source system; it flows through automatically', 'data-quality'),
 ('REF_PARENT_MISSING', 'REFERENTIAL', 'ANY', 'Referenced record has not arrived yet',  true,  'ERROR',      'Usually resolves itself; check the upstream job if it lasts > retry window', 'data-platform'),
 ('THRESHOLD_BREACHED', 'CONTROL',     'ANY', 'Failure rate above the job threshold',   false, 'FATAL',      'Check dq_results of the run: probably a code bug or a source schema change', 'data-platform')
 -- ... all codes from the table above ...
ON CONFLICT (error_code) DO UPDATE SET
    category = EXCLUDED.category, layer = EXCLUDED.layer, description = EXCLUDED.description,
    retryable = EXCLUDED.retryable, default_severity = EXCLUDED.default_severity,
    recommended_action = EXCLUDED.recommended_action, owner_team = EXCLUDED.owner_team, updated_at = now();
```

### 6.2 `job_registry`: the register of all jobs

> **In simple words:** one row per job, with everything you need to know about it at a glance. The real definition lives in the job's YAML file in Git. A **sync step** (run by CI/CD at every deploy) copies it into this table, so that Airflow, the ops UI and SQL queries can see all jobs in one place.

```sql
-- db/control/V3__job_registry.sql
CREATE TABLE etl_ctl.job_registry (
    job_name            varchar(100)  PRIMARY KEY,                  -- e.g. b2s_order, s2g_fact_sales_line
    job_type            varchar(30)   NOT NULL CHECK (job_type IN
                          ('B2S','S2G_DIMENSION','S2G_FACT','S2G_SNAPSHOT','S2G_AGGREGATE','INGESTION_REPLAY','MAINTENANCE')),
    layer               varchar(20)   NOT NULL CHECK (layer IN ('BRONZE_TO_SILVER','SILVER_TO_GOLD','GOLD','OPS')),
    domain              varchar(50)   NOT NULL,                     -- order, customer, sales ...
    owner_team          varchar(100)  NOT NULL,
    description         varchar(1000),
    enabled             boolean       NOT NULL DEFAULT true,        -- false = never scheduled or triggered
    schedule_cron       varchar(50),                                -- "15 * * * *" (env-resolved value); NULL = trigger-only
    default_run_mode    varchar(30)   NOT NULL DEFAULT 'INCREMENTAL',
    max_retries         integer       NOT NULL DEFAULT 5,           -- record-level retries (FAILED → QUARANTINED)
    thresholds          jsonb         NOT NULL,                     -- {"maxFailedPct":2.0,"maxFailedCount":5000,...}
    resources           jsonb,                                      -- {"executors":4,"executorMemory":"4g",...}
    sla_minutes         integer,                                    -- ➕ expected max duration / freshness promise
    config_hash         char(64)      NOT NULL,                     -- SHA-256 of the resolved config
    config_git_commit   varchar(40),                                -- Git commit the config came from
    config_json         jsonb         NOT NULL,                     -- full resolved config (for the ops UI and audits)
    synced_at           timestamptz   NOT NULL DEFAULT now(),
    created_at          timestamptz   NOT NULL DEFAULT now(),
    updated_at          timestamptz   NOT NULL DEFAULT now()
);
CREATE INDEX ix_job_registry_domain ON etl_ctl.job_registry (domain);
CREATE INDEX ix_job_registry_type   ON etl_ctl.job_registry (job_type);

-- ➕ Every config version ever used, so any old run can be explained
CREATE TABLE etl_ctl.job_config_version (
    job_name            varchar(100)  NOT NULL REFERENCES etl_ctl.job_registry (job_name),
    config_hash         char(64)      NOT NULL,
    config_git_commit   varchar(40),
    config_json         jsonb         NOT NULL,
    first_seen_at       timestamptz   NOT NULL DEFAULT now(),
    PRIMARY KEY (job_name, config_hash)
);
```

**Removed jobs:** if a YAML file is deleted from Git, the sync sets `enabled = false`. It never deletes the row, because old runs still point to it ➕.

### 6.3 `job_source` and `job_target`: what a job reads and writes

> **In simple words:** a list of "job X **reads** table A" and "job X **writes** table B". This feeds lineage (where data comes from), the Gold sync summary (doc 04 §5.4: "which Gold jobs read this Silver table?"), and a safety rule: **only one job may write a given table**.

```sql
-- db/control/V4__job_source_target.sql
CREATE TABLE etl_ctl.job_source (
    job_name            varchar(100)  NOT NULL REFERENCES etl_ctl.job_registry (job_name),
    source_table        varchar(200)  NOT NULL,                     -- "silver_order.sales_order" or "mongo:bronze_svc_order.orders"
    source_role         varchar(15)   NOT NULL CHECK (source_role IN ('DRIVING','SECONDARY','LOOKUP')),
    join_keys           text[],                                     -- for SECONDARY sources (doc 04 §7.2)
    active              boolean       NOT NULL DEFAULT true,
    PRIMARY KEY (job_name, source_table)
);
CREATE INDEX ix_job_source_table ON etl_ctl.job_source (source_table);

CREATE TABLE etl_ctl.job_target (
    job_name            varchar(100)  NOT NULL REFERENCES etl_ctl.job_registry (job_name),
    target_table        varchar(200)  NOT NULL,
    write_role          varchar(15)   NOT NULL DEFAULT 'OWNER' CHECK (write_role IN ('OWNER','INFERRED_ONLY')),
    active              boolean       NOT NULL DEFAULT true,
    PRIMARY KEY (job_name, target_table)
);
-- ➕ Single-writer rule (doc 04 §5.1): at most ONE active OWNER per table.
-- Fact jobs that only insert inferred members into a dimension are registered as INFERRED_ONLY.
CREATE UNIQUE INDEX ux_job_target_single_owner ON etl_ctl.job_target (target_table)
    WHERE active AND write_role = 'OWNER';
```

If a new YAML accidentally makes a second job the owner of a table, the registry sync **fails in CI**, before anything reaches production.

### 6.4 `job_dependency`: which job waits for which

> **In simple words:** "the fact job may start only after the customer dimension job is done". Airflow reads these links to build the order of tasks, and the cascade-rerun feature reads them to find "everything downstream of job X".

```sql
-- db/control/V5__job_dependency.sql
CREATE TABLE etl_ctl.job_dependency (
    job_name              varchar(100) NOT NULL REFERENCES etl_ctl.job_registry (job_name),
    depends_on_job        varchar(100) NOT NULL REFERENCES etl_ctl.job_registry (job_name),
    dependency_type       varchar(10)  NOT NULL DEFAULT 'HARD' CHECK (dependency_type IN ('HARD','SOFT')),
    on_upstream_failure   varchar(10)  NOT NULL DEFAULT 'WAIT' CHECK (on_upstream_failure IN ('RUN','SKIP','WAIT')),
    max_wait_minutes      integer      DEFAULT 60,                    -- for WAIT
    active                boolean      NOT NULL DEFAULT true,
    PRIMARY KEY (job_name, depends_on_job),
    CONSTRAINT ck_no_self_dependency CHECK (job_name <> depends_on_job)
);
CREATE INDEX ix_job_dependency_upstream ON etl_ctl.job_dependency (depends_on_job);
```

| Column | Meaning |
|---|---|
| `HARD` | Must succeed first |
| `SOFT` | Nice to run first, but not required (e.g. a fact can rely on inferred members) |
| `on_upstream_failure` | `RUN` anyway / `SKIP` this run / `WAIT` up to `max_wait_minutes` (full rules in Prompt 07) |

➕ **Loop check:** "A depends on B, B depends on A" would block both forever. The registry sync runs a cycle check (recursive query or a graph check in Java) and fails the deploy if a loop exists.

```sql
-- "Everything downstream of b2s_order" (used by cascade reruns)
WITH RECURSIVE downstream AS (
    SELECT job_name, 1 AS depth FROM etl_ctl.job_dependency WHERE depends_on_job = 'b2s_order' AND active
    UNION
    SELECT d.job_name, ds.depth + 1 FROM etl_ctl.job_dependency d
    JOIN downstream ds ON d.depends_on_job = ds.job_name WHERE d.active AND ds.depth < 20
)
SELECT DISTINCT job_name, min(depth) AS depth FROM downstream GROUP BY job_name ORDER BY depth;
```

### 6.5 `job_lock`: "machine in use"

> **In simple words:** before a job starts, it **writes its name on the board**. If the name is already there, a second copy of the same job does not start. When the job finishes, it **erases its name**.
> But what if the job crashes and never erases it? Then the lock would block forever. So the running job **"checks in" every minute** (a heartbeat). If nobody has checked in for, say, 10 minutes, the lock is considered dead and the next run may take it over.

```sql
-- db/control/V6__job_lock.sql
CREATE TABLE etl_ctl.job_lock (
    job_name        varchar(100)  PRIMARY KEY REFERENCES etl_ctl.job_registry (job_name),
    run_id          varchar(100)  NOT NULL,
    locked_at       timestamptz   NOT NULL DEFAULT now(),
    heartbeat_at    timestamptz   NOT NULL DEFAULT now(),
    expires_at      timestamptz   NOT NULL,                  -- heartbeat_at + lease (e.g. 10 minutes)
    holder_host     varchar(200),                            -- Spark driver host / pod name
    spark_app_id    varchar(100)
);
```

**Acquire, heartbeat and release** (single SQL statements, so they are race-free):

```sql
-- ACQUIRE: succeeds if no lock exists OR the existing lock has expired (holder crashed)
INSERT INTO etl_ctl.job_lock (job_name, run_id, expires_at, holder_host, spark_app_id)
VALUES (:job, :runId, now() + interval '10 minutes', :host, :appId)
ON CONFLICT (job_name) DO UPDATE
   SET run_id = EXCLUDED.run_id, locked_at = now(), heartbeat_at = now(),
       expires_at = EXCLUDED.expires_at, holder_host = EXCLUDED.holder_host, spark_app_id = EXCLUDED.spark_app_id
 WHERE etl_ctl.job_lock.expires_at < now()
RETURNING run_id;                          -- no row returned = lock is held by a live run → LOCK_NOT_ACQUIRED

-- HEARTBEAT: every 60 s from a background thread in the Spark driver
UPDATE etl_ctl.job_lock SET heartbeat_at = now(), expires_at = now() + interval '10 minutes'
WHERE job_name = :job AND run_id = :runId;          -- 0 rows = we lost the lock → stop the job safely

-- RELEASE: in the job's finally-block
DELETE FROM etl_ctl.job_lock WHERE job_name = :job AND run_id = :runId;
```

```java
// etl-control: lock/JobLockManager.java (sketch)
public final class JobLockManager {
    private final DataSource ds;
    private final Duration lease = Duration.ofMinutes(10);

    public JobLock acquire(String job, String runId, String host, String appId) {
        String owner = Sql.queryOptional(ds, ACQUIRE_SQL, job, runId, lease, host, appId);
        if (owner == null) throw new LockNotAcquiredException(job);           // error code LOCK_NOT_ACQUIRED
        JobLock lock = new JobLock(job, runId);
        lock.startHeartbeat(() -> {                                             // daemon thread, every 60 s
            if (Sql.update(ds, HEARTBEAT_SQL, lease, job, runId) == 0) lock.markLost();
        }, Duration.ofSeconds(60));
        return lock;
    }

    public void release(JobLock lock) {
        lock.stopHeartbeat();
        Sql.update(ds, RELEASE_SQL, lock.job(), lock.runId());
    }
}
```

➕ Before every commit, the job checks `lock.isLost()`. If the lock was lost (e.g. a very long GC (garbage collection) pause and another run took over), it rolls back instead of committing. This guarantees that two runs of the same job never both commit.

**Alternative considered:** PostgreSQL advisory locks (`pg_try_advisory_lock`). They are automatic and crash-safe, but they are invisible to the ops UI and tied to one database connection that must stay open for the whole Spark run. The table + heartbeat approach is visible ("who holds the lock since when?") and survives connection drops. Advisory locks are still used for short, in-transaction locking (doc 04 §6.6).

### 6.6 `job_run_audit`: the logbook of every run

> **In simple words:** one row per job run. It answers: **what** ran, **why** (scheduled? manual? rerun?), **who** started it, **when**, **how long**, **how many** records, and **did it work**. If a run fixes an earlier run, it points to it (`parent_run_id`), like a reply in an email thread.

**Two separate ideas ➕:** your list mixes "what kind of run" and "what started it". They are kept in two columns, plus a dry-run flag, because they combine freely (a *manual* `BACKFILL`, a *scheduled* `INCREMENTAL`, a *cascade* `INCREMENTAL`, a *dry-run* `FULL_LOAD` …):

| Column | Values | Answers the question |
|---|---|---|
| `run_mode` | `INCREMENTAL`, `FULL_LOAD`, `RERUN_JOB`, `RERUN_FAILED_RECORDS`, `REPROCESS_QUARANTINE`, `BACKFILL`, `REPLAY`, `ID_REPROCESS` | **What** data does this run process? |
| `trigger_type` | `SCHEDULED`, `MANUAL`, `DEPENDENCY`, `EVENT`, `CASCADE`, `AIRFLOW_RETRY`, `API` | **What** started it? |
| `is_dry_run` | true / false | Did it write anything? |

```sql
-- db/control/V7__job_run_audit.sql
CREATE TABLE etl_ctl.job_run_audit (
    run_id                  varchar(100)  PRIMARY KEY,                 -- b2s_order-20261030T091500Z-01
    job_name                varchar(100)  NOT NULL REFERENCES etl_ctl.job_registry (job_name),
    run_mode                varchar(30)   NOT NULL CHECK (run_mode IN
                              ('INCREMENTAL','FULL_LOAD','RERUN_JOB','RERUN_FAILED_RECORDS','REPROCESS_QUARANTINE',
                               'BACKFILL','REPLAY','ID_REPROCESS')),
    trigger_type            varchar(20)   NOT NULL CHECK (trigger_type IN
                              ('SCHEDULED','MANUAL','DEPENDENCY','EVENT','CASCADE','AIRFLOW_RETRY','API')),
    is_dry_run              boolean       NOT NULL DEFAULT false,
    triggered_by            varchar(100)  NOT NULL,                    -- user id / "airflow" / "system"
    request_ticket          varchar(50),                               -- ➕ incident/change ticket for manual runs (INC-1042)
    parent_run_id           varchar(100)  REFERENCES etl_ctl.job_run_audit (run_id),   -- the run this one fixes
    root_run_id             varchar(100),                              -- ➕ first run of the whole chain (easy grouping)
    run_params              jsonb         NOT NULL DEFAULT '{}',       -- {"from":"2026-10-01","to":"2026-10-08","ids":[...]}
    env                     varchar(10)   NOT NULL CHECK (env IN ('dev','qa','uat','prod')),
    config_hash             char(64)      NOT NULL,                    -- which config version was used
    code_version            varchar(50)   NOT NULL,                    -- etl-spark-app JAR version, e.g. 1.14.2
    status                  varchar(15)   NOT NULL CHECK (status IN
                              ('QUEUED','RUNNING','SUCCESS','PARTIAL','FAILED','SKIPPED','CANCELLED')),
    error_code              varchar(50)   REFERENCES etl_ctl.error_code_catalog (error_code),
    failure_reason          varchar(4000),                             -- human text, no PII
    queued_at               timestamptz,
    started_at              timestamptz,
    ended_at                timestamptz,
    duration_ms             bigint GENERATED ALWAYS AS
                              ((EXTRACT(EPOCH FROM (ended_at - started_at)) * 1000)::bigint) STORED,
    cutoff_value            varchar(100),                              -- run cutoff (timestamp or change_seq) for traceability
    -- record counts (doc 03 §12 equation 1)
    records_read            bigint        NOT NULL DEFAULT 0,
    records_success         bigint        NOT NULL DEFAULT 0,
    records_partial         bigint        NOT NULL DEFAULT 0,
    records_failed          bigint        NOT NULL DEFAULT 0,
    records_quarantined     bigint        NOT NULL DEFAULT 0,
    records_skipped         bigint        NOT NULL DEFAULT 0,
    -- row counts in the targets (all targets together; per target in reconciliation)
    rows_inserted           bigint        NOT NULL DEFAULT 0,
    rows_updated            bigint        NOT NULL DEFAULT 0,
    rows_unchanged          bigint        NOT NULL DEFAULT 0,
    rows_deleted            bigint        NOT NULL DEFAULT 0,
    -- write-back to the previous layer
    writeback_sent          bigint        NOT NULL DEFAULT 0,
    writeback_matched       bigint        NOT NULL DEFAULT 0,
    writeback_stale         bigint        NOT NULL DEFAULT 0,
    -- where it ran (for debugging)
    spark_app_id            varchar(100),
    spark_ui_url            varchar(500),
    airflow_dag_id          varchar(250),
    airflow_run_id          varchar(250),
    airflow_task_try        integer,
    created_at              timestamptz   NOT NULL DEFAULT now(),
    updated_at              timestamptz   NOT NULL DEFAULT now(),
    CONSTRAINT ck_run_counts CHECK (records_read = records_success + records_partial + records_failed
                                                   + records_quarantined + records_skipped
                                    OR status IN ('QUEUED','RUNNING','FAILED','CANCELLED','SKIPPED'))
);
CREATE INDEX ix_run_job_started   ON etl_ctl.job_run_audit (job_name, started_at DESC);
CREATE INDEX ix_run_status        ON etl_ctl.job_run_audit (status) WHERE status IN ('QUEUED','RUNNING','FAILED','PARTIAL');
CREATE INDEX ix_run_parent        ON etl_ctl.job_run_audit (parent_run_id) WHERE parent_run_id IS NOT NULL;
CREATE INDEX ix_run_root          ON etl_ctl.job_run_audit (root_run_id)   WHERE root_run_id IS NOT NULL;
```

**How a run moves through statuses:**

```
 QUEUED ──► RUNNING ──► SUCCESS
   │          │    ├──► PARTIAL      (some records not OK, below thresholds)
   │          │    └──► FAILED       (infra error / threshold breached / reconciliation mismatch)
   │          └──────► CANCELLED     (stopped by a user or by Airflow)
   └─────────────────► SKIPPED       (lock held by another run, upstream failed with SKIP, job disabled)
```

**Size:** 100 jobs × 24 runs/day ≈ 2,400 rows/day ≈ 0.9 million rows/year. That is small for EPAS, so this table is **not** partitioned. Old rows are removed by the retention job (§8).

➕ **The CHECK on the counts** makes the reconciliation equation impossible to break silently: a finished SUCCESS/PARTIAL run whose counts don't add up cannot even be saved.

### 6.7 `job_step_audit`: timing of each step inside a run

> **In simple words:** a run has ~13 steps (read, split, DQ, merge, write-back …). This table records how long each step took and how many rows went in and out. When a job suddenly becomes slow, this shows **which step** became slow.

```sql
-- db/control/V8__job_step_audit.sql
CREATE TABLE etl_ctl.job_step_audit (
    run_id          varchar(100)  NOT NULL REFERENCES etl_ctl.job_run_audit (run_id) ON DELETE CASCADE,
    step_no         smallint      NOT NULL,                  -- 1, 2, 3 ... in execution order
    step_name       varchar(50)   NOT NULL,                  -- READ, PRE_FILTER, SPLIT, STANDARDIZE, DQ, DEDUPE,
                                                             -- ENRICH, CLASSIFY, STAGE, MERGE, QUARANTINE,
                                                             -- WRITEBACK, RECONCILE, SWAP ...
    target_table    varchar(200),                            -- for per-target steps
    status          varchar(10)   NOT NULL CHECK (status IN ('SUCCESS','FAILED','SKIPPED')),
    started_at      timestamptz   NOT NULL,
    ended_at        timestamptz,
    duration_ms     bigint,
    rows_in         bigint,
    rows_out        bigint,
    details         jsonb,                                   -- e.g. {"partitions":24,"spilledMb":120}
    PRIMARY KEY (run_id, step_no)
);
```

It is **optional per environment** (`audit.stepLevel: true/false`): on in prod and uat, off in dev if not needed.

### 6.8 `watermark` and `watermark_history`: the bookmarks

> **In simple words:** the bookmark that says "this Gold job has processed Silver changes up to number 58,201". The history table keeps **old bookmark values**, so that in an emergency you can move the bookmark **back** and re-process from an earlier point.

```sql
-- db/control/V9__watermark.sql
CREATE TABLE etl_ctl.watermark (
    job_name          varchar(100)  NOT NULL REFERENCES etl_ctl.job_registry (job_name),
    source_table      varchar(200)  NOT NULL,
    watermark_type    varchar(15)   NOT NULL CHECK (watermark_type IN ('CHANGE_SEQ','TIMESTAMP','PARTITION')),
    last_change_seq   bigint,                                -- CHANGE_SEQ (Silver → Gold, doc 04 §5.2)
    last_ts           timestamptz,                           -- TIMESTAMP (optional safety net, e.g. Bronze lastIngestedAt)
    last_partition    varchar(50),                           -- PARTITION (aggregate jobs consuming touched dates)
    run_id            varchar(100)  NOT NULL,                -- run that set it
    updated_at        timestamptz   NOT NULL DEFAULT now(),
    PRIMARY KEY (job_name, source_table)
);

-- ➕ previous values (written by the same statement that moves the watermark)
CREATE TABLE etl_ctl.watermark_history (
    job_name          varchar(100)  NOT NULL,
    source_table      varchar(200)  NOT NULL,
    old_change_seq    bigint,
    new_change_seq    bigint,
    old_ts            timestamptz,
    new_ts            timestamptz,
    run_id            varchar(100)  NOT NULL,
    changed_by        varchar(100)  NOT NULL DEFAULT 'job',  -- 'job' or a user id for manual resets
    reason            varchar(500),
    changed_at        timestamptz   NOT NULL DEFAULT now()
);
CREATE INDEX ix_wm_hist ON etl_ctl.watermark_history (job_name, source_table, changed_at DESC);
```

**Move the bookmark (inside the job's main transaction):**

```sql
WITH old AS (SELECT last_change_seq FROM etl_ctl.watermark WHERE job_name = :job AND source_table = :src FOR UPDATE),
upd AS (
    INSERT INTO etl_ctl.watermark (job_name, source_table, watermark_type, last_change_seq, run_id)
    VALUES (:job, :src, 'CHANGE_SEQ', :cutoffSeq, :runId)
    ON CONFLICT (job_name, source_table) DO UPDATE
       SET last_change_seq = EXCLUDED.last_change_seq, run_id = EXCLUDED.run_id, updated_at = now()
     WHERE etl_ctl.watermark.last_change_seq IS NULL
        OR etl_ctl.watermark.last_change_seq <= EXCLUDED.last_change_seq      -- ➕ a normal run never moves it backwards
    RETURNING last_change_seq)
INSERT INTO etl_ctl.watermark_history (job_name, source_table, old_change_seq, new_change_seq, run_id)
SELECT :job, :src, (SELECT last_change_seq FROM old), (SELECT last_change_seq FROM upd), :runId;
```

Moving a bookmark **back** is a deliberate operator action through the ops API (with `changed_by` and `reason`), never done by a normal run.

### 6.9 `record_error_log`: every record-level error, as history

> **In simple words:** the Bronze/Silver status columns show only the **latest** error of a record. This log keeps **every** error of **every** run, like a diary. It answers questions like "how often did this customer record fail last month?" or "which rule fails most?".
>
> **Difference from quarantine:** the log is a **diary** (append-only, nothing to do). Quarantine is a **to-do list** (records waiting for someone to fix them).

```sql
-- db/control/V10__record_error_log.sql  (high volume → partitioned by day, no FKs)
CREATE TABLE etl_ctl.record_error_log (
    error_id          bigint        GENERATED ALWAYS AS IDENTITY,
    logged_at         timestamptz   NOT NULL DEFAULT now(),
    run_id            varchar(100)  NOT NULL,
    job_name          varchar(100)  NOT NULL,
    layer             varchar(10)   NOT NULL CHECK (layer IN ('INGESTION','B2S','S2G')),
    source_ref        jsonb         NOT NULL,       -- {"service":"svc-order","collection":"orders","id":"65f1…","version":7432…}
    target_table      varchar(200),
    rule_id           varchar(50),                  -- DQ rule id (SO-001) if from a rule
    column_name       varchar(100),
    error_code        varchar(50)   NOT NULL,
    severity          varchar(12)   NOT NULL,
    outcome           varchar(15)   NOT NULL CHECK (outcome IN ('WARN','FAILED','QUARANTINED','SKIPPED')),
    message           varchar(2000),                -- no field values (PII safety)
    PRIMARY KEY (logged_at, error_id)
) PARTITION BY RANGE (logged_at);

CREATE TABLE etl_ctl.record_error_log_2026_10_30 PARTITION OF etl_ctl.record_error_log
    FOR VALUES FROM ('2026-10-30') TO ('2026-10-31');
-- daily partitions are created ahead by the maintenance DAG; old ones are dropped (retention §8)
CREATE INDEX ix_rel_run   ON etl_ctl.record_error_log (run_id);
CREATE INDEX ix_rel_code  ON etl_ctl.record_error_log (error_code, logged_at);
```

➕ **Flood protection:** if a bad deploy makes 5 million records fail, logging all of them would hurt the database. Per run and rule, only the first **N** detailed rows are logged (`audit.maxErrorRowsPerRule`, e.g. 10,000). The **full counts** are always in `dq_results`, so nothing is hidden, only the detail is sampled. `WARN` rows are logged only if `audit.logWarnings: true`.

### 6.10 `quarantine_record` + `quarantine_status_history`: the repair shelf

> **In simple words:** a quarantined record is like an item put on a **"repair" shelf** with a tag: what is wrong, where it came from, and its current repair status. People (or automatic rules) move it through the steps until it is repaired and re-processed, or rejected.

**One generic table or one per entity?**

| Option | Pros | Cons | Verdict |
|---|---|---|---|
| **One generic table** with a source pointer + `jsonb` details | One ops UI screen for everything; one workflow; adding a new entity needs nothing | Row data is not typed columns (it's JSON) | ✅ **Chosen** |
| One table per entity (`quarantine_order`, `quarantine_customer` …) | Typed columns, easy SQL per entity | 100+ jobs → 100+ tables, 100+ UI screens, schema changes everywhere | ❌ Does not scale |

The record's **real data stays in its source layer** (Bronze document or Silver row). The quarantine row stores a **pointer** to it, plus an optional **masked** snapshot to help the person fixing it.

**Status lifecycle:**

```
                        ┌───────────────────────────────────────────────────────────────┐
                        │ (new source version arrives → job marks it automatically)     │
                        ▼                                                               │
 ┌─────────────┐  person fixes  ┌───────┐  approve (prod: 2nd person ➕) ┌──────────────────────┐
 │ QUARANTINED │ ─────────────► │ FIXED │ ─────────────────────────────► │ READY_TO_REPROCESS   │
 └──────┬──────┘                └───┬───┘                                └──────────┬───────────┘
        │                           │ fix was wrong                                  │ REPROCESS_QUARANTINE run
        │ not worth fixing          └──────────► back to QUARANTINED                  ▼
        ▼                                                                 ┌──────────────────────┐
 ┌─────────────┐                                                          │     REPROCESSED      │ ✔ done
 │  REJECTED   │ ✔ done (reason required)                                 └──────────────────────┘
 └─────────────┘                                                            (if it fails again → a NEW
                                                                             quarantine record is created,
 ┌─────────────┐                                                             linked via previous_quarantine_id)
 │ SUPERSEDED  │ ✔ done: a newer source version replaced this record (doc 03 §13)
 └─────────────┘
```

| Status | Meaning | Set by |
|---|---|---|
| `QUARANTINED` | Waiting for a fix | Job |
| `FIXED` | Someone says it is fixed (in the source, via a correction, or by a code/rule change) | Ops API (person) |
| `READY_TO_REPROCESS` | Approved; the next `REPROCESS_QUARANTINE` run will pick it up | Ops API (approver, or automatically in non-prod) |
| `REPROCESSED` | Re-processed successfully | Job |
| `REJECTED` | Will not be fixed (e.g. test data, garbage), with a reason | Ops API |
| `SUPERSEDED` | A newer source version arrived, this one is outdated | Job |

**How can a record be "fixed"?** (`fix_type`)

| fix_type | Meaning |
|---|---|
| `SOURCE_FIX` | Corrected in the source microservice. Best option: it flows through CDC automatically (and usually ends as `SUPERSEDED`) |
| `CORRECTION_OVERRIDE` ➕ | A person enters corrected values in the ops UI (stored in `correction_patch`); the reprocess run applies them on top of the source data. Use rarely, with approval, because Silver then differs from the source |
| `CODE_FIX` | The bug was in our transformation code; after the deploy, simply reprocess |
| `RULE_CHANGE` | The DQ rule was too strict and was changed in Git; reprocess |

```sql
-- db/control/V11__quarantine.sql
CREATE TABLE etl_ctl.quarantine_record (
    quarantine_id          uuid          PRIMARY KEY DEFAULT gen_random_uuid(),
    layer                  varchar(10)   NOT NULL CHECK (layer IN ('B2S','S2G')),
    job_name               varchar(100)  NOT NULL REFERENCES etl_ctl.job_registry (job_name),
    created_run_id         varchar(100)  NOT NULL REFERENCES etl_ctl.job_run_audit (run_id),
    target_table           varchar(200),
    -- pointer to the real data in the previous layer
    source_store           varchar(10)   NOT NULL CHECK (source_store IN ('MONGO','EPAS')),
    source_service         varchar(50),
    source_table           varchar(200)  NOT NULL,             -- "bronze_svc_order.orders" or "silver_order.sales_order"
    source_key             jsonb         NOT NULL,             -- {"_id":"65f1…"} or {"order_id":"…","line_id":1}
    source_version         bigint        NOT NULL,             -- Bronze sourceVersion or Silver _etl_change_seq
    -- what is wrong
    error_code             varchar(50)   NOT NULL REFERENCES etl_ctl.error_code_catalog (error_code),
    errors                 jsonb         NOT NULL,             -- [{code, column, rule_id, severity, message}]
    row_snapshot           jsonb,                              -- masked/encrypted copy of the failing row (optional)
    -- workflow
    status                 varchar(20)   NOT NULL DEFAULT 'QUARANTINED' CHECK (status IN
                             ('QUARANTINED','FIXED','READY_TO_REPROCESS','REPROCESSED','REJECTED','SUPERSEDED')),
    fix_type               varchar(25)   CHECK (fix_type IN ('SOURCE_FIX','CORRECTION_OVERRIDE','CODE_FIX','RULE_CHANGE')),
    correction_patch       jsonb,                              -- only for CORRECTION_OVERRIDE
    assigned_to            varchar(100),
    fixed_by               varchar(100),
    fixed_at               timestamptz,
    approved_by            varchar(100),                       -- ➕ four-eyes principle in prod
    approved_at            timestamptz,
    reject_reason          varchar(1000),
    reprocess_run_id       varchar(100)  REFERENCES etl_ctl.job_run_audit (run_id),
    reprocessed_at         timestamptz,
    previous_quarantine_id uuid          REFERENCES etl_ctl.quarantine_record (quarantine_id),
    legal_hold             boolean       NOT NULL DEFAULT false,  -- ➕ never purge while true
    created_at             timestamptz   NOT NULL DEFAULT now(),
    updated_at             timestamptz   NOT NULL DEFAULT now(),
    CONSTRAINT ck_reject_reason CHECK (status <> 'REJECTED' OR reject_reason IS NOT NULL),
    CONSTRAINT ck_override_patch CHECK (fix_type IS DISTINCT FROM 'CORRECTION_OVERRIDE' OR correction_patch IS NOT NULL)
);
-- at most ONE open quarantine record per source record and job
CREATE UNIQUE INDEX ux_quarantine_open ON etl_ctl.quarantine_record (job_name, source_table, source_key)
    WHERE status IN ('QUARANTINED','FIXED','READY_TO_REPROCESS');
CREATE INDEX ix_quarantine_status ON etl_ctl.quarantine_record (status, job_name);
CREATE INDEX ix_quarantine_code   ON etl_ctl.quarantine_record (error_code) WHERE status = 'QUARANTINED';

-- ➕ every status change, for audit
CREATE TABLE etl_ctl.quarantine_status_history (
    quarantine_id   uuid          NOT NULL REFERENCES etl_ctl.quarantine_record (quarantine_id) ON DELETE CASCADE,
    changed_at      timestamptz   NOT NULL DEFAULT now(),
    old_status      varchar(20),
    new_status      varchar(20)   NOT NULL,
    changed_by      varchar(100)  NOT NULL,
    note            varchar(1000),
    run_id          varchar(100),
    PRIMARY KEY (quarantine_id, changed_at)
);
```

**Allowed status changes are enforced in one place** (a SQL function used by the ops API and the jobs):

```sql
CREATE OR REPLACE FUNCTION etl_ctl.quarantine_transition(p_id uuid, p_new varchar, p_by varchar, p_note varchar DEFAULT NULL,
                                                         p_run varchar DEFAULT NULL)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE v_old varchar;
BEGIN
    SELECT status INTO v_old FROM etl_ctl.quarantine_record WHERE quarantine_id = p_id FOR UPDATE;
    IF NOT ( (v_old = 'QUARANTINED'        AND p_new IN ('FIXED','REJECTED','SUPERSEDED'))
          OR (v_old = 'FIXED'              AND p_new IN ('READY_TO_REPROCESS','QUARANTINED','SUPERSEDED'))
          OR (v_old = 'READY_TO_REPROCESS' AND p_new IN ('REPROCESSED','QUARANTINED','SUPERSEDED')) ) THEN
        RAISE EXCEPTION 'Illegal quarantine transition % -> % for %', v_old, p_new, p_id;
    END IF;
    UPDATE etl_ctl.quarantine_record
       SET status = p_new, updated_at = now(),
           fixed_by = CASE WHEN p_new = 'FIXED' THEN p_by ELSE fixed_by END,
           fixed_at = CASE WHEN p_new = 'FIXED' THEN now() ELSE fixed_at END,
           approved_by = CASE WHEN p_new = 'READY_TO_REPROCESS' THEN p_by ELSE approved_by END,
           approved_at = CASE WHEN p_new = 'READY_TO_REPROCESS' THEN now() ELSE approved_at END,
           reprocess_run_id = CASE WHEN p_new = 'REPROCESSED' THEN p_run ELSE reprocess_run_id END,
           reprocessed_at = CASE WHEN p_new = 'REPROCESSED' THEN now() ELSE reprocessed_at END
     WHERE quarantine_id = p_id;
    INSERT INTO etl_ctl.quarantine_status_history (quarantine_id, old_status, new_status, changed_by, note, run_id)
    VALUES (p_id, v_old, p_new, p_by, p_note, p_run);
END $$;
```

The "four-eyes" rule (the person who fixed it cannot also approve it in prod) is checked in the ops API (Prompt 11), using `fixed_by` ≠ `approved_by`.

### 6.11 `dq_results`: the quality inspection report

> **In simple words:** for every run, every table and every rule: **how many rows were checked and how many failed**. It is stored even when the run was rolled back by the circuit breaker, because that is exactly when you need it most.

```sql
-- db/control/V12__dq_results.sql
CREATE TABLE etl_ctl.dq_results (
    run_id          varchar(100)  NOT NULL REFERENCES etl_ctl.job_run_audit (run_id) ON DELETE CASCADE,
    job_name        varchar(100)  NOT NULL,
    target_table    varchar(200)  NOT NULL,
    rule_id         varchar(50)   NOT NULL,             -- SO-001 (from the YAML rule set)
    rule_set        varchar(100),                       -- order.sales_order
    rule_type       varchar(30)   NOT NULL,             -- not_null, range, referential ...
    column_name     varchar(100),
    severity        varchar(12)   NOT NULL,
    error_code      varchar(50)   REFERENCES etl_ctl.error_code_catalog (error_code),
    rows_checked    bigint        NOT NULL,
    rows_failed     bigint        NOT NULL,
    failed_pct      numeric(7,4)  GENERATED ALWAYS AS
                      (CASE WHEN rows_checked = 0 THEN 0 ELSE rows_failed * 100.0 / rows_checked END) STORED,
    result          varchar(5)    NOT NULL CHECK (result IN ('PASS','WARN','FAIL')),
    is_dry_run      boolean       NOT NULL DEFAULT false,
    measured_at     timestamptz   NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, target_table, rule_id)
);
CREATE INDEX ix_dq_rule_trend ON etl_ctl.dq_results (job_name, rule_id, measured_at);
```

The `ix_dq_rule_trend` index powers a trend chart: "rule SO-002 failed 0.1% for months, and suddenly 4% since yesterday" means a source problem started yesterday.

### 6.12 `reconciliation`: the tally sheet

> **In simple words:** after every run: "did everything that went in come out somewhere?" and "do the totals match?". One row per check, so new kinds of checks can be added without changing the table.

```sql
-- db/control/V13__reconciliation.sql
CREATE TABLE etl_ctl.reconciliation (
    run_id          varchar(100)  NOT NULL REFERENCES etl_ctl.job_run_audit (run_id) ON DELETE CASCADE,
    job_name        varchar(100)  NOT NULL,
    target_table    varchar(200)  NOT NULL,             -- '*' for job-level checks
    check_name      varchar(100)  NOT NULL,             -- RECORD_COUNTS, MERGE_COUNTS, WRITEBACK, SUM_NET_AMT,
                                                        -- NULL_SK, UNKNOWN_SK_SHARE, AGG_VS_FACT, SCD_INTEGRITY ...
    check_type      varchar(10)   NOT NULL CHECK (check_type IN ('COUNT','SUM','RATIO','INTEGRITY')),
    expected_value  numeric(38,6),                      -- source side (e.g. Bronze read, Σ Silver amount)
    actual_value    numeric(38,6),                      -- target side
    difference      numeric(38,6) GENERATED ALWAYS AS (actual_value - expected_value) STORED,
    tolerance       numeric(38,6) NOT NULL DEFAULT 0,
    result          varchar(15)   NOT NULL CHECK (result IN ('MATCH','MISMATCH_WARN','MISMATCH_FAIL')),
    details         jsonb,                              -- breakdown, e.g. {"success":980,"failed":12,"skipped":8}
    checked_at      timestamptz   NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, target_table, check_name)
);
CREATE INDEX ix_recon_mismatch ON etl_ctl.reconciliation (checked_at) WHERE result <> 'MATCH';
```

Example rows for one Bronze → Silver run:

| target_table | check_name | expected | actual | result | details |
|---|---|---|---|---|---|
| * | RECORD_COUNTS | 1000 (read) | 1000 (sum of outcomes) | MATCH | `{"success":975,"partial":3,"failed":10,"quarantined":7,"skipped":5}` |
| silver_order.sales_order | MERGE_COUNTS | 978 (staged) | 978 | MATCH | `{"inserted":120,"updated":800,"unchanged":58}` |
| * | WRITEBACK | 1000 (sent) | 1000 (matched + stale) | MATCH | `{"matched":996,"stale":4}` |
| silver_order.sales_order | SUM_TOTAL_AMOUNT | 1,452,300.00 | 1,452,300.00 | MATCH | — |

### 6.13 `gold_record_status` and `gold_touched_partition` (from doc 04)

```sql
-- db/control/V14__gold_sync.sql
CREATE TABLE etl_ctl.gold_record_status (
    job_name          varchar(100)  NOT NULL REFERENCES etl_ctl.job_registry (job_name),
    source_table      varchar(200)  NOT NULL,
    row_key           jsonb         NOT NULL,               -- {"order_id":"65f1…","line_id":1}
    change_seq        bigint        NOT NULL,
    status            varchar(15)   NOT NULL CHECK (status IN ('FAILED','QUARANTINED','PARTIAL','READY_TO_REPROCESS')),
    error_code        varchar(50)   NOT NULL REFERENCES etl_ctl.error_code_catalog (error_code),
    error_message     varchar(2000),
    retry_count       integer       NOT NULL DEFAULT 0,
    first_failed_at   timestamptz   NOT NULL DEFAULT now(),
    quarantine_ref    uuid          REFERENCES etl_ctl.quarantine_record (quarantine_id),
    run_id            varchar(100)  NOT NULL,
    updated_at        timestamptz   NOT NULL DEFAULT now(),
    PRIMARY KEY (job_name, source_table, row_key)
);
CREATE INDEX ix_grs_retry ON etl_ctl.gold_record_status (job_name, status, retry_count);

-- "dirty" dates for aggregate jobs (doc 04 §10.1)
CREATE TABLE etl_ctl.gold_touched_partition (
    target_table      varchar(200)  NOT NULL,              -- the fact table, e.g. gold_sales.fact_sales_line
    partition_value   varchar(50)   NOT NULL,              -- e.g. '20261029' (date key)
    run_id            varchar(100)  NOT NULL,
    touch_seq         bigint        GENERATED ALWAYS AS IDENTITY,   -- aggregate jobs bookmark on this
    touched_at        timestamptz   NOT NULL DEFAULT now(),
    PRIMARY KEY (target_table, partition_value, run_id)
);
CREATE INDEX ix_gtp_seq ON etl_ctl.gold_touched_partition (target_table, touch_seq);
```

The aggregate job keeps a `PARTITION`-type watermark on `touch_seq`, reads the distinct `partition_value`s above it, recomputes them, and moves the watermark (all in one transaction). Rows older than 30 days are removed by retention.

### 6.14 Ingestion tables (from doc 02)

```sql
-- db/control/V15__ingestion.sql
-- One row per message sent to the DLQ (Dead Letter Queue) topic. The payload itself stays in Kafka.
CREATE TABLE etl_ctl.ingestion_dlq_index (
    dlq_topic         varchar(200)  NOT NULL DEFAULT 'dlq.ingestion',
    dlq_partition     integer       NOT NULL,
    dlq_offset        bigint        NOT NULL,
    origin_topic      varchar(200)  NOT NULL,
    origin_partition  integer       NOT NULL,
    origin_offset     bigint        NOT NULL,
    source_service    varchar(50),
    source_collection varchar(100),
    source_id         varchar(100),
    error_code        varchar(50)   NOT NULL REFERENCES etl_ctl.error_code_catalog (error_code),
    error_message     varchar(2000),
    exception_fingerprint char(64),                       -- hash of the stack trace: groups identical bugs
    first_seen_at     timestamptz   NOT NULL DEFAULT now(),
    status            varchar(10)   NOT NULL DEFAULT 'NEW' CHECK (status IN ('NEW','REPLAYED','IGNORED')),
    replay_ticket     varchar(50),
    resolved_by       varchar(100),
    resolved_at       timestamptz,
    ignore_reason     varchar(1000),
    PRIMARY KEY (dlq_topic, dlq_partition, dlq_offset)
);
CREATE INDEX ix_dlq_open ON etl_ctl.ingestion_dlq_index (error_code, source_collection) WHERE status = 'NEW';

-- One row per replay run (doc 02 §15.2)
CREATE TABLE etl_ctl.ingestion_replay_audit (
    replay_id         uuid          PRIMARY KEY DEFAULT gen_random_uuid(),
    ticket            varchar(50)   NOT NULL,
    requested_by      varchar(100)  NOT NULL,
    replay_type       varchar(15)   NOT NULL CHECK (replay_type IN ('KAFKA_RANGE','DLQ','RESNAPSHOT')),
    from_ts           timestamptz,
    to_ts             timestamptz,
    filter            jsonb,                               -- {"services":["svc-order"],"collections":["orders"]}
    force_reapply     boolean       NOT NULL DEFAULT false,
    consumer_group    varchar(200),
    status            varchar(10)   NOT NULL CHECK (status IN ('RUNNING','SUCCESS','FAILED','CANCELLED')),
    events_read       bigint        NOT NULL DEFAULT 0,
    events_written    bigint        NOT NULL DEFAULT 0,
    events_stale      bigint        NOT NULL DEFAULT 0,
    events_dlq        bigint        NOT NULL DEFAULT 0,
    started_at        timestamptz   NOT NULL DEFAULT now(),
    ended_at          timestamptz
);
```

### 6.15 `purge_request` ➕: privacy deletes (GDPR / DPDP)

> **In simple words:** when a customer asks "delete my personal data", we must remove or anonymize it in **every** layer. This table is the request register, so we can prove later what was done and when. (Full process in Prompt 11.)

```sql
-- db/control/V16__purge_request.sql
CREATE TABLE etl_ctl.purge_request (
    purge_id          uuid          PRIMARY KEY DEFAULT gen_random_uuid(),
    request_ref       varchar(100)  NOT NULL,             -- legal/CRM ticket
    subject_type      varchar(30)   NOT NULL,             -- CUSTOMER, EMPLOYEE ...
    source_service    varchar(50)   NOT NULL,
    source_id         varchar(100)  NOT NULL,
    method            varchar(15)   NOT NULL CHECK (method IN ('HARD_DELETE','ANONYMIZE')),
    status            varchar(15)   NOT NULL DEFAULT 'REQUESTED' CHECK (status IN
                        ('REQUESTED','APPROVED','IN_PROGRESS','DONE','FAILED','REJECTED')),
    layers_done       jsonb         NOT NULL DEFAULT '{}', -- {"bronze":true,"silver":true,"gold":false,"quarantine":true,"dlq":true}
    requested_by      varchar(100)  NOT NULL,
    approved_by       varchar(100),
    requested_at      timestamptz   NOT NULL DEFAULT now(),
    completed_at      timestamptz,
    notes             varchar(2000)
);
```

### 6.16 `retention_policy` ➕: retention rules as data

> **In simple words:** instead of hard-coding "delete logs older than 90 days" in scripts, the rules are rows in a table. One nightly maintenance job reads them and applies them. Changing a rule = changing one row (via a migration in Git).

```sql
-- db/control/V17__retention_policy.sql
CREATE TABLE etl_ctl.retention_policy (
    object_name       varchar(200)  PRIMARY KEY,          -- table / collection / topic name or pattern
    store             varchar(10)   NOT NULL CHECK (store IN ('EPAS','MONGO','KAFKA')),
    time_column       varchar(100),                       -- column used to decide the age
    keep_days         integer       NOT NULL,
    action            varchar(15)   NOT NULL CHECK (action IN ('DELETE','DROP_PARTITION','ARCHIVE_THEN_DROP','TRUNCATE','KAFKA_RETENTION')),
    condition_sql     varchar(1000),                      -- extra filter, e.g. "status IN ('REPROCESSED','REJECTED','SUPERSEDED')"
    respects_legal_hold boolean     NOT NULL DEFAULT true,
    enabled           boolean       NOT NULL DEFAULT true,
    notes             varchar(500)
);
```

---

## 7. How the Job Registry Is Kept in Sync with Git

> **In simple words:** developers change **YAML files in Git**. At every deploy, a small program reads all the YAML files, checks them, and updates the registry tables. Nobody edits `job_registry` by hand.

```
 Git: pipelines/**/*.yml + config/environments/<env>/job-overrides.yml
        │
        ▼  (CI/CD deploy step, per environment)
 ┌───────────────────────────────────────────────────────────────────────────────┐
 │ JobRegistrySync                                                               │
 │  1. load + merge YAML (defaults → env → job)       (etl-config)               │
 │  2. validate against the JSON Schema                → fail deploy on error    │
 │  3. checks: single owner per target table, no dependency cycles,              │
 │     every error_code used exists in the catalog     → fail deploy on error    │
 │  4. in ONE transaction:                                                        │
 │     upsert job_registry, job_source, job_target, job_dependency               │
 │     insert job_config_version if the config hash is new                       │
 │     jobs missing from Git → enabled = false (never deleted)                   │
 └───────────────────────────────────────────────────────────────────────────────┘
        │
        ▼
 Airflow DAG factory reads the same YAML (or job_registry) → DAGs (Prompt 09)
```

```java
// etl-control: registry/JobRegistrySync.java (sketch)
public final class JobRegistrySync {

    public SyncReport sync(List<ResolvedJobConfig> configs, String env, String gitCommit) {
        validator.validateAll(configs);                         // JSON Schema + references
        graphChecks.assertNoDependencyCycles(configs);
        graphChecks.assertSingleOwnerPerTarget(configs);
        errorCodes.assertAllKnown(configs);

        return tx.inTransaction(conn -> {
            Set<String> seen = new HashSet<>();
            for (ResolvedJobConfig c : configs) {
                registryDao.upsert(conn, c, gitCommit);          // job_registry
                configVersionDao.insertIfNew(conn, c, gitCommit); // job_config_version
                sourceTargetDao.replace(conn, c);                 // job_source + job_target
                dependencyDao.replace(conn, c);                   // job_dependency
                seen.add(c.jobName());
            }
            int disabled = registryDao.disableMissing(conn, seen);
            return new SyncReport(configs.size(), disabled);
        });
    }
}
```

---

## 8. Data Retention and Archival

> **In simple words**
> Data cannot be kept forever: disks fill up, databases slow down, and privacy law says personal data must not be kept longer than needed. But some data **must** be kept for years (tax, audits). A **retention policy** is the rule "keep X for Y days, then delete or archive it".
> **Archive** means: move old data to cheap storage (files), out of the database, so it can still be restored if someone needs it.

### 8.1 Retention table (proposed defaults; confirm with legal/finance)

| Layer / object | Keep | After that | Why |
|---|---|---|---|
| **Kafka** `cdc.source.events` | 14 days (prod) | Kafka deletes automatically | Replay window (doc 02) |
| Kafka `cdc.source.snapshot` | 7 days | auto | Only for snapshot runs |
| Kafka `dlq.ingestion` | 30 days | auto | Time to fix and replay |
| **Bronze** current documents | **As long as the source exists** | — | Bronze is the long-term replay source |
| Bronze soft-deleted documents, synced to Silver | 365 days after `deletedAt` | Hard delete (per route setting) | Storage; Silver/Gold keep what the business needs |
| Bronze `__history` collections | 2 years | **Archive** (Parquet files) then delete | Needed for SCD2 rebuilds; old history rarely needed |
| **Silver** business tables | **As long as the business needs** (no automatic purge) | — | The clean truth |
| Silver soft-deleted rows | 2 years after `_deleted_at` | Archive then delete | Audit trail, then storage |
| Silver `_hist` (SCD2) | Same as the Gold facts that use them | Archive | History must outlive the facts that refer to it |
| **Staging** (`stg_*`) | Until the next run; idle tables truncated after 7 days | TRUNCATE | Temporary, may contain PII |
| **Gold** facts | Per legal/business need, e.g. **8 years** for finance/tax data ➕ (Indian GST records must be kept roughly 6 years from the annual return due date, so ~8 FY is a safe default; confirm with finance) | Detach old monthly partitions → **archive** → drop | Fast queries on recent data, legal compliance |
| Gold dimensions | **Never purged** (only anonymized for privacy requests) | — | Facts point to them |
| Gold aggregates | Same as the facts they summarize | Drop with the fact partitions | |
| `job_run_audit` | 2 years | DELETE | Run history for trends and audits |
| `job_step_audit` | 6 months | DELETE (cascade) | Only for performance debugging |
| `record_error_log` | 90 days | DROP daily partitions | High volume; counts stay in `dq_results` |
| `dq_results` | 2 years | DELETE | Quality trends |
| `reconciliation` | 2 years | DELETE | Audit evidence |
| `quarantine_record` (open) | **Until resolved** | — | It's a to-do list |
| `quarantine_record` (closed: REPROCESSED / REJECTED / SUPERSEDED) | 1 year after closing | DELETE (+ history) | Snapshots may contain masked PII |
| `gold_record_status` | Self-cleaning (only open items exist) | — | |
| `gold_touched_partition` | 30 days | DELETE | Consumed by aggregate jobs within hours |
| `watermark` | Current values forever | — | |
| `watermark_history` | 180 days | DELETE | Emergency resets look back weeks, not years |
| `ingestion_dlq_index` | 90 days after resolution (the Kafka payload is gone after 30 days anyway) | DELETE | |
| `ingestion_replay_audit`, `purge_request` | 5 years | DELETE | Legal proof of actions taken |
| `job_registry` | Forever | — | |
| `job_config_version` | 2 years (must cover `job_run_audit` retention) | DELETE | Explains old runs |
| `error_code_catalog`, `retention_policy` | Forever | — | |
| Airflow logs / metadata | Per Airflow cleanup settings (e.g. 90 days) | Airflow `db clean` | Owned by Airflow (Prompt 09) |

### 8.2 How retention is executed

```
 Airflow DAG: etl_maintenance_retention (daily, 02:00 UTC)
   │
   ├─ for each enabled row in etl_ctl.retention_policy:
   │     DELETE           → DELETE ... WHERE <time_column> < now() - keep_days AND <condition_sql>
   │                         AND NOT legal_hold   (in batches of 10,000 to avoid long locks)
   │     DROP_PARTITION   → DETACH + DROP partitions older than keep_days
   │     ARCHIVE_THEN_DROP→ Spark archive job: export the partition to Parquet files in object storage
   │                         (S3 / MinIO / Azure Blob), verify row counts, register it in OpenMetadata,
   │                         THEN detach + drop
   │     TRUNCATE         → idle staging tables
   │     KAFKA_RETENTION  → only verified (topic retention is set in topics.yml)
   │
   ├─ create future partitions (record_error_log daily, Gold facts monthly)  ← same DAG
   └─ every action is logged as a MAINTENANCE run in job_run_audit
```

➕ **Archive format:** Parquet (a compressed, column-based file format that Spark, Trino and many tools can read) in object storage, one folder per table and month, e.g. `archive/gold_sales/fact_sales_line/2018-04/`. Restoring = reading the files with Spark and inserting them back. If your organization has no object storage, a compressed `pg_dump` of the detached partition is the fallback.

➕ **Legal hold:** a record or table under investigation or a legal case must not be deleted. The `legal_hold` flag (on quarantine records) and `respects_legal_hold` (on policies) stop the purge.

### 8.3 Seed for the retention policy table

```sql
-- db/control/R__retention_policy_seed.sql (excerpt)
INSERT INTO etl_ctl.retention_policy (object_name, store, time_column, keep_days, action, condition_sql, notes) VALUES
 ('etl_ctl.record_error_log',      'EPAS', 'logged_at',   90,  'DROP_PARTITION', NULL, 'daily partitions'),
 ('etl_ctl.job_step_audit',        'EPAS', 'started_at',  180, 'DELETE',         NULL, NULL),
 ('etl_ctl.job_run_audit',         'EPAS', 'started_at',  730, 'DELETE',         'status NOT IN (''QUEUED'',''RUNNING'')', NULL),
 ('etl_ctl.dq_results',            'EPAS', 'measured_at', 730, 'DELETE',         NULL, NULL),
 ('etl_ctl.reconciliation',        'EPAS', 'checked_at',  730, 'DELETE',         NULL, NULL),
 ('etl_ctl.quarantine_record',     'EPAS', 'updated_at',  365, 'DELETE',
      'status IN (''REPROCESSED'',''REJECTED'',''SUPERSEDED'')', 'closed items only'),
 ('etl_ctl.gold_touched_partition','EPAS', 'touched_at',  30,  'DELETE',         NULL, NULL),
 ('etl_ctl.watermark_history',     'EPAS', 'changed_at',  180, 'DELETE',         NULL, NULL),
 ('stg_*',                         'EPAS', NULL,          7,   'TRUNCATE',       'job idle', 'idle staging'),
 ('gold_sales.fact_sales_line',    'EPAS', 'order_date_key', 2920, 'ARCHIVE_THEN_DROP', NULL, '~8 years, confirm with finance'),
 ('bronze_*.*__history',           'MONGO','_etl.sourceEventTs', 730, 'ARCHIVE_THEN_DROP', NULL, NULL)
ON CONFLICT (object_name) DO UPDATE SET keep_days = EXCLUDED.keep_days, action = EXCLUDED.action,
    condition_sql = EXCLUDED.condition_sql, notes = EXCLUDED.notes;
```

---

## 9. Handy Views for Operations ➕

> **In simple words:** ready-made "saved queries" for the ops UI and Grafana dashboards.

```sql
-- Last run per job (the "is everything green?" screen)
CREATE VIEW etl_ctl.v_job_last_run AS
SELECT DISTINCT ON (r.job_name)
       r.job_name, j.domain, j.owner_team, r.run_id, r.run_mode, r.trigger_type, r.status, r.error_code,
       r.started_at, r.ended_at, r.duration_ms, r.records_read, r.records_failed, r.records_quarantined
FROM etl_ctl.job_run_audit r
JOIN etl_ctl.job_registry j USING (job_name)
WHERE NOT r.is_dry_run
ORDER BY r.job_name, r.started_at DESC;

-- Open quarantine items per job and error code (the "repair shelf" screen)
CREATE VIEW etl_ctl.v_quarantine_open AS
SELECT job_name, error_code, status, count(*) AS items, min(created_at) AS oldest
FROM etl_ctl.quarantine_record
WHERE status IN ('QUARANTINED','FIXED','READY_TO_REPROCESS')
GROUP BY job_name, error_code, status;

-- Runs that broke their SLA (Service Level Agreement)
CREATE VIEW etl_ctl.v_sla_breaches AS
SELECT r.run_id, r.job_name, j.sla_minutes, r.duration_ms / 60000.0 AS duration_min
FROM etl_ctl.job_run_audit r JOIN etl_ctl.job_registry j USING (job_name)
WHERE j.sla_minutes IS NOT NULL AND r.duration_ms > j.sla_minutes * 60000;
```

---

## 10. Folder Layout for These Migrations

```
db/control/
├── V1__schema.sql                     # CREATE SCHEMA etl_ctl; roles and grants
├── V2__error_code_catalog.sql
├── V3__job_registry.sql               # + job_config_version
├── V4__job_source_target.sql
├── V5__job_dependency.sql
├── V6__job_lock.sql
├── V7__job_run_audit.sql
├── V8__job_step_audit.sql
├── V9__watermark.sql                  # + watermark_history
├── V10__record_error_log.sql          # partitioned
├── V11__quarantine.sql                # + history + transition function
├── V12__dq_results.sql
├── V13__reconciliation.sql
├── V14__gold_sync.sql                 # gold_record_status + gold_touched_partition
├── V15__ingestion.sql                 # dlq index + replay audit
├── V16__purge_request.sql
├── V17__retention_policy.sql
├── V18__ops_views.sql
├── V19__run_request.sql               # doc 07 §10.2: manual run requests + approvals
├── V20__lifecycle_additions.sql       # doc 07 §12: next_retry_at, SLA columns, has_record_errors
├── V21__pii_access_log.sql            # doc 11 §5.5: audited PII reveals
├── R__error_code_catalog_seed.sql     # repeatable: re-applied when the file changes
└── R__retention_policy_seed.sql
```

(`V<n>__` = versioned migration, applied once in order. `R__` = repeatable migration, re-applied whenever its content changes. Both are Flyway conventions.)

---

## 11. ➕ Added by Architect (summary)

| # | Addition | Why |
|---|---|---|
| C1 | Control tables in the **same EPAS database** as Silver/Gold | One transaction for data + bookmarks + statuses |
| C2 | `run_mode` + `trigger_type` + `is_dry_run` as separate columns | They combine freely; one mixed list can't express "manual backfill" |
| C3 | `root_run_id`, `request_ticket`, `config_hash`, `code_version` in the run audit | Any run can be explained and reproduced |
| C4 | CHECK constraint on the record-count equation | Reconciliation can't be broken silently |
| C5 | `job_config_version` | Old runs stay explainable after configs change |
| C6 | `job_target` single-owner unique index | Enforces the single-writer rule from doc 04 in the database |
| C7 | Dependency cycle check + recursive "downstream" query | No deadlocked DAGs; cascade reruns know what to rerun |
| C8 | Lock with heartbeat + lease expiry; lost-lock check before commit | No stuck locks after crashes; two runs never both commit |
| C9 | `watermark_history` + "never backwards" rule for normal runs | Safe emergency resets, no accidental rewinds |
| C10 | `record_error_log` partitioned by day + per-rule row cap | A mass failure can't flood the database |
| C11 | One generic quarantine table + transition function + history + four-eyes approval + `CORRECTION_OVERRIDE` | One workflow for 100+ jobs, auditable fixes |
| C12 | Long-format reconciliation table | New checks need no schema change |
| C13 | Why not TEMP tables for staging; truncate at start, keep for debugging, idle cleanup | Correct with many Spark connections; debuggable; PII not left forever |
| C14 | `retention_policy` table + one maintenance DAG + legal hold + Parquet archive | Retention is visible, configurable and safe |
| C15 | `purge_request` register | Proof of privacy deletes |
| C16 | `varchar + CHECK` instead of ENUM types; `timestamptz` UTC everywhere | Easy to evolve, no time-zone bugs |
| C17 | Ops views (`v_job_last_run`, `v_quarantine_open`, `v_sla_breaches`) | Ready for the ops UI and Grafana |

---

## 12. Changes to Earlier Decisions

| Earlier | Now | Reason |
|---|---|---|
| Doc 03 §12.2 showed a wide reconciliation row (one column per count) | Counts live in `job_run_audit`; `reconciliation` is one row per check (long format) | Flexible for new checks |
| Run mode list mixed modes and triggers (`DEPENDENCY_TRIGGERED`, `CASCADE`, `DRY_RUN` as modes) | `run_mode` (what), `trigger_type` (why it started), `is_dry_run` (flag) | They combine freely |
| Doc 04 referred to `etl_ctl.job_source.active` | Now formally defined (§6.3), together with `job_target` | — |
| Doc 02 `ingestion_dlq_index` columns | Formalized (§6.14), plus `exception_fingerprint` and `ignore_reason` | — |

---

## 13. Key Decisions

1. **Location:** all control tables in schema `etl_ctl`, in the **same EPAS database** as Silver and Gold (one transaction for data + control). Airflow's metadata DB stays separate.
2. **Staging:** only for Spark → EPAS merge jobs (B→S, S2G dimension/fact); one `UNLOGGED` table per target in `stg_<schema>`; lifecycle = truncate at start → COPY → ANALYZE → merge in one transaction → keep until the next run → idle cleanup after 7 days; shadow-table swap only for full fact rebuilds. Not for Bronze, aggregates, snapshots or write-back.
3. **Registry:** `job_registry` (+ `job_config_version`, `job_source`, `job_target`, `job_dependency`) synced from Git YAML at every deploy, with validation, a single-owner check and a cycle check; removed jobs are disabled, never deleted.
4. **Runs:** `job_run_audit` with `run_mode`, `trigger_type`, `is_dry_run`, `parent_run_id`/`root_run_id`, params, config hash, code version, full counts (with a CHECK on the equation), Spark/Airflow ids; optional `job_step_audit`.
5. **Locking:** `job_lock` with a 10-minute lease and a 60-second heartbeat; acquire via `INSERT … ON CONFLICT … WHERE expired`; lost-lock check before commit.
6. **Bookmarks:** `watermark` (CHANGE_SEQ / TIMESTAMP / PARTITION) + `watermark_history`; normal runs never move a bookmark backwards.
7. **Errors:** `error_code_catalog` (seeded from Git, `retryable` flag drives FAILED vs QUARANTINED); `record_error_log` as a daily-partitioned diary with a per-rule cap.
8. **Quarantine:** one generic `quarantine_record` table with a source pointer, masked snapshot, lifecycle QUARANTINED → FIXED → READY_TO_REPROCESS → REPROCESSED / REJECTED / SUPERSEDED, enforced by `quarantine_transition()`, full history, four-eyes approval in prod, `fix_type` incl. `CORRECTION_OVERRIDE`.
9. **Quality and tally:** `dq_results` per run × table × rule (kept even on rollback); `reconciliation` one row per check.
10. **Gold/ingestion helpers:** `gold_record_status`, `gold_touched_partition`, `ingestion_dlq_index`, `ingestion_replay_audit`, `purge_request`.
11. **Retention:** rules in `retention_policy`, executed by one nightly maintenance DAG; Parquet archive to object storage before dropping old partitions; legal hold respected; defaults per the table in §8.1 (to be confirmed with legal/finance).
