# 12 — Final Consolidated Architecture

> This is the **single summary document** of the whole platform. It combines the decisions of docs [01](01-architecture-and-structure.md) to [11](11-production-readiness.md), resolves the conflicts between them, lists the remaining gaps and risks, and gives a phased roadmap.
> Written in **simple language**: every section starts with an **"In simple words"** box, and every abbreviation is written out in full. For details, each section links to the document that explains it.

---

## 0. Abbreviations Used in This Document

| Short form | Full form | Meaning in simple words |
|---|---|---|
| **ETL** | Extract, Transform, Load | Take data out, clean/reshape it, load it somewhere |
| **CDC** | Change Data Capture | Capturing every insert/update/delete from a source database |
| **DB** | Database | Where data is stored |
| **EPAS** | EDB Postgres Advanced Server (EDB = EnterpriseDB) | Our PostgreSQL-based database (Silver, Gold, control) |
| **B2S / S2G** | Bronze to Silver / Silver to Gold | The two job families |
| **DQ** | Data Quality | Checks that data is correct |
| **DLQ** | Dead Letter Queue | Kafka "problem box" topic |
| **SK** | Surrogate Key | A number created in Gold to identify a dimension row |
| **SCD** | Slowly Changing Dimension | Rules for keeping history of dimension changes (Type 1/2/3) |
| **DAG** | Directed Acyclic Graph | An Airflow workflow |
| **YAML / JSON** | YAML Ain't Markup Language / JavaScript Object Notation | Config / data text formats |
| **JAR** | Java ARchive | A packaged Java program |
| **K8s** | Kubernetes | The container platform |
| **API / UI** | Application Programming Interface / User Interface | Program-to-program calls / screens |
| **RBAC** | Role-Based Access Control | Permissions by role |
| **SSO** | Single Sign-On | One company login for all tools |
| **PII** | Personally Identifiable Information | Name, email, phone, ID numbers |
| **GDPR / DPDP** | General Data Protection Regulation / Digital Personal Data Protection Act | EU / India privacy laws |
| **OM / OL** | OpenMetadata / OpenLineage | Governance tool / lineage standard |
| **CI/CD** | Continuous Integration / Continuous Delivery | Automatic build, test and deploy |
| **SLA / SLO** | Service Level Agreement / Objective | A promise / target (e.g. "data ready by 8 AM") |
| **RPO / RTO** | Recovery Point Objective / Recovery Time Objective | Max data loss / max downtime in a disaster |
| **PITR** | Point-In-Time Recovery | Restoring a database to an exact moment |
| **MVP** | Minimum Viable Product | The smallest first version that is truly useful |
| **PoC** | Proof of Concept | A small experiment to prove something works |
| **ADR** | Architecture Decision Record | A short file explaining one decision |
| **IST** | India Standard Time (UTC+05:30) | Business time zone (assumed) |
| **UTC** | Coordinated Universal Time | One global time zone for storage |

---

## 1. The Platform in One Page

> **In simple words**
> Our microservices write their data in MongoDB. Every change is **captured** (CDC) and sent through **one Kafka topic** to a small **ingestion service**, which stores a faithful copy in **Bronze** (MongoDB) and adds tracking information. **Spark jobs**, all built from **one reusable framework** and described by **one YAML file per job**, clean and split the data into **Silver** (EPAS) and model it into a **star schema** in **Gold** (EPAS). Every record knows whether it was processed successfully, and every run is logged. **Airflow** decides when jobs run, an **ops UI** lets people run, rerun and repair things safely, **OpenMetadata** shows what data exists and where it came from, and **monitoring** tells us when something goes wrong.

```
 ┌────────────────────── 20+ Spring Boot microservices (MongoDB) ───────────────────────┐
 └─────────────┬────────────────────────────────────────────────────────────────────────┘
               │ change streams
               ▼
 Kafka Connect (1 Mongo source connector per DB) ──► ONE topic cdc.source.events  (key = service:collection:id)
               │                                     + cdc.source.snapshot, dlq.ingestion, etl.events.bronze-loaded,
               ▼                                       etl.lineage.events, __mongodb_heartbeats
 etl-ingestion (Spring Boot): route by metadata → enrich (_etl) → version guard → soft delete → upsert
               ▼
 BRONZE  MongoDB  bronze_<service>.<collection>   { _id, data, _etl }   (+ optional __history)
               │  Spark B2S job (one per Bronze collection): read pending → split → standardize → DQ → dedupe
               │  → lookups → classify → stage (COPY) → merge in ONE transaction → write-back → reconcile
               ▼
 SILVER  EPAS  silver_<domain>.*  (+ silver_ref, later silver_mdm)   natural keys, _etl_change_seq, SCD2 _hist where needed
               │  Spark S2G jobs: dimensions (SCD 1/2/3, SKs) → facts (as-of SK lookup, inferred members)
               │  → snapshots → aggregates (SQL in EPAS, dirty dates)
               ▼
 GOLD    EPAS  gold_core (conformed dims) + gold_<area> (facts, aggs)  → BI reads v_* views

 Cross-cutting:
   etl_ctl (EPAS, same DB): registry, runs, locks, bookmarks, quarantine, DQ, reconciliation, requests, audit
   Airflow: one generated DAG per job + platform DAGs;  EtlSparkOperator → Spark Operator on K8s
   etl-ops-api + UI: runs, all run modes, approvals, quarantine, DLQ, trace, privacy, audit
   OpenMetadata: catalog, lineage (OpenLineage from the framework), DQ results, glossary, PII tags, contracts
   Prometheus/Grafana/Alertmanager + Loki: metrics, dashboards, alerts, logs
   Vault → External Secrets → K8s Secrets: all passwords
```

---

## 2. Decisions by Area

> **In simple words:** this is the "table of contents of all decisions". Each row says what we decided and where it is explained.

### 2.1 Structure and code

| Topic | Decision | Doc |
|---|---|---|
| Repository | Separate `data-platform` repo for all ETL assets | 01 §3 |
| Build | Maven multi-module: `etl-common`, `etl-config`, `etl-control`, `etl-core`, `etl-jobs`, `etl-spark-app`, `etl-ingestion`, `etl-ops-api`, `etl-governance`, `etl-testkit` | 01 §5, 10 §10 |
| Spark code | One shaded JAR, one generic `EtlJobRunner`; no Spring in Spark | 06 §8–9 |
| Spring | Only in `etl-ingestion` and `etl-ops-api` | 06 §9 |
| Config-driven jobs | One YAML per job under `pipelines/<layer>/<domain>/`; JSON Schema + reference/identifier/DDL checks in CI | 06 §3 |
| Framework patterns | Template Method (lifecycle), Strategy, Factory/Registry, Builder, Decorator (`TimedStep`), Null Object (dry-run), Composition Root | 06 §5 |
| Run modes in code | Neutral `ReadFilter`/`ReadPlan` + one `RunModePolicy` per mode | 06 §7 |
| Plugins | `CustomTransformer`, `DqRuleType` via `ServiceLoader`; pure functions; promoted after 3 uses | 06 §10 |
| Exceptions | Only for job-level problems; bad records are data (`_errors`) | 06 §14 |
| Logging | JSON logs, MDC with `runId`, executor MDC, Spark job group, JDBC `ApplicationName` | 06 §13 |

### 2.2 Ingestion and Bronze

| Topic | Decision | Doc |
|---|---|---|
| Connectors | One per source DB; `$match` collections/ops; `$addFields` service, envelope version, `cdcKey`; full envelope; `updateLookup`; canonical Extended JSON; heartbeats | 02 §2 |
| Topic | One shared `cdc.source.events`, 24 partitions in prod, RF 3, delete policy, 14 days; hot/PII collections can move to their own topic by config | 02 §3 |
| Initial load | `copy_existing` by default; snapshot topic for big collections; re-snapshot + reconciliation for recovery | 02 §4 |
| Bronze | MongoDB `bronze_<service>.<collection>`, `{_id, data, _etl}`, optional `__history` | 02 §5 |
| Idempotency | Version guard on `clusterTime`; no-op detection by `recordHash` | 02 §6–8 |
| Delivery | At-least-once + idempotent writes; manual commit after the Bronze write | 02 §9 |
| Errors | Data errors → DLQ + index; infra errors → retry forever + alert | 02 §11 |
| Schema | Phase 1: contracts + drift detection; phase 2: optional Schema Registry | 02 §12 |

### 2.3 Silver and Gold

| Topic | Decision | Doc |
|---|---|---|
| B2S steps | read → pre-filter → split → standardize/`try_cast` → DQ → dedupe → lookups → classify (+ circuit breaker) → deletes → stage + merge → quarantine → write-back → reconcile | 03 §1 |
| Splitting | 1:1 flattened; arrays → child/grandchild tables; deterministic child keys; orphan handling; `_unmapped` jsonb | 03 §3 |
| DQ | YAML rules → Spark column expressions → `_errors`; WARN / ERROR (retryable → FAILED) / QUARANTINE | 03 §5 |
| Silver write | UNLOGGED staging + COPY → one EPAS transaction; guard on `_src_version` + `_row_hash` | 03 §8 |
| Write-back | After the commit only; conditional on `{_id, sourceVersion}` | 03 §9 |
| Silver modeling | Integrated entities, natural keys, `silver_ref` codes, SCD2 captured in Silver where needed | 03 §10 |
| Gold | EPAS only; star schema; `gold_core` + `gold_<area>`; four job types (dimension, fact, snapshot, aggregate) | 04 |
| Gold incremental | `_etl_change_seq` on Silver rows + bookmark per Gold job and source table | 04 §5 |
| Keys and SCD | EPAS identity SKs (`-1` Unknown, `-2` N/A); SCD per column; inferred members; as-of lookups inside the merge | 04 §6–7 |
| Aggregates | SQL in EPAS, recompute only touched dates | 04 §10 |
| Deletes | Dimensions never physically deleted; facts SOFT / HARD / REVERSAL; BI reads `v_*` views | 04 §11 |

### 2.4 Control, lifecycle and orchestration

| Topic | Decision | Doc |
|---|---|---|
| Control tables | Schema `etl_ctl` in the **same** EPAS database as Silver/Gold (one transaction) | 05 §2 |
| Staging | Only for Spark → EPAS merges; one UNLOGGED table per target; truncate at start | 05 §3 |
| Lock | `job_lock` with a 10-min lease + 60-s heartbeat | 05 §6.5 |
| Run audit | `run_mode` + `trigger_type` + `is_dry_run`; every attempt a new `run_id`; parent/root chain; counts with a CHECK | 05 §6.6, 07 §1 |
| Quarantine | One generic table, pointer + masked snapshot, lifecycle with four-eyes approval | 05 §6.10, 07 §6 |
| Status rules | Decision table (first match wins); per-job thresholds; default: one bad record → PARTIAL | 07 §4 |
| Retries | Statement → Spark task → job (retryable only) → record (backoff 0/1/4/12/24 h, max 5) | 07 §3.5, §8.2 |
| Dependencies | Upstream PARTIAL → run; FAILED → WAIT 60 min then SKIPPED (HARD) | 07 §7 |
| Airflow | One generated DAG per job; Spark Operator via deferrable `EtlSparkOperator`; Datasets (OR of sources); `wait_upstream`; `catchup=False`; `run_etl` + `quality_gate` | 09 |
| Manual runs | Only through the ops API in uat/prod (approvals) → `etl_manual_run` DAG | 07 §10, 09 §5 |

### 2.5 Environments, governance and production

| Topic | Decision | Doc |
|---|---|---|
| Environments | Build once; config in Git under `config/environments/{base,<env>}`; secrets only in Vault → ESO → K8s Secrets | 08 |
| Spark sizing | T-shirt profiles per environment, applied by Airflow at submit time | 08 §4, §7.2 |
| Governance tool | OpenMetadata; tool-neutral through OpenLineage + governance-as-code | 10 §2 |
| Lineage | Config-driven `LineageEmitter` (not the automatic listener) | 10 §3–4 |
| Contracts | Per source collection, SemVer, co-approved, checked in the microservices' CI | 10 §8 |
| Monitoring/alerts | Prometheus + Grafana + Alertmanager; 27 alerts; no high-cardinality labels | 11 §2–3 |
| Security | SSO + RBAC, least privilege, TLS + encryption at rest, PII controls + purge workflow, audit trail | 11 §5 |
| DR | PITR backups, monthly restore tests, replay/re-snapshot, procedures D1–D8 | 11 §10 |
| Go-live | Pilot + shadow mode + gated checklist | 11 §13 |

---

## 3. Key Numbers and Defaults (Cheat Sheet)

> **In simple words:** the most important "starting values" in one place. All of them are configurable; these are the defaults to start with.

| Item | Default | Doc |
|---|---|---|
| CDC topic partitions (dev/qa/uat/prod) | 3 / 6 / 12 / 24 | 02 §3 |
| CDC topic retention (prod) | 14 days | 02 §3.3 |
| DLQ retention | 30 days | 02 §3.1 |
| Source oplog window | ≥ 72 h (alert < 48 h) | 02 §1 |
| Kafka max message size | 8 MB | 02 §3.1 |
| Record retries | max 5, backoff 0 h / 1 h / 4 h / 12 h / 24 h | 07 §3.5 |
| Referential retry window | 24 h, then WARN-load (Gold inferred member) | 03 §5.4 |
| Job thresholds | `successTolerance` 0; `maxFailedPct` 2%; `maxFailedCount` 5,000; `minRecordsForPct` 100 | 07 §4.4 |
| Job lock | lease 10 min, heartbeat 60 s | 05 §6.5 |
| Airflow job retries | 3, 5 → 15 → 45 min, only for retryable failures | 09 §7.1 |
| HARD dependency wait | 60 min, then SKIPPED | 07 §7 |
| Task timeout | 3 × `maxDurationMinutes` (else 2 h) | 09 §7.2 |
| DB timeouts | statement 30 min, lock 30 s, idle-in-transaction 10 min | 07 §8.3 |
| Write-back batch | 1,000 per bulk, 8 parallel writers | 03 §9 |
| Staging idle cleanup | 7 days | 05 §3.5 |
| Gold SCD2 range | first version from 1900-01-01, `valid_to` 9999-12-31 | 04 §6.4 |
| Special SKs | `-1` Unknown, `-2` Not applicable | 04 §6.2 |
| Fact partitions | monthly by date key (facts > ~50 M rows) | 04 §12.3 |
| Run id format | `<job>-<yyyyMMddTHHmmssZ>[-m\|-c]-a<attempt>` (see §4, C1) | — |
| Retention | Gold finance facts ~8 years (confirm), `record_error_log` 90 days, `job_run_audit` 2 years, closed quarantine 1 year | 05 §8 |
| Backups | EPAS + Mongo PITR, 35 days, monthly restore test | 11 §10.2 |

---

## 4. Conflicts and Inconsistencies Found, and How They Are Resolved

> **In simple words:** while writing 11 documents one after the other, some details changed along the way. Here every difference is listed with the **final rule**. Where a later document refined an earlier one, the **later document wins** unless stated otherwise. Small fixes were also applied directly in the earlier documents.

| # | Conflict | Final rule | Status |
|---|---|---|---|
| C1 | **Run id format** differs: doc 05 `…-01`, doc 07 `-a<n>` for attempts, `-m` manual, `-c` cascade | `<job>-<yyyyMMddTHHmmssZ>[-m\|-c]-a<attempt>`, e.g. `b2s_order-20261030T090000Z-a1`, `b2s_order-20261030T091512Z-m-a1` | Rule defined here |
| C2 | **Retry backoff** missing in the doc 03 read plan and the doc 06 Mongo filter | Incremental picks FAILED records only when `retryCount < maxRetries AND nextRetryAt ≤ cutoff` | ✅ Fixed in docs 03 and 06 |
| C3 | **Double alert for PARTIAL** in Airflow: the failing `quality_gate` would also trigger the generic failure callback | `quality_gate` has no failure callback; `on_partial` sends the only (WARNING) alert | ✅ Fixed in doc 09 |
| C4 | **Referential `retryWindow` (24 h) vs `maxRetries` (5 tries ≈ 41 h)** for `REF_PARENT_MISSING` | For referential rules the `retryWindow` decides first (→ `onExhausted`: WARN-load or QUARANTINE); `maxRetries` applies to all other retryable errors | Rule defined here |
| C5 | **Maintenance / platform runs** (retention, `xlayer_recon`, purge, governance publisher bookmark) are not covered by the `job_type` / `run_mode` CHECKs and need a `job_registry` row (FK from `watermark`, `job_run_audit`) | Register them as platform jobs; extend the CHECKs (migration below) | Migration V22 below |
| C6 | **Job config layers:** doc 06 had 4, doc 08 has 5 | 5 layers: defaults → env job-defaults → job YAML → env job-overrides → CLI | Doc 08 wins |
| C7 | **Resources in job YAML:** docs 03/06 used exact executor numbers | `resources: {profile: small\|medium\|large}` + optional exact overrides | Doc 08 wins |
| C8 | **Image version pin:** doc 08 = Airflow Variable; doc 09 = inside `jobs.<env>.json` | `jobs.<env>.json` is the pin; the Variable is only an emergency override | Doc 09 wins |
| C9 | **OM ingestion location:** doc 01 "reuses Airflow"; doc 10 "own deployment" | OM ingestion in its own deployment | ✅ Doc 01 updated |
| C10 | **Lineage source:** doc 01 "OpenLineage Spark listener"; doc 10 "framework emitter" | Framework `LineageEmitter` (OpenLineage format); listener optional | ✅ Doc 01 updated |
| C11 | **Schema Registry:** doc 01 guardrail; doc 02 phase 2 | Phase 1 contracts + drift detection; registry optional later | ✅ Doc 01 updated |
| C12 | **Silver Gold-sync columns:** doc 03 vs doc 04 | Doc 04 columns (`_etl_change_seq`, `_sync_gold_*` summary, `_sync_gold_failed_jobs`) | ✅ Doc 03 updated |
| C13 | **Reconciliation table shape:** wide (doc 03) vs long (doc 05) | Long format (one row per check); counts in `job_run_audit` | Doc 05 wins |
| C14 | **Upstream FAILED → downstream FAILED** (implicit early) vs SKIPPED | SKIPPED (`DEPENDENCY_FAILED`) by default | Doc 07 wins |
| C15 | **Mode vs trigger:** early lists mixed `DEPENDENCY_TRIGGERED`, `CASCADE`, `DRY_RUN` into modes | `run_mode` + `trigger_type` + `is_dry_run` | Doc 05 wins |
| C16 | **Manual run pool:** doc 09 manual DAG always used `etl_heavy` | Pool by mode: `FULL_LOAD`, `REPLAY`, `BACKFILL` > 7 days → `etl_heavy`; all other manual modes → `etl_spark` | Rule defined here |
| C17 | **Dependency skip recording:** doc 09 said "the runner records SKIPPED", but the runner never starts | The `wait_upstream` sensor calls the ops API, which writes the `SKIPPED (DEPENDENCY_FAILED)` audit row | Rule defined here |
| C18 | **Kafka topic list:** `etl.lineage.events` (doc 10) is missing in `topics.yml` (doc 02) and `kafka.yml` (doc 08) | Add it: 6 partitions, delete policy, 7 days | Rule defined here |
| C19 | **Helper SQL functions** `status_rank()` / `rank_to_status()` (doc 04 §5.4) not in the migration list | Add `V23__helper_functions.sql` | Migration list below |
| C20 | **SLA column:** doc 05 `sla_minutes`; doc 07 three SLA columns | Doc 07 (`V20`); the view `v_sla_breaches` uses the renamed column (PostgreSQL keeps views valid on rename) | Doc 07 wins |

```sql
-- db/control/V22__platform_jobs.sql   (resolves C5)
ALTER TABLE etl_ctl.job_registry DROP CONSTRAINT IF EXISTS job_registry_job_type_check;
ALTER TABLE etl_ctl.job_registry ADD CONSTRAINT job_registry_job_type_check CHECK (job_type IN
  ('B2S','S2G_DIMENSION','S2G_FACT','S2G_SNAPSHOT','S2G_AGGREGATE',
   'INGESTION_REPLAY','MAINTENANCE','RECONCILIATION','PURGE','GOVERNANCE'));

ALTER TABLE etl_ctl.job_run_audit DROP CONSTRAINT IF EXISTS job_run_audit_run_mode_check;
ALTER TABLE etl_ctl.job_run_audit ADD CONSTRAINT job_run_audit_run_mode_check CHECK (run_mode IN
  ('INCREMENTAL','FULL_LOAD','RERUN_JOB','RERUN_FAILED_RECORDS','REPROCESS_QUARANTINE',
   'BACKFILL','REPLAY','ID_REPROCESS','MAINTENANCE'));

-- Platform pseudo-jobs are registered by the registry sync from pipelines/_platform/*.yml:
--   etl_retention (MAINTENANCE), etl_partition_maintenance (MAINTENANCE), etl_zombie_check (MAINTENANCE),
--   etl_bronze_safety_sweep (MAINTENANCE), xlayer_recon (RECONCILIATION), etl_purge (PURGE),
--   governance_publisher (GOVERNANCE)

-- db/control/V23__helper_functions.sql   (resolves C19): status_rank(), rank_to_status()
```

**Final migration list for `db/control/`:** V1–V18 (doc 05), V19 `run_request`, V20 lifecycle additions (doc 07), V21 `pii_access_log` (doc 11), V22 platform jobs, V23 helper functions.

---

## 5. Gaps, Risks and Recommendations Still Open

> **In simple words:** these are things the documents **did not fully cover**, or that are **risky** and need attention early. Each has a recommendation. Items marked 🔴 can change the design and should be answered **before** building the MVP.

### 5.1 Gaps

| # | Gap | Why it matters | Recommendation |
|---|---|---|---|
| G1 🔴 | **EPAS as a source.** The tech stack says sources are "MongoDB + EPAS", but the ingestion design covers **only MongoDB** CDC | If some microservices store data in EPAS, they need CDC too | Use the **Debezium PostgreSQL connector** (logical replication) into the **same shared topic**, with a second envelope parser in `etl-ingestion` (`DebeziumEnvelopeParser`, version = LSN (Log Sequence Number), the PostgreSQL position in its change log). Bronze stays MongoDB. PoC needed (EPAS logical replication settings) |
| G2 🔴 | **Business time zone for date keys.** Timestamps are stored in UTC, and `order_date_key` is derived from them. An order placed at 02:00 IST is 20:30 UTC **the day before** | Daily reports would put early-morning orders on the wrong day | Define a **business time zone per domain** (default `Asia/Kolkata`). Date keys and "business day" are computed in that zone; timestamps stay UTC. Add `businessTimeZone` to the job YAML and `dim_date` |
| G3 🔴 | **Data volumes unknown** (documents/day, sizes, growth) | Partition counts, Spark profiles, EPAS sizing, Kafka retention storage and cost all depend on it | A 1–2 week **measurement** per source (counts, change rate, document size p50/p99) before final sizing |
| G4 | **Multi-tenancy / branch-level access** (if the microservices are multi-tenant) | Users of one tenant/branch must not see others' data | If yes: `tenant_id` through all layers, part of keys, and EPAS **row-level security** on Gold views |
| G5 | **Multi-currency** amounts | Summing INR and USD gives wrong totals | Gold `dim_currency` + daily exchange-rate table; store original amount + amount in the reporting currency |
| G6 | **BI tool and semantic layer** not chosen | KPI definitions must also live in the BI layer | Choose the BI tool; generate its metric definitions from `kpis.yml` where possible |
| G7 | **Reference data maintenance by business users** (`silver_ref` mappings are Git seeds) | Business users can't open pull requests | Small "reference data" screen in the ops UI that creates a reviewed change (PR or approval) |
| G8 | **Analyst sandbox / self-service** | Analysts will want to explore Silver/Gold | A sandbox schema with read access to Gold views and masked Silver, auto-cleaned |
| G9 | **Silver/Gold DDL for new fields** is manual | Many small migration files as sources evolve | Tool that **suggests** the Flyway migration from the contract + job YAML diff (still reviewed by a person) |
| G10 | **Source-side load of `updateLookup`** | Every update event causes an extra read on the source MongoDB | Monitor source load; use post-images (MongoDB 6.0+) for heavy collections |
| G11 | **Sharded source MongoDB** | Change streams then run via `mongos`; ordering across shards by `clusterTime` | Confirm the topology of each source DB in the PoC |
| G12 | **Team, skills and on-call** | Spark, K8s, Kafka Connect, Airflow and EPAS skills are all needed | Name owners per component, train the team, set up an on-call rotation before go-live |
| G13 | **Licensing** | EPAS (incl. containers for tests), MongoDB encryption at rest (edition), Spark Operator project status | Check licences and support contracts early |

### 5.2 Risks

| # | Risk | Likelihood / impact | Mitigation |
|---|---|---|---|
| K1 | **Framework built too big too early** ("second system effect") | High / High | Strict MVP scope (§6.2 "not in MVP"); build features when the second job needs them |
| K2 | OpenMetadata + EPAS connector gaps | Medium / Medium | PoC in phase 0; the design is tool-neutral (OpenLineage, governance as code) |
| K3 | One shared CDC topic becomes a bottleneck (noisy neighbour) | Medium / Medium | Over-provisioned partitions, per-collection metrics, the "own topic" escape hatch (doc 02 §3.4) |
| K4 | Source teams change documents without notice | High / High | Data contracts + contract check in their CI + drift alerts to both teams |
| K5 | Mongo connector property/behaviour differences by version (key projection, snapshot marking) | Medium / Medium | PoC; pin the connector version |
| K6 | EPAS-specific behaviour hidden by plain PostgreSQL tests | Medium / Medium | Nightly integration tests on real EPAS |
| K7 | Lost resume token after long connector downtime | Low / High | Oplog ≥ 72 h, heartbeats, alerts, tested re-snapshot runbook |
| K8 | Merge load on EPAS as jobs grow to 100+ | Medium / High | Pools, write parallelism limits, partitioning, fillfactor/autovacuum, read replica |
| K9 | Airflow 3 upgrade (Datasets → Assets, SLA removal) | Medium / Low | Our own SLA monitor; Datasets used in only two places (factory, bridge) |
| K10 | PII exposure through new paths (logs, catalogs, exports) | Medium / High | PII rules in CI, `generateSampleData: false`, log scanning, audits |
| K11 | Spark-vs-ingestion race marking unprocessed changes as synced | Low / High (if missed) | Optimistic write-back filter (doc 02 §6.3) + nightly safety sweep (doc 07 §3.3) |

### 5.3 Extra recommendations

| # | Recommendation |
|---|---|
| X1 | Write an **ADR** for every 🔴 decision and every exception to the defaults (`docs/adr/`) |
| X2 | Keep a **"golden path" example**: one fully documented source → Bronze → Silver → Gold chain that new team members copy |
| X3 | Review the defaults in §3 after the first month of production with real metrics |
| X4 | Track "time to add a new job" as a platform KPI (target: < 1 day for a standard B2S job) |
| X5 | Hold a design review with the microservice teams before the MVP (contracts, CDC load, naming) |

---

## 6. Phased Implementation Roadmap

> **In simple words:** build the house **floor by floor**. First the **foundation** (proofs and infrastructure), then a **small but complete** house where people can live (MVP: a few sources end to end, safe and monitored), then **more rooms** (all run modes, more sources, full ops UI and governance), and finally the **finishing touches** (advanced features, cost and resilience).
> The durations assume a team of about **4–5 engineers** (2 data/Spark, 1 platform/K8s, 1 backend for ingestion/ops API, part-time QA). Adjust to your team.

### 6.1 Overview

```
 Phase 0: Foundations & PoCs        Phase 1: MVP (pilot)              Phase 2: Scale out                  Phase 3: Optimize & harden
 ~3–4 weeks                         ~10–12 weeks                      ~3–4 months                         ongoing
 ─────────────────────────────►─────────────────────────────────►──────────────────────────────────►──────────────────────────►
 decide 🔴 gaps, prove the risky    2–3 sources end to end,           all run modes, 10–30+ jobs,         MDM, schema registry,
 parts, set up infra + repo         safe + monitored, shadow mode     full ops UI, governance, DR         dynamic creds, DR site, cost
```

### 6.2 Phase 0: Foundations and PoCs (~3–4 weeks)

| Priority | Item |
|---|---|
| P0 | Answer the 🔴 gaps: EPAS sources (G1), business time zone (G2), volume measurement (G3), multi-tenancy (G4) |
| P0 | **PoC 1:** MongoDB source connector: full envelope, `cdcKey` key projection, `topic.namespace.map`, snapshot marking, heartbeats (doc 02) |
| P0 | **PoC 2:** Spark on K8s via Spark Operator: shaded JAR, secrets on executors, Mongo Spark connector read, COPY into EPAS, one merge transaction |
| P0 | **PoC 3:** OpenMetadata with the Postgres connector on EPAS + one OpenLineage event + one DQ test result |
| P1 | **PoC 4:** Debezium on EPAS (only if G1 = yes) |
| P0 | Infrastructure: namespaces, Kafka topics, EPAS database + schemas, Bronze MongoDB, Vault + ESO, Airflow (scheduler + triggerer), Prometheus/Grafana/Loki access |
| P0 | Repo skeleton (doc 01 §5), CI with build + unit tests + config validation; ADRs for the PoC results |

**Exit criteria:** all P0 PoCs pass (or the design is adjusted with an ADR); infrastructure for dev and qa is ready.

### 6.3 Phase 1: MVP / pilot (~10–12 weeks)

**Scope: 2–3 source collections** (e.g. `customers`, `orders`, `products`) → Bronze → Silver → **2 dimensions + 1 fact + 1 aggregate**, running in dev, qa and prod (shadow mode).

| Area | In the MVP (P0) | Doc |
|---|---|---|
| Ingestion | Connectors for the pilot DBs; `etl-ingestion` with the generic handler, version guard, soft delete, DLQ + index, metrics | 02 |
| Framework | `EtlJobRunner`, config loader (5 layers) + schema validation, `AbstractEtlJob`, step pipeline, `ComponentFactory`, Null-Object dry-run | 06 |
| B2S | Read (status-based + cutoff), split, standardize/`try_cast`, DQ engine (not_null, type, range, allowed_values, regex, referential), dedupe, stage + guarded merge, write-back, reconciliation | 03 |
| S2G | SCD1 + SCD2 dimensions, one transactional fact with inferred members, one SQL aggregate | 04 |
| Control | V1–V23 migrations (all tables exist; some features used later) | 05, §4 |
| Run modes | `INCREMENTAL`, `FULL_LOAD`, `RERUN_JOB`, `RERUN_FAILED_RECORDS`, dry-run | 07 |
| Status | Full decision table, thresholds, circuit breaker, record backoff | 07 |
| Airflow | DAG factory, `EtlSparkOperator`, `wait_upstream`, `quality_gate`, callbacks, pools; `etl_manual_run`; `etl_maintenance` (zombie check, retention, partitions) | 09 |
| Environments | dev/qa/prod configs, Vault secrets, size profiles, deploy pipeline (Flyway → registry sync → connectors → Helm → DAG bundle → smoke dry-run) | 08 |
| Ops API (minimal) | Runs list/detail/chain, request run (MVP modes), approvals, quarantine list + fix/approve/reprocess (basic) | 07, 11 |
| Monitoring | Dashboards #1, #2, #3, #7; alerts A1, A2, A6, A9, A15, A19, A21, A23, A25 | 11 |
| Testing | Unit, SQL-generator, golden dataset, integration (Testcontainers) | 11 §7 |
| Security | SSO, RBAC roles, least-privilege DB roles, TLS, masking of PII in Gold and quarantine | 11 §5 |
| Recovery | EPAS + Mongo backups, one restore test | 11 §10 |
| Governance (light) | Contracts for the pilot sources; owners/domains in YAML; OM metadata ingestion | 10 |

**Not in the MVP (on purpose) ➕:** MDM crosswalk, Schema Registry, Kafka replay job (manual procedure only), cascade DAG, event-triggered runs, accumulating snapshots, back-dated SCD timeline rebuild, PII reveal, purge automation, Debezium (unless G1 requires it), regression diff, spot nodes, DR site. They are designed, so adding them later does not change the foundation.

**Exit criteria:** pilot runs in prod in **shadow mode for 2 weeks**; reconciliation always matches; no CRITICAL alerts unresolved; business owner signs off the first Gold mart; runbooks R01–R10 exist.

### 6.4 Phase 2: Scale out (~3–4 months)

| Priority | Item | Doc |
|---|---|---|
| P0 | Onboard sources domain by domain (target 10–30+ jobs); measure "time to add a job" | 06 §16 |
| P0 | Remaining run modes: `BACKFILL`, `ID_REPROCESS`, `REPROCESS_QUARANTINE` (+ periodic auto-reprocess), `REPLAY` (B2S); Kafka/DLQ replay job | 07, 02 §15 |
| P0 | Cascade (`etl_cascade`) and dependency/event triggers (ops API bridge) | 09 §4 |
| P0 | Full ops UI: approvals, quarantine workflow incl. bulk actions, DLQ, **record trace**, watermark reset, audit | 11 §11 |
| P0 | All alerts A1–A27, dashboards #1–#9, SLA monitor DAG, SQL exporter | 11 §2–3 |
| P0 | Cross-layer reconciliation job + DQ digest | 11 §6 |
| P1 | Governance: `LineageEmitter` (design-time + runtime), DQ/run publisher, glossary + KPIs, PII propagation, contract check in the microservices' CI | 10 |
| P1 | More Gold patterns: periodic and accumulating snapshots, junk dimensions, Type 3, `REBUILD_TIMELINE` where needed | 04 |
| P1 | PII reveal with `pii_access_log`; purge workflow | 07 §6, 11 §5.3 |
| P1 | Nightly real-EPAS tests, performance tests, recovery/chaos tests, regression diff | 11 §7 |
| P1 | DR drill #1; runbooks R11–R19 | 11 §10, §12 |
| P2 | Bronze `__history` + Silver SCD2 timelines for entities that need exact history | 02 §5.4, 03 §10.4 |

**Exit criteria:** 10–30+ jobs in production; all run modes usable by operators through the ops UI; DQ and lineage visible in OM; one successful DR drill.

### 6.5 Phase 3: Optimize and harden (ongoing)

| Priority | Item |
|---|---|
| P1 | MDM crosswalk for entities with more than one source (`silver_mdm`) |
| P1 | Cost: right-sizing reviews, spot nodes for LOW-priority jobs, cost per team reports, Kafka tiered storage |
| P1 | Vault dynamic database credentials per run |
| P2 | Optional Schema Registry (phase 2 of doc 02 §12) |
| P2 | DR site (warm standby) if the business RTO requires it |
| P2 | Column-level encryption for national IDs; image signing + admission policy everywhere |
| P2 | Self-service: analyst sandbox, reference-data screen, DDL suggestion tool (G7–G9) |
| P2 | Airflow 3 readiness (Assets) |

### 6.6 Priority legend

| Priority | Meaning |
|---|---|
| **P0** | Must have for the phase's exit criteria |
| **P1** | Should have in the phase; may slip to the next phase with an owner and a date |
| **P2** | Nice to have; plan when capacity allows |

---

## 7. Where to Find What

| I want to understand… | Read |
|---|---|
| The overall structure, repo and modules | [01](01-architecture-and-structure.md) |
| How changes get from MongoDB into Bronze | [02](02-ingestion-bronze.md) |
| How Bronze becomes Silver (splitting, DQ, merge, write-back) | [03](03-bronze-to-silver.md) |
| Facts, dimensions, SCD, aggregates | [04](04-silver-to-gold.md) |
| Control/audit/staging tables and retention | [05](05-control-audit-tables.md) |
| The reusable framework and how to add a job | [06](06-reusable-framework.md) |
| Run modes, statuses, retries, quarantine, rerun API | [07](07-job-lifecycle-run-modes.md) |
| Environment config and secrets | [08](08-environment-config.md) |
| Airflow DAGs and Spark submission | [09](09-airflow-orchestration.md) |
| Catalog, lineage, PII tags, contracts | [10](10-governance-lineage.md) |
| Monitoring, security, CI/CD, DR, ops UI, runbooks, go-live | [11](11-production-readiness.md) |

---

## 8. Final Key Decisions (One List)

1. **Medallion on two stores:** Bronze = MongoDB (faithful copy + `_etl` metadata), Silver and Gold = EPAS, control tables `etl_ctl` in the same EPAS database.
2. **Ingestion:** one Mongo connector per source DB → one shared CDC topic keyed by `service:collection:id` → `etl-ingestion` with a version guard, soft deletes, DLQ; EPAS sources (if any) via Debezium into the same topic (to confirm, G1).
3. **One framework, config-driven:** one shaded Spark JAR, one `EtlJobRunner`, one YAML per job, five config layers, typed configs, plugins only when needed; no Spring inside Spark.
4. **Correctness everywhere:** version/row-hash guards, one transaction for data + bookmarks + statuses, write-back only after the commit, strict reconciliation, circuit breaker, every record counted.
5. **Silver** = integrated entities with natural keys; **Gold** = star schema with EPAS identity SKs, per-column SCD, inferred members, dirty-date aggregates; dimensions never deleted.
6. **Run lifecycle:** `run_mode` + `trigger_type` + dry-run flag; every attempt its own run id; decision-table statuses; four retry levels with record backoff; dependency policy with root-cause alerting.
7. **Orchestration:** one generated Airflow DAG per job, Spark Operator, results read from `job_run_audit`, Datasets with OR-of-sources, PARTIAL shown as "green run + red quality gate", manual runs only through the ops API in uat/prod.
8. **Environments and secrets:** build once, config in Git, secrets only in Vault → ESO → K8s Secrets, size profiles per environment, explicit environment guards.
9. **Governance:** OpenMetadata, framework-emitted OpenLineage lineage (table + column), governance as code, PII classification with propagation, DQ results published, data contracts co-owned with the microservice teams.
10. **Production:** Prometheus/Grafana/Alertmanager + Loki, 27 alerts with runbooks, record trace API, least-privilege security with full audit, tested backups and DR procedures, expand/contract migrations, pilot + shadow-mode go-live.
11. **Delivery:** Phase 0 (PoCs + 🔴 gaps) → Phase 1 MVP pilot (2–3 sources end to end) → Phase 2 scale out (all run modes, full ops UI, governance) → Phase 3 optimize and harden.
