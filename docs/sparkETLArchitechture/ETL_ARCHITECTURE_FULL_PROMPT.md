# Prompt: Production-Ready Spark + Java ETL Architecture (Medallion: Bronze → Silver → Gold)

## 1. Role and Goal
You are a senior data engineering architect. Design a **production-ready, scalable, config-driven ETL platform** using Spark + Java. It must plug into my **existing microservices project**: 20+ Spring Boot services, with Kafka already configured.

For every section below, give me:
- the recommended design, with the reasoning behind it
- the project and folder structure
- sample config and code skeletons (Java / YAML / SQL / Airflow DAG)
- trade-offs and alternatives, where they apply

If you see a gap, risk or better technique that I have not mentioned, **add it yourself and call it out clearly** (mark it as "➕ Added by architect").

---

## 2. Tech Stack
| Layer | Tool |
|---|---|
| Pipeline processing | Apache Spark (Java) |
| Application framework | Spring Boot |
| Source / Bronze / Silver / Gold DBs | MongoDB + EDB Postgres Advanced Server (EPAS) |
| Ingestion (Source → Bronze) | Kafka Connect + MongoDB Source Connector → ONE shared Kafka topic for all collections (with metadata) → custom consumer |
| Scheduling and orchestration | Apache Airflow |
| Data governance and lineage | OpenMetadata or DataHub (recommend one and justify it) |

---

## 3. Data Flow Overview
```
Original DB ──(Kafka Connect + Mongo Connector)──► ONE shared CDC Kafka topic (all collections + metadata)
        ──► Custom Consumer/Producer (route by collection, enrich + add metadata columns, soft delete) ──► BRONZE
BRONZE ──(Spark ETL: filter, validate, dedupe, cleanse, table division/normalization,
          dimensional modeling where needed)──► SILVER
SILVER ──(Spark ETL: aggregate, dimensional model: facts + dimensions)──► GOLD
```

---

## 4. Ingestion Layer (Source → Bronze)
1. Where should I put the **Kafka Connect / Mongo connector configuration files** in my existing project structure? Give me a recommended folder layout (per environment).
2. Monorepo vs separate repo: should the ETL project live inside my existing microservices repo or separately?
3. **All collections are published into ONE shared topic** (not a topic per collection). Every event carries metadata: source service, db, collection, operation type, document key and event time. Use one connector per source DB with `topic.namespace.map`, the change-stream pipeline (`$match` for collections, `$addFields` for `sourceService`) and the full change-event envelope.
4. The **custom consumer must be customizable**: it reads the shared topic and **routes each event to a per-collection handler using the event metadata**. Config-driven and reusable (no new code for each new collection); unmapped collections go to the DLQ.
5. The custom producer/consumer **enriches the original object** with extra metadata columns before writing to Bronze.
6. **Deletes in the source DB become soft deletes in Bronze** (a flag + `deletedAt`, not a physical delete).
7. Also address:
   - full snapshot (initial load) vs streaming change events
   - shared topic design: message key (collection + documentKey) for per-document ordering, partition count, retention, compaction
   - trade-offs of one shared topic (noisy neighbour, per-collection lag, replaying one collection, ACL/PII per collection) and mitigations, including moving a hot/PII collection to its own topic by config only
   - per-collection metrics (throughput, errors, lag) even though Kafka sees one topic
   - CDC event ordering, and out-of-order or duplicate events
   - Kafka offset management, idempotent writes (upsert by `sourceId` + version/offset), at-least-once vs exactly-once
   - Dead Letter Queue (DLQ) for messages that cannot be parsed, plus poison message handling
   - Schema evolution (new or removed fields in the source documents) and whether to use a Schema Registry (many record types in one topic → subject naming strategy)
   - Backpressure, consumer lag handling and retries
   - **Replay for data-loss recovery**: replay from a Kafka offset/timestamp (within topic retention), re-push DLQ messages after a fix, rebuild Bronze from Kafka without duplicates. What topic retention is needed to support this?

---

## 5. Bronze Metadata / Audit Columns
My initial list is below. Review it, improve it, and add anything missing (with purpose and data type for each, and **which component updates it**: the ingestion layer or the Silver ETL job):
- `isDeleted`
- `syncSilver` (sync status: PENDING / SUCCESS / FAILED / PARTIAL / QUARANTINED)
- `errorMessageWhileSync`
- `syncUpdatedDate`

Suggestions to evaluate: `sourceSystem`, `sourceCollection`, `sourceId`, `kafkaTopic`, `kafkaPartition`, `kafkaOffset`, `cdcOperation` (INSERT/UPDATE/DELETE), `sourceEventTs`, `ingestedAt`, `recordHash` (for change detection and dedupe), `version`, `batchId` / `runId`, `retryCount`, `errorCode`, `lastSyncJobName`, `syncGold`, `isQuarantined`, `deletedAt`.

The ETL job must **update these Bronze columns** after each record succeeds or fails in Silver (and likewise Silver metadata columns after Gold sync). Status must be written back efficiently at scale (bulk updates, not row-by-row).

---

## 6. Bronze → Silver ETL
- Read Bronze: incremental (only `syncSilver` = PENDING / retryable FAILED, or by watermark) or full load
- Data filtering
- Data validation (schema, nulls, types, formats, ranges, referential checks, business rules), using a **reusable rule-based DQ framework** with rules defined in config, not hard-coded
- Duplicate detection and removal (by business key / `recordHash` / latest version)
- Standardization, cleansing, type casting, enrichment/lookups
- **Table division / normalization**: Bronze Mongo documents are often nested (arrays, sub-documents). How should they be split into multiple Silver tables (parent/child tables, flattening arrays, exploding nested objects)? Cover key generation and relationships between the split tables.
- **Dimensional modeling in Silver (where applicable)**:
  - which master/reference entities should become dimension-like tables in Silver vs. only in Gold
  - surrogate keys vs natural keys at the Silver level
  - SCD handling (Type 1 / Type 2): when to use which
  - conformed/shared dimensions across multiple source collections
- Error handling at record level (bad record → quarantine/error table, job continues) and at job level (infra failure → job fails)
- Incremental processing (see section 10)
- Applying soft deletes downstream
- Reconciliation: Bronze read count = Silver written + failed + skipped + quarantined
- Performance: partitioning, Mongo/JDBC read parallelism, caching, broadcast joins, small-file / small-batch handling

## 7. Silver → Gold ETL
- Same controls as section 6 (filter, validate, dedupe, error handling, incremental/full, reconciliation, status write-back to Silver: `syncGold`, error message, `syncUpdatedDate`, `runId`)
- **Dimensional modeling**:
  - star vs snowflake schema
  - fact table types (transactional, periodic snapshot, accumulating snapshot)
  - dimension tables, surrogate key generation in Spark + EPAS
  - SCD Type 1 / Type 2 / Type 3 implementation in Spark (merge logic)
  - conformed, junk, degenerate and date/time dimensions
  - late-arriving dimensions (inferred/unknown members) and late-arriving facts
  - load order: dimensions first, then facts
- Aggregations, summary tables and business KPIs
- Soft-delete handling in facts and dimensions
- Should Gold be in EPAS, Mongo or both?

---

## 8. Table Design Questions
1. **Fact and dimension tables**: naming conventions (`dim_*`, `fact_*`, `agg_*`), keys, partitioning and indexing strategy in EPAS and Mongo.
2. **Are staging tables required for each job?** When should I use staging vs. a direct merge/upsert? What is the staging lifecycle (truncate/load/swap/cleanup)?
3. **Audit and control tables**: what is needed? For example:
   - `job_registry` / `job_config` (job metadata, type, layer, source, target, mode, schedule, dependencies, thresholds, active flag, owner)
   - `job_dependency`
   - `job_run_audit` (runId, runMode/triggerType FULL_LOAD / SCHEDULED / RERUN_JOB / RERUN_FAILED_RECORDS / REPROCESS_QUARANTINE / BACKFILL / REPLAY / ID_REPROCESS / DEPENDENCY_TRIGGERED / CASCADE / DRY_RUN, parentRunId (the original run that this rerun fixes), run parameters (from/to, ids), triggeredBy, start/end time, status, counts read/written/failed/skipped/quarantined, failure reason, error code, Spark appId)
   - `job_step_audit` (optional, step-level timing and counts)
   - `record_error_log` / quarantine table (one generic table or one per entity?), with a status lifecycle QUARANTINED → FIXED → READY_TO_REPROCESS → REPROCESSED / REJECTED, plus fixedBy, fixedAt, reprocessRunId
   - `watermark` / checkpoint table (for incremental loads)
   - `data_quality_results`
   - `reconciliation` (source vs target counts and checksums)
   - `error_code_catalog` (standard codes, descriptions, retryable flag)
   - `job_lock` (to stop the same job running twice)
4. Which DB should hold the audit/control tables?
5. Data retention and archival policy for each layer (Bronze/Silver/Gold/audit/quarantine/DLQ).

---

## 9. Job Execution Model and Run Modes
For each run mode, explain when to use it, what data it picks, how it avoids duplicates, and how it is recorded in `job_run_audit`:

| Run mode | When | How |
|---|---|---|
| **Full load** | Initial load or a manual full refresh | Triggered manually by a user (via ops API/UI or Airflow, with parameters) |
| **Scheduled incremental** | Normal operation | Every hour, every 5 hours, daily, etc. (configurable per job; not fixed yet) |
| **Rerun failed job** | The whole job failed (infra/DB down) | Resume from the last saved watermark/checkpoint |
| **Rerun failed records only** | The job was PARTIAL | Pick only records with `syncSilver` = FAILED (retryable), without reprocessing everything |
| **Reprocess quarantine** | Data failed sanitization/validation | Fix the data, mark it READY_TO_REPROCESS, then the job picks only those records |
| **Backfill / date range** | Data for a period is wrong or missing | Re-run only that range (`--from --to`) |
| **Replay (data loss recovery)** | Data lost in Bronze/Silver/Gold | Replay from Kafka offsets, or rebuild from the previous layer; soft delete in Bronze keeps history |
| **Single entity / ID reprocess** | One customer/order is wrong | Re-run for a specific list of `sourceId`s |
| **Dependency / event triggered** | An upstream job finished | Triggered automatically via Airflow sensors/Datasets |
| **Cascade rerun** | An upstream layer was re-run | Dependent downstream jobs are re-run automatically |
| **Dry-run / validate only** | Check before writing to prod | Read + validate + show counts, write nothing |

- Who is allowed to trigger each mode? Does a rerun create a new `runId` linked to the original run (`parentRunId`)?
- **Job dependencies**: one job may depend on another (DAG-level and cross-DAG dependencies). If an upstream job fails or partially fails, should downstream run, skip or wait?
- Concurrency control: stop the same job from running twice (locking)
- Retries with backoff, timeouts, SLA miss alerts
- Idempotency: safe re-run of any job with no duplicates

## 10. Incremental Load Strategy
Mainly driven by Bronze sync columns (`syncSilver` status + `syncUpdatedDate`). Also evaluate a combination with:
- watermark/checkpoint on `ingestedAt` / `updatedAt`
- `recordHash` for change detection
- handling of late-arriving and reprocessed records
- the switch from full load to incremental (and resetting the watermark)

---

## 11. Job Status Rules: SUCCESS / PARTIAL / FAILED
Suggest clear, configurable rules. For example:
- **SUCCESS**: all records processed, 0 failures (or below a configurable tolerance %)
- **PARTIAL (PARTIALLY_FAILED)**: some records failed validation/transform, but the job finished; failures stay below a threshold
- **FAILED**: infrastructure or system error, an exception that stops the job, a failed dependency, or failures above the threshold
- Even if **one** record fails, it must be counted. Failed/partial counts must be accurate.
- Should failure thresholds (count and/or %) be configured per job?
- How should the status and **reason** (validation, schema mismatch, connection, timeout, dependency failed, etc.) be recorded in a standard error code catalog?
- Full status lifecycle: QUEUED → RUNNING → SUCCESS / PARTIAL / FAILED / SKIPPED / CANCELLED

## 12. Failed and Sensitive Data Handling
- If a record (especially **sensitive/PII data**) fails, should it be:
  - stored in a separate **quarantine table** (encrypted/masked)?
  - marked with a status flag in Bronze?
  - or both?
- Workflow to **correct, re-validate and reprocess** quarantined records (manual fix via UI/API, auto retry for retryable errors, approval step, max retries)
- PII masking/encryption at rest and in transit, and access control on quarantine data

---

## 13. Reusable Code Structure (100+ Jobs Must Not Become a Mess)
The number of jobs is unknown (could be 20, could be 100+). Design for:
- **Metadata / config-driven pipelines**: adding a new job = adding config (YAML or `job_registry` rows), with no or minimal new code
- A common framework module: Reader, Writer, Validator (DQ rule engine), Transformer, Deduplicator, Normalizer/TableSplitter, SCD handler, Auditor, ErrorHandler/QuarantineWriter, StatusWriteBack, WatermarkManager, LineageEmitter, MetricsPublisher, LockManager
- Template Method / Strategy / Factory / Builder patterns for job types (Bronze→Silver, Silver→Gold) and all run modes from section 9, plus a plugin approach for custom transformations
- How the framework switches reader/filter logic per run mode while the transform/validate/write steps stay the same
- A single generic Spark entry point (e.g. `EtlJobRunner --job=<name> --mode=<full|incremental|rerun-job|rerun-failed|reprocess-quarantine|backfill|replay|ids|dry-run> --env=<env> --from --to --ids=<list> --parentRunId=<id>`) vs one main class per job
- Spring Boot + Spark integration: use Spring inside Spark jobs, or only for the ops API?
- Multi-module Maven/Gradle structure (e.g. `etl-core`, `etl-common`, `etl-ingestion`, `etl-jobs`, `etl-config`, `etl-orchestration`, `etl-ops-api`)
- Versioning, packaging (fat/shaded JAR), Spark vs Spring dependency conflicts, and deployment of Spark jobs
- Coding standards, a logging standard (with `runId` correlation) and an exception hierarchy

## 14. Environment-Based Configuration
Separate config for dev / QA / UAT / prod:
- Database connections (Mongo, EPAS)
- **Spark cluster properties** (executors, memory, cores, shuffle partitions, master/deploy mode, dynamic allocation)
- Kafka / Kafka Connect / connector properties
- **Airflow DAG configuration** per environment (schedule, connections, variables, pools, queues)
- Job-level overrides (thresholds, schedules, batch sizes)
- Config layering: default → environment → job-level override
- Secrets management (Vault / K8s Secrets / Airflow Connections). No credentials in code or Git.
- Profile strategy (Spring profiles, `application-{env}.yml`, external config server, K8s ConfigMaps)

## 15. Airflow Orchestration
- DAG design: one DAG per job, per domain/layer, or dynamically generated DAGs from the job config
- How Airflow triggers Spark (SparkSubmitOperator / KubernetesPodOperator / Livy / REST)
- Dependency handling (ExternalTaskSensor / Datasets), dependency/event-triggered runs, **cascade rerun** of downstream jobs, sensors, retries, SLA, alert callbacks
- Manual trigger with parameters for every run mode (full load, rerun failed job, rerun failed records, reprocess quarantine, backfill, replay, specific IDs, dry-run). One generic "manual run" DAG vs parameters on each DAG?
- The same DAG code for all environments, with environment-based properties
- Concurrency, pools, priority, catchup/backfill behaviour
- Mapping Airflow task status to `job_run_audit` status. PARTIAL should not show as a plain green success.

## 16. Data Governance and Lineage
- Final choice: OpenMetadata vs DataHub, with justification
- How Spark jobs emit lineage to OpenMetadata/DataHub (OpenLineage / Spark listener / custom emitter)
- Table-level and column-level lineage from Source → Kafka → Bronze → Silver → Gold (including split tables and dimensions)
- Metadata ingestion for Airflow, Kafka and the DBs
- Data catalog, ownership, glossary, domains, PII tagging and classification
- Publishing data quality results and job run status to the governance tool
- Data contracts between source teams (microservices) and the ETL team

---

## 17. Production Readiness (add anything missing)
- **Monitoring**: Spark metrics, job metrics (counts, duration, failure %), Kafka consumer lag, DB health (Prometheus + Grafana or similar)
- **Alerting**: Slack/Email/Teams for failed or partial jobs, SLA breaches, lag spikes and DQ threshold breaches
- **Logging and debugging**: centralized structured logs (ELK/Loki), correlation via `runId`/`batchId`, Spark UI/history server, and debugging one failed record end to end
- **Security**: RBAC, encryption, PII masking, network isolation, audit trail for who triggered or re-ran what
- **Data reconciliation**: count and checksum checks between layers
- **Idempotency**: safe re-runs with no duplicates
- **Testing**: unit tests for transforms, integration tests (Testcontainers for Kafka/Mongo/Postgres), data contract tests, test data sets, regression tests
- **CI/CD**: build, test and deploy Spark JARs, connector configs and DAGs per environment, with versioning and rollback
- **Performance and cost**: partitioning, caching, broadcast joins, small-file handling, Mongo/JDBC read parallelism, dynamic allocation, Airflow pools/queues
- **Disaster recovery**: backup, restore, replay from Kafka, re-run from a watermark
- **Operational UI/API**: a Spring Boot service to view job status/history (with the rerun chain via `parentRunId`), view/fix quarantine data, view DQ results, and trigger every run mode from section 9, with role-based permissions per mode
- **Runbooks**: common failure scenarios and the steps to fix them

---

## 18. Expected Output Format
Answer step by step, in this order:
1. High-level architecture diagram (text/ASCII)
2. Project folder structure (including where Kafka Connect configs go)
3. Bronze/Silver/Gold table and column design (including Silver table division and dimensions), plus audit tables
4. Framework design (classes, interfaces, patterns) with code skeletons
5. Job lifecycle, status rules and all run modes (rerun, reprocess, backfill, replay, dry-run)
6. Incremental and full-load logic
7. Error, quarantine and reprocess workflow
8. Environment config strategy
9. Airflow DAG design with a sample DAG
10. Governance and lineage integration
11. Monitoring, security and the CI/CD checklist
12. Gaps, risks, and a phased implementation roadmap (MVP → Phase 2 → Phase 3)
