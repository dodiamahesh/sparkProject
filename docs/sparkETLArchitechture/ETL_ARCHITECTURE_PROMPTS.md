# Spark + Java ETL Architecture — Step-by-Step Prompts

> **How to use**
> 1. Run the prompts **in order** (Prompt 1 → Prompt 11).
> 2. Paste the **Common Context** block at the top of **every** prompt.
> 3. After each prompt, write a short summary of what was decided and paste it into the **Previous Decisions** section of the next prompt, so context is never lost.
> 4. Do not move to the next prompt until the current one is final.

---

## 🔁 Common Context (paste at the top of EVERY prompt)

```
You are a senior data engineering architect. I am building a production-ready,
scalable, config-driven ETL platform. It must plug into my EXISTING microservices
project (20+ Spring Boot microservices, Kafka already configured).

Tech stack:
- Pipeline processing: Apache Spark (Java)
- Application framework: Spring Boot
- Databases (Source / Bronze / Silver / Gold): MongoDB + EDB Postgres Advanced Server (EPAS)
- Ingestion (Source → Bronze): Kafka Connect + MongoDB Source Connector → ONE shared
  Kafka topic for ALL collections (each event carries metadata: source service, db,
  collection, operation, document key, event time) → custom customizable
  consumer/producer that routes events by that metadata → Bronze
- Scheduling & orchestration: Apache Airflow
- Data governance & lineage: OpenMetadata or DataHub

Data flow (Medallion architecture):
Original DB ──(Kafka Connect + Mongo Connector)──► ONE shared CDC Kafka topic
   (all collections + metadata)
   ──► Custom Consumer/Producer (route by collection, enrich object + add metadata
       columns, soft delete) ──► BRONZE
BRONZE ──(Spark ETL: filter, validate, dedupe, cleanse, normalize/split tables,
          dimensional modeling where needed)──► SILVER
SILVER ──(Spark ETL: aggregate, dimensional modeling: facts + dimensions)──► GOLD

Key facts:
- Number of jobs is unknown: could be 20, could be 100+. The architecture must not
  become messy as pipelines grow.
- Jobs run in 2 main modes: manual full load (user triggered) and scheduled incremental
  (every 1 hr / 5 hr / daily, etc., not fixed yet, must be configurable).
- Recovery/re-run modes are also required: rerun failed job, rerun only failed
  records (partial), reprocess quarantined records after fix, backfill a date range,
  replay from Kafka/Bronze after data loss, reprocess specific IDs, dependency/event
  triggered runs, cascade rerun of downstream jobs, and dry-run (validate only).
- Jobs can depend on other jobs.
- Each record's success/failure must be written back to the previous layer's
  metadata columns (Bronze gets updated after Silver sync; Silver after Gold sync).
- Every environment (dev/QA/UAT/prod) can have different DB, Spark cluster, Kafka
  and Airflow properties.

Rules for your answer:
- Give the recommended design WITH reasoning, trade-offs and alternatives.
- Give folder structures, sample configs (YAML/JSON), SQL DDL and Java code
  skeletons where relevant.
- If you see any gap, risk, missing technique or better approach that I did not
  mention, ADD IT and clearly mark it as "➕ Added by architect".
- Write your answer in a .md file.

Previous decisions (from earlier prompts):
<paste summary here — leave empty for Prompt 1>
```

---

## Prompt 1 — High-Level Architecture & Project Structure

```
[Paste Common Context]

TASK: Design the high-level architecture and the project structure.

1. Give a high-level architecture diagram (ASCII/text) showing every component:
   Source DB, Kafka Connect, Kafka topics, custom consumer/producer, Bronze,
   Spark jobs, Silver, Gold, Airflow, OpenMetadata/DataHub, monitoring, audit DB,
   operational API/UI.
2. Monorepo vs separate repo: should this ETL project live inside my existing
   microservices repo or in a separate repo? Give pros/cons and a recommendation.
3. Give a full folder structure, including:
   - WHERE to put Kafka Connect / Mongo connector configuration files in my
     existing project structure (per environment)
   - the custom consumer/producer module
   - a multi-module Maven/Gradle layout (e.g. etl-core, etl-common, etl-ingestion,
     etl-jobs, etl-config, etl-orchestration/airflow-dags, etl-ops-api)
   - job config files, DQ rule files, SQL/DDL scripts, Airflow DAGs, environment
     configs, tests, CI/CD files
4. Explain each module's responsibility and how the modules depend on each other.
5. Recommend OpenMetadata vs DataHub for this stack, with justification
   (details come later in Prompt 10).

OUTPUT: architecture diagram, repo decision, full folder tree with a one-line
description per folder, module dependency diagram.
```

---

## Prompt 2 — Ingestion Layer (Source DB → Kafka → Bronze)

```
[Paste Common Context]

TASK: Design the ingestion layer from the original DB to Bronze.

1. Kafka Connect + MongoDB Source Connector setup:
   - ALL collections are published into ONE shared topic (not a topic per
     collection), with metadata in every event (source service, db, collection,
     operation type, document key, event time)
   - sample connector config (one connector per source DB) using
     topic.namespace.map, the change-stream pipeline ($match for collections,
     $addFields for sourceService) and the full change-event envelope, plus
     per-environment variants
   - shared topic design: message key (collection + documentKey) for
     per-document ordering, partition count, retention, compaction
   - trade-offs of one shared topic (noisy neighbour, per-collection lag,
     replaying one collection, ACL/PII per collection) and mitigations,
     including moving a hot/PII collection to its own topic by config only
   - full snapshot (initial load) vs streaming change events
2. Custom consumer/producer (must be CUSTOMIZABLE and reusable):
   - reads the shared topic and ROUTES each event to a per-collection handler using
     the event metadata; config-driven (no new code for each new collection);
     unknown/unmapped collections go to the DLQ
   - it takes the original object, manipulates/enriches it, and adds metadata columns
   - writes to Bronze (Mongo and/or EPAS: recommend which)
   - Spring Boot design: interfaces, handlers, mapper/enricher strategy, sample code
3. Soft delete: when data is deleted in the original DB, mark it in Bronze with a
   flag (isDeleted = true + deletedAt) instead of a physical delete.
4. Must handle:
   - CDC event ordering, out-of-order events, duplicate events
   - Kafka offset management / commit strategy
   - idempotent writes (upsert by sourceId + version/offset)
   - at-least-once vs exactly-once: recommendation
   - Dead Letter Queue (DLQ) for messages that cannot be parsed or processed
   - schema evolution (fields added/removed in source docs); is a Schema Registry
     needed? (many record types in one topic → subject naming strategy)
   - per-collection metrics (throughput, errors, lag) even though Kafka sees one topic
   - backpressure, consumer lag, retries, poison messages
   - REPLAY for data-loss recovery: replay from a Kafka offset/timestamp (within
     topic retention), re-push DLQ messages after a fix, and rebuild Bronze from
     Kafka without creating duplicates. What topic retention is needed to support this?
5. Bronze metadata/audit columns. My initial list:
   - isDeleted
   - syncSilver (status: PENDING / SUCCESS / FAILED / PARTIAL / QUARANTINED)
   - errorMessageWhileSync
   - syncUpdatedDate
   Review this, improve it, and add missing columns with purpose + data type.
   Consider: sourceSystem, sourceCollection, sourceId, kafkaTopic, kafkaPartition,
   kafkaOffset, cdcOperation (INSERT/UPDATE/DELETE), sourceEventTs, ingestedAt,
   recordHash (change detection + dedupe), version, batchId/runId, retryCount,
   errorCode, lastSyncJobName, syncGold (if needed), isQuarantined, deletedAt.
   Also explain HOW and BY WHICH component these columns get updated
   (the ingestion layer vs the Silver ETL job).

OUTPUT: connector configs, consumer/producer design + code skeleton, final Bronze
column list (name, type, purpose, who updates it), error/DLQ flow diagram.
```

---

## Prompt 3 — Bronze → Silver ETL (Transformation + Table Division + Dimensional Modeling)

```
[Paste Common Context]

TASK: Design the Bronze → Silver ETL process in Spark (Java).

1. Processing steps (in order) with Spark Java code skeletons:
   - read Bronze (incremental: only syncSilver = PENDING/FAILED-retryable, or by
     watermark; full load: everything)
   - data filtering
   - data validation: schema, null checks, data types, formats, ranges, referential
     checks, business rules, using a REUSABLE rule-based DQ framework (rules
     defined in config, not hard-coded)
   - duplicate detection & removal (by business key / recordHash / latest version)
   - standardization, cleansing, type casting, trimming, enrichment/lookups
   - soft-delete propagation (isDeleted records handled downstream)
   - write to Silver (merge/upsert strategy)
2. TABLE DIVISION / NORMALIZATION in Silver:
   - Bronze Mongo documents are often nested (arrays, sub-documents). How should
     they be split into multiple Silver tables (parent/child tables, flattening
     arrays, exploding nested objects)?
   - key generation and relationships between the split tables
3. DIMENSIONAL MODELING IN SILVER (where applicable):
   - which master/reference entities should become dimension-like tables in Silver
     vs. being done only in Gold?
   - surrogate keys vs natural keys at the Silver level
   - SCD handling in Silver (Type 1 / Type 2): when to use which
   - conformed/shared dimensions across multiple source collections
4. Error handling:
   - record-level errors (bad record → quarantine/error table, job continues)
   - job-level errors (infra failure → job fails)
   - write the result back to Bronze for EACH record: syncSilver status,
     errorMessageWhileSync, errorCode, syncUpdatedDate, retryCount, runId
   - efficient way to write status back to Bronze at scale (bulk update, not row-by-row)
5. Reconciliation: Bronze read count = Silver written + failed + skipped + quarantined.
6. Performance: partitioning, Mongo/JDBC read parallelism, caching, broadcast
   joins, avoiding small files/small batches.

OUTPUT: step-by-step flow diagram, Silver table design rules (with an example of
splitting a nested document), DQ framework design, Spark Java skeleton,
Bronze write-back logic.
```

---

## Prompt 4 — Silver → Gold ETL (Dimensional Modeling: Facts & Dimensions)

```
[Paste Common Context]

TASK: Design the Silver → Gold ETL process in Spark (Java).

1. Apply the same controls as Bronze → Silver: filtering, validation, dedupe,
   error handling, incremental/full, reconciliation, and write-back of status to
   Silver metadata columns (syncGold, errorMessage, syncUpdatedDate, runId, etc.).
   Define which metadata columns Silver tables need for this.
2. Dimensional modeling in Gold:
   - star vs snowflake schema: recommendation
   - fact table types (transactional, periodic snapshot, accumulating snapshot)
   - dimension tables, surrogate key generation strategy in Spark + EPAS
   - SCD Type 1 / Type 2 / Type 3 implementation in Spark (merge logic + sample code)
   - conformed dimensions, junk dimensions, degenerate dimensions, date/time dimension
   - late-arriving dimensions (inferred/unknown members) and late-arriving facts
   - load order: dimensions first, then facts; dependency handling
3. Aggregations / business KPIs / summary tables.
4. Soft-delete handling in facts/dimensions.
5. Naming conventions (dim_*, fact_*, agg_*), keys, partitioning and indexing
   strategy in EPAS and Mongo for Gold.
6. Should Gold be in EPAS, Mongo or both? Recommendation with reasons.

OUTPUT: sample star schema (ASCII), DDL for one fact + dimensions, SCD2 merge
skeleton in Spark Java, load-order diagram, naming convention table.
```

---

## Prompt 5 — Staging, Audit, Quarantine & Control Tables

```
[Paste Common Context]

TASK: Design all supporting tables (control/metadata/audit layer).

1. Staging tables:
   - are staging tables required for EACH job? When to use staging vs direct
     merge/upsert?
   - staging lifecycle (truncate/load/swap/cleanup)
2. Audit and control tables. Give DDL + purpose for each:
   - job_registry / job_config: job name, type, layer, source, target, mode,
     schedule, dependencies, thresholds, active flag, owner
   - job_dependency
   - job_run_audit: runId, jobName, runMode/triggerType (FULL_LOAD / SCHEDULED /
     RERUN_JOB / RERUN_FAILED_RECORDS / REPROCESS_QUARANTINE / BACKFILL / REPLAY /
     ID_REPROCESS / DEPENDENCY_TRIGGERED / CASCADE / DRY_RUN), parentRunId (the
     original run that this rerun fixes), run parameters (from/to, ids), triggeredBy,
     start/end time, status, records read/written/failed/skipped/quarantined,
     failure reason, error code, Spark appId
   - job_step_audit (optional, step-level timing/counts)
   - record_error_log
   - quarantine table(s): one generic table or one per entity? Include a status
     lifecycle (QUARANTINED → FIXED → READY_TO_REPROCESS → REPROCESSED / REJECTED),
     fixedBy, fixedAt, reprocessRunId
   - watermark / checkpoint table (for incremental loads)
   - data_quality_results (rule, table, passed/failed counts per run)
   - reconciliation table (source vs target counts and checksums per run)
   - error_code catalog (standard codes + descriptions + retryable flag)
   - job_lock table (to stop the same job running twice)
3. Which DB should hold these tables (EPAS recommended?) and why.
4. Data retention & archival policy for each layer (Bronze/Silver/Gold/audit/
   quarantine/DLQ).

OUTPUT: ER diagram (ASCII), DDL for every table, retention policy table.
```

---

## Prompt 6 — Reusable, Config-Driven Framework (Scales to 100+ Jobs)

```
[Paste Common Context]

TASK: Design the reusable code framework so that 20 or 100+ jobs never turn
into messy, duplicated code.

1. Metadata/config-driven pipelines: adding a new job = adding config
   (YAML or DB rows in job_registry) with no or minimal new Java code.
   Show a sample job config file.
2. Common framework components (interfaces + base classes):
   Reader, Writer, Validator (DQ rule engine), Transformer, Deduplicator,
   Normalizer/TableSplitter, SCD handler, Auditor, ErrorHandler/QuarantineWriter,
   StatusWriteBack (to previous layer), WatermarkManager, LineageEmitter,
   MetricsPublisher, LockManager.
3. Design patterns: Template Method (job lifecycle), Strategy (read/write/merge
   modes), Factory/Registry (job lookup by name), Builder (pipeline steps),
   plugin approach for custom transformations.
4. Job types: Bronze→Silver, Silver→Gold. Run modes: full load, incremental,
   rerun job, rerun failed records, reprocess quarantine, backfill, replay,
   ID reprocess, dry-run. How the framework switches reader/filter logic per run mode
   while the transform/validate/write steps stay the same.
5. A single generic Spark entry point (e.g. `EtlJobRunner --job=<name>
   --mode=<full|incremental|rerun-job|rerun-failed|reprocess-quarantine|backfill|replay|ids|dry-run>
   --env=<env> --from --to --ids=<list> --parentRunId=<id>`) vs one main class per
   job: recommendation.
6. Spring Boot + Spark integration: should Spring be used inside Spark jobs or
   only for the ops API? Trade-offs.
7. Versioning, packaging (fat/shaded JAR), dependency conflicts (Spark vs Spring),
   and deployment of Spark jobs.
8. Coding standards, logging standard (with runId correlation), exception hierarchy.

OUTPUT: class diagram (ASCII), Java interface/abstract class skeletons, sample
job config YAML, how-to-add-a-new-job checklist.
```

---

## Prompt 7 — Job Lifecycle, Status Rules, Incremental/Full Load, Failure & Rerun

```
[Paste Common Context]

TASK: Design job execution behaviour end to end.

1. RUN MODES: for each mode, explain when to use it, what data it picks, how it
   avoids duplicates, and how it is recorded in job_run_audit:
   - FULL LOAD: triggered manually by a user (via ops API/UI or Airflow, with parameters)
   - SCHEDULED INCREMENTAL: every 1 hr / 5 hr / daily, configurable per job
   - RERUN FAILED JOB: the whole job failed (infra/DB down) → resume from the last
     saved watermark/checkpoint
   - RERUN FAILED RECORDS ONLY: the job was PARTIAL → pick only records with
     syncSilver = FAILED (retryable), without reprocessing everything
   - REPROCESS QUARANTINE: records that failed sanitization/validation are fixed,
     marked READY_TO_REPROCESS, then processed again
   - BACKFILL / DATE-RANGE: re-run only a given period (--from --to)
   - REPLAY (data loss recovery): rebuild a layer from Kafka offsets or from the
     previous layer (Bronze → Silver → Gold); soft delete in Bronze keeps history
   - SINGLE ENTITY / ID REPROCESS: re-run for a specific list of sourceIds
   - DEPENDENCY / EVENT TRIGGERED: run automatically when an upstream job finishes
   - CASCADE RERUN: when an upstream layer is re-run, re-run dependent downstream jobs
   - DRY-RUN / VALIDATE-ONLY: read + validate + show counts, write nothing
   Also: who is allowed to trigger each mode, and whether a rerun creates a new runId
   linked to the original run (parentRunId).
2. Incremental strategy: mainly driven by Bronze sync columns (syncSilver status
   + syncUpdatedDate). Also evaluate combining with:
   - watermark/checkpoint on ingestedAt/updatedAt
   - recordHash for change detection
   - late-arriving and reprocessed records
   - switching from full load to incremental (and resetting the watermark)
3. Job status rules: suggest clear, CONFIGURABLE rules:
   - SUCCESS: all records processed, 0 failures (or below a tolerance %)
   - PARTIAL (PARTIALLY_FAILED): some records failed validation/transform but the
     job finished; failures below a threshold
   - FAILED: system/infra error, a stopping exception, a failed dependency, or
     failures above the threshold
   - should thresholds be per job (count and/or %)?
   - how to store the status + REASON (validation, schema mismatch, connection,
     timeout, dependency failed, etc.) using the error code catalog
   - full status lifecycle: QUEUED → RUNNING → SUCCESS / PARTIAL / FAILED / SKIPPED / CANCELLED
4. Even if ONE record fails, it must be counted (failed/partial counts must be accurate).
5. Failed & sensitive (PII) data handling:
   - should failed records go to a separate quarantine table (encrypted/masked),
     get a status flag in Bronze, or both? Recommendation.
   - workflow to CORRECT, RE-VALIDATE and REPROCESS quarantined records
     (manual fix via UI/API, auto retry for retryable errors, approval step, max retries)
   - PII masking/encryption, and who can view quarantine data
6. Job dependencies: upstream failure/partial → should downstream run, skip or wait?
7. Concurrency control (locking), retries with backoff, timeouts, SLA.
8. Idempotency: safe re-run of any job with no duplicates.

OUTPUT: state machine diagram, status decision table, incremental logic
pseudo-code, quarantine → fix → reprocess flow diagram, rerun API design.
```

---

## Prompt 8 — Environment-Based Configuration & Secrets

```
[Paste Common Context]

TASK: Design environment-specific configuration (dev / QA / UAT / prod).

1. What changes per environment:
   - DB connections (Mongo, EPAS)
   - Spark cluster properties (master/deploy mode, executors, memory, cores,
     shuffle partitions, dynamic allocation)
   - Kafka / Kafka Connect / connector properties
   - Airflow properties (schedules, connections, variables, pools, queues)
   - job-level overrides (thresholds, schedules, batch sizes)
2. Config layering: default → environment → job-level override.
   Spring profiles (application-{env}.yml), external config server, or
   Kubernetes ConfigMaps: recommendation.
3. How Spark jobs receive the environment config (spark-submit args, config files,
   config DB).
4. Secrets management: Vault / Kubernetes Secrets / Airflow Connections.
   No credentials in code or Git.
5. Folder layout for environment configs, and how CI/CD picks the right one.

OUTPUT: config folder tree, sample YAML for dev and prod, config precedence
diagram, secrets flow.
```

---

## Prompt 9 — Airflow Orchestration

```
[Paste Common Context]

TASK: Design Airflow orchestration for a growing number of jobs (20 → 100+).

1. DAG strategy: one DAG per job, one per domain/layer, or DAGs dynamically
   generated from job_registry/config: recommendation for scale.
2. How Airflow triggers Spark: SparkSubmitOperator / KubernetesPodOperator /
   Livy / REST: recommendation.
3. Dependencies: within a DAG, cross-DAG (ExternalTaskSensor / Datasets),
   Bronze→Silver→Gold chaining, dependency/event-triggered runs, and CASCADE rerun
   (re-running an upstream job automatically re-triggers dependent downstream jobs).
4. Manual trigger with parameters for every run mode: full load, rerun failed job,
   rerun failed records, reprocess quarantine, backfill (date range), replay,
   specific IDs, dry-run. One generic "manual run" DAG vs parameters on each DAG:
   recommendation.
5. Environment-based DAG properties (schedule, connections, pools, retries)
   and the same DAG code for all environments.
6. Retries, SLA, timeouts, on_failure/on_success callbacks, alert integration.
7. Concurrency, pools, priority, and catchup/backfill behaviour.
8. How Airflow task status maps to job_run_audit status (SUCCESS/PARTIAL/FAILED);
   PARTIAL should not show as a plain green success.

OUTPUT: DAG folder structure, sample dynamic DAG generator (Python), sample
env config for DAGs, dependency diagram.
```

---

## Prompt 10 — Data Governance & Lineage

```
[Paste Common Context]

TASK: Design data governance and lineage integration.

1. Final choice: OpenMetadata vs DataHub for this stack (Spark Java, Kafka,
   Mongo, EPAS, Airflow), with justification.
2. How Spark jobs emit lineage: OpenLineage Spark listener, native agent, or a
   custom LineageEmitter in the framework.
3. Table-level AND column-level lineage: Source → Kafka → Bronze → Silver → Gold
   (including split/normalized tables and dimensions).
4. Airflow + Kafka + DB metadata ingestion into the governance tool.
5. Data catalog, ownership, glossary, domains, PII tagging & classification.
6. Publishing data quality results and job run status to the governance tool.
7. Data contracts between source teams (microservices) and the ETL team.

OUTPUT: lineage flow diagram, integration config samples, governance checklist.
```

---

## Prompt 11 — Production Readiness (Monitoring, Security, CI/CD, Operations)

```
[Paste Common Context]

TASK: Make the whole platform production-ready. Cover each item below and ADD
anything missing.

1. Monitoring: Spark metrics, job metrics (counts, duration, failure %), Kafka
   consumer lag, DB health (Prometheus + Grafana or similar); suggested dashboards.
2. Alerting: Slack/Email/Teams for FAILED, PARTIAL, SLA breach, lag spikes,
   DQ threshold breaches.
3. Logging & debugging: centralized structured logs (ELK/Loki), correlation via
   runId/batchId, Spark UI/history server, and how to debug one failed record end to end.
4. Security: RBAC, encryption at rest/in transit, PII masking, network isolation,
   audit trail of who triggered/re-ran what.
5. Data reconciliation & data quality reports between layers.
6. Testing: unit tests for transforms, integration tests (Testcontainers for
   Kafka/Mongo/Postgres), data contract tests, test datasets, regression testing.
7. CI/CD: build, test and deploy Spark JARs, connector configs and Airflow DAGs
   per environment; versioning & rollback.
8. Performance & cost: dynamic allocation, partitioning, small-file handling,
   Airflow pools/queues, resource quotas.
9. Disaster recovery: backups, restore, replay from Kafka, re-run from a watermark.
10. Operational API/UI (Spring Boot service): view job status/history (with the
    rerun chain via parentRunId), view/fix quarantine data, view DQ results, and
    endpoints for every run mode: full load, rerun failed job, rerun failed records,
    reprocess quarantine, backfill, replay, ID reprocess, dry-run. Include role-based
    permissions for who can trigger which mode.
11. Runbooks: common failure scenarios and the steps to fix them.

OUTPUT: production readiness checklist, monitoring/alerting matrix, CI/CD
pipeline diagram, ops API endpoint list.
```

---

## Final Prompt — Consolidation & Gap Review

```
[Paste Common Context]

Previous decisions: <paste summaries from Prompts 1–11>

TASK:
1. Combine all decisions into one final architecture document.
2. Check for conflicts or inconsistencies between the earlier answers and resolve them.
3. List the gaps, risks and recommendations I still missed.
4. Give a phased implementation roadmap (MVP → Phase 2 → Phase 3) with priorities.
```
