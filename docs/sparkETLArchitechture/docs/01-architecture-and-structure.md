# 01 — High-Level Architecture & Project Structure

> Scope: overall architecture, repo strategy, folder structure, module responsibilities and dependencies, and the governance tool choice.
> The detailed design of each area comes in later prompts (ingestion → 02, Silver → 03, Gold → 04, control tables → 05, framework → 06, run modes → 07, env config → 08, Airflow → 09, governance → 10, production → 11).

---

## 1. Design Principles

These principles drive every decision below and in the later documents.

| # | Principle | What it means in practice |
|---|---|---|
| P1 | **Config over code** | A new pipeline is a YAML file plus DQ rules. Java code is written only for custom transformations (plugins). This is what keeps 100+ jobs manageable. |
| P2 | **One engine, many jobs** | One Spark application JAR with one generic entry point (`EtlJobRunner`). Jobs are selected by `--job=<name>`. No main class per job. |
| P3 | **Separate control plane from data plane** | Job registry, run audit, watermarks, locks, DQ results and quarantine metadata live in a dedicated **control DB**, not mixed into Bronze/Silver/Gold. |
| P4 | **Idempotent everything** | Every write is an upsert/merge keyed by business key + version. Any job, in any run mode, can be re-run safely. |
| P5 | **Record-level traceability** | Every record carries `runId`/`batchId` and sync status, so one bad record can be traced end to end (Kafka offset → Bronze → Silver → Gold). |
| P6 | **Environment-agnostic artifacts** | The same JAR, Docker image and DAG code is promoted dev → QA → UAT → prod. Only config and secrets change. |
| P7 | **No Spring inside Spark executors** | Spring Boot is used for long-running services (ingestion, ops API). Spark jobs use plain Java + a light config loader, which avoids dependency conflicts and slow startup. |

---

## 2. High-Level Architecture Diagram

```
┌──────────────────────── SOURCE SYSTEMS (existing, 20+ Spring Boot microservices) ────────────────────────┐
│   svc-order (MongoDB)      svc-customer (MongoDB)      svc-payment (MongoDB)      svc-... (MongoDB/EPAS)   │
└───────────────┬──────────────────────────────────────────────────────────────────────────────────────────┘
                │  MongoDB change streams (oplog)
                ▼
┌──────────────────────────────── KAFKA PLATFORM (existing cluster) ───────────────────────────────────────┐
│  Kafka Connect (distributed)                                                                              │
│    └─ MongoDB Source Connector  (1 connector per source DB; ALL collections → ONE shared topic)           │
│                    │                                                                                       │
│                    ▼                                                                                       │
│  Topics:  cdc.source.events                    ONE topic for ALL collections (raw change events)          │
│                                                + metadata: sourceService, db, coll, op, eventTs           │
│                                                key = <svc>:<coll>:<docId> (per-doc ordering)              │
│           dlq.ingestion                        messages that cannot be parsed/processed                   │
│           etl.events.bronze-loaded (optional)  "new data landed" signal for event-triggered runs          │
│  ➕ Schema Registry  (optional, phase 2; phase 1 = data contracts + drift detection, see doc 02)          │
└───────────────┬──────────────────────────────────────────────────────────────────────────────────────────┘
                │ consume (one consumer group; route each event by its collection metadata)
                ▼
┌──────────────────── etl-ingestion  (Spring Boot service, deployed like other microservices) ─────────────┐
│  consume → deserialize → route by collection → handler → enrich (+ metadata columns) → soft delete        │
│  → idempotent upsert into Bronze → commit offset          errors → DLQ topic + ingestion error log         │
└───────────────┬──────────────────────────────────────────────────────────────────────────────────────────┘
                ▼
        ┌──────────────────┐   Spark batch job (B→S)        ┌──────────────────┐   Spark batch job (S→G)   ┌──────────────────┐
        │ BRONZE           │ ─────────────────────────────► │ SILVER           │ ────────────────────────► │ GOLD             │
        │ MongoDB          │ filter · validate · dedupe ·   │ EPAS             │ aggregate · facts ·       │ EPAS             │
        │ (source-shaped   │ cleanse · split nested docs ·  │ (cleansed,       │ dimensions · SCD ·        │ (star schema,    │
        │  docs + metadata)│ early dimensions               │  normalized)     │ KPIs                      │  marts, aggs)    │
        └──────────────────┘ ◄── status write-back ──────── └──────────────────┘ ◄── status write-back ──── └──────────────────┘
                 ▲                                                  ▲                                             ▲
                 │                     ┌────────────────────────────┴─────────────────────────────┐               │
                 │                     │  SPARK CLUSTER (K8s Spark Operator / Standalone / YARN)    │───────────────┘
                 └─────────────────────│  etl-spark-app.jar  →  EtlJobRunner --job=<name> --mode=…  │
                                       │  reads pipelines/*.yml + config/environments/<env>/*       │
                                       └───────────┬──────────────────────────┬─────────────────────┘
                     spark-submit / SparkApplication│                          │ writes run audit, DQ results,
                                       ┌────────────┴───────────┐  ┌──────────▼─────────────────────────┐
                                       │ APACHE AIRFLOW          │  │ CONTROL DB (EPAS, schema etl_ctl)   │
                                       │ DAGs generated from job │  │ job_registry · job_dependency ·     │
                                       │ configs · schedules ·   │  │ job_run_audit · watermark ·         │
                                       │ dependencies · reruns   │  │ job_lock · quarantine · dq_results ·│
                                       └────────────▲───────────┘  │ reconciliation · error_code_catalog │
                                                    │ REST API     └──────────▲─────────────────────────┘
                                       ┌────────────┴─────────────────────────┴───────┐
                                       │ etl-ops-api (Spring Boot) + Ops UI            │
                                       │ job status/history · trigger any run mode ·   │
                                       │ fix & reprocess quarantine · DQ reports · RBAC│
                                       └───────────────────────────────────────────────┘

 Cross-cutting:
 ┌─────────────────────────────┐  ┌──────────────────────────────────────┐  ┌──────────────────────────────┐
 │ OPENMETADATA (governance)   │  │ MONITORING & ALERTING                 │  │ SECURITY                      │
 │ catalog · lineage (Spark via│  │ Prometheus · Grafana · Loki/ELK ·     │  │ Vault / K8s Secrets · RBAC ·  │
 │ OpenLineage) · DQ · PII tags│  │ Kafka lag exporter · Alertmanager →   │  │ TLS · PII masking · audit log │
 │ · glossary · ownership      │  │ Slack/Email/Teams                     │  │                                │
 └─────────────────────────────┘  └──────────────────────────────────────┘  └──────────────────────────────┘
```

### 2.1 Layer storage decision (preliminary, finalized in 02/03/04)

| Layer | Store | Why |
|---|---|---|
| Bronze | **MongoDB** | Source data is Mongo documents. Keeping the same shape means no schema mapping at ingestion, schema evolution is painless, and replay is simple. |
| Silver | **EPAS** | Cleansed, typed, normalized data fits relational tables well: constraints, joins, MERGE/upsert, indexing. |
| Gold | **EPAS** | Star schema, SQL access for BI tools, partitioning by date. |
| Control | **EPAS** (separate schema/DB `etl_ctl`) | Transactional (locks, status updates), easy to query from the ops API, Airflow and BI. |

**Alternative:** Bronze in EPAS as `JSONB`. That gives one DB technology for every layer, but you lose the native Mongo shape, and huge JSONB tables are harder to tune. Keep it as a fallback if Mongo ops capacity is a concern.

### 2.2 Runtime shape of each component

| Component | Runtime type | Deployed as |
|---|---|---|
| Kafka Connect + Mongo connector | Long-running | Existing Kafka Connect cluster (connector JSON pushed via REST) |
| etl-ingestion | Long-running Spring Boot | K8s Deployment / same platform as your microservices (scaled by topic partitions) |
| Spark jobs | Batch, short-lived | `spark-submit` or Spark Operator `SparkApplication`, triggered by Airflow |
| Airflow | Long-running scheduler | Existing or new Airflow (K8s Helm chart / managed) |
| etl-ops-api | Long-running Spring Boot | K8s Deployment |
| OpenMetadata | Long-running | Helm chart; reuses Airflow for metadata ingestion |

### 2.3 Ingestion flow choice

```
Option A (recommended):  cdc topic ──► etl-ingestion ──► Bronze (direct idempotent upsert)
Option B:                cdc topic ──► etl-ingestion ──► enriched topic ──► Mongo Sink Connector ──► Bronze
```

| | Option A: direct write | Option B: enriched topic + sink |
|---|---|---|
| Hops / latency | Fewer | One more topic, plus one more connector |
| Soft-delete and custom upsert logic | Full control in Java | Must fit the sink connector's write strategies |
| Error handling | One place (service + DLQ) | Split between service and sink connector |
| Re-use of enriched stream | No | Yes (other consumers can read enriched events) |

**Recommendation: Option A.** Optionally, publish a small `etl.events.bronze-loaded` event (batch-level, not per record) to support event-triggered Spark runs. Details are in Prompt 02.

### 2.4 CDC topic strategy: ONE shared topic for all collections (decided)

All collections from all source DBs are published into **one shared topic** (`cdc.source.events`). Every event carries metadata that tells the consumer where it came from. The consumer routes each event to the right handler using that metadata.

**Event metadata carried in every message**

| Field | Source | Purpose |
|---|---|---|
| `ns.db`, `ns.coll` | Native change-stream field | Routing key for the consumer: which handler / Bronze collection |
| `operationType` | Native | insert / update / replace / delete → upsert or soft delete |
| `documentKey._id` | Native | Business identity of the record (also used in the Kafka message key) |
| `clusterTime` / `wallTime` | Native | Event time for ordering and late-event checks |
| `fullDocument` | Native (with `updateLookup`) | The full document for inserts/updates; null for deletes |
| `sourceService` | Added by the connector pipeline (`$addFields`) | Which microservice owns the data (two services can have the same collection name) |
| `_id` (resume token) | Native | Debugging / exactly where in the change stream the event came from |

**How it is configured (one connector per source DB, all mapped to the same topic)**

```json
{
  "name": "mongo-source-order-db",
  "config": {
    "connector.class": "com.mongodb.kafka.connect.MongoSourceConnector",
    "connection.uri": "${secret:mongo.order.uri}",
    "database": "order_db",
    "pipeline": "[{\"$match\": {\"ns.coll\": {\"$in\": [\"orders\", \"order_items\"]}}}, {\"$addFields\": {\"sourceService\": \"svc-order\", \"cdcKey\": {\"$concat\": [\"svc-order:\", \"$ns.coll\", \":\", {\"$toString\": \"$documentKey._id\"}]}}}]",
    "topic.namespace.map": "{\"*\": \"cdc.source.events\"}",
    "change.stream.full.document": "updateLookup",
    "publish.full.document.only": "false",
    "output.format.key": "schema",
    "output.schema.key": "{\"type\":\"record\",\"name\":\"CdcKey\",\"fields\":[{\"name\":\"cdcKey\",\"type\":\"string\"}]}",
    "output.format.value": "json",
    "startup.mode": "copy_existing",
    "errors.tolerance": "none"
  }
}
```

Important points:
- `publish.full.document.only = false` keeps the **full change-event envelope**. Without it, `ns`, `operationType` and delete events are lost, and soft delete becomes impossible.
- ➕ **Message key = `sourceService:collection:documentId`** (`cdcKey`, built in the pipeline), not the connector default. By default the key is the change-stream resume token, which spreads events of the same document across partitions and **breaks per-document ordering**. With the custom key, all changes of one document land on the same partition, in order.
- `topic.namespace.map` with `"*"` sends every collection of that DB to the shared topic. The `$match` stage in `pipeline` controls which collections are captured.
- Exact property names (`startup.mode` vs older `copy.existing`, key projection syntax) depend on the connector version. Verify them in Prompt 02.

**Trade-offs of one shared topic vs topic per collection**

| | One shared topic (chosen) | Topic per collection |
|---|---|---|
| Ops | One topic, one set of ACLs, one consumer group. A new collection needs **no new topic**. | Many topics to create, monitor and secure |
| Consumer code | One listener + router | One listener per topic (or pattern subscription) |
| Noisy neighbour | ⚠ A hot/bulk collection can delay events of other collections | Isolated |
| Per-collection retention / compaction | ⚠ Not possible; one policy for all | Possible |
| Replay of ONE collection | ⚠ Must read the whole topic and filter | Read only that topic |
| Security / PII | ⚠ Topic-level ACLs cannot restrict access per collection | Per-topic ACLs |
| Schema Registry | ⚠ Many record types in one topic → needs `RecordNameStrategy` / `TopicRecordNameStrategy`, or a generic JSON envelope | Default strategy works |
| Lag monitoring | ⚠ Kafka lag is per topic/partition; per-collection lag needs custom metrics | Native per topic |

**➕ Mitigations (built into the design)**
1. **Enough partitions** (e.g. 24–48, sized in Prompt 02) and the `service:coll:docId` key, so load spreads evenly and ordering holds per document.
2. **Consumer metrics tagged by `sourceService` / `collection`** (throughput, errors, event-time lag), so lag is visible per collection even though Kafka sees one topic.
3. **Bronze is the main replay source** for a single collection. Kafka replay of the full topic is used only for real data-loss recovery within retention.
4. **Escape hatch, config only:** if one collection becomes too hot or too sensitive (PII), add one entry to `topic.namespace.map` to send it to its own topic (e.g. `cdc.source.events.payments`). The consumer subscribes with a topic **pattern** (`cdc\.source\.events.*`) and the router works on metadata, so no code change is needed.
5. **Consumer router is config-driven:** `(sourceService, db, collection) → handler → Bronze collection` in `ingestion-mappings.yml`. Unknown collections go to the DLQ with error code `UNMAPPED_COLLECTION`, never silently dropped.

---

## 3. Monorepo vs Separate Repo

### 3.1 Options

| Option | Pros | Cons |
|---|---|---|
| **A. Inside the existing microservices repo** | One place for everything; Kafka config already there; shared CI | Spark (Scala 2.12/2.13, Jackson, Guava, Netty versions) conflicts with Spring Boot dependency management; release cycles differ (ETL changes daily, services don't); Python (Airflow) mixed into a Java service repo; CI builds slow down for everyone; unclear ownership |
| **B. Separate `data-platform` repo** (recommended) | Independent release cycle and versioning; clean dependency management for Spark; one home for Java + Python + SQL + connector configs; clear ownership (CODEOWNERS per domain); its own CI/CD | Source schema changes in microservices are not visible in the same PR (solved with data contracts) |
| **C. Hybrid** | Connector configs stay in the existing repo's Kafka/infra folder; everything else goes to the ETL repo | Connector and consumer changes are split across two repos and must be released together |

### 3.2 Recommendation: Option B (separate `data-platform` repo)

- The ETL repo owns **everything that moves data**: connector configs, the ingestion service, Spark jobs, DAGs, DDL, governance and monitoring configs.
- ➕ **Data contracts** bridge the two repos. Each source microservice publishes (or the ETL team maintains) a contract file per collection: fields, types, owner, PII flags. CI in the ETL repo validates incoming CDC schemas against it, and the Schema Registry rejects incompatible changes.
- ➕ A shared library (if needed, e.g. common DTOs or event envelope) is published as a versioned Maven artifact, never copied between repos.

### 3.3 If you must keep connector configs in the existing repo (Option C)

Place them next to your existing Kafka configuration, mirroring the same structure used in the ETL repo:

```
<existing-microservices-repo>/
└── infra/
    └── kafka/
        ├── topics/                          # (already existing) topic definitions
        └── connect/
            ├── connectors/
            │   ├── base/                    # env-independent connector templates
            │   │   └── mongo-source-order-db.json
            │   └── overlays/
            │       ├── dev/  qa/  uat/  prod/   # only env-specific overrides
            └── scripts/deploy-connectors.sh
```

Even in this case, keep **topic names and the connector → collection mapping** documented in the ETL repo (`kafka-connect/README.md`), because the ingestion service depends on them.

---

## 4. Build Tool

**Maven multi-module** is the default, because your 20+ Spring Boot services most likely use it and the team already knows it. Gradle works equally well. The structure below maps 1:1 to Gradle subprojects.

Key build points:
- The parent `pom.xml` holds a `dependencyManagement` BOM: Spark, Mongo Spark connector, PostgreSQL JDBC (EPAS compatible) and Jackson versions pinned **once**.
- Spring Boot modules import the Spring Boot BOM **only in their own pom**, so Spark modules are never affected by it.
- `etl-spark-app` builds a **shaded fat JAR** with relocated conflicting packages (e.g. Guava, Jackson if needed). Spark's own jars are marked `provided`.

---

## 5. Full Folder Structure

```
data-platform/                                    # Separate ETL repo (recommended)
├── pom.xml                                       # Parent POM: modules, BOM, plugin management, pinned versions
├── README.md                                     # Onboarding, build/run instructions, links to docs
├── CODEOWNERS                                    # Ownership per domain folder (pipelines/, dq-rules/, ...)
├── .editorconfig / .gitignore
│
├── docs/                                         # Architecture docs (this file and the following ones)
│   ├── 01-architecture-and-structure.md
│   └── adr/                                      # ➕ Architecture Decision Records (one file per decision)
│
├── etl-common/                                   # Pure Java shared model: NO Spark, NO Spring
│   └── src/main/java/com/company/etl/common/
│       ├── model/                                # JobConfig, RunContext, RunMode, JobStatus, SyncStatus POJOs
│       ├── error/                                # EtlException hierarchy, ErrorCode enum (mirrors error_code_catalog)
│       ├── metadata/                             # Metadata column names (isDeleted, syncSilver, runId, ...) as constants
│       └── util/                                 # Hashing (recordHash), time (UTC), ID utilities
│
├── etl-config/                                   # Config loading + merging: default → env → job override
│   └── src/main/java/com/company/etl/config/
│       ├── loader/                               # YAML loader, placeholder resolution (${ENV}, ${secret:...})
│       ├── validation/                           # Validate job YAML against JSON Schema at load time and in CI
│       └── secrets/                              # SecretResolver interface (Vault / env vars / K8s secrets)
│
├── etl-control/                                  # Control-plane DAO (plain JDBC): used by Spark AND Spring apps
│   └── src/main/java/com/company/etl/control/
│       ├── registry/                             # job_registry, job_dependency repository
│       ├── audit/                                # job_run_audit, job_step_audit writer
│       ├── watermark/                            # watermark/checkpoint read & update
│       ├── lock/                                 # job_lock acquire/release (prevent concurrent runs)
│       ├── quarantine/                           # quarantine metadata + status lifecycle
│       └── dq/                                   # dq_results, reconciliation writers
│
├── etl-core/                                     # Spark framework engine (no business logic)
│   └── src/main/java/com/company/etl/core/
│       ├── runner/                               # EtlJobRunner (generic main), CLI args, RunContext builder
│       ├── pipeline/                             # Template-method job lifecycle, step builder
│       ├── runmode/                              # Strategies per run mode: full, incremental, rerun-failed, backfill, replay, ids, dry-run
│       ├── io/reader/                            # MongoReader, JdbcReader (EPAS), partitioned/parallel reads
│       ├── io/writer/                            # JdbcMergeWriter (upsert/MERGE), MongoWriter, StagingWriter
│       ├── dq/                                   # Rule-based DQ engine: rules from YAML → Spark column expressions
│       ├── transform/                            # Generic transforms: cleanse, cast, flatten/explode, dedupe
│       ├── modeling/                             # Table splitter (nested → parent/child), SCD1/SCD2 merge, surrogate keys
│       ├── writeback/                            # Bulk status write-back to the previous layer
│       ├── quarantine/                           # Quarantine writer (masks/encrypts PII)
│       ├── lineage/                              # OpenLineage emitter / listener config
│       ├── metrics/                              # Spark listener → Prometheus Pushgateway / metrics sink
│       └── plugin/                               # Plugin SPI: CustomTransformer interface + registry
│
├── etl-jobs/                                     # ONLY custom code that config cannot express (plugins)
│   └── src/main/java/com/company/etl/jobs/
│       ├── order/                                # Per domain: custom transformers, complex business rules
│       ├── customer/
│       └── payment/
│
├── etl-spark-app/                                # Packaging module: core + jobs + deps → shaded fat JAR
│   └── pom.xml                                   # maven-shade-plugin, relocations, Spark deps = provided
│
├── etl-ingestion/                                # Spring Boot CDC consumer: Kafka → enrich → Bronze
│   └── src/
│       ├── main/java/com/company/etl/ingestion/
│       │   ├── consumer/                         # Kafka listener on the shared CDC topic, offset/commit strategy
│       │   ├── router/                           # Routes each event by metadata (sourceService, db, collection) to a handler
│       │   ├── handler/                          # CollectionHandler interface + registry (config-driven, per collection)
│       │   ├── enrich/                           # MetadataEnricher (adds metadata columns), recordHash
│       │   ├── softdelete/                       # Delete events → isDeleted=true, deletedAt
│       │   ├── writer/                           # Idempotent Bronze upsert (bulkWrite)
│       │   ├── dlq/                              # DLQ publisher, poison-message handling
│       │   └── health/                           # Actuator health, lag & throughput metrics
│       └── main/resources/
│           ├── application.yml                   # Defaults
│           ├── application-{dev,qa,uat,prod}.yml # Env overrides (no secrets)
│           └── ingestion-mappings.yml            # (sourceService, db, collection) → handler → bronze collection mapping
│
├── etl-ops-api/                                  # Spring Boot REST API (+ optional UI) for operations
│   └── src/main/java/com/company/etl/ops/
│       ├── job/                                  # Job list, status, history, rerun chain (parentRunId)
│       ├── trigger/                              # Trigger any run mode via the Airflow REST API
│       ├── quarantine/                           # View/fix/approve/reprocess quarantined records
│       ├── dq/                                   # DQ & reconciliation reports
│       └── security/                             # RBAC (who can trigger which run mode), audit log
│
├── etl-testkit/                                  # Shared test utilities
│   └── src/main/java/...                         # Testcontainers (Kafka, Mongo, Postgres), SparkSession fixture, data builders
│
├── pipelines/                                    # ★ THE CONFIG-DRIVEN HEART: one YAML per job
│   ├── _schema/job-config.schema.json            # JSON Schema used to validate every job YAML (in CI)
│   ├── _defaults/job-defaults.yml                # Default thresholds, retries, write mode, etc.
│   ├── bronze-to-silver/
│   │   ├── order/
│   │   │   ├── b2s_order.yml                     # source, target(s), keys, split rules, dq ref, schedule, deps
│   │   │   └── b2s_order_item.yml
│   │   └── customer/b2s_customer.yml
│   └── silver-to-gold/
│       ├── sales/
│       │   ├── s2g_dim_customer.yml
│       │   ├── s2g_dim_product.yml
│       │   └── s2g_fact_sales.yml
│       └── finance/...
│
├── dq-rules/                                     # Reusable DQ rule sets, referenced by job YAMLs
│   ├── _common/                                  # Generic rules (not-null, email format, date range ...)
│   ├── order/order.rules.yml
│   └── customer/customer.rules.yml
│
├── contracts/                                    # ➕ Data contracts per source collection (fields, types, PII, owner)
│   └── svc-order/orders.contract.yml
│
├── kafka-connect/                                # Kafka Connect / Mongo connector configuration
│   ├── README.md                                 # Shared topic, event envelope/metadata, connector → collection mapping
│   ├── connectors/
│   │   ├── base/                                 # Env-independent templates with ${placeholders}
│   │   │   ├── mongo-source-order-db.json
│   │   │   └── mongo-source-customer-db.json
│   │   └── overlays/                             # Only what differs per env (connection URI ref, tasks.max, topics prefix)
│   │       ├── dev/  qa/  uat/  prod/
│   ├── topics/                                   # Shared CDC topic + DLQ: partitions, retention (replay window!)
│   │   └── topics.yml
│   └── scripts/
│       └── deploy-connectors.sh                  # Render base+overlay → PUT /connectors/{name}/config (idempotent)
│
├── db/                                           # All DDL as versioned migrations (Flyway)
│   ├── control/                                  # V1__job_registry.sql, V2__job_run_audit.sql ...
│   ├── silver/                                   # Silver schemas and tables
│   ├── gold/                                     # Dimension, fact and aggregate tables
│   └── bronze-mongo/                             # Mongo indexes and validators (JS scripts / mongock)
│
├── orchestration/
│   └── airflow/
│       ├── dags/                                 # (full layout in doc 09 §10.1)
│       │   ├── etl_job_dags.py                   # ONE file → one generated DAG per job (from generated/jobs.<env>.json)
│       │   ├── etl_manual_run.py                 # Generic parameterized DAG for every manual run mode
│       │   ├── etl_cascade.py                    # Level-by-level cascade reruns
│       │   └── etl_maintenance.py ...            # Maintenance, SLA monitor, quarantine reprocess, replay
│       ├── etl_airflow/                          # Our package: factory, EtlSparkOperator, triggers, wait_upstream,
│       │                                         # quality gate, callbacks/notifier, ops API client
│       ├── generated/                            # jobs.<env>.json built by CI (not in Git)
│       ├── requirements.txt
│       └── tests/                                # DAG integrity, factory, manifest, result-mapping tests
│
├── config/
│   └── environments/                             # Single source of env-specific values for Spark jobs
│       ├── base/                                 # Shared defaults
│       │   ├── spark.yml                         # Default executors, memory, shuffle partitions
│       │   └── datasources.yml                   # Logical datasource names → placeholders
│       ├── dev/  qa/  uat/  prod/
│       │   ├── spark.yml                         # Cluster master, deploy mode, resources
│       │   ├── datasources.yml                   # Mongo/EPAS hosts, DB names (secrets via ${secret:...})
│       │   ├── kafka.yml                         # Bootstrap servers, schema registry URL
│       │   └── job-overrides.yml                 # Per-env overrides (thresholds, schedules, batch sizes)
│
├── governance/
│   └── openmetadata/
│       ├── ingestion/                            # Metadata ingestion workflows: mongo, postgres(EPAS), kafka, airflow
│       ├── classification/                       # PII tags, glossary terms
│       └── lineage/                              # OpenLineage transport config for Spark
│
├── monitoring/
│   ├── grafana/dashboards/                       # Pipeline health, Kafka lag, DQ trends, SLA
│   ├── prometheus/alerts/                        # Alert rules (failed/partial jobs, lag, SLA breach)
│   └── logging/                                  # Log format / Loki or ELK pipeline config
│
├── deploy/
│   ├── docker/                                   # Dockerfiles: spark-app image, ingestion, ops-api
│   ├── helm/                                     # Charts: etl-ingestion, etl-ops-api, spark-app templates
│   │   └── values-{dev,qa,uat,prod}.yaml
│   └── k8s/spark/                                # SparkApplication templates (if using Spark Operator)
│
├── scripts/                                      # Dev helpers: local stack up/down, seed data, validate configs
├── docker-compose.local.yml                      # ➕ Local stack: Kafka, Connect, Mongo, Postgres, Airflow
│
└── .github/workflows/  (or Jenkinsfile / .gitlab-ci.yml)
    ├── ci.yml                                    # Build, unit tests, config schema validation, DAG tests
    ├── integration.yml                           # Testcontainers integration tests
    └── deploy.yml                                # Promote artifacts dev → qa → uat → prod
```

### 5.1 Where things live: quick lookup

| I want to... | Go to |
|---|---|
| Add a new source collection to CDC | Add it to the connector's `$match` filter in `kafka-connect/connectors/base/` (same shared topic, no new topic) + a route in `etl-ingestion/.../ingestion-mappings.yml` + `contracts/` |
| Add a new Bronze → Silver job | `pipelines/bronze-to-silver/<domain>/<job>.yml` (+ `dq-rules/`, + `db/silver/` DDL) |
| Add a custom transformation | `etl-jobs/<domain>/` implementing the `CustomTransformer` plugin SPI |
| Change Spark resources for prod | `config/environments/prod/spark.yml` (or `resources:` in the job YAML) |
| Change a schedule | the job YAML `schedule:` (+ `config/environments/<env>/job-overrides.yml`) |
| Add a DAG | Nothing: `etl_job_dags.py` (DAG factory) generates it from the job YAML |
| Add a table | `db/<layer>/V<n>__<desc>.sql` |

### 5.2 Sample job config (to show the shape; full spec in Prompt 06)

```yaml
# pipelines/bronze-to-silver/order/b2s_order.yml
job:
  name: b2s_order
  layer: BRONZE_TO_SILVER
  domain: order
  owner: team-order-data
  enabled: true

source:
  datasource: bronze_mongo           # logical name → resolved per env
  collection: order_bronze
  incremental:
    strategy: SYNC_STATUS            # syncSilver IN (PENDING, FAILED_RETRYABLE)
    watermarkColumn: ingestedAt      # secondary safety net

targets:
  - table: silver.order
    keys: [order_id]
    writeMode: MERGE
  - table: silver.order_item         # nested array split into a child table
    explode: items
    keys: [order_id, line_no]
    writeMode: MERGE

dq:
  ruleSets: [_common/not_null_keys, order/order]
  thresholds: { maxFailedPct: 2.0, maxFailedCount: 1000 }

writeBack:
  statusColumn: syncSilver

schedule: "0 * * * *"                # hourly (overridable per env)
dependsOn: [b2s_customer]
plugins: []                          # custom transformers (FQCN) if needed
```

---

## 6. Module Responsibilities

| Module | Type | Responsibility | Must NOT contain |
|---|---|---|---|
| `etl-common` | Java lib | Shared model (JobConfig, RunContext, enums for RunMode / JobStatus / SyncStatus), exceptions, error codes, metadata column constants, hashing/time utils | Spark, Spring, DB drivers |
| `etl-config` | Java lib | Load & merge YAML (defaults → env → job), resolve placeholders and secrets, validate against JSON Schema | Business logic |
| `etl-control` | Java lib | Plain-JDBC access to control tables: registry, audit, watermark, lock, quarantine, DQ, reconciliation | Spark, Spring (so both sides can use it) |
| `etl-core` | Spark framework | Generic job lifecycle, run-mode strategies, readers/writers, DQ engine, transforms, table splitting, SCD, write-back, quarantine, lineage, metrics, plugin SPI | Domain-specific logic |
| `etl-jobs` | Spark plugins | Domain-specific custom transformers that config cannot express | Framework code, copies of core logic |
| `etl-spark-app` | Packaging | Builds the single shaded JAR (`core` + `jobs` + deps); entry point `EtlJobRunner` | Source code |
| `etl-ingestion` | Spring Boot app | Consumes the shared CDC topic, routes events by collection metadata to per-collection handlers, enrichment, soft delete, idempotent Bronze writes, DLQ | Spark |
| `etl-ops-api` | Spring Boot app | REST API/UI for job status, triggering run modes (via Airflow), quarantine fix & reprocess, reports, RBAC | Spark, direct data transformations |
| `etl-testkit` | Test lib | Testcontainers setups, Spark test session, data builders, golden datasets | Production code |
| `pipelines/`, `dq-rules/`, `contracts/` | Config | Declarative job definitions, DQ rules, source contracts | Code |
| `kafka-connect/` | Config | Connector templates + env overlays, topic definitions, deploy script | — |
| `orchestration/airflow/` | Python | DAG factory, generic manual-run DAG, operators, callbacks, sensors | ETL logic (Airflow only orchestrates) |
| `db/` | SQL | Versioned Flyway migrations per layer and the control schema | — |
| `config/environments/` | Config | Env-specific Spark, datasource, Kafka values and job overrides | Secrets in plain text |

---

## 7. Module Dependency Diagram

```
                           ┌──────────────┐
                           │  etl-common  │   (pure Java, no deps on other modules)
                           └──────┬───────┘
                ┌─────────────────┼───────────────────────┐
                ▼                 ▼                       │
        ┌──────────────┐  ┌──────────────┐                │
        │  etl-config  │  │ etl-control  │                │
        └──────┬───────┘  └──────┬───────┘                │
               │   ┌─────────────┤                        │
               ▼   ▼             │                        │
        ┌──────────────┐         │                        │
        │   etl-core   │ (Spark) │                        │
        └──────┬───────┘         │                        │
               ▼                 │                        │
        ┌──────────────┐         │                        │
        │   etl-jobs   │         │                        │
        └──────┬───────┘         │                        │
               ▼                 │                        │
        ┌──────────────┐         │                        │
        │ etl-spark-app│ ──► etl-spark-app.jar (shaded)   │
        └──────────────┘         │                        │
                                 ▼                        ▼
                ┌─────────────────────────┐   ┌──────────────────────┐
                │ etl-ops-api (Spring)    │   │ etl-ingestion (Spring)│
                │ uses: common, config,   │   │ uses: common, config  │
                │       control           │   │       (+ control for  │
                └─────────────────────────┘   │        ingestion audit)│
                                              └──────────────────────┘

 etl-testkit ──(test scope)──► used by core, jobs, ingestion, ops-api

 Runtime (non-Maven) dependencies:
   Airflow DAGs  ──reads──► pipelines/*.yml  ──triggers──► etl-spark-app.jar
   etl-ops-api   ──REST───► Airflow API       ──reads────► control DB
   Spark jobs    ──JDBC───► control DB (audit/watermark/lock)
```

**Rules enforced in the build** (➕ Maven Enforcer / ArchUnit tests):
- `etl-common`, `etl-config` and `etl-control` must not depend on Spark or Spring.
- `etl-jobs` may depend on `etl-core`, never the other way around.
- Spring Boot modules must not depend on `etl-core` (keeps Spark out of services).
- No dependency cycles.

---

## 8. Governance Tool: OpenMetadata vs DataHub

| Criteria | OpenMetadata | DataHub |
|---|---|---|
| Operational footprint | Lighter: server + MySQL/Postgres + Elasticsearch/OpenSearch; ingestion runs on **Airflow** (which you already have) | Heavier: GMS, frontend, Kafka, Elasticsearch, MySQL, schema registry |
| Connectors needed here | MongoDB, Postgres (EPAS via the Postgres-compatible connector), Kafka, Airflow, Spark (via OpenLineage) | Same coverage |
| Spark lineage | OpenLineage-based Spark agent/listener | Mature Spark lineage agent |
| Column-level lineage | Supported | Supported, strong |
| Built-in Data Quality | **Yes**: test suites, DQ dashboards, results via API | Assertions exist; DQ is usually external |
| Glossary, ownership, PII classification | Built in, including auto-classification | Built in |
| UI for business users | Simple, collaboration features (tasks, announcements) | Powerful, more technical |
| Scale sweet spot | Small/medium data teams, tens to hundreds of pipelines | Very large organizations, high metadata event volume |

### Recommendation: **OpenMetadata**

1. **Airflow-based ingestion framework**, which the team already knows. (Doc 10 refines this: OM ingestion runs in OM's own deployment, not inside the ETL Airflow, to avoid Python dependency conflicts.)
2. **Built-in data quality** matches the DQ results we will publish from the Spark DQ engine (Prompt 10).
3. **Lower ops cost** for a team that is just starting a data platform; it fits 20 → 100+ pipelines comfortably.
4. **Lineage via OpenLineage** keeps the Spark lineage emitter tool-neutral. If you ever move to DataHub, the emitter stays the same and only the transport changes.

**Risk / to verify (➕):** EPAS-specific objects (Oracle-compatible packages, synonyms) may not be fully parsed by the Postgres connector. Standard tables and views work. Run a PoC on one EPAS schema before committing.

---

## 9. ➕ Added by Architect: Gaps and Additions

| # | Addition | Why it matters |
|---|---|---|
| A1 | **Dedicated control DB/schema (`etl_ctl`)** | Keeps audit, locks and watermarks transactional and separate from data. The ops API, Airflow and Spark all read the same source of truth. |
| A2 | **Schema Registry + data contracts** | Microservice teams change documents without warning. Contracts + compatibility checks catch breaking changes before they corrupt Silver/Gold. |
| A3 | **Single shaded Spark JAR + generic runner** | Avoids 100 deployables. One artifact version is promoted through environments. |
| A4 | **DAG factory from job YAML** | DAGs never drift from job configs. Adding a job does not require writing Python. |
| A5 | **JSON Schema validation of job YAML in CI** | Misconfigured jobs fail at PR time, not at 3 AM in prod. |
| A6 | **Flyway migrations for all DDL** | Repeatable, versioned schema changes per environment. |
| A7 | **Kafka topic retention sized for replay** | The replay run mode only works within topic retention. Decide the replay window (e.g. 7–14 days) explicitly. Bronze is the long-term replay source. |
| A8 | **All timestamps in UTC** | Incremental windows, watermarks and backfills break across time zones and DST otherwise. |
| A9 | **ADR folder (`docs/adr/`)** | Records why decisions were made, so new team members do not re-litigate them. |
| A10 | **CODEOWNERS per domain** | Clear ownership as pipelines grow; domain teams review their own job configs. |
| A11 | **Local dev stack (docker-compose)** | Developers can run Kafka → Bronze → Silver end to end on a laptop. |
| A12 | **Architecture tests (ArchUnit/Enforcer)** | Prevents module boundaries from eroding as code grows. This is the main "doesn't get messy" safeguard. |
| A13 | **Custom message key (`service:coll:docId`) on the shared topic** | The connector's default key (resume token) breaks per-document ordering. With one shared topic this matters even more. See 2.4. |
| A14 | **Per-collection metrics + config-only escape hatch** | One shared topic hides per-collection lag and allows noisy neighbours. Tagged metrics make it visible, and `topic.namespace.map` can move a hot/PII collection to its own topic without code changes. |

---

## 10. Key Decisions

1. **Separate `data-platform` repo** for all ETL assets. Data contracts + Schema Registry link it to the microservices repo.
2. **Kafka Connect configs** live in `kafka-connect/connectors/base` (templates) + `overlays/<env>` (env overrides), deployed by an idempotent script. If they must stay in the existing repo: `infra/kafka/connect/` with the same layout.
3. **Storage:** Bronze = MongoDB, Silver = EPAS, Gold = EPAS, control = EPAS schema `etl_ctl` (preliminary; finalized in 02–05).
4. **Ingestion:** `etl-ingestion` Spring Boot service consumes the **single shared CDC topic** (`cdc.source.events`), routes each event by its metadata (`sourceService`, `ns.db`, `ns.coll`) to a config-driven handler, and writes directly (idempotent upsert) to Bronze, with a DLQ topic for failures.
11. **CDC topic strategy:** ALL collections → ONE shared topic, one connector per source DB (`topic.namespace.map: {"*": "cdc.source.events"}`), full change-event envelope kept (`publish.full.document.only=false`), `sourceService` added via the connector pipeline, message key = `sourceService:collection:documentId` for per-document ordering (refined in doc 02). A hot or PII collection can be moved to its own topic by config only.
5. **Build:** Maven multi-module: `etl-common`, `etl-config`, `etl-control`, `etl-core`, `etl-jobs`, `etl-spark-app`, `etl-ingestion`, `etl-ops-api`, `etl-testkit`.
6. **Spark:** one shaded JAR, one generic entry point `EtlJobRunner --job --mode --env ...`. No Spring inside Spark.
7. **Config-driven jobs:** `pipelines/<layer>/<domain>/<job>.yml` + `dq-rules/`, validated by JSON Schema. Airflow DAGs are generated from these files.
8. **Environment config:** `config/environments/{base,dev,qa,uat,prod}/` for Spark jobs; Spring profiles for Spring apps; `config/environments/<env>/airflow.yml` for Airflow (moved there in doc 08); secrets only via a secret manager.
9. **Governance:** OpenMetadata, with lineage in OpenLineage format emitted by the framework's config-driven `LineageEmitter` (doc 10; PoC on EPAS first).
10. **Guardrails:** ArchUnit/Enforcer module rules, CI config validation, Flyway, UTC everywhere, topic retention sized for replay.
