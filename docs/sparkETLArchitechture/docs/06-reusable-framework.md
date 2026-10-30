# 06 — Reusable, Config-Driven Framework (So 100+ Jobs Never Become Messy)

> Builds on docs [01](01-architecture-and-structure.md) to [05](05-control-audit-tables.md).
> Written in **simple language**: every section starts with an **"In simple words"** box, and every abbreviation is written out in full.

---

## 0. Abbreviations Used in This Document

| Short form | Full form | Meaning in simple words |
|---|---|---|
| **ETL** | Extract, Transform, Load | Take data out, clean/reshape it, load it somewhere |
| **JAR** | Java ARchive | One zip file containing compiled Java code |
| **JVM** | Java Virtual Machine | The program that runs Java code |
| **CLI** | Command Line Interface | Running a program with text arguments (`--job=b2s_order`) |
| **SPI** | Service Provider Interface | A Java way to plug in extra implementations without changing the main code |
| **DI** | Dependency Injection | Giving an object the other objects it needs, instead of it creating them itself |
| **POJO** | Plain Old Java Object | A simple Java class with fields, no framework magic |
| **JDBC** | Java Database Connectivity | Standard Java API to talk to relational databases (EPAS) |
| **SQL** | Structured Query Language | Language for relational databases |
| **DQ** | Data Quality | Checks that data is correct |
| **SCD** | Slowly Changing Dimension | Rules for keeping history of dimension changes |
| **SK** | Surrogate Key | A number created in Gold to identify a dimension row |
| **B2S / S2G** | Bronze to Silver / Silver to Gold | The two main job families |
| **YAML** | YAML Ain't Markup Language | The config file format for jobs |
| **JSON** | JavaScript Object Notation | A text format for structured data |
| **MDC** | Mapped Diagnostic Context | Extra fields (like `runId`) automatically added to every log line |
| **SLF4J** | Simple Logging Facade for Java | The standard Java logging API we code against |
| **UDF** | User Defined Function | Your own function used inside Spark SQL (slower than built-in functions) |
| **AQE** | Adaptive Query Execution | Spark feature that re-optimizes a query while it runs |
| **OOM** | Out Of Memory | A crash because memory ran out |
| **GC** | Garbage Collection | The JVM cleaning up unused memory (can cause pauses) |
| **BOM** | Bill Of Materials | A Maven file that pins the versions of many libraries in one place |
| **SemVer** | Semantic Versioning | Version numbers `MAJOR.MINOR.PATCH` (e.g. `1.14.2`) |
| **CI/CD** | Continuous Integration / Continuous Delivery | Automatic build, test and deploy |
| **K8s** | Kubernetes | The container platform the services run on |
| **DAG** | Directed Acyclic Graph | An Airflow workflow |
| **API** | Application Programming Interface | A way for programs to call each other |
| **ADR** | Architecture Decision Record | A short file explaining why a decision was made |
| **PII** | Personally Identifiable Information | Name, email, phone, ... |
| **EPAS** | EDB Postgres Advanced Server (EDB = EnterpriseDB) | Our PostgreSQL-based database |

---

## 1. The Big Idea: One Kitchen, Many Recipes

> **In simple words**
> Imagine a restaurant with **100 dishes**. A bad restaurant builds a **new kitchen for every dish**. A good restaurant has **one kitchen** (stove, oven, knives, fridge) and a **recipe card** for each dish.
>
> In our platform:
> - The **kitchen** = the framework code (`etl-core`): readers, writers, DQ engine, merge logic, audit, locks … written **once**.
> - The **recipe card** = one **YAML file per job**: which table to read, which columns, which rules, where to write.
> - A **special spice** that no standard tool can make = a small **plugin** class (rare).
>
> Adding job number 101 = writing recipe card number 101. No new kitchen.

### 1.1 What is code, what is config

| Lives in **code** (written once, in `etl-core`) | Lives in **config** (one YAML per job) | Lives in a **plugin** (rare, in `etl-jobs`) |
|---|---|---|
| How to read Mongo / EPAS | Which collection / table to read | A very special business calculation |
| How to split arrays into child tables | Which arrays, which keys | A custom DQ rule type (e.g. an Indian GSTIN checksum) |
| How DQ rules run | Which rules, which severities | A complex normalization (e.g. legacy address parsing) |
| How merges, SCD, inferred members work | Which SCD type per column, which dimensions | — |
| Locks, audit, watermarks, write-back, lineage, metrics | Schedule, dependencies, thresholds, resources | — |

**Goal:** 90%+ of jobs need **zero** new Java code.

---

## 2. Framework Layers

> **In simple words:** the framework is like a building with floors. Each floor only talks to the floor directly below it. This keeps the code tidy as it grows.

```
 ┌───────────────────────────────────────────────────────────────────────────────────────┐
 │ 1. ENTRY            EtlJobRunner (CLI)  →  RunContext                                 │  "start the engine"
 ├───────────────────────────────────────────────────────────────────────────────────────┤
 │ 2. LIFECYCLE        AbstractEtlJob (Template Method): lock → audit → steps → finish   │  "the fixed recipe skeleton"
 ├───────────────────────────────────────────────────────────────────────────────────────┤
 │ 3. PIPELINE         PipelineBuilder → List<Step> per job type                         │  "the list of cooking steps"
 │                     ReadStep, SplitStep, StandardizeStep, DqStep, DedupeStep,         │
 │                     EnrichStep, ClassifyStep, StageStep, MergeStep, QuarantineStep,   │
 │                     WriteBackStep, ReconcileStep, LineageStep                         │
 ├───────────────────────────────────────────────────────────────────────────────────────┤
 │ 4. COMPONENTS       Reader · Writer · MergeStrategy · DqEngine · Transformer ·        │  "the kitchen tools"
 │                     Deduplicator · TableSplitter · ScdHandler · StatusWriteBack ·     │
 │                     QuarantineWriter · Reconciler · LineageEmitter · MetricsPublisher │
 ├───────────────────────────────────────────────────────────────────────────────────────┤
 │ 5. CONTROL          LockManager · RunAuditor · WatermarkManager · ErrorCatalog ·      │  "the control room" (doc 05)
 │  (etl-control)      RegistryDao · TransactionRunner                                   │
 ├───────────────────────────────────────────────────────────────────────────────────────┤
 │ 6. FOUNDATION       etl-config (YAML load/merge/validate) · etl-common (models, enums,│  "the building's foundation"
 │                     errors, hashing, time)                                            │
 └───────────────────────────────────────────────────────────────────────────────────────┘
        ▲
        │ plugs in from the side (SPI)
 ┌──────┴──────────────┐
 │ PLUGINS (etl-jobs)  │  CustomTransformer, DqRuleType  (only when YAML cannot express it)
 └─────────────────────┘
```

---

## 3. Config-Driven Jobs

### 3.1 How a job's final config is built

> **In simple words:** a job's settings come from **four layers**, like transparent sheets stacked on top of each other. A value on a higher sheet covers the one below.

```
 4. CLI overrides (only an allow-listed few, e.g. --override resources.executors=8)   ← highest
 3. config/environments/<env>/job-overrides.yml   (e.g. prod: schedule, thresholds, resources)
 2. pipelines/<layer>/<domain>/<job>.yml          (the job's own recipe)
 1. pipelines/_defaults/job-defaults.yml          (company-wide defaults)                ← lowest
        │
        ▼  merge → resolve ${placeholders} and ${secret:...} → validate with JSON Schema
 ResolvedJobConfig  (typed Java object: B2SConfig, DimensionConfig, FactConfig, AggregateConfig ...)
```

### 3.2 Defaults file

```yaml
# pipelines/_defaults/job-defaults.yml
configVersion: 1                      # schema version of the YAML format itself (§9.3)
job:
  enabled: true
  maxRetries: 5
thresholds:
  maxFailedPct: 2.0
  maxFailedCount: 5000
  minRecordsForPct: 100
  onBreach: ROLLBACK
write:
  method: COPY
  parallelism: 8
  transactionScope: JOB
writeBack:
  parallelism: 8
  batchSize: 1000
audit:
  stepLevel: true
  maxErrorRowsPerRule: 10000
  logWarnings: false
resources:
  executors: 2
  executorMemory: 2g
  executorCores: 2
  driverMemory: 2g
reconcile:
  countCheck: STRICT
```

### 3.3 Sample job config (complete, annotated)

```yaml
# pipelines/bronze-to-silver/order/b2s_order.yml
configVersion: 1

job:
  name: b2s_order                       # must match the file name (checked in CI)
  type: B2S                             # → picks the job class from the registry (§6.3)
  domain: order
  owner: team-order-data                # who gets the alerts
  description: "Orders with items, discounts, payments and tags from svc-order"
  slaMinutes: 20

source:                                 # WHAT to read (the run mode decides WHICH records, §7)
  kind: BRONZE_MONGO
  datasource: bronze_mongo              # logical name → real URI per environment
  database: bronze_svc_order
  collection: orders
  contract: contracts/svc-order/orders.contract.yml   # → explicit Spark schema
  staticFilter: { "data.isTestOrder": { $ne: true } }
  maxRecordsPerRun: 2000000

preFilter:
  - { expr: "data.channel IS NULL OR data.channel <> 'INTERNAL_TEST'", reason: TEST_CHANNEL }

targets:                                # HOW to split one document into Silver tables
  - name: sales_order
    table: silver_order.sales_order
    from: root
    key: [order_id]
    columns:
      order_id:     { source: _id,           type: varchar(24),   transform: [objectIdToString] }
      order_no:     { source: data.orderNo,  type: varchar(30),   transform: [trim, upper] }
      order_ts:     { source: data.orderDate,type: timestamptz }
      status_code:  { source: data.status,   type: varchar(20),   transform: [trim, upper],
                      map: { refTable: silver_ref.order_status_map, onUnmapped: ERROR } }
      total_amount: { source: data.amount,   type: "numeric(18,2)" }
      customer_id:  { source: data.customer.id, type: varchar(24), transform: [objectIdToString] }
    keepUnmapped: true
  - name: sales_order_item
    table: silver_order.sales_order_item
    from: data.items
    parentKey: { order_id: _id }
    key: [order_id, line_id]
    childKey: { line_id: { source: lineId, fallback: POSITION } }
    orphanPolicy: SOFT_DELETE
    columns:
      sku:        { source: sku,   type: varchar(40),    transform: [trim, upper] }
      qty:        { source: qty,   type: integer }
      unit_price: { source: price, type: "numeric(18,2)" }
childFailurePolicy: PARTIAL

dq:                                     # WHICH quality rules (rule files in dq-rules/)
  ruleSets: [_common/keys_not_null, order/sales_order, order/sales_order_item]

dedupe:
  businessKey: [order_no]
  strategy: LATEST_VERSION
  orderBy: [_src_event_ts DESC, _bronze_id DESC]

lookups:
  - { name: order_status, table: silver_ref.order_status_map, keys: [source_service, source_value], broadcast: true }

plugins:                                # OPTIONAL custom code, at a fixed hook point (§10)
  - { name: order.gift_message_cleaner, hook: AFTER_STANDARDIZE, target: sales_order,
      params: { maxLength: 200 } }

deleteStrategy: SOFT
thresholds: { maxFailedPct: 2.0 }       # only what differs from the defaults
reconcile:
  sumColumns:
    - { bronze: data.amount, silver: silver_order.sales_order.total_amount, tolerance: 0.01, severity: WARN }

schedule: "15 * * * *"
dependsOn: [b2s_customer]
resources: { executors: 4, executorMemory: 4g }
```

### 3.4 Environment override

```yaml
# config/environments/prod/job-overrides.yml
jobs:
  b2s_order:
    schedule: "*/30 * * * *"            # every 30 minutes in prod
    resources: { executors: 8, executorMemory: 8g }
    write: { parallelism: 16 }
  s2g_fact_sales_line:
    thresholds: { maxUnknownSkPct: 0.2 }
```

### 3.5 Validation: catch mistakes before they run

> **In simple words:** a **JSON Schema** is a rulebook for the YAML file: "`job.type` must be one of these", "`targets` must have at least one entry", "`thresholds.maxFailedPct` must be a number between 0 and 100". CI (Continuous Integration) checks every YAML against it on every pull request.

```json
// pipelines/_schema/job-config.schema.json (excerpt)
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["configVersion", "job", "source"],
  "properties": {
    "configVersion": { "const": 1 },
    "job": {
      "type": "object",
      "required": ["name", "type", "domain", "owner"],
      "properties": {
        "name":   { "type": "string", "pattern": "^(b2s|s2g)_[a-z0-9_]+$" },
        "type":   { "enum": ["B2S", "S2G_DIMENSION", "S2G_FACT", "S2G_SNAPSHOT", "S2G_AGGREGATE"] },
        "owner":  { "type": "string", "minLength": 3 }
      }
    },
    "thresholds": {
      "type": "object",
      "properties": {
        "maxFailedPct":   { "type": "number", "minimum": 0, "maximum": 100 },
        "maxFailedCount": { "type": "integer", "minimum": 0 },
        "onBreach":       { "enum": ["ROLLBACK", "COMMIT_AND_FAIL"] }
      }
    }
  },
  "allOf": [
    { "if":   { "properties": { "job": { "properties": { "type": { "const": "B2S" } } } } },
      "then": { "required": ["targets", "dq"] } }
  ]
}
```

**Checks beyond the schema** (Java, in CI and in the registry sync, doc 05 §7):

| Check | Example of a mistake it catches |
|---|---|
| File name = `job.name` | `b2s_order.yml` contains `name: b2s_orders` |
| Every rule set / contract / plugin / ref table referenced exists | `ruleSets: [order/sales_ordr]` (typo) |
| Single owner per target table | Two jobs write `silver_order.sales_order` |
| No dependency cycles | `a → b → a` |
| Every error code used exists in `error_code_catalog` | `errorCode: DQ_NOTNULL` |
| Identifiers are safe ➕ | A table name like `x; DROP TABLE y` (identifiers must match `^[a-z_][a-z0-9_.]*$`) |
| Target columns exist in the Flyway DDL ➕ | YAML maps `order_note`, but the table has no such column |

### 3.6 Typed config classes

> **In simple words:** after loading, the YAML becomes normal Java objects with proper types, so the code writes `cfg.thresholds().maxFailedPct()` instead of `map.get("thresholds").get("maxFailedPct")`. Typos become compile errors instead of 3 AM failures.

```java
// etl-common: model/config/JobConfig.java — Jackson picks the subclass from job.type
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "jobType")
@JsonSubTypes({
    @JsonSubTypes.Type(value = B2SConfig.class,       name = "B2S"),
    @JsonSubTypes.Type(value = DimensionConfig.class, name = "S2G_DIMENSION"),
    @JsonSubTypes.Type(value = FactConfig.class,      name = "S2G_FACT"),
    @JsonSubTypes.Type(value = SnapshotConfig.class,  name = "S2G_SNAPSHOT"),
    @JsonSubTypes.Type(value = AggregateConfig.class, name = "S2G_AGGREGATE")})
public sealed interface JobConfig permits B2SConfig, DimensionConfig, FactConfig, SnapshotConfig, AggregateConfig {
    JobMeta job();
    Thresholds thresholds();
    WriteSettings write();
    AuditSettings audit();
    Resources resources();
    List<PluginRef> plugins();
    List<String> dependsOn();
}

public record B2SConfig(JobMeta job, BronzeSourceSpec source, List<PreFilterSpec> preFilter,
                        List<TargetSpec> targets, ChildFailurePolicy childFailurePolicy, DqSpec dq,
                        DedupeSpec dedupe, List<LookupSpec> lookups, DeleteStrategy deleteStrategy,
                        Thresholds thresholds, WriteSettings write, WriteBackSettings writeBack,
                        ReconcileSpec reconcile, AuditSettings audit, Resources resources,
                        List<PluginRef> plugins, List<String> dependsOn) implements JobConfig {}
```

---

## 4. Class Diagram

> **In simple words:** boxes are classes or interfaces. `◁──` means "implements / extends". `──►` means "uses". Read it top to bottom: the runner creates a job, the job runs steps, steps use components.

```
                                   ┌──────────────────────┐
                                   │     EtlJobRunner     │  main(): parse CLI → RunContext
                                   └──────────┬───────────┘
                                              │ uses
              ┌───────────────────────────────┼─────────────────────────────────┐
              ▼                               ▼                                 ▼
   ┌─────────────────────┐        ┌───────────────────────┐        ┌───────────────────────┐
   │   ConfigLoader      │        │   JobTypeRegistry     │        │   ComponentFactory    │
   │ (etl-config)        │        │ "B2S" → factory       │        │ composition root:     │
   │ merge+validate YAML │        │ "S2G_FACT" → factory  │        │ builds all components │
   └─────────────────────┘        └──────────┬────────────┘        └───────────────────────┘
                                             │ creates
                                             ▼
                         ┌────────────────────────────────────────┐
                         │   «abstract» AbstractEtlJob             │  TEMPLATE METHOD
                         │  + run() final: lock, audit, steps,     │
                         │    reconcile, finish, release           │
                         │  # pipeline(): List<Step>  (abstract)   │
                         └───▲─────────▲─────────▲─────────▲───────┘
            ┌────────────────┘         │         │         └──────────────────┐
 ┌──────────┴──────────┐ ┌─────────────┴───────┐ ┌┴────────────────────┐ ┌──────┴───────────────┐
 │ BronzeToSilverJob   │ │ SilverToGoldDimJob  │ │ SilverToGoldFactJob │ │ SqlAggregateJob /    │
 │                     │ │                     │ │                     │ │ SnapshotJob          │
 └─────────────────────┘ └─────────────────────┘ └─────────────────────┘ └──────────────────────┘
            │ pipeline() uses PipelineBuilder (BUILDER) to assemble:
            ▼
 ┌──────────────────────────────────────────────────────────────────────────────────────────────┐
 │ «interface» Step   name() · execute(PipelineState, RunContext) · runsInDryRun()               │
 │   ◁── ReadStep ◁── PreFilterStep ◁── SplitStep ◁── StandardizeStep ◁── DqStep ◁── DedupeStep  │
 │   ◁── EnrichStep ◁── ClassifyStep ◁── ScdDetectStep ◁── StageStep ◁── MergeStep              │
 │   ◁── QuarantineStep ◁── WriteBackStep ◁── ReconcileStep ◁── LineageStep ◁── PluginStep      │
 │ TimedStep (DECORATOR): wraps every step → timing, job_step_audit, metrics, MDC "step"         │
 └──────────────────────────────────────────────────────────────────────────────────────────────┘
            │ steps use components (STRATEGY where marked ◆)
            ▼
 ┌────────────────────┐ ┌─────────────────────┐ ┌──────────────────────┐ ┌──────────────────────┐
 │ «i» SourceReader ◆ │ │ «i» ReadPlanStrategy◆│ │ «i» TargetLoader ◆   │ │ «i» MergeStrategy ◆  │
 │ BronzeMongoReader  │ │ one per RunMode     │ │ CopyLoader           │ │ GuardedUpsertMerge   │
 │ SilverEpasReader   │ │ (§7)                │ │ JdbcBatchLoader      │ │ Scd1Merge, Scd2Merge │
 │ + FilterTranslator │ │                     │ │ NoOpLoader (dry-run) │ │ FactMerge, Partition │
 └────────────────────┘ └─────────────────────┘ └──────────────────────┘ │ Replace, ShadowSwap  │
                                                                          └──────────────────────┘
 ┌────────────────────┐ ┌─────────────────────┐ ┌──────────────────────┐ ┌──────────────────────┐
 │ DqEngine           │ │ «i» Transformer     │ │ «i» Deduplicator ◆   │ │ TableSplitter        │
 │ RuleFactory        │ │ ColumnTransforms    │ │ LatestVersion,       │ │ (nested → child rows)│
 │ «i» DqRule         │ │ CustomTransformer   │ │ KeepFirst,           │ └──────────────────────┘
 │ «i» DqRuleType(SPI)│ │   (SPI plugin)      │ │ QuarantineAll        │ ┌──────────────────────┐
 └────────────────────┘ └─────────────────────┘ └──────────────────────┘ │ ScdHandler           │
                                                                          │ ChangeDetector +     │
 ┌────────────────────┐ ┌─────────────────────┐ ┌──────────────────────┐ │ ScdSqlGenerator      │
 │ «i» StatusWriteBack│ │ QuarantineWriter    │ │ Reconciler           │ └──────────────────────┘
 │ ◆ BronzeMongoWB    │ │ RecordErrorLogger   │ │                      │
 │ ◆ SilverSummaryWB  │ │ ErrorClassifier     │ │                      │
 │ ◆ NoOpWriteBack    │ └─────────────────────┘ └──────────────────────┘
 └────────────────────┘
 ┌────────────────────┐ ┌─────────────────────┐ ┌──────────────────────┐ ┌──────────────────────┐
 │ «i» LineageEmitter │ │ «i» MetricsPublisher│ │ LockManager          │ │ WatermarkManager     │
 │ OpenLineageEmitter │ │ PushgatewayPublisher│ │ RunAuditor           │ │ TransactionRunner    │
 │ NoOpLineage        │ │ LogOnlyPublisher    │ │ (etl-control)        │ │ (etl-control)        │
 └────────────────────┘ └─────────────────────┘ └──────────────────────┘ └──────────────────────┘
```

---

## 5. Design Patterns, Explained Simply

| Pattern | Simple picture | Where we use it | What it saves us from |
|---|---|---|---|
| **Template Method** | A fixed **exam answer format**: name, question, answer, signature. You only fill in the answer | `AbstractEtlJob.run()` fixes the order: lock → audit → steps → reconcile → finish → release. Subclasses only provide `pipeline()` | Every job re-implementing (and forgetting) locking, audit, error handling |
| **Strategy** | **Interchangeable drill bits**: same drill, different bit for wood or metal | `ReadPlanStrategy` per run mode, `MergeStrategy` per write type, `TargetLoader` (COPY vs JDBC batch), `Deduplicator` | Huge `if/else` or `switch` blocks |
| **Factory / Registry** | A **phone book**: look up a name, get the number | `JobTypeRegistry` maps `"B2S"` → `BronzeToSilverJob`; `RuleFactory` maps `"not_null"` → `NotNullRule`; `TransformRegistry` maps `"trim"` → trim function | Code knowing every class by name |
| **Builder** | **Lego instructions**: add pieces step by step, then "build" | `PipelineBuilder` assembles the step list per job type, adding optional steps only when configured (e.g. `ScdDetectStep` only for SCD2 dims, `PluginStep` only if plugins exist) | Constructors with 20 parameters |
| **Plugin (SPI)** | A **power socket**: any appliance with the right plug works | `CustomTransformer`, `DqRuleType` found via Java `ServiceLoader` | Changing framework code for one special case |
| ➕ **Decorator** | **Gift wrapping**: the box stays the same, you add something around it | `TimedStep` wraps every step and adds timing, step audit, metrics and log context automatically | Copy-pasting timing/logging code into every step |
| ➕ **Null Object** | A **dummy remote**: buttons work but do nothing | **Dry-run** swaps the real loaders/write-backs for `NoOpLoader`, `NoOpWriteBack`, `NoOpQuarantineWriter` | `if (dryRun)` checks scattered everywhere (and one being forgotten = a dry run that writes!) |
| ➕ **Composition Root** | **One switchboard** where all wires are connected | `ComponentFactory` creates and wires every component once per run (plain Java, no Spring) | Hidden object creation all over the code |

---

## 6. Core Interfaces and Base Classes (Java Skeletons)

### 6.1 `RunContext`: everything a run needs to know

> **In simple words:** a **job card** that travels with the run: which job, which run id, which mode, which environment, the parameters, the config, and the shared tools. Every step receives it.

```java
// etl-common: model/RunContext.java
public record RunContext(
        String jobName,
        String runId,                 // b2s_order-20261030T091500Z-01 (or given by Airflow, §8.2)
        RunMode runMode,              // INCREMENTAL, FULL_LOAD, RERUN_JOB, RERUN_FAILED_RECORDS, ...
        TriggerType triggerType,      // SCHEDULED, MANUAL, DEPENDENCY, EVENT, CASCADE, AIRFLOW_RETRY, API
        boolean dryRun,
        String env,                   // dev | qa | uat | prod
        RunParams params,             // from, to, ids, parentRunId, rootRunId, ticket, triggeredBy
        JobConfig config,             // resolved, validated config
        String configHash,
        String codeVersion,           // JAR version
        Instant startedAt) {

    public <T extends JobConfig> T config(Class<T> type) { return type.cast(config); }
}

public record RunParams(Optional<Instant> from, Optional<Instant> to, List<String> ids,
                        Optional<String> parentRunId, Optional<String> rootRunId,
                        Optional<String> ticket, String triggeredBy) {}
```

### 6.2 `Step` and `PipelineState`

> **In simple words:** a **step** is one station on an assembly line. The **pipeline state** is the **tray** that moves from station to station, carrying the datasets and counts produced so far.

```java
// etl-core: pipeline/Step.java
public interface Step {
    String name();                                         // "DQ", "MERGE" ... (used in logs and job_step_audit)
    void execute(PipelineState state, RunServices svc);
    default boolean runsInDryRun() { return true; }        // write steps are swapped to no-op versions instead
}

// etl-core: pipeline/PipelineState.java — the tray
public final class PipelineState {
    private final Map<String, Dataset<Row>> datasets = new LinkedHashMap<>();  // "bronze", "target:sales_order", "status"
    private final RunCounts counts = new RunCounts();
    private final List<AutoCloseable> cleanups = new ArrayList<>();           // unpersist, temp files
    private MergeStats mergeStats = MergeStats.none();
    private WriteBackStats writeBackStats = WriteBackStats.none();
    private boolean empty;                                                     // empty-run short-circuit

    public Dataset<Row> get(String key) { return Objects.requireNonNull(datasets.get(key), "missing dataset " + key); }
    public void put(String key, Dataset<Row> ds) { datasets.put(key, ds); }
    public void persist(String key, Dataset<Row> ds) {
        ds.persist(StorageLevel.MEMORY_AND_DISK_SER());
        put(key, ds);
        cleanups.add(ds::unpersist);
    }
    // getters/setters for counts, stats, empty ...
}

// etl-core: pipeline/RunServices.java — the shared tools, built once by ComponentFactory
public record RunServices(RunContext ctx, SparkSession spark, ComponentFactory components,
                          ControlStore control, MetricsPublisher metrics, LineageEmitter lineage) {}
```

### 6.3 `JobTypeRegistry` (Factory/Registry)

```java
// etl-core: runner/JobTypeRegistry.java
public final class JobTypeRegistry {
    private final Map<String, Function<RunServices, AbstractEtlJob>> factories = Map.of(
            "B2S",           BronzeToSilverJob::new,
            "S2G_DIMENSION", SilverToGoldDimensionJob::new,
            "S2G_FACT",      SilverToGoldFactJob::new,
            "S2G_SNAPSHOT",  SilverToGoldSnapshotJob::new,
            "S2G_AGGREGATE", SqlAggregateJob::new);

    public AbstractEtlJob create(RunServices svc) {
        String type = svc.ctx().config().job().type();
        var factory = factories.get(type);
        if (factory == null) throw new ConfigException(ErrorCode.CONFIG_INVALID, "Unknown job type " + type);
        return factory.apply(svc);
    }
}
```

Adding a **new job** never touches this class. Adding a **new job type** (rare, e.g. a future `S2G_BRIDGE`) adds one line.

### 6.4 `AbstractEtlJob` (Template Method)

```java
// etl-core: pipeline/AbstractEtlJob.java
public abstract class AbstractEtlJob {

    protected final RunServices svc;
    protected AbstractEtlJob(RunServices svc) { this.svc = svc; }

    /** Subclasses only decide WHICH steps, never the lifecycle around them. */
    protected abstract List<Step> pipeline(PipelineBuilder builder);

    public final JobResult run() {
        RunContext ctx = svc.ctx();
        JobLock lock = svc.control().locks().acquire(ctx);                    // LOCK_NOT_ACQUIRED → SKIPPED
        svc.control().audit().started(ctx);
        PipelineState state = new PipelineState();
        try (LogContext ignored = LogContext.forRun(ctx)) {                    // MDC: runId, jobName, mode, env
            for (Step step : pipeline(new PipelineBuilder(svc))) {
                if (state.isEmpty() && !(step instanceof AlwaysRunStep)) continue;   // empty-run short-circuit
                lock.assertHeld();                                             // stop safely if the lock was lost
                new TimedStep(step).execute(state, svc);                       // decorator: timing + step audit + metrics
            }
            JobResult result = JobStatusResolver.resolve(ctx, state);          // SUCCESS / PARTIAL (rules: Prompt 07)
            svc.control().audit().finished(ctx, result);
            svc.metrics().runFinished(ctx, result);
            return result;
        } catch (EtlException e) {
            svc.control().audit().failed(ctx, e.errorCode(), e);
            svc.metrics().runFailed(ctx, e.errorCode());
            throw e;
        } catch (Exception e) {
            ErrorCode code = svc.components().errorClassifier().classify(e);   // e.g. SQLException 08xxx → INFRA_DB_UNAVAILABLE
            svc.control().audit().failed(ctx, code, e);
            svc.metrics().runFailed(ctx, code);
            throw new JobFailedException(code, e);
        } finally {
            state.cleanupQuietly();                                            // unpersist cached datasets
            svc.control().locks().release(lock);
            svc.metrics().flush();
        }
    }
}
```

### 6.5 Job types: only the step list differs

```java
// etl-core: pipeline/BronzeToSilverJob.java
public final class BronzeToSilverJob extends AbstractEtlJob {
    public BronzeToSilverJob(RunServices svc) { super(svc); }

    @Override
    protected List<Step> pipeline(PipelineBuilder b) {
        B2SConfig cfg = svc.ctx().config(B2SConfig.class);
        return b.read(cfg.source())                         // ReadStep: ReadPlanStrategy picked by run mode (§7)
                .preFilter(cfg.preFilter())
                .plugins(Hook.AFTER_READ)
                .split(cfg.targets())
                .standardize(cfg.targets())
                .plugins(Hook.AFTER_STANDARDIZE)
                .dq(cfg.dq())
                .dedupe(cfg.dedupe())
                .enrich(cfg.lookups())
                .classify(cfg.childFailurePolicy(), cfg.thresholds())     // throws ThresholdBreachedException
                .deletes(cfg.deleteStrategy())
                .plugins(Hook.BEFORE_WRITE)
                .stage(cfg.targets(), cfg.write())                        // NoOpLoader in dry-run
                .merge(MergeStrategies.guardedUpsert(cfg))                // commit happens here
                .quarantine()
                .writeBack(StatusWriteBacks.bronzeMongo(cfg.source(), cfg.writeBack()))
                .reconcile(cfg.reconcile())
                .lineage()
                .build();
    }
}

// etl-core: pipeline/SilverToGoldFactJob.java — same builder, different pieces
@Override
protected List<Step> pipeline(PipelineBuilder b) {
    FactConfig cfg = svc.ctx().config(FactConfig.class);
    return b.read(cfg.sources())                            // SilverEpasReader + secondary-table key expansion
            .dq(cfg.dq())
            .dedupe(cfg.grainDedupe())
            .computeMeasures(cfg.measures(), cfg.dateKeys(), cfg.junkDimensions())
            .classify(ChildFailurePolicy.PARTIAL, cfg.thresholds())
            .stage(cfg.target(), cfg.write())
            .merge(MergeStrategies.fact(cfg))               // inferred members + SK lookup + partition moves, one tx
            .quarantine()
            .writeBack(StatusWriteBacks.silverSummary(cfg.sources()))   // gold_record_status + Silver summary + bookmarks
            .reconcile(cfg.reconcile())
            .lineage()
            .build();
}
```

### 6.6 Component interfaces

```java
// ── Reading ──────────────────────────────────────────────────────────────────────────
public interface SourceReader {
    boolean isEmpty(ReadPlan plan);                                // cheap "is there anything to do?" check
    Dataset<Row> read(ReadPlan plan);
}

// ── Writing ──────────────────────────────────────────────────────────────────────────
public interface TargetLoader {                                     // Spark → staging tables
    LoadStats load(String stagingTable, Dataset<Row> rows);
}
public interface MergeStrategy {                                    // staging → target, inside ONE transaction
    MergeStats merge(TransactionHandle tx, RunContext ctx);
}

// ── Validation ───────────────────────────────────────────────────────────────────────
public interface DqRule extends Serializable {
    String id(); Severity severity(); String errorCode();
    Column evaluate(DqContext ctx);                                 // null = passed, struct = error (doc 03 §5)
}
public interface DqRuleType {                                       // SPI: lets plugins add NEW rule types
    String typeName();                                              // e.g. "gstin"
    DqRule create(RuleSpec spec);
}

// ── Transforming ─────────────────────────────────────────────────────────────────────
public interface Transformer {
    Dataset<Row> apply(Dataset<Row> input, TransformContext ctx);
}
public interface ColumnTransform {                                  // the small built-ins: trim, upper, toUtc ...
    String name();
    Column apply(Column c, Map<String, Object> params);
}

// ── Dedupe, splitting, SCD ───────────────────────────────────────────────────────────
public interface Deduplicator {
    Dataset<Row> apply(Dataset<Row> rows, DedupeSpec spec);         // losers flagged SKIPPED, not dropped silently
}
public interface TableSplitter {
    Map<String, Dataset<Row>> split(Dataset<Row> source, List<TargetSpec> targets);
}
public interface ScdHandler {
    Dataset<Row> detectChanges(Dataset<Row> incoming, DimSpec spec);   // adds the "action" column (doc 04 §6.4)
    List<String> applySql(DimSpec spec);                                 // statements for the merge transaction
}

// ── Errors, quarantine, write-back ───────────────────────────────────────────────────
public interface QuarantineWriter {
    QuarantineStats write(RunContext ctx, Dataset<Row> quarantinedRows);
}
public interface StatusWriteBack {                                  // to the PREVIOUS layer
    WriteBackStats apply(RunContext ctx, Dataset<Row> recordStatuses);
}
public interface ErrorClassifier {
    ErrorCode classify(Throwable t);
}

// ── Control (etl-control, plain JDBC, also used by Spring apps) ──────────────────────
public interface LockManager {
    JobLock acquire(RunContext ctx);
    void release(JobLock lock);
}
public interface RunAuditor {
    void started(RunContext ctx);
    void stepFinished(RunContext ctx, StepAudit step);
    void finished(RunContext ctx, JobResult result);
    void failed(RunContext ctx, ErrorCode code, Throwable t);
}
public interface WatermarkManager {
    Optional<Long> current(String job, String sourceTable);
    void advance(TransactionHandle tx, String job, String sourceTable, long newValue, String runId);  // never backwards
}

// ── Observability ────────────────────────────────────────────────────────────────────
public interface LineageEmitter {
    void runStarted(RunContext ctx, List<String> inputs, List<String> outputs);
    void runCompleted(RunContext ctx, JobResult result);
}
public interface MetricsPublisher {
    void runFinished(RunContext ctx, JobResult r);
    void runFailed(RunContext ctx, ErrorCode code);
    void stepTimed(RunContext ctx, String step, Duration d, long rowsOut);
    void flush();
}
```

### 6.7 `TimedStep` (Decorator)

```java
// etl-core: pipeline/TimedStep.java
public final class TimedStep implements Step {
    private final Step inner;
    public TimedStep(Step inner) { this.inner = inner; }
    @Override public String name() { return inner.name(); }

    @Override
    public void execute(PipelineState state, RunServices svc) {
        Instant start = Instant.now();
        try (var ignored = LogContext.step(inner.name())) {                       // MDC "step"
            svc.spark().sparkContext().setJobGroup(svc.ctx().runId(),              // ➕ Spark UI groups by runId
                    svc.ctx().jobName() + ":" + inner.name(), true);
            inner.execute(state, svc);
            record(svc, start, StepStatus.SUCCESS, state);
        } catch (RuntimeException e) {
            record(svc, start, StepStatus.FAILED, state);
            throw e;
        }
    }

    private void record(RunServices svc, Instant start, StepStatus status, PipelineState state) {
        Duration d = Duration.between(start, Instant.now());
        svc.control().audit().stepFinished(svc.ctx(), StepAudit.of(inner.name(), status, start, d, state.lastRowsOut()));
        svc.metrics().stepTimed(svc.ctx(), inner.name(), d, state.lastRowsOut());
    }
}
```

### 6.8 `ComponentFactory` (Composition Root, no Spring)

> **In simple words:** one class creates all the tools and connects them. Like plugging all appliances into one switchboard. If you want to know "where does the Mongo reader come from?", there is exactly one place to look.

```java
// etl-core: runner/ComponentFactory.java
public final class ComponentFactory {
    private final RunContext ctx;
    private final EnvConfig env;                 // datasources, Spark, Kafka (doc 08)
    private final SecretResolver secrets;

    public SourceReader readerFor(SourceSpec src, SparkSession spark) {
        return switch (src.kind()) {
            case BRONZE_MONGO -> new BronzeMongoReader(spark, env.mongo(src.datasource(), secrets), new MongoFilterTranslator());
            case SILVER_EPAS  -> new SilverEpasReader(spark, env.jdbc(src.datasource(), secrets), new SqlFilterTranslator());
        };
    }

    public TargetLoader loader(WriteSettings w) {
        if (ctx.dryRun()) return new NoOpLoader();                                // Null Object: dry-run never writes
        return w.method() == WriteMethod.COPY ? new CopyLoader(env.jdbc("warehouse", secrets))
                                              : new JdbcBatchLoader(env.jdbc("warehouse", secrets), w.batchSize());
    }

    public StatusWriteBack bronzeWriteBack(BronzeSourceSpec src, WriteBackSettings s) {
        return ctx.dryRun() ? new NoOpWriteBack() : new BronzeMongoWriteBack(env.mongo(src.datasource(), secrets), s);
    }

    public QuarantineWriter quarantineWriter() {
        return ctx.dryRun() ? new NoOpQuarantineWriter() : new EpasQuarantineWriter(env.jdbc("warehouse", secrets));
    }

    public ErrorClassifier errorClassifier() { return new DefaultErrorClassifier(); }
    // ... one method per component
}
```

The merge step also gets a no-op transaction in dry-run, so **no write path can be forgotten**. `dq_results` and `job_run_audit` **are** written in dry-run (with `is_dry_run = true`), because seeing the results is the whole point of a dry run.

---

## 7. Run Modes: Same Steps, Different "What to Read"

> **In simple words**
> All run modes use the **same assembly line**. The only thing that changes is the **shopping list** at the start: which records to take from the shelf.
> - Incremental: "everything new since last time"
> - Backfill: "everything from 1 to 8 October"
> - ID reprocess: "only these 5 orders"
>
> A small object called **ReadPlan** is the shopping list. It says **what** to read in a neutral way. Each reader then translates it into its own language: a MongoDB `$match` for Bronze, or a SQL `WHERE` for Silver.

### 7.1 ReadPlan = the neutral shopping list

```java
// etl-core: runmode/ReadPlan.java — a tiny filter "tree", independent of Mongo or SQL
public sealed interface ReadFilter {
    record StatusIn(Set<SyncStatus> statuses, int maxRetries, Instant retryDueBefore) implements ReadFilter {}  // Bronze sync status + backoff
    record ChangeSeqAbove(String sourceTable, long fromExclusive, long toInclusive) implements ReadFilter {}  // Silver
    record RetryList(String jobName) implements ReadFilter {}                                 // gold_record_status
    record EventTimeBetween(Instant from, Instant to) implements ReadFilter {}                // backfill
    record IdIn(List<String> ids) implements ReadFilter {}                                    // ID reprocess
    record QuarantineReady(String jobName) implements ReadFilter {}                           // reprocess quarantine
    record IngestedBefore(Instant cutoff) implements ReadFilter {}                            // run cutoff
    record All() implements ReadFilter {}                                                     // full load
    record Or(List<ReadFilter> any) implements ReadFilter {}
    record And(List<ReadFilter> all) implements ReadFilter {}
}

public record ReadPlan(ReadFilter filter, OptionalLong maxRecords, Map<String, Long> cutoffs) {}
```

```java
// The Bronze reader translates the plan into a Mongo $match
public final class MongoFilterTranslator {
    public Document toMatch(ReadFilter f) {
        return switch (f) {
            case ReadFilter.StatusIn s -> new Document("$or", List.of(
                    new Document("_etl.syncSilver.status", new Document("$in", List.of("PENDING", "PARTIAL"))),
                    new Document("_etl.syncSilver.status", "FAILED")
                            .append("_etl.syncSilver.retryCount", new Document("$lt", s.maxRetries()))
                            .append("_etl.syncSilver.nextRetryAt", new Document("$lte", Date.from(s.retryDueBefore())))));   // doc 07 §3.5
            case ReadFilter.EventTimeBetween t -> new Document("_etl.sourceEventTs",
                    new Document("$gte", Date.from(t.from())).append("$lt", Date.from(t.to())));
            case ReadFilter.IdIn i -> new Document("_etl.sourceId", new Document("$in", i.ids()));
            case ReadFilter.IngestedBefore c -> new Document("_etl.lastIngestedAt", new Document("$lte", Date.from(c.cutoff())));
            case ReadFilter.All a -> new Document();
            case ReadFilter.And a -> new Document("$and", a.all().stream().map(this::toMatch).toList());
            case ReadFilter.Or o -> new Document("$or", o.any().stream().map(this::toMatch).toList());
            case ReadFilter.QuarantineReady q -> idsFromQuarantine(q);   // looks up ids in etl_ctl, then $in
            default -> throw new ConfigException(ErrorCode.CONFIG_INVALID, f + " not supported for Bronze");
        };
    }
}
// The Silver reader has an SqlFilterTranslator with the same shape that produces a WHERE clause with bind parameters.
```

### 7.2 RunModePolicy = everything that depends on the run mode

> **In simple words:** besides the shopping list, a run mode also decides a few "after" actions, e.g. "may this run move the bookmark forward?". All these decisions sit together in **one small class per run mode**, instead of being spread over the code.

```java
// etl-core: runmode/RunModePolicy.java
public interface RunModePolicy {
    ReadPlan readPlan(RunContext ctx, SourceSpec src, ControlStore control);
    boolean advancesWatermark();               // may this run move the bookmark?
    boolean usesRunCutoff();
    default List<PostCommitHook> postCommitHooks() { return List.of(); }   // e.g. quarantine → REPROCESSED
}
```

| Run mode | ReadPlan (Bronze → Silver) | ReadPlan (Silver → Gold) | Moves bookmark? | After commit |
|---|---|---|---|---|
| `INCREMENTAL` | `StatusIn(PENDING, FAILED<max, PARTIAL)` AND `IngestedBefore(cutoff)` | `ChangeSeqAbove(bookmark, cutoff)` OR `RetryList` | ✅ yes | — |
| `FULL_LOAD` | `All` AND `IngestedBefore(cutoff)` | `All` (facts: optional shadow swap) | ✅ yes (to the cutoff) | — |
| `RERUN_JOB` | same as INCREMENTAL (the failed run wrote nothing) | same as INCREMENTAL | ✅ yes | link `parent_run_id` |
| `RERUN_FAILED_RECORDS` | `StatusIn(FAILED, PARTIAL)` (optionally only of `parentRunId`) | `RetryList` only | ❌ no | — |
| `REPROCESS_QUARANTINE` | `QuarantineReady(job)` | `QuarantineReady(job)` | ❌ no | quarantine → `REPROCESSED` |
| `BACKFILL` | `EventTimeBetween(from, to)` | `EventTimeBetween(from, to)` | ❌ no | — |
| `REPLAY` | `All` or `EventTimeBetween` (after Bronze was rebuilt) | `All` or `EventTimeBetween` | ❌ no | — |
| `ID_REPROCESS` | `IdIn(ids)` | `IdIn(ids)` | ❌ no | — |
| any mode + `--dry-run` | same as the mode | same as the mode | ❌ never | nothing is written (Null Objects) |

```java
// etl-core: runmode/RunModePolicies.java
public final class RunModePolicies {
    public static RunModePolicy forMode(RunMode mode) {
        return switch (mode) {
            case INCREMENTAL           -> new IncrementalPolicy();
            case FULL_LOAD             -> new FullLoadPolicy();
            case RERUN_JOB             -> new IncrementalPolicy();          // same reading rule
            case RERUN_FAILED_RECORDS  -> new RerunFailedPolicy();
            case REPROCESS_QUARANTINE  -> new ReprocessQuarantinePolicy();
            case BACKFILL              -> new BackfillPolicy();
            case REPLAY                -> new ReplayPolicy();
            case ID_REPROCESS          -> new IdReprocessPolicy();
        };
    }
}
```

Because every step after `ReadStep` works on "whatever was read", **adding a new run mode = one new policy class**. No step changes.

---

## 8. One Generic Entry Point: `EtlJobRunner`

### 8.1 Generic runner vs one main class per job

| | **One generic runner** (chosen) | One `main` class per job |
|---|---|---|
| New job | Add YAML only | Add YAML + a Java class + a build/deploy change |
| 100+ jobs | Still 1 entry point | 100+ classes, most of them copy-paste |
| Consistency | Every job gets the same lifecycle, logs, audit, exit codes | Each class can drift |
| Airflow | One operator template for all jobs | Different arguments per job |
| Debugging a single job | `--job=x --dry-run` locally | Same |

### 8.2 Command line

```bash
spark-submit --class com.company.etl.core.runner.EtlJobRunner etl-spark-app-1.14.2.jar \
  --job=b2s_order \
  --mode=BACKFILL \
  --from=2026-10-01T00:00:00Z --to=2026-10-08T00:00:00Z \
  --trigger=MANUAL --triggered-by=asha.p --ticket=INC-1042 \
  --env=prod \
  --run-id=b2s_order-20261030T091500Z-01      # optional: Airflow passes it so a task retry reuses the same id
  [--dry-run]
  [--ids=65f1…,65f2…  |  --ids-file=/tmp/ids.txt]   # ➕ a file for long ID lists
  [--parent-run-id=b2s_order-20261029T091500Z-01]
  [--jobs=b2s_customer,b2s_product]                 # ➕ run several small jobs in one Spark app (doc 03 §11)
  [--override=resources.executors=8]                # ➕ only allow-listed keys
```

**Your mode names → our values** (the run mode and the dry-run flag are separate, see doc 05 §6.6):

| You wrote | `--mode` value | Note |
|---|---|---|
| full | `FULL_LOAD` | |
| incremental | `INCREMENTAL` | default if `--mode` is missing |
| rerun-job | `RERUN_JOB` | needs `--parent-run-id` |
| rerun-failed | `RERUN_FAILED_RECORDS` | `--parent-run-id` optional |
| reprocess-quarantine | `REPROCESS_QUARANTINE` | |
| backfill | `BACKFILL` | needs `--from` and `--to` |
| replay | `REPLAY` | optional `--from`/`--to` |
| ids | `ID_REPROCESS` | needs `--ids` or `--ids-file` |
| dry-run | **`--dry-run` flag** | works with any mode; `--mode=dry-run` is accepted as an alias for `INCREMENTAL --dry-run`, with a warning |

**Exit codes** (Airflow reads these, Prompt 09):

| Exit code | Meaning |
|---|---|
| `0` | SUCCESS |
| `10` | PARTIAL (finished, some records not OK) |
| `20` | SKIPPED (lock held, job disabled, upstream policy SKIP) |
| `30` | FAILED, retryable (infra) |
| `40` | FAILED, not retryable (config invalid, threshold breached, reconciliation mismatch) |

➕ The runner also prints **one machine-readable line** at the end, which Airflow captures and shows in the UI:
`ETL_RESULT {"runId":"…","status":"PARTIAL","read":1000,"failed":10,"quarantined":7,"errorCode":null}`

### 8.3 Runner skeleton

```java
// etl-core: runner/EtlJobRunner.java  (picocli = a small CLI-parsing library, no Spring needed)
@Command(name = "etl-job-runner", mixinStandardHelpOptions = true)
public final class EtlJobRunner implements Callable<Integer> {

    @Option(names = "--job")            String job;
    @Option(names = "--jobs", split = ",") List<String> jobs;
    @Option(names = "--mode", defaultValue = "INCREMENTAL", converter = RunModeConverter.class) RunMode mode;
    @Option(names = "--trigger", defaultValue = "SCHEDULED") TriggerType trigger;
    @Option(names = "--triggered-by", defaultValue = "airflow") String triggeredBy;
    @Option(names = "--env", required = true) String env;
    @Option(names = "--dry-run")        boolean dryRun;
    @Option(names = "--from")           Instant from;
    @Option(names = "--to")             Instant to;
    @Option(names = "--ids", split = ",") List<String> ids;
    @Option(names = "--ids-file")       Path idsFile;
    @Option(names = "--parent-run-id")  String parentRunId;
    @Option(names = "--run-id")         String runId;
    @Option(names = "--ticket")         String ticket;
    @Option(names = "--override")       List<String> overrides;

    public static void main(String[] args) { System.exit(new CommandLine(new EtlJobRunner()).execute(args)); }

    @Override
    public Integer call() {
        List<String> jobNames = jobs != null ? jobs : List.of(Objects.requireNonNull(job, "--job or --jobs required"));
        SparkSession spark = SparkSessions.create(env);                    // one session, reused for grouped jobs
        int worst = 0;
        for (String name : jobNames) {
            worst = Math.max(worst, runOne(spark, name));                   // each job: own lock, audit, runId
        }
        spark.stop();
        return worst;
    }

    private int runOne(SparkSession spark, String name) {
        try {
            ResolvedJobConfig cfg = ConfigLoader.load(name, env, overrides);           // merge + validate
            RunArgsValidator.check(mode, from, to, ids, idsFile, parentRunId);        // e.g. BACKFILL needs from/to
            RunContext ctx = RunContextFactory.create(name, runId, mode, trigger, dryRun, env,
                                                      params(), cfg, BuildInfo.version());
            RunServices svc = ServicesFactory.create(ctx, spark);
            JobResult result = new JobTypeRegistry().create(svc).run();
            ResultPrinter.print(result);                                               // ETL_RESULT {...}
            return ExitCodes.of(result.status());
        } catch (LockNotAcquiredException e) {
            return ExitCodes.SKIPPED;
        } catch (EtlException e) {
            return e.retryable() ? ExitCodes.FAILED_RETRYABLE : ExitCodes.FAILED_FINAL;
        } catch (Exception e) {
            LoggerFactory.getLogger(EtlJobRunner.class).error("Unclassified failure", e);
            return ExitCodes.FAILED_RETRYABLE;                                         // INFRA_UNKNOWN
        }
    }
}
```

---

## 9. Spring Boot and Spark: Where Each One Belongs

### 9.1 Recommendation

> **In simple words**
> **Spring Boot** is great for programs that **run all the time** and answer requests (our ingestion service and the ops API).
> **Spark jobs** are **short programs that start, crunch data on many machines, and stop**. Spring's strengths (a long-lived application context, web server, auto-configuration) don't help there, and it causes several real problems.
> So: **Spring only in the always-running services; plain Java in Spark jobs.** The shared code lives in plain libraries that both sides use.

| Component | Spring Boot? |
|---|---|
| `etl-ingestion` (Kafka consumer, runs 24×7) | ✅ Yes |
| `etl-ops-api` (REST API + UI backend) | ✅ Yes |
| `etl-spark-app` (all Spark jobs) | ❌ No: plain Java + `ComponentFactory` |
| `etl-common`, `etl-config`, `etl-control` (shared libraries) | ❌ No Spring inside; the Spring apps wrap them as beans |

### 9.2 Why not Spring inside Spark (trade-offs)

| Problem | Explanation |
|---|---|
| **Executors don't have the Spring context** | Spark sends your functions to executor machines by serializing them. Spring beans (often proxies) are usually not serializable, and the executors never started a Spring context. Code that works on the driver breaks on executors |
| **Library version conflicts** | Spring Boot pins its own versions of Jackson, SnakeYAML, Netty, logging (Logback). Spark ships its own versions (and Log4j 2). Two versions of the same library on one classpath cause `NoSuchMethodError` at runtime |
| **Fat-JAR layout** | A Spring Boot JAR uses a special layout (`BOOT-INF/`) and its own class loader. `spark-submit` expects a normal JAR |
| **Startup time** | Spring scanning adds seconds to every one of the hundreds of daily runs, for no benefit |
| **Hidden magic** | Auto-configuration makes it harder to see what a job does; in batch jobs, explicit wiring is clearer |

**Alternative considered:** a minimal plain Spring context (not Spring Boot) only in the driver. It is possible, but it still brings version conflicts and gives little value over one `ComponentFactory` class.

### 9.3 How the two sides share code

```
                ┌──────────────────────────────┐
                │ etl-common  (models, enums,  │
                │ errors, hashing, time)       │
                ├──────────────────────────────┤
                │ etl-config  (YAML, schema,   │
                │ secrets interface)           │
                ├──────────────────────────────┤
                │ etl-control (plain JDBC DAOs │
                │ for etl_ctl tables)          │
                └───────┬──────────────┬───────┘
          plain Java    │              │   wrapped as Spring @Bean
                        ▼              ▼
          ┌──────────────────┐   ┌──────────────────────────────┐
          │ etl-spark-app    │   │ etl-ops-api / etl-ingestion  │
          │ ComponentFactory │   │ @Configuration               │
          │ (new ...)        │   │ JobLockManager jobLock(...)  │
          └──────────────────┘   └──────────────────────────────┘
```

```java
// etl-ops-api: config/ControlConfig.java — Spring simply wraps the plain library
@Configuration
class ControlConfig {
    @Bean JobLockManager jobLockManager(DataSource ds) { return new JobLockManager(ds); }
    @Bean QuarantineDao quarantineDao(DataSource ds)  { return new QuarantineDao(ds); }
}
```

---

## 10. Plugins: Custom Code Without Touching the Framework

> **In simple words:** sometimes a rule is too special for YAML (for example "normalize old address formats from the 2015 system"). Instead of adding that to the framework, a developer writes a **small class** in `etl-jobs/<domain>/`, gives it a name, and refers to that name in the YAML. The framework calls it at a fixed point in the pipeline (a **hook**).

### 10.1 Plugin contracts

```java
// etl-core: plugin/CustomTransformer.java
public interface CustomTransformer extends Serializable {
    String name();                                         // "order.gift_message_cleaner" (domain.prefix required)
    Set<String> requiredColumns();                         // checked before the call
    Set<String> producedColumns();                         // checked after the call
    Dataset<Row> apply(Dataset<Row> input, Map<String, Object> params, PluginContext ctx);
}

// Hook points where plugins may run
public enum Hook { AFTER_READ, AFTER_SPLIT, AFTER_STANDARDIZE, BEFORE_DQ, BEFORE_WRITE }
```

### 10.2 Example plugin

```java
// etl-jobs: order/GiftMessageCleaner.java
public final class GiftMessageCleaner implements CustomTransformer {
    @Override public String name() { return "order.gift_message_cleaner"; }
    @Override public Set<String> requiredColumns() { return Set.of("gift_message"); }
    @Override public Set<String> producedColumns() { return Set.of("gift_message"); }

    @Override
    public Dataset<Row> apply(Dataset<Row> in, Map<String, Object> params, PluginContext ctx) {
        int max = ((Number) params.getOrDefault("maxLength", 200)).intValue();
        Column cleaned = regexp_replace(col("gift_message"), "[\\p{Cntrl}]", "");      // built-in functions, no UDF
        return in.withColumn("gift_message", substring(trim(cleaned), 1, max));
    }
}
```

```
# etl-jobs/src/main/resources/META-INF/services/com.company.etl.core.plugin.CustomTransformer
com.company.etl.jobs.order.GiftMessageCleaner
```

### 10.3 Plugin registry

```java
// etl-core: plugin/PluginRegistry.java — Java ServiceLoader finds every plugin on the classpath
public final class PluginRegistry {
    private final Map<String, CustomTransformer> transformers = new HashMap<>();
    private final Map<String, DqRuleType> ruleTypes = new HashMap<>();

    public PluginRegistry() {
        ServiceLoader.load(CustomTransformer.class).forEach(p -> register(transformers, p.name(), p));
        ServiceLoader.load(DqRuleType.class).forEach(p -> register(ruleTypes, p.typeName(), p));
    }
    private static <T> void register(Map<String, T> map, String name, T p) {
        if (map.putIfAbsent(name, p) != null) throw new ConfigException(ErrorCode.CONFIG_INVALID, "Duplicate plugin " + name);
    }
    public CustomTransformer transformer(String name) {
        return Optional.ofNullable(transformers.get(name))
                .orElseThrow(() -> new ConfigException(ErrorCode.CONFIG_INVALID, "Unknown plugin " + name));
    }
}
```

### 10.4 Plugin rules ➕

| Rule | Why |
|---|---|
| Pure function: `Dataset in → Dataset out`; **no** database writes, no network calls | Writes must go through the framework (transactions, audit, dry-run) |
| Built-in Spark functions; a UDF only with a reviewer's approval | UDFs are slow and invisible to the optimizer |
| No `collect()` on big data | Pulling millions of rows to the driver causes OOM (Out Of Memory) |
| Name prefixed by the domain (`order.*`) and lives in `etl-jobs/<domain>/` | Clear ownership (CODEOWNERS) |
| Unit test required (with `etl-testkit`) | Plugins are the most "custom", so the most risky |
| If 3+ jobs need the same plugin → **promote it** to a built-in transform/rule in `etl-core` | Prevents copy-paste plugins |

---

## 11. Versioning, Packaging and Deployment

### 11.1 Packaging the Spark JAR

> **In simple words:** a Spark job is shipped as **one big JAR** ("fat" or "shaded" JAR) that contains our code and the libraries Spark does **not** already provide. Libraries that Spark already has are marked `provided`, so we don't ship a second, conflicting copy. "Shading" = renaming a library's packages inside our JAR, so our copy can't clash with Spark's copy.

```xml
<!-- etl-spark-app/pom.xml (key parts) -->
<dependencies>
  <!-- Provided by the Spark runtime: NOT packaged -->
  <dependency><groupId>org.apache.spark</groupId><artifactId>spark-sql_2.12</artifactId><scope>provided</scope></dependency>

  <!-- Packaged -->
  <dependency><groupId>com.company.etl</groupId><artifactId>etl-core</artifactId></dependency>
  <dependency><groupId>com.company.etl</groupId><artifactId>etl-jobs</artifactId></dependency>
  <dependency><groupId>org.mongodb.spark</groupId><artifactId>mongo-spark-connector_2.12</artifactId></dependency>  <!-- Scala suffix MUST match Spark's -->
  <dependency><groupId>org.postgresql</groupId><artifactId>postgresql</artifactId></dependency>  <!-- or the EDB JDBC driver -->
  <dependency><groupId>info.picocli</groupId><artifactId>picocli</artifactId></dependency>
</dependencies>

<build><plugins><plugin>
  <groupId>org.apache.maven.plugins</groupId><artifactId>maven-shade-plugin</artifactId>
  <configuration>
    <createDependencyReducedPom>false</createDependencyReducedPom>
    <transformers>
      <!-- ➕ MERGE META-INF/services files: without this, format("mongodb"), JDBC drivers and our plugins disappear -->
      <transformer implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
      <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
        <mainClass>com.company.etl.core.runner.EtlJobRunner</mainClass>
      </transformer>
    </transformers>
    <relocations>
      <relocation><pattern>com.google.common</pattern><shadedPattern>etlshaded.com.google.common</shadedPattern></relocation>
      <relocation><pattern>picocli</pattern><shadedPattern>etlshaded.picocli</shadedPattern></relocation>
    </relocations>
    <filters><filter><artifact>*:*</artifact>
      <excludes><exclude>META-INF/*.SF</exclude><exclude>META-INF/*.DSA</exclude><exclude>META-INF/*.RSA</exclude></excludes>
    </filter></filters>
  </configuration>
</plugin></plugins></build>
```

**Dependency conflict rules:**

| Rule | How |
|---|---|
| Pin versions **once** | Parent POM `dependencyManagement` (our BOM). Spark modules align Jackson to **Spark's** Jackson version |
| Detect conflicts in the build | `maven-enforcer-plugin` with `dependencyConvergence` and banned dependencies (e.g. no `spring-*` in `etl-core`, `etl-jobs`, `etl-spark-app`) |
| Match the Scala version | All `_2.12` / `_2.13` artifacts must match the Spark build |
| Java version ➕ | Spark 3.5 runs on Java 17 but needs `--add-opens` JVM options (set in the Spark defaults, Prompt 08) |
| Never use `spark.driver.userClassPathFirst` as a fix | It hides conflicts and creates new ones; fix the dependency instead |
| Architecture tests | ArchUnit tests fail the build if, e.g., `etl-core` imports `org.springframework` |

### 11.2 Versions

| Item | Versioning |
|---|---|
| `etl-spark-app` JAR / Docker image | SemVer (Semantic Versioning) `MAJOR.MINOR.PATCH`, e.g. `1.14.2`. Stored in every `job_run_audit.code_version` |
| YAML format | `configVersion: 1`. The JAR supports the current and the previous `configVersion` ➕, so configs and code can be upgraded in two steps |
| Job config content | `config_hash` + Git commit in `job_config_version` (doc 05) |
| DB schema | Flyway migration numbers |
| Error codes | Catalog in Git; codes are never renamed, only deprecated |

**What is MAJOR / MINOR / PATCH here:**
- PATCH (`1.14.2 → 1.14.3`): bug fix, no behaviour change for correct data.
- MINOR (`1.14 → 1.15`): new feature (a new transform or rule type), backwards compatible.
- MAJOR (`1 → 2`): a breaking change (e.g. `configVersion` 2 required). Planned and announced.

### 11.3 Deployment

> **In simple words:** build once, then promote the **same** artifact through the environments. Only the configuration differs per environment, never the code.

```
 git push / merge to main
   │
   ▼  CI (Continuous Integration)
 build all modules → unit tests → config validation (all YAML) → integration tests (Testcontainers)
   │
   ▼
 Docker image  registry/etl-spark-app:1.14.2
   contains: Spark runtime + etl-spark-app-1.14.2.jar + pipelines/ + dq-rules/ + contracts/ + config/environments/
   (configs are BAKED IN → code and config of one release always belong together)  ➕
   │
   ▼  CD (Continuous Delivery): promote by changing ONE pinned version per environment
 dev  (auto)  →  qa  (auto after tests)  →  uat  (approval)  →  prod (approval + change ticket)
   Airflow Variable per env:  etl_spark_app_version = 1.14.2
   Registry sync (doc 05 §7) runs for that env with the same commit
   │
   ▼
 Airflow → SparkKubernetesOperator / spark-submit with image tag 1.14.2 (Prompt 09)
 Rollback = set the pinned version back to 1.14.1 (secrets and data untouched)
```

| Decision | Why |
|---|---|
| Configs baked into the image (not mounted from somewhere else) | A run is fully reproducible from one image tag; no "config changed but code didn't" mismatch |
| Secrets **not** in the image | Read at runtime from the secret manager (Prompt 08) |
| One pinned version per environment | Promotion and rollback are one-line changes |
| A **hotfix of one YAML** still goes through the pipeline | Even config changes are code changes: reviewed, tested, versioned |

---

## 12. Coding Standards

### 12.1 General Java rules

| Rule | Example / reason |
|---|---|
| Immutable models (`record`) for configs and contexts | No accidental changes during a run |
| No mutable `static` state | Executors run many tasks in one JVM (Java Virtual Machine); static state leaks between tasks |
| Constructor injection, created only in `ComponentFactory` | One place to see the wiring |
| `Optional` instead of returning `null` | Fewer `NullPointerException`s |
| No `System.out`; only SLF4J logging | Structured logs (§13) |
| Package by feature (`dq`, `io.reader`, `modeling.scd`), not by layer type | Related code stays together |
| Public interfaces have Javadoc; every step has a one-line purpose comment | New developers find their way |
| Formatting/linting enforced in the build (Spotless/Checkstyle) | No style discussions in code reviews |

### 12.2 Spark rules ➕

| Rule | Why |
|---|---|
| Built-in functions, no UDFs (except approved plugins) | Speed + optimizer visibility |
| No `collect()`, `toLocalIterator()` or `count()` inside loops on large datasets | Driver OOM, repeated full scans |
| `persist()` only through `PipelineState.persist()` | Guarantees `unpersist()` at the end |
| Explicit schemas for reads | No unstable schema inference |
| No accumulators for business counts | Wrong values on task retries (use aggregations) |
| `repartition(n)` before database writes, n from config | Controls the number of DB connections |
| Every `Dataset` transformation is lazy until an action; avoid triggering an action twice on the same uncached data | Avoids reading the source twice |

### 12.3 SQL rules ➕

| Rule | Why |
|---|---|
| SQL is **generated** from config by generator classes, never hand-built per job | One tested place |
| **Values** always as bind parameters (`?` / `:name`) | Prevents SQL injection and plan-cache bloat |
| **Identifiers** (table/column names from YAML) validated against a strict pattern and quoted | YAML can't inject SQL |
| Every generated statement is logged at DEBUG with its `runId` | Easy to reproduce a merge by hand |
| `ApplicationName=etl:<runId>` in the JDBC (Java Database Connectivity) connection ➕ | DBAs see in `pg_stat_activity` which run executes a slow query |

### 12.4 Testing standards

| Level | What |
|---|---|
| Unit | Every component, rule type, transform and plugin, with a local `SparkSession` from `etl-testkit` |
| ➕ Golden dataset tests | A **generic test** runs any job YAML against fixture files: `src/test/resources/jobs/b2s_order/input/*.json` → `expected/*.csv`. A new job gets a test by **adding files**, not code |
| Integration | Testcontainers (EPAS-compatible PostgreSQL, MongoDB replica set) for merges, SCD, locks, write-back |
| Architecture | ArchUnit: module boundaries, no Spring in Spark modules, no `System.out` |
| Config | Every YAML validated in CI (schema + reference checks, §3.5) |

---

## 13. Logging Standard (with `runId` Correlation)

> **In simple words:** every log line carries the **run id** like a tracking number. When a run fails, you search for that one id and find **all** its log lines: from Airflow, the Spark driver, every executor and the database.

### 13.1 Log format

JSON (JavaScript Object Notation) lines, one per event, via SLF4J + Log4j 2 (Spark's logging backend) with a JSON layout:

```json
{"ts":"2026-10-30T09:15:42.118Z","level":"INFO","logger":"c.c.etl.core.pipeline.TimedStep",
 "msg":"step finished","runId":"b2s_order-20261030T091500Z-01","jobName":"b2s_order","runMode":"INCREMENTAL",
 "env":"prod","step":"MERGE","durationMs":8421,"rowsOut":978,"sparkAppId":"spark-7f3a…","codeVersion":"1.14.2"}
```

### 13.2 MDC fields (Mapped Diagnostic Context)

| Field | Set where |
|---|---|
| `runId`, `jobName`, `runMode`, `env`, `codeVersion` | `LogContext.forRun(ctx)` at the start of `AbstractEtlJob.run()` |
| `step` | `TimedStep` |
| `sparkAppId` | After the `SparkSession` starts |
| `ticket`, `triggeredBy` | For manual runs |

**Executors** ➕: the MDC of the driver is **not** copied to executors automatically. The framework sets Spark "local properties" with an `mdc.` prefix (e.g. `mdc.runId`), which Spark 3.x adds to the executor log context, so executor logs carry the same `runId`. (Verify the exact property behaviour on your Spark version in the PoC.)

```java
// etl-core: logging/LogContext.java (excerpt)
public static AutoCloseable forRun(RunContext ctx) {
    MDC.put("runId", ctx.runId()); MDC.put("jobName", ctx.jobName());
    MDC.put("runMode", ctx.runMode().name()); MDC.put("env", ctx.env()); MDC.put("codeVersion", ctx.codeVersion());
    SparkContext sc = SparkSession.active().sparkContext();
    sc.setLocalProperty("mdc.runId", ctx.runId());              // → executor log lines
    sc.setLocalProperty("mdc.jobName", ctx.jobName());
    return MDC::clear;
}
```

### 13.3 What to log at which level

| Level | Use for | Example |
|---|---|---|
| `ERROR` | The run failed, or something needs human action | "Run failed: INFRA_DB_UNAVAILABLE" |
| `WARN` | Unusual but handled | "Write-back stale: 4 documents changed during the run" |
| `INFO` | Run and step boundaries, counts (a few lines per step) | "step finished MERGE rowsOut=978" |
| `DEBUG` | Generated SQL, read plans, config details (off in prod by default) | the merge SQL |
| ❌ never | Field **values** of records (PII), secrets, full documents | — |

### 13.4 One id everywhere

```
 Airflow task log ──┐
 Spark driver log ──┤
 Executor logs   ───┼──► all contain runId=b2s_order-20261030T091500Z-01
 Spark UI job group ┤        → search once in Loki/ELK (log search tools, Prompt 11)
 EPAS pg_stat_activity (ApplicationName=etl:<runId>)
 job_run_audit.run_id, dq_results.run_id, Bronze _etl.syncSilver.runId, Silver _etl_run_id
```

---

## 14. Exception Hierarchy

> **In simple words:** exceptions are for **job-level** problems only ("the database is down", "the config is wrong"). A bad **record** is **not** an exception: it is data that goes into the `_errors` column (doc 03). This keeps one bad row from ever crashing a job of a million rows.

```
 RuntimeException
 └── EtlException  (abstract: errorCode, retryable)
     ├── ConfigException                    CONFIG_INVALID                        not retryable
     ├── ControlException
     │   ├── LockNotAcquiredException       LOCK_NOT_ACQUIRED                     → exit SKIPPED
     │   ├── LockLostException              LOCK_NOT_ACQUIRED                     retryable
     │   └── DependencyFailedException      DEPENDENCY_FAILED                     retryable
     ├── JobAbortedException  (the data itself says "stop")
     │   ├── ThresholdBreachedException     THRESHOLD_BREACHED                    not retryable (needs analysis)
     │   ├── ReconciliationMismatchException RECONCILIATION_MISMATCH              not retryable (framework bug)
     │   └── ScdIntegrityException          SCD_INTEGRITY_VIOLATION               not retryable
     ├── InfraException  (the environment says "stop")
     │   ├── SourceUnavailableException     INFRA_MONGO_UNAVAILABLE / INFRA_DB_UNAVAILABLE   retryable
     │   ├── TargetUnavailableException     INFRA_DB_UNAVAILABLE                  retryable
     │   ├── SparkExecutionException        INFRA_SPARK_FAILURE                   retryable
     │   └── EtlTimeoutException            INFRA_TIMEOUT                         retryable
     ├── PluginException                    HANDLER_ERROR                         not retryable (code fix)
     └── JobFailedException                 wraps anything unclassified           retryable (INFRA_UNKNOWN)
```

```java
// etl-common: error/EtlException.java
public abstract class EtlException extends RuntimeException {
    private final ErrorCode errorCode;
    protected EtlException(ErrorCode code, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = code;
    }
    public ErrorCode errorCode() { return errorCode; }
    public boolean retryable()  { return errorCode.retryable(); }   // from error_code_catalog (doc 05 §6.1)
}

// etl-core: error/DefaultErrorClassifier.java — turns "foreign" exceptions into our codes
public final class DefaultErrorClassifier implements ErrorClassifier {
    @Override
    public ErrorCode classify(Throwable t) {
        Throwable root = Throwables.rootCause(t);                       // unwrap SparkException → real cause
        if (root instanceof EtlException e) return e.errorCode();
        if (root instanceof SQLException s && s.getSQLState() != null && s.getSQLState().startsWith("08"))
            return ErrorCode.INFRA_DB_UNAVAILABLE;                      // SQLState 08xxx = connection errors
        if (root instanceof com.mongodb.MongoTimeoutException || root instanceof com.mongodb.MongoSocketException)
            return ErrorCode.INFRA_MONGO_UNAVAILABLE;
        if (root instanceof java.util.concurrent.TimeoutException) return ErrorCode.INFRA_TIMEOUT;
        if (root instanceof OutOfMemoryError) return ErrorCode.INFRA_SPARK_FAILURE;
        return ErrorCode.INFRA_UNKNOWN;                                 // ➕ alert: "please classify me"
    }
}
```

➕ `INFRA_UNKNOWN` creates a low-priority alert "unclassified exception". Over time every common failure gets its own code, so alerts become more precise.

---

## 15. Final Package Layout of `etl-core`

```
etl-core/src/main/java/com/company/etl/core/
├── runner/        EtlJobRunner, RunContextFactory, ServicesFactory, ComponentFactory, JobTypeRegistry,
│                  ExitCodes, ResultPrinter, RunArgsValidator, SparkSessions
├── pipeline/      AbstractEtlJob, Step, TimedStep, PipelineState, PipelineBuilder, RunServices,
│                  JobStatusResolver, BronzeToSilverJob, SilverToGoldDimensionJob, SilverToGoldFactJob,
│                  SilverToGoldSnapshotJob, SqlAggregateJob
├── pipeline/steps/ReadStep, PreFilterStep, SplitStep, StandardizeStep, DqStep, DedupeStep, EnrichStep,
│                  ClassifyStep, ScdDetectStep, MeasureStep, StageStep, MergeStep, QuarantineStep,
│                  WriteBackStep, ReconcileStep, LineageStep, PluginStep
├── runmode/       ReadFilter, ReadPlan, RunModePolicy, RunModePolicies, *Policy classes
├── io/reader/     SourceReader, BronzeMongoReader, SilverEpasReader, MongoFilterTranslator, SqlFilterTranslator
├── io/writer/     TargetLoader, CopyLoader, JdbcBatchLoader, NoOpLoader, TransactionHandle
├── merge/         MergeStrategy, GuardedUpsertMerge, Scd1Merge, Scd2Merge, FactMerge, PartitionReplace,
│                  ShadowSwap, NoOpMerge, SqlTemplates
├── dq/            DqEngine, DqRule, DqRuleType, RuleFactory, DqError, DqResultCollector, rules/*
├── transform/     Transformer, ColumnTransform, TransformRegistry, Standardizer, Deduplicator(+strategies),
│                  LookupEnricher
├── modeling/      TableSplitter, scd/ (ScdChangeDetector, ScdSqlGenerator), keys/ (InferredMemberSql,
│                  FactMergeSqlGenerator), junk/
├── writeback/     StatusWriteBack, BronzeMongoWriteBack, SilverSummaryWriteBack, NoOpWriteBack
├── quarantine/    QuarantineWriter, EpasQuarantineWriter, NoOpQuarantineWriter, RecordErrorLogger
├── control/       Reconciler, GoldSyncTracker, TouchedPartitionRecorder (use etl-control DAOs)
├── lineage/       LineageEmitter, OpenLineageEmitter, NoOpLineageEmitter
├── metrics/       MetricsPublisher, PushgatewayPublisher, LogOnlyPublisher
├── plugin/        CustomTransformer, Hook, PluginContext, PluginRegistry
├── error/         ErrorClassifier, DefaultErrorClassifier, exception classes
└── logging/       LogContext, LogFields
```

---

## 16. How to Add a New Job: Checklist

> **In simple words:** follow these steps in order. For most jobs, every step is a **file**, not Java code.

### 16.1 New Bronze → Silver job (example: `returns` collection of `svc-order`)

| # | Step | File(s) | Code? |
|---|---|---|---|
| 1 | Make sure the collection is captured by CDC (Change Data Capture): add it to the connector `$match` list | `kafka-connect/connectors/base/mongo-source-svc-order.json` | No |
| 2 | Add the ingestion route | `etl-ingestion/.../ingestion-mappings.yml` | No |
| 3 | Write the data contract (fields, types, PII flags, owner) | `contracts/svc-order/returns.contract.yml` | No |
| 4 | Create the Silver tables (copy the technical column block from the template) | `db/silver/V<n>__silver_order_returns.sql` (+ staging via the template) | No (SQL) |
| 5 | Write the DQ rule set | `dq-rules/order/sales_return.rules.yml` | No |
| 6 | Write the job YAML (copy the closest existing job, adjust) | `pipelines/bronze-to-silver/order/b2s_return.yml` | No |
| 7 | Add a golden test: a few input documents + expected rows | `etl-spark-app/src/test/resources/jobs/b2s_return/{input,expected}/` | No |
| 8 | Only if YAML can't express something: write a plugin + unit test | `etl-jobs/order/...` + `META-INF/services/...` | **Yes, small** |
| 9 | Run locally: `--job=b2s_return --env=dev --dry-run` and read the DQ results | — | — |
| 10 | Open a pull request → CI validates (schema, references, single owner, cycles, golden test) | — | — |
| 11 | After the merge: deploy to dev → the registry sync adds the job → Airflow creates its DAG automatically | — | — |
| 12 | First run: `FULL_LOAD` (manual), then the schedule takes over | ops UI / Airflow | — |

### 16.2 New Silver → Gold job (example: `fact_return_line`)

| # | Step | File(s) |
|---|---|---|
| 1 | Define the **grain** in one sentence ("one row per returned order line") and the measures | ADR (`docs/adr/`) for important facts |
| 2 | Check which conformed dimensions exist in `gold_core`; add missing ones first (their own dimension jobs) | `pipelines/silver-to-gold/core/...` |
| 3 | Create the fact table (partitions, indexes, `v_` view) | `db/gold/V<n>__fact_return_line.sql` |
| 4 | Write the fact YAML: sources (driving + secondary), dimensions with `onMissing`/`onNullKey`, date keys, measures, delete strategy | `pipelines/silver-to-gold/sales/s2g_fact_return_line.yml` |
| 5 | Gold DQ rules | `dq-rules/gold/sales/fact_return_line.yml` |
| 6 | Aggregates/KPIs if needed | `s2g_agg_*.yml`, `kpis.yml` |
| 7 | Golden test, dry-run, pull request, deploy, `FULL_LOAD`, then schedule | as above |

### 16.3 Definition of done for a new job

- [ ] YAML valid, `owner` and `description` filled, `slaMinutes` set
- [ ] DQ rules cover keys, mandatory fields and the main business rules
- [ ] Reconciliation: at least the count check, plus one sum check for money columns
- [ ] Golden test passes
- [ ] Dry-run on dev reviewed (DQ results make sense)
- [ ] PII fields marked in the contract (masked in Gold)
- [ ] Downstream jobs and dashboards informed (new table, new KPI)

---

## 17. ➕ Added by Architect (summary)

| # | Addition | Why |
|---|---|---|
| F1 | Four-layer config merge with an allow-listed CLI override | Flexible, but no uncontrolled prod changes |
| F2 | Reference, identifier and DDL-column checks beyond the JSON Schema | Mistakes caught at pull-request time |
| F3 | Typed, sealed config classes chosen by `job.type` | Compile-time safety for configs |
| F4 | `ReadFilter` tree + per-store translators | One run-mode logic for Mongo and SQL |
| F5 | `RunModePolicy` (read plan + bookmark rule + post-commit hooks) | All run-mode differences in one place; new mode = one class |
| F6 | Decorator `TimedStep` | Timing, step audit, metrics and log context for free |
| F7 | Null Objects for dry-run | A dry run can never write by mistake |
| F8 | Composition root without Spring | Clear wiring, no classpath conflicts |
| F9 | Exit codes + `ETL_RESULT` line | Airflow can tell PARTIAL from SUCCESS and FAILED |
| F10 | `--run-id` from Airflow, `--ids-file`, `--jobs` grouping | Idempotent task retries, long ID lists, cheaper small jobs |
| F11 | `ServicesResourceTransformer` in the shade plugin | Without it `format("mongodb")`, JDBC drivers and plugins silently vanish |
| F12 | `configVersion` with N/N-1 support | Safe, two-step upgrades of the YAML format |
| F13 | Configs baked into the image; one pinned version per env | Reproducible runs, one-line promote/rollback |
| F14 | Plugin rules + "promote after 3 uses" | Plugins don't turn into a second, messy framework |
| F15 | Golden dataset tests by files | Every job gets tests without new test code |
| F16 | SQL rules: generated SQL, bind values, validated identifiers, `ApplicationName=etl:<runId>` | Safety and DBA visibility |
| F17 | Executor MDC via `mdc.*` local properties; Spark job group = `runId` | One id across all logs and UIs |
| F18 | `INFRA_UNKNOWN` alert to drive exception classification | Error codes get more precise over time |

---

## 18. Changes to Earlier Decisions

| Earlier | Now | Reason |
|---|---|---|
| Doc 03 skeleton used `if (!ctx.isDryRun())` inside the job | Dry-run is handled by Null-Object components from `ComponentFactory` | No scattered checks |
| Doc 03 `BronzeToSilverJob.execute()` contained all steps inline | Steps are separate `Step` classes assembled by `PipelineBuilder`; `AbstractEtlJob.run()` loops over them | Steps are reused by every job type |
| CLI `--mode=dry-run` | `--dry-run` flag (alias kept with a warning) | Matches `run_mode` + `is_dry_run` in doc 05 |

---

## 19. Key Decisions

1. **Config over code:** one YAML per job; final config = defaults → job YAML → env overrides → allow-listed CLI overrides; JSON Schema + reference/identifier/DDL checks in CI; typed sealed config classes per `job.type`.
2. **Layers:** entry (`EtlJobRunner`) → lifecycle (`AbstractEtlJob`, Template Method) → pipeline (`Step`s from `PipelineBuilder`) → components (strategies) → control (`etl-control`) → foundation (`etl-config`, `etl-common`); plugins via SPI.
3. **Patterns:** Template Method (lifecycle), Strategy (reader, read plan, loader, merge, dedupe, write-back), Factory/Registry (job types, rules, transforms, plugins), Builder (steps), Decorator (`TimedStep`), Null Object (dry-run), Composition Root (`ComponentFactory`).
4. **Run modes:** a neutral `ReadFilter`/`ReadPlan` translated per store; one `RunModePolicy` per mode decides the read plan, whether the bookmark moves, and post-commit hooks; all other steps are identical for every mode.
5. **Entry point:** one generic `EtlJobRunner` (picocli) with `--job/--jobs --mode --trigger --dry-run --env --from --to --ids/--ids-file --parent-run-id --run-id --ticket --override`; exit codes 0/10/20/30/40 + an `ETL_RESULT` line.
6. **Spring:** only in `etl-ingestion` and `etl-ops-api`; Spark jobs are plain Java; shared libraries are Spring-free and wrapped as beans by the Spring apps.
7. **Packaging:** one shaded `etl-spark-app` JAR (Spark `provided`, `ServicesResourceTransformer`, relocations, enforcer convergence, ArchUnit bans); SemVer; `configVersion` N/N-1; a Docker image with configs baked in; promotion by one pinned version per environment.
8. **Plugins:** `CustomTransformer` and `DqRuleType` via `ServiceLoader`, called at fixed hooks, pure functions only, unit-tested, promoted to built-ins after 3 uses.
9. **Standards:** immutable records, no static state, Spark/SQL rules, golden dataset tests, ArchUnit.
10. **Logging:** JSON logs via SLF4J/Log4j 2; MDC with `runId`, `jobName`, `runMode`, `env`, `step`; executor MDC via `mdc.*` properties; Spark job group and JDBC `ApplicationName` carry the `runId`; no PII in logs.
11. **Exceptions:** only for job-level problems (`EtlException` tree with error code + retryable flag); bad records are data, never exceptions; `DefaultErrorClassifier` maps foreign exceptions to catalog codes.
