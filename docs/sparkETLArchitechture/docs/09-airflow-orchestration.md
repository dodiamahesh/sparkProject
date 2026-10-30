# 09 — Airflow Orchestration (20 → 100+ Jobs)

> Builds on docs [01](01-architecture-and-structure.md) to [08](08-environment-config.md).
> Written in **simple language**: every section starts with an **"In simple words"** box, and every abbreviation is written out in full.
> Code targets **Airflow 2.9 / 2.10** (Datasets, `DatasetOrTimeSchedule`, dataset-event REST API). Notes for **Airflow 3** are marked ⚠.

---

## 0. Abbreviations Used in This Document

| Short form | Full form | Meaning in simple words |
|---|---|---|
| **DAG** | Directed Acyclic Graph | An Airflow workflow: tasks connected by arrows, without loops |
| **ETL** | Extract, Transform, Load | Our data pipelines |
| **K8s** | Kubernetes | The container platform where Spark runs |
| **CRD** | Custom Resource Definition | A new "object type" added to Kubernetes (e.g. `SparkApplication`) |
| **API** | Application Programming Interface | A way for programs to call each other |
| **REST** | Representational State Transfer | The common web API style |
| **UI** | User Interface | Screens people click on |
| **XCom** | Cross-Communication | Airflow's small key-value store for passing data between tasks |
| **SLA** | Service Level Agreement | A promise like "data ready by 8 AM" |
| **CI/CD** | Continuous Integration / Continuous Delivery | Automatic build, test and deploy |
| **RBAC** | Role-Based Access Control | Permissions given by role |
| **JSON** | JavaScript Object Notation | Text format for structured data |
| **YAML** | YAML Ain't Markup Language | Our config file format |
| **URI** | Uniform Resource Identifier | A unique name/address, e.g. `etl://silver_order.sales_order` |
| **B2S / S2G** | Bronze to Silver / Silver to Gold | The two job families |
| **EPAS** | EDB Postgres Advanced Server (EDB = EnterpriseDB) | Our PostgreSQL-based database |
| **PII** | Personally Identifiable Information | Name, email, phone, ... |

---

## 1. What Airflow Does (and Does Not Do) Here

> **In simple words**
> Airflow is the **conductor of an orchestra**. It doesn't play any instrument itself. It says **who plays when**: "now the customer job, then the order job, then the sales fact".
> In our platform:
> - Airflow **decides when** a job runs (schedule, upstream finished, manual request).
> - Airflow **starts** the Spark job and **waits** for it.
> - Airflow **reads the result** and shows it, retries if useful, and sends alerts.
> - Airflow does **not** contain any ETL logic and does **not** touch the data. All of that is in the Spark jobs (docs 03–07).

| Responsibility | Airflow | Spark job (`EtlJobRunner`) | Ops API |
|---|---|---|---|
| When to run (schedule, events, dependencies) | ✅ | | |
| Validate manual requests, approvals | | | ✅ (doc 07 §10) |
| Start / stop Spark | ✅ | | |
| Read, transform, write, write-back | | ✅ | |
| Locking, audit, statuses, counts | | ✅ (`etl_ctl`) | reads |
| Retry the whole job (retryable failures only) | ✅ | | |
| Alerts | ✅ (callbacks) + monitoring (Prompt 11) | | |
| Source of truth for run status | ❌ shows it | ✅ `job_run_audit` | reads |

---

## 2. DAG Strategy

### 2.1 Options compared

> **In simple words:** a DAG is a **timetable card**. Do we print one card per job, one card per department, or one giant card for the whole company?

| Option | Pros | Cons | Verdict |
|---|---|---|---|
| **One DAG per job, generated automatically from the job configs** | Each job has its own schedule, history, retries and "Trigger" button; a failure in one job doesn't block others; easy to find in the UI; Datasets connect them | 100+ DAGs in the UI (solved with tags and naming) | ✅ **Chosen** |
| One DAG per domain/layer (e.g. `b2s_order_domain`) | Fewer DAGs | All jobs in it share one schedule; one stuck job blocks the domain's DAG run; reruns of one job are awkward | ❌ |
| One giant DAG for everything | One picture | Very fragile; one failure makes the whole DAG run red; schedules can't differ | ❌ |
| Hand-written DAG file per job | Full control | 100+ files of copy-paste; they drift from the job YAML | ❌ |

### 2.2 How the DAGs are generated

> **In simple words:** we write **one Python file** that reads the list of all jobs and creates a DAG for each one, like a **mail-merge** that prints 100 letters from one template.

```
 CI (doc 08 §9) for image version 1.14.2 and environment ENV
   │  render + validate all job configs
   ▼
 generated/jobs.<env>.json          ← list of resolved jobs: name, type, schedule, trigger mode,
   (shipped together with the DAG      sources/targets (→ Datasets), resources, pool slots, priority,
    code in the Airflow DAG bundle)    retries, SLA, owner, dependencies, image version
   │
   ▼  Airflow parses dags/etl_job_dags.py (every ~30 s)
 factory.build_all(jobs.<env>.json, config/environments/<env>/airflow.yml)
   │
   ▼
 100+ DAGs:  etl__b2s_order,  etl__s2g_dim_customer,  etl__s2g_fact_sales_line, ...
```

| Decision | Why |
|---|---|
| The factory reads a **JSON file**, not the database ➕ | Airflow re-parses DAG files every ~30 seconds. Database queries at parse time slow the scheduler down and break when the database is unavailable |
| The JSON is built by CI from the **same commit** as the image | DAGs and Spark code always match. The JSON contains the image version, so there is no separate version to keep in sync (refines doc 08, §6.2) |
| Disabled jobs → no DAG | Disabled jobs disappear from the UI; their history stays in `job_run_audit` |
| DAG id = `etl__<job_name>` | Easy to search; the prefix separates our DAGs from others |
| Tags: `layer:b2s`, `domain:order`, `owner:team-order-data`, `type:fact` | Filter 100+ DAGs in the UI by layer, domain or team |

### 2.3 The special (hand-written) DAGs

Besides the generated job DAGs, a small fixed set of DAGs handles the platform-wide work:

| DAG | Purpose | Schedule |
|---|---|---|
| `etl_manual_run` | One generic DAG for **every manual run mode** (§5) | Triggered only (by the ops API) |
| `etl_cascade` | Runs a list of downstream jobs **level by level** (§4.6) | Triggered only |
| `etl_quarantine_reprocess` | Starts `REPROCESS_QUARANTINE` for jobs that have `READY_TO_REPROCESS` items (doc 07 §2.5) | Every 4 hours (env config) |
| `etl_sla_monitor` | Freshness and ready-by checks (§7.3) | Every 10 minutes |
| `etl_maintenance` | Zombie-run check, Bronze safety-net sweep, retention, partition creation, staging cleanup, drift check (docs 05 and 07) | Several schedules (nightly + every 5 min for zombies) |
| `etl_ingestion_replay` | Kafka/DLQ replay and re-snapshot jobs (doc 02 §15) | Triggered only |

---

## 3. How Airflow Starts Spark

### 3.1 Options compared

| Option | How it works | Pros | Cons | Verdict |
|---|---|---|---|---|
| `SparkSubmitOperator` | Airflow worker runs `spark-submit` | Simple, classic | Spark binaries and config on every Airflow worker; client mode keeps a worker busy for the whole job | ❌ |
| `KubernetesPodOperator` running `spark-submit` | A pod that submits the job | Works anywhere | Two layers of pods; exit codes in cluster mode are unreliable | ⚠️ fallback |
| **Spark Operator (`SparkApplication` CRD) + our `EtlSparkOperator`** | Airflow creates a `SparkApplication` object in K8s; the Spark Operator starts driver and executor pods; Airflow waits (deferred) and then reads the run result | Declarative; native K8s secrets, node selectors, restarts and cleanup (TTL); no Spark on Airflow workers; waiting costs no worker slot | Needs the Spark Operator installed in K8s | ✅ **Chosen** |
| Apache Livy (REST) | REST server that submits Spark jobs | REST API | Project with low activity; one more server | ❌ |
| Spark standalone REST submission | Built-in REST | — | Legacy, not for K8s | ❌ |

### 3.2 Our custom operator: `EtlSparkOperator`

> **In simple words:** one operator that knows our whole contract:
> 1. **Build** the Spark application description (image, size, secrets, arguments).
> 2. **Start** it in Kubernetes.
> 3. **Go to sleep** while Spark works (a "deferred" task frees its Airflow worker slot).
> 4. When Spark finishes, **ask the ops API**: "what does `job_run_audit` say about run X?"
> 5. **Translate** that into an Airflow result: success, retry, fail without retry, or skip.

Why ask the ops API instead of trusting the Spark exit code? Because **`job_run_audit` is the source of truth** (doc 07). It knows PARTIAL vs SUCCESS, the error code and whether the error is retryable. Exit codes are only a backup when no audit row exists (the driver crashed before writing one). And Airflow has **no database password** (doc 08 §5.3), so it reads through the ops API.

```
 EtlSparkOperator.execute()
   │ 1. run_id = <base>-a<try_number>           (doc 07 §1.1)
   │ 2. manifest = SparkManifestBuilder(job, env, run_id, mode, params)
   │ 3. create SparkApplication in namespace etl-<env>      (labels: etl/job, etl/run-id)
   │ 4. defer → SparkAppTrigger polls the SparkApplication status (in the Airflow triggerer)
   ▼
 execute_complete(event)                        (runs when the Spark app has finished)
   │ 5. result = ops_api.get_run(run_id)        (job_run_audit row)
   │ 6. map result → Airflow outcome (table §9)
   │ 7. push a small summary to XCom (status, counts, error code, links); set a task note
   │ 8. on_kill(): delete the SparkApplication ➕ (a cancelled task must not leave Spark running)
```

### 3.3 Generated `SparkApplication` (example)

```yaml
# built by etl_airflow/spark_manifest.py for b2s_order in prod (profile "large", doc 08 §7.2)
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: b2s-order-20261030t090000z-a1          # K8s-safe version of the run id
  namespace: etl-prod
  labels: { etl/job: b2s_order, etl/run-id: b2s_order-20261030T090000Z-a1, etl/layer: b2s, etl/domain: order }
spec:
  type: Java
  mode: cluster
  image: registry.internal/etl-spark-app:1.14.2
  mainClass: com.company.etl.core.runner.EtlJobRunner
  mainApplicationFile: local:///opt/etl/lib/etl-spark-app.jar
  arguments: ["--job=b2s_order", "--env=prod", "--mode=INCREMENTAL", "--trigger=SCHEDULED",
              "--run-id=b2s_order-20261030T090000Z-a1"]
  sparkVersion: "3.5.1"
  restartPolicy: { type: Never }                 # retries are decided by Airflow, not by the Spark Operator
  timeToLiveSeconds: 86400                       # ➕ finished apps (and their pods) are cleaned up after 1 day
  sparkConf:                                     # from config/environments/{base,prod}/spark.yml
    spark.dynamicAllocation.enabled: "true"
    spark.dynamicAllocation.minExecutors: "4"
    spark.dynamicAllocation.maxExecutors: "20"
    spark.sql.shuffle.partitions: "200"
    spark.eventLog.enabled: "true"
    spark.eventLog.dir: "s3a://etl-prod-spark-logs/events"
  driver:
    cores: 2
    memory: 8g
    serviceAccount: spark-etl
    env: [{ name: ETL_ENV, value: prod }]
    secrets: [{ name: etl-secrets, path: /opt/etl/secrets, secretType: Generic }]
    nodeSelector: { workload: spark }
  executor:
    instances: 8
    cores: 4
    memory: 8g
    env: [{ name: ETL_ENV, value: prod }]
    secrets: [{ name: etl-secrets, path: /opt/etl/secrets, secretType: Generic }]   # executors need them (doc 08 §5.3)
    nodeSelector: { workload: spark }
```

---

## 4. Dependencies

### 4.1 The picture

> **In simple words:** each job announces "**I have updated table X**". Other jobs that use table X **listen** for that announcement. In Airflow this announcement is called a **Dataset event**. It is like a **WhatsApp group**: the customer job posts "customers updated", and the sales-fact job, which is in that group, wakes up.

```
                       Kafka: etl.events.bronze-loaded (throttled, doc 02)
                                  │  event bridge (ops API) → Airflow dataset-event REST call
                                  ▼
   Dataset etl://bronze/svc-customer/customers        Dataset etl://bronze/svc-order/orders
          │ (schedule OR event)                                │ (schedule OR event)
          ▼                                                    ▼
   ┌─────────────────────┐                             ┌─────────────────────┐
   │ etl__b2s_customer   │                             │ etl__b2s_order      │
   └─────────┬───────────┘                             └─────────┬───────────┘
             │ outlet: etl://silver_customer.customer            │ outlets: etl://silver_order.sales_order
             │                                                    │          etl://silver_order.sales_order_item
             ▼                                                    │
   ┌──────────────────────────┐                                   │
   │ etl__s2g_dim_customer    │  schedule: OR(customer)           │
   └─────────┬────────────────┘                                   │
             │ outlet: etl://gold_core.dim_customer               │
             ▼                                                    ▼
   ┌───────────────────────────────────────────────────────────────────────────┐
   │ etl__s2g_fact_sales_line                                                   │
   │ schedule: cron "45 * * * *"  OR  any of (sales_order, sales_order_item)    │
   │ + wait_upstream task: HARD deps s2g_dim_customer, s2g_dim_product, b2s_order│
   └─────────┬─────────────────────────────────────────────────────────────────┘
             │ outlet: etl://gold_sales.fact_sales_line
             ▼
   ┌──────────────────────────────┐
   │ etl__s2g_agg_sales_daily_store│  schedule: OR(fact_sales_line)
   └──────────────────────────────┘
```

### 4.2 Dataset naming

| Thing | Dataset URI |
|---|---|
| A Bronze collection (from ingestion events) | `etl://bronze/<service>/<collection>` |
| A Silver table | `etl://silver_<domain>.<table>` |
| A Gold table | `etl://gold_<area>.<table>` |

Each job DAG's main task declares its **targets** as `outlets` (from `job_target`, doc 05). A Dataset event is emitted **only when the task succeeds**, so:
- job `SUCCESS` or `PARTIAL` → event emitted (good data was committed) → downstream may run (doc 07 §7: upstream PARTIAL → run) ✅
- job `FAILED` or `SKIPPED` → no event → downstream not triggered by this upstream ✅

### 4.3 How each job is triggered

```yaml
# job YAML (doc 06), new optional block
orchestration:
  trigger: SCHEDULE_OR_UPSTREAM     # SCHEDULE | UPSTREAM | SCHEDULE_OR_UPSTREAM | MANUAL_ONLY
  priority: HIGH                    # HIGH | NORMAL | LOW (§8.3)
```

| Trigger | Airflow schedule | Use for |
|---|---|---|
| `SCHEDULE` | cron from the job config | Daily snapshots, maintenance-like jobs |
| `UPSTREAM` | Datasets (OR of the job's source tables) | Dimensions and aggregates that should follow their source quickly |
| `SCHEDULE_OR_UPSTREAM` (default) | `DatasetOrTimeSchedule(cron, OR of sources)` | Most jobs: react quickly to new data, and still run at least on schedule |
| `MANUAL_ONLY` | `None` | One-off or dev-only jobs |

➕ **The "AND" trap.** By default, a DAG scheduled on several Datasets waits until **all** of them have new events. A fact that depends on `dim_customer`, `dim_product` **and** `sales_order` would never run in an hour where products didn't change. So:
- the Dataset schedule is always the **OR** of the job's **driving/secondary source tables** (conditional Dataset expressions, Airflow 2.9+),
- the "must the dimensions be ready?" question is answered separately by the `wait_upstream` task (§4.4), using the dependency rules of doc 07.

### 4.4 The `wait_upstream` task: dependency rules in one place

> **In simple words:** before the job starts, a small task asks: "are my **HARD** upstream jobs OK right now?" It looks at their latest runs in `job_run_audit` (through the ops API) and applies the policy from doc 07 §7: run, wait, or skip.

| Latest run of a HARD upstream | `wait_upstream` does |
|---|---|
| `RUNNING` / `QUEUED` | Wait (deferred, re-checks every 2 minutes) |
| `SUCCESS` / `PARTIAL` | Continue (or wait if `requireUpstream: SUCCESS_ONLY` and it was PARTIAL) |
| `FAILED` / `CANCELLED` | `on_upstream_failure`: `WAIT` up to `max_wait_minutes` (default 60) → then **skip** the DAG run with `DEPENDENCY_FAILED`; `SKIP` → skip at once; `RUN` → continue |
| No run yet (new job) | Continue |

When it skips, the job DAG run is marked **skipped**, and the runner records a `SKIPPED (DEPENDENCY_FAILED)` row (through the ops API) so the history is complete. No alert is sent for it (root-cause alerting, doc 07 §7.2).

➕ **Why not `ExternalTaskSensor`?** It waits for a task in another DAG **with the same logical date**. Our DAGs have different schedules (every 30 min vs hourly vs event-driven), so the dates rarely match and the sensor waits forever or needs fragile date math. Datasets + `wait_upstream` (based on real run statuses) avoid that problem completely.

### 4.5 Event-triggered runs from Kafka

```
 etl-ingestion ──► Kafka etl.events.bronze-loaded  {service, collection, count, maxSourceVersion, windowEnd}
                         │
                         ▼
 ops API "event bridge" (a small Kafka consumer inside etl-ops-api)
   → POST {airflow}/api/v1/datasets/events   {"dataset_uri": "etl://bronze/svc-order/orders", "extra": {...}}
                         │
                         ▼
 etl__b2s_order (scheduled on DatasetOrTimeSchedule) → one DAG run
```

| Point | Detail |
|---|---|
| ➕ **Coalescing is built in** | With `max_active_runs = 1`, many Dataset events that arrive while a run is going on produce **one** follow-up run, not one run per event (doc 07 §2.9) |
| Why a bridge instead of a Kafka sensor in Airflow | A sensor per collection would keep 100 consumers open inside Airflow. One small bridge in a service that already runs 24×7 is simpler |
| ⚠ Airflow 3 | Datasets are renamed **Assets** and the REST path changes; the bridge and the factory have one place each to adapt |

### 4.6 Cascade reruns

**Automatic (data-driven):** happens by itself. When `b2s_order` re-processes data, the `sales_order` Dataset gets an event, and every DAG listening to it runs (doc 07 §2.10).

**Explicit cascade** (the ops API request with `cascade.enabled = true`) uses the generic `etl_cascade` DAG:

> **In simple words:** the ops API sends a **list of jobs grouped in levels**: level 0 = the upstream job, level 1 = jobs that depend only on level 0, level 2 = jobs that depend on level 1, and so on. The cascade DAG runs level 0, then all of level 1 **in parallel**, then level 2, etc. An empty level is simply skipped.

```
 conf = {
   "rootRunId": "b2s_order-20261030T091512Z-m",
   "levels": [
     [{"job": "b2s_order", "mode": "BACKFILL", "params": {"from": "...", "to": "..."}}],
     [{"job": "s2g_fact_sales_line", "mode": "INCREMENTAL"}, {"job": "s2g_fact_order_fulfillment", "mode": "INCREMENTAL"}],
     [{"job": "s2g_agg_sales_daily_store", "mode": "INCREMENTAL"}],
     [], [], []
   ]
 }

 etl_cascade:  level_0 (mapped) ──► level_1 (mapped) ──► level_2 (mapped) ──► ... ──► level_5
               every mapped task = EtlSparkOperator(job, mode, trigger=CASCADE, parent=previous level's run)
```

A level only starts if the previous level finished `SUCCESS` or `PARTIAL` (trigger rule), matching the dependency policy.

---

## 5. Manual Runs

### 5.1 One generic DAG vs parameters on every DAG

| Option | Pros | Cons | Verdict |
|---|---|---|---|
| **One generic `etl_manual_run` DAG**, triggered by the ops API | One place for every run mode; approvals, validation and the `run_request` audit stay in the ops API (doc 07 §10); job DAGs stay simple | The run appears under `etl_manual_run`, not under the job's own DAG (solved by tags, the DAG run name and the ops UI) | ✅ **Chosen** |
| Parameters on each of the 100+ job DAGs | Run from the job's own page | 100+ DAGs × 8 modes of parameters; approvals can be bypassed from the Airflow UI | ❌ |

**Allowed on the job DAGs themselves:** a plain "Trigger DAG" (no parameters) = an extra `INCREMENTAL` run. Handy and harmless.

### 5.2 Who may trigger what in Airflow ➕

| Environment | Airflow UI: `etl_manual_run` "Trigger with config" | Through the ops API |
|---|---|---|
| dev / qa | Allowed for developers (fast testing) | Allowed |
| uat / prod | **Only the ops API service account** (Airflow RBAC) | ✅ the only way, so approvals and the four-eyes rule can't be skipped |

### 5.3 The `etl_manual_run` DAG (with a form for dev/qa)

```python
# orchestration/airflow/dags/etl_manual_run.py
from airflow import DAG
from airflow.models.param import Param
from etl_airflow.settings import SETTINGS
from etl_airflow.registry import JOBS
from etl_airflow.operators.etl_spark_operator import EtlSparkOperator
from etl_airflow.callbacks import on_failure, on_success

RUN_MODES = ["INCREMENTAL", "FULL_LOAD", "RERUN_JOB", "RERUN_FAILED_RECORDS",
             "REPROCESS_QUARANTINE", "BACKFILL", "REPLAY", "ID_REPROCESS"]

with DAG(
    dag_id="etl_manual_run",
    schedule=None,
    catchup=False,
    max_active_runs=SETTINGS.manual_max_active_runs,        # e.g. 5; job locks prevent same-job overlap
    tags=["etl", "manual"],
    params={
        "job":           Param(type="string", enum=sorted(JOBS.names())),
        "mode":          Param("INCREMENTAL", type="string", enum=RUN_MODES),
        "dry_run":       Param(False, type="boolean"),
        "from":          Param(None, type=["null", "string"], format="date-time"),
        "to":            Param(None, type=["null", "string"], format="date-time"),
        "ids":           Param([], type="array", items={"type": "string"}),
        "parent_run_id": Param(None, type=["null", "string"]),
        "run_id":        Param(None, type=["null", "string"]),   # set by the ops API (doc 07 §10)
        "ticket":        Param(None, type=["null", "string"]),
        "triggered_by":  Param("airflow-ui", type="string"),
        "reason":        Param("", type="string"),
    },
    render_template_as_native_obj=True,
) as dag:
    EtlSparkOperator(
        task_id="run_etl",
        job="{{ params.job }}",
        mode="{{ params.mode }}",
        trigger="MANUAL",
        dry_run="{{ params.dry_run }}",
        run_params={"from": "{{ params['from'] }}", "to": "{{ params.to }}", "ids": "{{ params.ids }}",
                    "parentRunId": "{{ params.parent_run_id }}", "ticket": "{{ params.ticket }}",
                    "triggeredBy": "{{ params.triggered_by }}"},
        run_id="{{ params.run_id }}",
        pool=SETTINGS.pools.heavy_manual,        # ➕ FULL_LOAD/REPLAY/BACKFILL share a small "heavy" pool (§8.1)
        on_failure_callback=on_failure,
        on_success_callback=on_success,
    )
```

The ops API names the DAG run after the request (`manual__b2s_order__BACKFILL__<requestId>`), so the Airflow UI list is readable.

---

## 6. Same DAG Code for All Environments

> **In simple words:** the DAG code is identical in dev and prod. What differs is **read from config**: the schedules, pools, retries and connection names of that environment. Airflow knows which environment it is from one environment variable: `ETL_ENV`.

### 6.1 Sources of environment-specific values

| Value | Comes from |
|---|---|
| Which environment | `ETL_ENV` environment variable on the Airflow deployment |
| Schedules per job | `generated/jobs.<env>.json` (job YAML + `<env>/job-overrides.yml`, doc 08) |
| Paused on creation | `schedulesActive` in `config/environments/<env>/airflow.yml` (dev: paused) |
| Retries, timeouts, pools, priorities, alert channels | `config/environments/{base,<env>}/airflow.yml` |
| Connection **names** | `airflow.yml` → `connections:`; their **values** come from Vault via the Airflow secrets backend (doc 08 §5.3) |
| Spark image version | `imageVersion` inside `jobs.<env>.json` (same commit as the DAG code) |
| ➕ Emergency override | Airflow Variable `etl_spark_app_version_override` (normally empty): roll the image back without redeploying the DAG bundle |

### 6.2 Extended `airflow.yml` (adds to doc 08 §7.4)

```yaml
# config/environments/base/airflow.yml
airflow:
  defaultArgs:
    owner: data-platform
    retries: 3
    retryDelay: 5m
    retryExponentialBackoff: true
    maxRetryDelay: 45m
  executionTimeoutFactor: 3            # execution_timeout = 3 × job sla.maxDurationMinutes
  defaultExecutionTimeout: 2h          # when the job has no SLA
  dag:
    catchup: false
    maxActiveRunsPerDag: 1
    dagrunTimeout: 6h
  deferrable: true                     # waiting for Spark frees the worker slot (needs the triggerer)
  pollIntervalSeconds: 30
  pools:                               # §8.1
    etl_spark:      { slots: 10, description: "concurrent Spark apps (weighted by profile)" }
    etl_heavy:      { slots: 2,  description: "FULL_LOAD / REPLAY / big BACKFILL" }
    etl_sql:        { slots: 6,  description: "SQL-in-EPAS aggregate/snapshot jobs" }
  poolSlotsByProfile: { small: 1, medium: 2, large: 4 }
  priorityWeights: { HIGH: 100, NORMAL: 50, LOW: 10 }
  partialAs: QUALITY_GATE_FAILED       # §9.2: QUALITY_GATE_FAILED | NOTE_ONLY
  wait_upstream: { pokeIntervalSeconds: 120 }
  manual: { maxActiveRuns: 5 }
  alerts:
    channel: "slack:#etl-alerts"
    pager: null
    onSuccess: false
    criticalJobsPage: true             # FAILED of jobs with priority HIGH → pager (prod)
  connections: { k8s: etl_k8s, opsApi: etl_ops_api }
```

```yaml
# config/environments/dev/airflow.yml
airflow:
  schedulesActive: false               # DAGs created paused (doc 08)
  defaultArgs: { retries: 0 }
  pools:
    etl_spark: { slots: 3 }
    etl_heavy: { slots: 1 }
    etl_sql:   { slots: 2 }
  partialAs: NOTE_ONLY                 # dev: don't paint the UI red for PARTIAL
  alerts: { channel: "slack:#etl-dev-alerts", criticalJobsPage: false }
  connections: { k8s: etl_dev_k8s, opsApi: etl_dev_ops_api }
  manual: { maxActiveRuns: 10 }
```

```yaml
# config/environments/prod/airflow.yml
airflow:
  schedulesActive: true
  pools:
    etl_spark: { slots: 24 }
    etl_heavy: { slots: 2 }
    etl_sql:   { slots: 8 }
  alerts:
    channel: "slack:#etl-prod-alerts"
    pager: "pagerduty:etl-oncall"
    criticalJobsPage: true
  connections: { k8s: etl_prod_k8s, opsApi: etl_prod_ops_api }
```

### 6.3 What one job looks like in `jobs.<env>.json` (generated by CI)

```json
{
  "env": "prod",
  "imageVersion": "1.14.2",
  "gitCommit": "a1b2c3d",
  "generatedAt": "2026-10-30T07:12:00Z",
  "jobs": [
    {
      "name": "s2g_fact_sales_line",
      "type": "S2G_FACT",
      "layer": "s2g",
      "domain": "sales",
      "owner": "team-sales-analytics",
      "enabled": true,
      "schedule": "45 * * * *",
      "trigger": "SCHEDULE_OR_UPSTREAM",
      "priority": "HIGH",
      "sources": ["silver_order.sales_order_item", "silver_order.sales_order", "silver_order.sales_order_item_discount"],
      "targets": ["gold_sales.fact_sales_line"],
      "dependsOn": [
        {"job": "s2g_dim_customer", "type": "HARD", "onUpstreamFailure": "WAIT", "maxWaitMinutes": 60},
        {"job": "s2g_dim_product",  "type": "HARD", "onUpstreamFailure": "WAIT", "maxWaitMinutes": 60},
        {"job": "b2s_order",        "type": "HARD", "onUpstreamFailure": "WAIT", "maxWaitMinutes": 60}
      ],
      "resources": {"profile": "medium", "driverMemory": "4g", "driverCores": 2, "executors": 4,
                    "minExecutors": 2, "maxExecutors": 10, "executorMemory": "8g", "executorCores": 4},
      "sla": {"maxDurationMinutes": 20, "freshnessMinutes": 120},
      "runner": "SPARK"
    }
  ]
}
```

`runner: SQL` is used for `S2G_AGGREGATE` and `S2G_SNAPSHOT` jobs that run SQL inside EPAS (doc 04 §10): they still use the same Docker image and runner, just with the small `sql` profile (a tiny driver, no executors), so the lifecycle (lock, audit, reconciliation) stays identical.

---

## 7. Retries, Timeouts, SLA, Callbacks and Alerts

### 7.1 Retries

> **In simple words:** Airflow retries **only when retrying can help** (the database was briefly down). If the error is "the config is wrong" or "60% of the data is bad", retrying gives the same result, so Airflow stops at once and alerts.

| Run result | Airflow behaviour |
|---|---|
| `FAILED`, error code `retryable = true` (`INFRA_*`, `LOCK_LOST`, …) | Raise a normal exception → Airflow retries (3×, 5 → 15 → 45 min). Each retry is a **new run id** `-a2`, `-a3` (doc 07 §1.1) |
| `FAILED`, not retryable (`CONFIG_INVALID`, `THRESHOLD_BREACHED`, `RECONCILIATION_MISMATCH`, …) | `AirflowFailException` → **no retry**, immediate alert |
| Spark app ended but **no audit row** (driver crashed before writing it) | Treated as retryable (`INFRA_UNKNOWN`) |

### 7.2 Timeouts

| Timeout | Value | Effect |
|---|---|---|
| Task `execution_timeout` | `3 × sla.maxDurationMinutes` (or 2 h) | Task fails; `on_kill` deletes the `SparkApplication` ➕ |
| `dagrun_timeout` | 6 h | Stuck DAG runs are closed |
| `wait_upstream` timeout | `maxWaitMinutes` of the dependency | DAG run skipped (`DEPENDENCY_FAILED`) |
| Inside the job | Statement, lock, idle and watchdog timeouts (doc 07 §8.3) | The job fails cleanly with a code |

### 7.3 SLA

> **In simple words:** Airflow's old built-in "SLA miss" feature is limited (it only looks at task duration relative to the schedule) and ⚠ it is removed in Airflow 3. We check our three SLAs (doc 07 §8.4) **ourselves**, in a small monitor DAG and in Prometheus alerts, so it works in every Airflow version.

| SLA | Checked by |
|---|---|
| Duration (`maxDurationMinutes`) | `execution_timeout` (hard stop) + `etl_sla_monitor` warning at 100% / alert at 150% while running |
| Freshness (`freshnessMinutes`) | `etl_sla_monitor` every 10 min: asks the ops API for the age of the newest committed data per job/target |
| Ready-by (`readyBy`) | `etl_sla_monitor`: at the ready-by time, "is there a successful run covering yesterday?" |

The same numbers are exported as metrics, so Grafana/Prometheus alerting (Prompt 11) is the second line of defence.

### 7.4 Callbacks and alerts

```python
# orchestration/airflow/etl_airflow/callbacks.py
from etl_airflow.notifier import send_alert
from etl_airflow.settings import SETTINGS

def on_failure(context):
    ti = context["task_instance"]
    summary = ti.xcom_pull(key="etl_result") or {}
    code = summary.get("errorCode", "INFRA_UNKNOWN")
    if code == "DEPENDENCY_FAILED":
        return                                            # ➕ root-cause alerting: the upstream already alerted
    job = context["params"].get("job") or context["dag"].dag_id.removeprefix("etl__")
    severity = "CRITICAL" if summary.get("priority") == "HIGH" else "ERROR"
    send_alert(
        severity=severity,
        title=f"ETL FAILED {job} ({code})",
        fields={
            "runId": summary.get("runId"), "env": SETTINGS.env, "mode": summary.get("runMode"),
            "attempt": ti.try_number, "willRetry": ti.state == "up_for_retry",
            "reason": summary.get("failureReason"),        # short text, no PII
        },
        links={"Airflow log": ti.log_url, "Spark UI": summary.get("sparkUiUrl"),
               "Ops UI": f"{SETTINGS.ops_ui_url}/runs/{summary.get('runId')}"},
        owner=summary.get("ownerTeam"),
        page=(severity == "CRITICAL" and SETTINGS.alerts.critical_jobs_page and ti.state != "up_for_retry"),
    )

def on_partial(context, summary):
    send_alert(severity="WARNING",
               title=f"ETL PARTIAL {summary['jobName']}: {summary['bad']} bad of {summary['read']} ({summary['topErrorCode']})",
               fields={"runId": summary["runId"], "env": SETTINGS.env},
               links={"Ops UI": f"{SETTINGS.ops_ui_url}/runs/{summary['runId']}"},
               owner=summary.get("ownerTeam"), page=False)

def on_success(context):
    if SETTINGS.alerts.on_success:
        ...                                               # off by default: success is silent
```

| Event | Alert | Channel |
|---|---|---|
| FAILED, will retry | none (or a low-priority note) | — |
| FAILED, final, priority HIGH (prod) | CRITICAL | Pager + Slack/Teams |
| FAILED, final, other jobs | ERROR | Slack/Teams (owner team mentioned) |
| PARTIAL | WARNING | Slack/Teams, **grouped**: one message per job per hour at most ➕ |
| SKIPPED `DEPENDENCY_FAILED` | none (upstream alert lists blocked jobs) | — |
| SKIPPED `LOCK_NOT_ACQUIRED` | INFO only if it happens 3 times in a row ➕ (means a run is stuck) | Slack |
| SLA breach | WARNING / CRITICAL by SLA type | Slack / pager |

---

## 8. Concurrency, Pools, Priority, Catchup and Backfill

### 8.1 Pools

> **In simple words:** a **pool** is a limited number of **tokens**. A task needs tokens to start. If all tokens are in use, the task waits in line. This stops 100 jobs from hitting the Spark cluster or the database at the same time.

| Pool | Slots (prod) | Used by | Tokens per task |
|---|---|---|---|
| `etl_spark` | 24 | All Spark job tasks | by Spark profile: small 1, medium 2, large 4 ➕ (a large job takes more of the cluster) |
| `etl_heavy` | 2 | Manual `FULL_LOAD`, `REPLAY`, `BACKFILL` > 7 days | 1 (in addition, the Spark resources of the job) |
| `etl_sql` | 8 | `runner: SQL` jobs (aggregates, snapshots) | 1 |

An Airflow task can use **one** pool. Database write pressure is therefore limited **inside** the jobs (`write.parallelism`, doc 06) and by the size of `etl_spark`. Heavy manual runs use `etl_heavy` so two full loads can never run at the same time.

### 8.2 Concurrency settings

| Setting | Value | Why |
|---|---|---|
| `max_active_runs` per job DAG | 1 | One run at a time per job (the job lock is the second guard) |
| `max_active_tasks` per job DAG | 2 (`wait_upstream` + `run_etl`) | Small DAGs |
| `etl_manual_run` `max_active_runs` | 5 (prod) | Several manual runs of **different** jobs; same-job overlap is stopped by the lock |
| Deferrable operator | on | Waiting for Spark does not occupy an Airflow worker slot ➕ (100 waiting jobs ≠ 100 busy workers) |

### 8.3 Priority

`priority_weight` from the job's `orchestration.priority` (HIGH 100, NORMAL 50, LOW 10), with `weight_rule="absolute"`. When the `etl_spark` pool is full, HIGH jobs (e.g. Gold facts behind morning dashboards) get the next free slot first.

### 8.4 Catchup and backfill: why both are switched off in Airflow ➕

> **In simple words**
> Airflow's **catchup** means: "if the scheduler was off for 5 hours, start the 5 missed hourly runs one by one". That makes sense for jobs that process "the data of 09:00–10:00". **Our jobs don't work like that.** They process **everything that is pending** (statuses, change numbers), no matter which hour it belongs to. One run after an outage already picks up all 5 hours of data.
> So: `catchup=False` on every DAG, and we **never** use Airflow's `airflow dags backfill` command. When we need to re-process a period, we use **our** `BACKFILL` run mode with `--from/--to` (doc 07 §2.6), through the ops API.

| Airflow feature | Our setting | Reason |
|---|---|---|
| `catchup` | `False` | The next incremental run naturally covers missed intervals |
| `airflow dags backfill` | Not used | Our backfill is data-range based, not schedule-date based |
| `start_date` | One fixed date in the factory | Changing schedules doesn't need new start dates |
| Logical date | Used only for naming run ids | Not used to select data |

---

## 9. Mapping Run Results to Airflow States

### 9.1 The job DAG's tasks

```
 etl__s2g_fact_sales_line
 ┌───────────────┐     ┌────────────────────────┐     ┌──────────────────┐
 │ wait_upstream │ ──► │ run_etl                │ ──► │ quality_gate     │
 │ (deferrable)  │     │ EtlSparkOperator       │     │ green = clean    │
 │               │     │ outlets = target       │     │ red   = PARTIAL  │
 │               │     │ Datasets               │     │ (no retries)     │
 └───────────────┘     └────────────────────────┘     └──────────────────┘
```

> **In simple words:** Airflow has only "success" (green) and "failed" (red); it has no "yellow" state. So we use **two boxes**:
> - `run_etl` turns **green** when the job **committed its data** (SUCCESS or PARTIAL). This lets the downstream jobs continue (via the Dataset event).
> - `quality_gate` turns **red** when the run was PARTIAL. So a PARTIAL run looks like "green, then red": **"data loaded, but not clean"**. It is never a plain green success.

### 9.2 The full mapping table

| `job_run_audit` result | `wait_upstream` | `run_etl` | `quality_gate` | Dataset event | Airflow retry | Alert |
|---|---|---|---|---|---|---|
| SUCCESS | ✅ success | ✅ success | ✅ success | ✅ yes | — | none |
| SUCCESS with record errors (within tolerance) | ✅ | ✅ | ✅ success + **task note** "12 records with errors (tolerated)" | ✅ | — | none (visible in the ops UI) |
| **PARTIAL** | ✅ | ✅ success | ❌ **failed** (`partialAs: QUALITY_GATE_FAILED`) or ✅ + note (`NOTE_ONLY`, dev) | ✅ yes (good data committed) | ❌ never (rerun failed records is a deliberate action) | WARNING (grouped) |
| FAILED, retryable | ✅ | 🔁 up_for_retry → ❌ failed after the last attempt | ⏭ upstream_failed | ❌ no | ✅ yes (new run id per attempt) | ERROR/CRITICAL after the final attempt |
| FAILED, not retryable | ✅ | ❌ failed (`AirflowFailException`) | ⏭ upstream_failed | ❌ | ❌ | ERROR/CRITICAL at once |
| SKIPPED (lock held, job disabled) | ✅ | ⏭ skipped | ⏭ skipped | ❌ | — | none (INFO after 3 in a row) |
| SKIPPED (dependency) | ⏭ skipped | ⏭ skipped | ⏭ skipped | ❌ | — | none (root cause alerted upstream) |
| CANCELLED (by a user via the ops API) | — | ❌ failed + note "cancelled by &lt;user&gt;" | ⏭ | ❌ | ❌ | none |
| No audit row (crash before audit) | ✅ | 🔁 retry as `INFRA_UNKNOWN` | ⏭ | ❌ | ✅ | after the final attempt |

**Extra visibility for PARTIAL ➕:**
- The task **note** and the DAG run **note** say: "PARTIAL: 17 of 1,000 records failed (DQ_NOT_NULL). Rerun failed records: <ops UI link>".
- The XCom `etl_result` holds the counts; the ops UI shows the run in **amber**.
- Grafana shows PARTIAL runs as their own colour (Prompt 11).

---

## 10. Python Code

### 10.1 Folder structure

```
orchestration/airflow/
├── dags/
│   ├── etl_job_dags.py              # ONE file → all generated job DAGs (100+)
│   ├── etl_manual_run.py            # generic manual DAG (§5.3)
│   ├── etl_cascade.py               # level-by-level cascade (§4.6)
│   ├── etl_quarantine_reprocess.py  # periodic REPROCESS_QUARANTINE (doc 07 §2.5)
│   ├── etl_sla_monitor.py           # freshness / ready-by / duration checks (§7.3)
│   ├── etl_maintenance.py           # zombie check, safety-net sweep, retention, partitions, staging cleanup, drift
│   └── etl_ingestion_replay.py      # Kafka/DLQ replay + re-snapshot (doc 02)
├── etl_airflow/                     # our Python package (shipped with the DAG bundle)
│   ├── __init__.py
│   ├── settings.py                  # loads config/environments/{base,ETL_ENV}/airflow.yml
│   ├── registry.py                  # loads generated/jobs.<env>.json
│   ├── factory.py                   # builds one DAG per job
│   ├── naming.py                    # dag ids, run ids, K8s names
│   ├── datasets.py                  # Dataset URIs for sources/targets
│   ├── spark_manifest.py            # SparkApplication builder (§3.3)
│   ├── ops_api.py                   # small client for the ops API (runs, upstream status)
│   ├── operators/
│   │   └── etl_spark_operator.py    # submit + defer + result mapping (§3.2, §9)
│   ├── triggers/
│   │   ├── spark_app_trigger.py     # polls the SparkApplication status (async, in the triggerer)
│   │   └── upstream_trigger.py      # polls upstream run statuses (for wait_upstream)
│   ├── sensors/
│   │   └── wait_upstream.py         # dependency policy (§4.4)
│   ├── quality_gate.py              # PARTIAL handling (§9)
│   ├── callbacks.py                 # on_failure / on_partial / on_success (§7.4)
│   └── notifier.py                  # Slack/Teams/PagerDuty sender, alert grouping
├── generated/                       # produced by CI, not committed to Git
│   └── jobs.<env>.json
├── tests/
│   ├── test_dag_integrity.py        # every DAG imports, no cycles, ids unique, pools exist
│   ├── test_factory.py              # schedules/triggers/outlets built correctly from sample JSON
│   ├── test_spark_manifest.py       # resources, secrets, labels, env guard
│   └── test_result_mapping.py       # every row of the §9.2 table
├── requirements.txt                 # apache-airflow-providers-cncf-kubernetes, -hashicorp, -slack ...
└── README.md
```

### 10.2 The DAG factory

```python
# orchestration/airflow/dags/etl_job_dags.py
"""Creates one DAG per enabled ETL job. Keep this file tiny: all logic lives in etl_airflow.factory."""
from etl_airflow.factory import build_all

for dag in build_all():
    globals()[dag.dag_id] = dag            # Airflow discovers DAG objects in module globals
```

```python
# orchestration/airflow/etl_airflow/factory.py
from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.datasets import Dataset
from airflow.timetables.datasets import DatasetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable

from etl_airflow.settings import SETTINGS
from etl_airflow.registry import JOBS
from etl_airflow.datasets import dataset_for_table
from etl_airflow.naming import dag_id_for
from etl_airflow.operators.etl_spark_operator import EtlSparkOperator
from etl_airflow.sensors.wait_upstream import WaitUpstreamSensor
from etl_airflow.quality_gate import QualityGateOperator
from etl_airflow.callbacks import on_failure, on_success

START_DATE = pendulum.datetime(2026, 1, 1, tz="UTC")     # fixed; never changes (§8.4)


def _schedule(job):
    """SCHEDULE | UPSTREAM | SCHEDULE_OR_UPSTREAM | MANUAL_ONLY → an Airflow schedule object."""
    sources = [dataset_for_table(t) for t in job.sources]
    any_source = None
    for ds in sources:                                    # OR of the sources (never the AND trap, §4.3)
        any_source = ds if any_source is None else (any_source | ds)
    cron = CronTriggerTimetable(job.schedule, timezone="UTC") if job.schedule else None

    if job.trigger == "MANUAL_ONLY" or (cron is None and any_source is None):
        return None
    if job.trigger == "SCHEDULE":
        return cron
    if job.trigger == "UPSTREAM":
        return any_source
    # SCHEDULE_OR_UPSTREAM
    if cron is None:
        return any_source
    if any_source is None:
        return cron
    return DatasetOrTimeSchedule(timetable=cron, datasets=any_source)


def _default_args(job):
    a = SETTINGS.default_args
    timeout_min = (job.sla.max_duration_minutes * SETTINGS.execution_timeout_factor
                   if job.sla and job.sla.max_duration_minutes else SETTINGS.default_execution_timeout_min)
    return {
        "owner": job.owner,
        "retries": a.retries,
        "retry_delay": timedelta(minutes=a.retry_delay_min),
        "retry_exponential_backoff": a.retry_exponential_backoff,
        "max_retry_delay": timedelta(minutes=a.max_retry_delay_min),
        "execution_timeout": timedelta(minutes=timeout_min),
        "on_failure_callback": on_failure,
        "on_success_callback": on_success,
    }


def build_dag(job) -> DAG:
    with DAG(
        dag_id=dag_id_for(job.name),                      # etl__s2g_fact_sales_line
        description=f"{job.type} {job.name} (owner {job.owner})",
        schedule=_schedule(job),
        start_date=START_DATE,
        catchup=False,
        max_active_runs=1,
        max_active_tasks=2,
        dagrun_timeout=timedelta(hours=SETTINGS.dagrun_timeout_h),
        is_paused_upon_creation=not SETTINGS.schedules_active,
        default_args=_default_args(job),
        tags=["etl", f"layer:{job.layer}", f"domain:{job.domain}", f"type:{job.type.lower()}",
              f"owner:{job.owner}", f"priority:{job.priority.lower()}"],
        doc_md=f"**{job.name}**  \nOwner: {job.owner}  \nSources: {', '.join(job.sources)}  \n"
               f"Targets: {', '.join(job.targets)}  \n[Ops UI]({SETTINGS.ops_ui_url}/jobs/{job.name})",
    ) as dag:
        hard_deps = [d for d in job.depends_on if d.type == "HARD"]
        wait = WaitUpstreamSensor(task_id="wait_upstream", job=job.name, dependencies=hard_deps,
                                  poke_interval=SETTINGS.wait_upstream_poke_s, deferrable=True) if hard_deps else None

        run = EtlSparkOperator(
            task_id="run_etl",
            job=job.name,
            mode="INCREMENTAL",
            trigger="{{ 'DEPENDENCY' if dag_run.run_type == 'dataset_triggered' else 'SCHEDULED' }}",
            pool="etl_sql" if job.runner == "SQL" else "etl_spark",
            pool_slots=SETTINGS.pool_slots_for(job.resources.profile),
            priority_weight=SETTINGS.priority_weight(job.priority),
            weight_rule="absolute",
            outlets=[dataset_for_table(t) for t in job.targets],   # emitted on success (SUCCESS or PARTIAL)
            deferrable=SETTINGS.deferrable,
        )

        gate = QualityGateOperator(task_id="quality_gate", partial_as=SETTINGS.partial_as, retries=0,
                                   on_failure_callback=None)   # on_partial() already alerted; avoid a second "FAILED" alert

        if wait:
            wait >> run
        run >> gate
    return dag


def build_all() -> list[DAG]:
    return [build_dag(j) for j in JOBS.enabled()]
```

### 10.3 The operator (submit → defer → map the result)

```python
# orchestration/airflow/etl_airflow/operators/etl_spark_operator.py
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException, AirflowFailException, AirflowSkipException

from etl_airflow.settings import SETTINGS
from etl_airflow.registry import JOBS
from etl_airflow.naming import base_run_id, attempt_run_id, k8s_name
from etl_airflow.spark_manifest import build_manifest
from etl_airflow.triggers.spark_app_trigger import SparkAppTrigger
from etl_airflow.ops_api import OpsApi
from etl_airflow.k8s import SparkAppClient


class EtlSparkOperator(BaseOperator):
    template_fields = ("job", "mode", "trigger", "dry_run", "run_params", "run_id")

    def __init__(self, *, job, mode="INCREMENTAL", trigger="SCHEDULED", dry_run=False,
                 run_params=None, run_id=None, deferrable=True, **kwargs):
        super().__init__(**kwargs)
        self.job, self.mode, self.trigger, self.dry_run = job, mode, trigger, dry_run
        self.run_params, self.run_id, self.deferrable = run_params or {}, run_id, deferrable

    # ── 1. submit ─────────────────────────────────────────────────────────────────
    def execute(self, context):
        ti = context["ti"]
        base = self.run_id or base_run_id(self.job, context["dag_run"])          # e.g. s2g_fact_sales_line-20261030T094500Z
        run_id = attempt_run_id(base, ti.try_number)                             # …-a1, …-a2
        trigger = "AIRFLOW_RETRY" if ti.try_number > 1 else self.trigger
        job = JOBS.get(self.job)
        manifest = build_manifest(job=job, env=SETTINGS.env, run_id=run_id, mode=self.mode, trigger=trigger,
                                  dry_run=self.dry_run, params=self.run_params,
                                  parent_run_id=attempt_run_id(base, ti.try_number - 1) if ti.try_number > 1 else None,
                                  image_version=SETTINGS.image_version_override or JOBS.image_version)
        name = k8s_name(run_id)
        SparkAppClient(SETTINGS.k8s_conn).create(SETTINGS.namespace, manifest)
        ti.xcom_push(key="run_id", value=run_id)
        if self.deferrable:
            self.defer(trigger=SparkAppTrigger(namespace=SETTINGS.namespace, name=name,
                                               conn_id=SETTINGS.k8s_conn, poll_seconds=SETTINGS.poll_interval_s),
                       method_name="execute_complete", kwargs={"run_id": run_id, "app_name": name})
        else:
            event = SparkAppClient(SETTINGS.k8s_conn).wait(SETTINGS.namespace, name)
            return self.execute_complete(context, event, run_id=run_id, app_name=name)

    # ── 2. map the result from job_run_audit (source of truth) ────────────────────
    def execute_complete(self, context, event, run_id, app_name):
        ti = context["ti"]
        result = OpsApi(SETTINGS.ops_api_conn).get_run(run_id)        # None if the driver died before writing audit
        if result is None:
            raise AirflowException(f"No audit row for {run_id}; Spark app state={event.get('state')} (INFRA_UNKNOWN)")

        ti.xcom_push(key="etl_result", value=result.summary())        # status, counts, errorCode, links (no PII)
        status, code = result.status, result.error_code
        self.log.info("ETL run %s finished: %s %s", run_id, status, code or "")

        if status in ("SUCCESS", "PARTIAL"):
            return result.summary()                                    # green; outlets → Dataset event; gate decides colour
        if status == "SKIPPED":
            raise AirflowSkipException(f"{run_id} skipped: {code}")
        if status == "CANCELLED":
            raise AirflowFailException(f"{run_id} cancelled by {result.cancelled_by}")
        if result.retryable:                                           # from error_code_catalog
            raise AirflowException(f"{run_id} FAILED {code} (retryable)")
        raise AirflowFailException(f"{run_id} FAILED {code} (not retryable)")

    # ── 3. cancellation / timeout ─────────────────────────────────────────────────
    def on_kill(self):
        run_id = self.xcom_pull(self.get_template_context(), key="run_id")   # sketch
        if run_id:
            SparkAppClient(SETTINGS.k8s_conn).delete(SETTINGS.namespace, k8s_name(run_id))   # ➕ no orphan Spark apps
```

### 10.4 The quality gate

```python
# orchestration/airflow/etl_airflow/quality_gate.py
from airflow.models import BaseOperator
from airflow.exceptions import AirflowFailException
from etl_airflow.callbacks import on_partial

class QualityGateOperator(BaseOperator):
    def __init__(self, *, partial_as="QUALITY_GATE_FAILED", **kwargs):
        super().__init__(**kwargs)
        self.partial_as = partial_as

    def execute(self, context):
        s = context["ti"].xcom_pull(task_ids="run_etl", key="etl_result") or {}
        bad = s.get("failed", 0) + s.get("quarantined", 0) + s.get("partial", 0)
        if s.get("status") == "SUCCESS":
            if bad:
                self._note(context, f"{bad} records with errors (within tolerance). See ops UI.")
            return
        # PARTIAL
        note = (f"PARTIAL: {bad} of {s.get('read')} records not OK (top error {s.get('topErrorCode')}). "
                f"Rerun failed records from the ops UI: {s.get('opsUrl')}")
        self._note(context, note)
        on_partial(context, s)
        if self.partial_as == "QUALITY_GATE_FAILED":
            raise AirflowFailException(note)                 # red box, no retry: data is already committed

    def _note(self, context, text):
        # Airflow 2.5+: task-instance / DAG-run notes (set via the session or the REST API)
        context["ti"].note = text
```

### 10.5 The cascade DAG

```python
# orchestration/airflow/dags/etl_cascade.py
from airflow import DAG
from airflow.decorators import task
from etl_airflow.operators.etl_spark_operator import EtlSparkOperator
from etl_airflow.settings import SETTINGS

MAX_LEVELS = 6                                              # deep enough for B2S → dims → facts → aggs → marts

with DAG(dag_id="etl_cascade", schedule=None, catchup=False, max_active_runs=2,
         tags=["etl", "cascade"], render_template_as_native_obj=True) as dag:

    @task
    def level_items(level: int, **context):
        levels = context["dag_run"].conf.get("levels", [])
        items = levels[level] if level < len(levels) else []
        root = context["dag_run"].conf["rootRunId"]
        return [{"job": i["job"], "mode": i.get("mode", "INCREMENTAL"),
                 "run_params": {**i.get("params", {}), "parentRunId": root}} for i in items]

    previous = None
    for lvl in range(MAX_LEVELS):
        items = level_items.override(task_id=f"items_l{lvl}")(lvl)
        runs = (EtlSparkOperator.partial(task_id=f"level_{lvl}", trigger="CASCADE", pool="etl_spark",
                                         trigger_rule="none_failed_min_one_success" if lvl else "all_success")
                .expand_kwargs(items))                      # empty list → the mapped task is skipped
        if previous is not None:
            previous >> items
        items >> runs
        previous = runs
```

(`PARTIAL` counts as success for `run_etl`-style tasks, so a level with PARTIAL runs still lets the next level start, as doc 07 requires.)

---

## 11. Testing the DAGs

| Test | Checks |
|---|---|
| DAG integrity | All DAG files import without errors; DAG ids unique; no cycles; every referenced pool exists in `airflow.yml`; parse time < 2 s for 150 jobs ➕ |
| Factory | Given a sample `jobs.json`: correct schedule type per trigger mode, OR-datasets (never AND), outlets = targets, pool slots by profile, paused flag per env |
| Manifest | Resources from the profile, secrets on driver **and** executors, `ETL_ENV` set, labels, TTL, no secret values anywhere in the manifest |
| Result mapping | Every row of the §9.2 table (mocked ops API) |
| End to end (CI, kind/minikube) ➕ | A tiny K8s cluster with the Spark Operator runs one canary job in dry-run |

---

## 12. ➕ Added by Architect (summary)

| # | Addition | Why |
|---|---|---|
| A1 | DAG factory reads a CI-built JSON (no DB at parse time), versioned with the image | Fast, reliable parsing; DAGs and code always match |
| A2 | `EtlSparkOperator`: submit → defer → read `job_run_audit` via the ops API → map the outcome | One contract for all jobs; correct PARTIAL/retry handling; no DB password in Airflow |
| A3 | `on_kill` deletes the `SparkApplication`; `timeToLiveSeconds` cleanup | No orphan Spark apps after cancels/timeouts |
| A4 | OR-of-sources Dataset schedules + `wait_upstream` instead of AND datasets / `ExternalTaskSensor` | No "never triggers" trap; dependency policy from doc 07 applied uniformly |
| A5 | Kafka → dataset-event bridge in the ops API; coalescing via `max_active_runs=1` | Event-driven runs without 100 Kafka sensors in Airflow |
| A6 | Level-by-level `etl_cascade` with dynamic task mapping | Explicit cascades in correct order, in parallel per level |
| A7 | Generic `etl_manual_run`; in uat/prod only the ops API may trigger it | Approvals can't be bypassed |
| A8 | Pool slots weighted by Spark profile; a separate `etl_heavy` pool | Fair cluster use; never two full loads at once |
| A9 | Deferrable waiting | 100 waiting jobs don't occupy 100 workers |
| A10 | `catchup=False` everywhere; our BACKFILL mode instead of Airflow backfill | Matches how our incremental jobs really work |
| A11 | `run_etl` + `quality_gate` (two boxes) + notes | PARTIAL is never shown as a plain green success |
| A12 | Own SLA monitor DAG (version-independent) | Airflow's SLA feature is limited and removed in Airflow 3 |
| A13 | Alert rules: no alert for dependency skips, grouped PARTIAL warnings, pager only for HIGH-priority final failures | Fewer, more meaningful alerts |
| A14 | Image-version override Variable for emergency rollback | Roll back Spark code without redeploying DAGs |
| A15 | DAG tests incl. a parse-time budget and an end-to-end canary | 150 generated DAGs stay healthy |

---

## 13. Changes to Earlier Decisions

| Earlier | Now | Reason |
|---|---|---|
| Doc 06 §8.2: Airflow reads exit codes | Airflow reads the **audit row** via the ops API; exit codes are a fallback | The audit row knows PARTIAL and retryable; Airflow has no DB password |
| Doc 08 §9: Airflow Variable `etl_spark_app_version` pins the image | The image version is part of `jobs.<env>.json` (same commit as the DAGs); the Variable is only an emergency override | DAGs and code can't drift apart |
| Doc 01 tree `orchestration/airflow/dags/dag_factory.py`, `maintenance_dags.py`, `plugins/` | Package `etl_airflow/` + `dags/` files as in §10.1 | Clearer structure |
| Job YAML had no orchestration block | New optional `orchestration: {trigger, priority}` | Per-job trigger mode and priority |

---

## 14. Key Decisions

1. **One DAG per job**, generated by one factory file from `generated/jobs.<env>.json` (built by CI with the image), plus a fixed set of platform DAGs: `etl_manual_run`, `etl_cascade`, `etl_quarantine_reprocess`, `etl_sla_monitor`, `etl_maintenance`, `etl_ingestion_replay`.
2. **Spark start:** Spark Operator `SparkApplication` created by our deferrable `EtlSparkOperator`; resources from size profiles, secrets on driver and executors, labels, TTL cleanup, `on_kill` deletes the app.
3. **Result:** read from `job_run_audit` via the ops API; SUCCESS/PARTIAL → `run_etl` success (+ Dataset event), PARTIAL → `quality_gate` failed (configurable per env), retryable FAILED → Airflow retry with a new run id, non-retryable → fail without retry, SKIPPED → skipped.
4. **Dependencies:** Dataset per table (`etl://<schema>.<table>`, `etl://bronze/<svc>/<coll>`); schedules = cron, OR-of-source datasets, or both (`DatasetOrTimeSchedule`); HARD-dependency policy in a deferrable `wait_upstream` task; no `ExternalTaskSensor`.
5. **Events:** `etl.events.bronze-loaded` → ops API bridge → Airflow dataset events; coalesced by `max_active_runs=1`.
6. **Cascade:** automatic via Dataset events; explicit cascades via `etl_cascade` (levels, dynamic task mapping, PARTIAL lets the next level run).
7. **Manual runs:** one generic `etl_manual_run` DAG with typed Params; in uat/prod only the ops API (with approvals) may trigger it; plain triggers of job DAGs = extra incremental runs.
8. **Environment:** identical DAG code; `ETL_ENV` + `config/environments/{base,<env>}/airflow.yml` + `jobs.<env>.json`; dev DAGs paused on creation; connection values from Vault.
9. **Retries/timeouts/SLA/alerts:** 3 retries with exponential backoff for retryable failures only; `execution_timeout = 3 × maxDuration`; own SLA monitor DAG; callbacks with grouped, owner-routed alerts; pager only for HIGH-priority final failures in prod.
10. **Concurrency:** `max_active_runs=1` per job, pools `etl_spark` (slots weighted by profile), `etl_heavy`, `etl_sql`; priority weights HIGH/NORMAL/LOW; deferrable waiting; `catchup=False`; our BACKFILL mode instead of Airflow backfill.
