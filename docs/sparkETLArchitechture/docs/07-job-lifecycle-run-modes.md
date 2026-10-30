# 07 — Job Lifecycle, Run Modes, Status Rules and Recovery

> Builds on docs [01](01-architecture-and-structure.md) to [06](06-reusable-framework.md).
> Written in **simple language**: every section starts with an **"In simple words"** box, and every abbreviation is written out in full.

---

## 0. Abbreviations Used in This Document

| Short form | Full form | Meaning in simple words |
|---|---|---|
| **ETL** | Extract, Transform, Load | Take data out, clean/reshape it, load it somewhere |
| **API** | Application Programming Interface | A way for programs to call each other |
| **REST** | Representational State Transfer | The common style of web APIs (`POST /api/v1/runs`) |
| **UI** | User Interface | Screens people click on |
| **DAG** | Directed Acyclic Graph | An Airflow workflow (tasks with arrows, no loops) |
| **CLI** | Command Line Interface | Running a program with text arguments |
| **CDC** | Change Data Capture | Capturing every insert/update/delete from the source DB |
| **DB** | Database | Where data is stored |
| **DQ** | Data Quality | Checks that data is correct |
| **DLQ** | Dead Letter Queue | A Kafka "problem box" topic for unprocessable messages |
| **PII** | Personally Identifiable Information | Name, email, phone, address, ID numbers |
| **RBAC** | Role-Based Access Control | Permissions given by role (viewer, operator, admin) |
| **SLA** | Service Level Agreement | A promise like "data ready by 8 AM" |
| **TDE** | Transparent Data Encryption | The database encrypts its files on disk automatically |
| **KMS** | Key Management Service | A safe place that stores and manages encryption keys |
| **MVCC** | Multi-Version Concurrency Control | How PostgreSQL lets readers see a stable snapshot while others write |
| **SK** | Surrogate Key | A number created in Gold to identify a dimension row |
| **B2S / S2G** | Bronze to Silver / Silver to Gold | The two job families |
| **EPAS** | EDB Postgres Advanced Server (EDB = EnterpriseDB) | Our PostgreSQL-based database |
| **UUID** | Universally Unique Identifier | A random ID that practically never repeats |
| **JSON** | JavaScript Object Notation | Text format for structured data |
| **HTTP** | HyperText Transfer Protocol | The protocol of the web; `202`, `409` are HTTP status codes |
| **SQLState** | SQL State code | A standard 5-character database error code (e.g. `40P01` = deadlock) |

---

## 1. The Life of One Run (Big Picture)

> **In simple words**
> Think of a **taxi booking**:
> 1. You **request** a ride (someone or something asks for a run).
> 2. The app **checks** you are allowed and finds a driver (permissions, approval, no other ride running).
> 3. The ride is **queued**, then **starts** (`QUEUED` → `RUNNING`).
> 4. It **ends** in one of a few ways: arrived (`SUCCESS`), arrived but some luggage was left behind (`PARTIAL`), car broke down (`FAILED`), you cancelled (`CANCELLED`), or the ride was not needed (`SKIPPED`).
> 5. Everything is written in the **trip log** (`job_run_audit`), with a link to an earlier trip if this one is a "redo" (`parent_run_id`).

```
  WHO ASKS                      CHECKS                          RUNS                        RESULT
 ┌──────────────────┐   ┌───────────────────────────┐   ┌──────────────────────┐   ┌──────────────────────┐
 │ Airflow schedule │──►│ job enabled?              │──►│ EtlJobRunner (Spark) │──►│ job_run_audit:       │
 │ Upstream finished│   │ dependencies OK? (§7)     │   │ lock → steps →       │   │ status + error_code  │
 │ Kafka event      │   │ user allowed? (§2.13)     │   │ commit → write-back →│   │ + counts             │
 │ User via ops UI  │   │ prod approval? (§2.13)    │   │ reconcile → release  │   │ Airflow exit code    │
 │ Cascade          │   │ parameters valid?         │   │                      │   │ alerts / next jobs   │
 └──────────────────┘   └───────────────────────────┘   └──────────────────────┘   └──────────────────────┘
```

### 1.1 Who creates the run id

| Trigger | Run id created by | Format |
|---|---|---|
| Airflow schedule / dependency / event | Airflow task (from the DAG run's logical time) | `b2s_order-20261030T090000Z` |
| Manual (ops UI / API) | `etl-ops-api` when it accepts the request | `b2s_order-20261030T091512Z-m` |
| Cascade | `etl-ops-api` or the cascade task in Airflow | `s2g_fact_sales_line-20261030T093000Z-c` |

➕ **Every attempt is its own row.** If Airflow automatically retries a failed task, the retry gets the id `<base>-a2`, `<base>-a3` …, with `trigger_type = AIRFLOW_RETRY`, `parent_run_id` = the previous attempt and `root_run_id` = attempt 1. So the first failure is never overwritten, and you can see the whole story of a run.

---

## 2. Run Modes, One by One

> **In simple words**
> All run modes go through the **same machine** (doc 06). The difference is only:
> 1. **Which records** are picked (the "shopping list"),
> 2. **Whether the bookmark moves forward** after the run,
> 3. **Who** is allowed to start it.
>
> Duplicates are prevented by the same safety locks in every mode (§9), so you can re-run things without fear.

Quick reference (details per mode below):

| # | Mode / trigger | `run_mode` | `trigger_type` | Picks | Bookmark moves? | `parent_run_id` |
|---|---|---|---|---|---|---|
| 2.1 | Full load | `FULL_LOAD` | `MANUAL` (or `API`) | everything | ✅ to the cutoff | optional |
| 2.2 | Scheduled incremental | `INCREMENTAL` | `SCHEDULED` | new/changed + retryable | ✅ | — |
| 2.3 | Rerun failed job | `RERUN_JOB` | `MANUAL` / `AIRFLOW_RETRY` | same as incremental | ✅ | ✅ the failed run |
| 2.4 | Rerun failed records | `RERUN_FAILED_RECORDS` | `MANUAL` | only FAILED / PARTIAL records | ❌ | ✅ the partial run |
| 2.5 | Reprocess quarantine | `REPROCESS_QUARANTINE` | `MANUAL` / `SCHEDULED` | only `READY_TO_REPROCESS` items | ❌ | optional |
| 2.6 | Backfill | `BACKFILL` | `MANUAL` | a date range | ❌ | optional |
| 2.7 | Replay | `REPLAY` | `MANUAL` | everything (or a range) after a rebuild | ❌ | optional |
| 2.8 | ID reprocess | `ID_REPROCESS` | `MANUAL` | given ids | ❌ | optional |
| 2.9 | Dependency / event | `INCREMENTAL` | `DEPENDENCY` / `EVENT` | like incremental | ✅ | — |
| 2.10 | Cascade | usually `INCREMENTAL` | `CASCADE` | like incremental | ✅ | ✅ the upstream run |
| 2.11 | Dry-run | any mode + `is_dry_run = true` | any | same as that mode | ❌ never | optional |

---

### 2.1 FULL LOAD

| | |
|---|---|
| **When** | First load of a new job; after a big logic change (new columns, fixed transformation); after restoring a table |
| **Picks** | **All** records of the source, up to the run cutoff (the time/number at run start). Bronze → Silver: all Bronze documents (also deleted ones, so deletes are applied). Silver → Gold: all Silver rows |
| **No duplicates because** | Merges are upserts guarded by version + row hash (docs 03/04). Existing rows that did not change are counted as `unchanged`, not inserted again. Dimensions keep their surrogate keys (never truncated) |
| **Special** | Gold facts can use the **shadow-table swap** (doc 04 §9) so dashboards never see a half-loaded table |
| **Audit** | `run_mode = FULL_LOAD`, `trigger_type = MANUAL`, `triggered_by = <user>`, `request_ticket`, `run_params = {}` |
| **Who** | Operator in dev/qa; **approval required** in uat/prod (§2.13) |

### 2.2 SCHEDULED INCREMENTAL

| | |
|---|---|
| **When** | Normal daily life. The schedule is in the job YAML (`schedule: "15 * * * *"`) and can differ per environment (`job-overrides.yml`): every hour, every 5 hours, daily … |
| **Picks** | Bronze → Silver: documents with `syncSilver.status` `PENDING`, `PARTIAL`, or `FAILED` that are due for retry (§3.5). Silver → Gold: rows with a change number above the bookmark + this job's retry list |
| **No duplicates because** | Only changed records are picked; the merge guards make repeated records "unchanged" |
| **Audit** | `run_mode = INCREMENTAL`, `trigger_type = SCHEDULED`, `triggered_by = airflow` |
| **Who** | Airflow (system) |

### 2.3 RERUN FAILED JOB

| | |
|---|---|
| **When** | The whole run **FAILED** (database down, Spark crash, timeout) |
| **Picks** | Exactly what an incremental run would pick. A failed run **rolled back** and wrote nothing back, so all its records are still pending, and the bookmark did not move. "Resume from the last checkpoint" therefore happens automatically |
| **No duplicates because** | Nothing from the failed run was committed |
| **Audit** | `run_mode = RERUN_JOB`, `parent_run_id = <failed run>`, `root_run_id = <first run of the chain>`. Automatic Airflow retries use `trigger_type = AIRFLOW_RETRY` |
| **Who** | Airflow automatically (only for retryable errors, §8.2); operators manually |

### 2.4 RERUN FAILED RECORDS ONLY

| | |
|---|---|
| **When** | The run ended **PARTIAL** (most records OK, some FAILED) and you don't want to wait for the next schedule, or the retry backoff (§3.5) is too slow for an urgent case |
| **Picks** | Bronze → Silver: only documents with `syncSilver.status` `FAILED` or `PARTIAL` (optionally only those written by `parent_run_id`), **ignoring** the retry backoff. Silver → Gold: only this job's entries in `gold_record_status` |
| **No duplicates because** | The whole document is re-processed; parts that were already written are "unchanged" thanks to the row-hash guard (doc 03 §7.2) |
| **Audit** | `run_mode = RERUN_FAILED_RECORDS`, `parent_run_id = <partial run>` |
| **Bookmark** | Does **not** move (this run only looks at old failures) |
| **Who** | Operator |

### 2.5 REPROCESS QUARANTINE

| | |
|---|---|
| **When** | Quarantined records were fixed (§6) and approved → status `READY_TO_REPROCESS` |
| **Picks** | Only the source records of quarantine items with status `READY_TO_REPROCESS` for this job |
| **No duplicates because** | Same merge guards; one open quarantine item per record (unique index, doc 05) |
| **After commit** | Successful items → `REPROCESSED` (with `reprocess_run_id`). Items that fail again get a **new** quarantine item linked by `previous_quarantine_id` |
| **Audit** | `run_mode = REPROCESS_QUARANTINE`; `run_params = {"quarantineIds": [...]}` (or a count when many) |
| **Who** | Data steward / operator manually, **or** ➕ automatically: a small scheduled run every few hours per job when items are `READY_TO_REPROCESS` |

### 2.6 BACKFILL / DATE RANGE

| | |
|---|---|
| **When** | Data of a period is wrong or missing (e.g. a mapping table was wrong for 1–8 October and is now fixed) |
| **Picks** | Records whose **event time** is in `[from, to)`. Bronze: `_etl.sourceEventTs`. Silver: the event date column configured for the job |
| **No duplicates because** | Same merge guards. Changed Silver rows get new change numbers, so Gold picks them up automatically |
| **Bookmark** | Does **not** move (it is a "look back" run) |
| **Audit** | `run_mode = BACKFILL`, `run_params = {"from": "2026-10-01T00:00:00Z", "to": "2026-10-08T00:00:00Z"}` |
| **Who** | Operator; approval in prod if the range is longer than N days (configurable, e.g. 31) |

### 2.7 REPLAY (data-loss recovery)

| | |
|---|---|
| **When** | A layer **lost** data or was rebuilt (e.g. a Silver table restored from an old backup, or Bronze rebuilt from Kafka) |
| **How, per layer** | **Source → Bronze:** Kafka replay job with a time range and service/collection filters, or a re-snapshot (doc 02 §15). **Bronze → Silver:** `REPLAY` of the B2S job (all, or a range); Bronze keeps soft-deleted documents, so deletes are replayed correctly. **Silver → Gold:** nothing special: changed Silver rows get new change numbers and Gold catches up (or use a cascade, §2.10). For a lost Gold table: `FULL_LOAD` of that Gold job |
| **No duplicates because** | Version guards everywhere: older data never overwrites newer data, same data becomes "unchanged" |
| **Audit** | `run_mode = REPLAY`, `request_ticket` (incident id) required, `run_params` with range/filters |
| **Who** | Platform admin, **always with approval** in uat/prod |

### 2.8 SINGLE ENTITY / ID REPROCESS

| | |
|---|---|
| **When** | A support ticket: "order ORD-1001 looks wrong in the report" |
| **Picks** | Only the given source ids (`--ids` or `--ids-file`). For Silver → Gold, the natural keys |
| **No duplicates because** | Same merge guards |
| **Audit** | `run_mode = ID_REPROCESS`, `run_params = {"ids": [...]}` (stored as a count + a hash if longer than 1,000 ids) |
| **Who** | Operator. Limit per run: e.g. 100,000 ids (configurable); more → use a backfill |

### 2.9 DEPENDENCY / EVENT TRIGGERED

| | |
|---|---|
| **When** | Run "as soon as the upstream data is ready" instead of on a fixed clock |
| **How** | **Dependency:** when an upstream job finishes `SUCCESS` or `PARTIAL`, Airflow starts its downstream jobs (Airflow "Datasets", Prompt 09). **Event:** `etl-ingestion` publishes throttled `etl.events.bronze-loaded` events (doc 02), and a sensor starts the Bronze → Silver job for that collection |
| **Picks** | Like incremental |
| **➕ Coalescing** | 50 events in 2 minutes must **not** start 50 runs. If a run is already running or queued for the job, new triggers are **merged** into one "run again after this one" flag. One extra run at most |
| **Audit** | `run_mode = INCREMENTAL`, `trigger_type = DEPENDENCY` or `EVENT`, `run_params = {"upstreamRunId": "..."}` |
| **Who** | System |

### 2.10 CASCADE RERUN

> **In simple words:** when you fix something upstream, everything downstream should be fixed too, like **fixing a spelling mistake in a book and reprinting the chapters that quote it**.

There are two kinds, and the first one is **free**:

| Kind | How it works |
|---|---|
| **Automatic (data-driven)** | When a Bronze → Silver re-run changes Silver rows, those rows get **new change numbers**. Every Gold job reading that table picks them up in its next incremental run. No action needed. Aggregates follow through "touched dates" (doc 04 §10) |
| **Explicit cascade** ➕ | Don't want to wait for the schedules? Request a run with `cascade: true`. The ops API finds all downstream jobs (recursive query on `job_dependency`, doc 05 §6.4) and queues them in dependency order (dimensions → facts → aggregates), as `INCREMENTAL` runs with `trigger_type = CASCADE` and `parent_run_id` = the upstream run |

Downstream jobs normally run as `INCREMENTAL` (they only need to process what changed). Only if the **downstream logic** itself changed should you choose `FULL_LOAD` for them, and the cascade request allows that per job.

```
 POST /runs {job: b2s_order, mode: BACKFILL, from..to, cascade: {enabled: true}}
   └─► b2s_order (BACKFILL)                         run A
         └─► s2g_fact_sales_line (INCREMENTAL, CASCADE, parent=A)      run B
         └─► s2g_fact_order_fulfillment (INCREMENTAL, CASCADE, parent=A)  run C
               └─► s2g_agg_sales_daily_store (INCREMENTAL, CASCADE, parent=B)  run D
```

### 2.11 DRY-RUN / VALIDATE ONLY

| | |
|---|---|
| **When** | Before a risky run in prod; to test a new job or a new DQ rule; to answer "how many records would this backfill touch?" |
| **Picks** | Exactly what the chosen mode would pick |
| **Writes** | Nothing to Silver/Gold/Bronze/quarantine (Null-Object writers, doc 06 §5). **Does** write `job_run_audit` (`is_dry_run = true`) and `dq_results`, because seeing the result is the purpose |
| **Bookmark** | Never moves |
| **Who** | Any operator, in every environment (it is safe) |

### 2.12 Run-mode cards in one table

| Mode | Typical question it answers | Needs | Typical size |
|---|---|---|---|
| FULL_LOAD | "Load everything fresh" | ticket in prod | all |
| INCREMENTAL | "What's new?" | — | small |
| RERUN_JOB | "Try the failed run again" | `parent_run_id` | small |
| RERUN_FAILED_RECORDS | "Retry only the broken ones now" | `parent_run_id` (optional) | tiny |
| REPROCESS_QUARANTINE | "Process the repaired ones" | approved items | tiny |
| BACKFILL | "Redo 1–8 October" | `from`, `to` | medium |
| REPLAY | "We lost data, rebuild" | ticket, approval | large |
| ID_REPROCESS | "Fix these 5 orders" | `ids` | tiny |
| DEPENDENCY/EVENT | "Upstream is ready" | — | small |
| CASCADE | "Fix flows downstream now" | upstream run | small/medium |
| DRY-RUN | "What would happen?" | — | any |

### 2.13 Who may trigger what (RBAC: Role-Based Access Control)

| Role | Who | dev / qa | uat / prod |
|---|---|---|---|
| `VIEWER` | Anyone in the data team, analysts | See runs, statuses, DQ results | Same (no PII) |
| `OPERATOR` | Data engineers on support | All modes except REPLAY | INCREMENTAL, RERUN_JOB, RERUN_FAILED_RECORDS, ID_REPROCESS, DRY_RUN directly; FULL_LOAD and BACKFILL > 31 days **need approval** |
| `DATA_STEWARD` | Business data owners | Quarantine: fix, approve (other person's fixes), reject | Same; can trigger REPROCESS_QUARANTINE |
| `PII_VIEWER` | Few named people | May "reveal" masked values in quarantine (audited, §6.4) | Same |
| `PLATFORM_ADMIN` | Platform team | Everything | REPLAY, watermark reset, lock override: **always with approval + ticket** |
| `SYSTEM` | Airflow, `etl-ops-api` itself | Scheduled, dependency, event, cascade, retries | Same |

**Approval ("four-eyes") rule in uat/prod ➕:** the approver must be a **different person** than the requester, with `OPERATOR` or higher for the same domain. Approvals expire after 24 hours.

---

## 3. Incremental Strategy

### 3.1 Bronze → Silver: options compared

> **In simple words:** how does the job know what is "new"? Three ideas:
> 1. **Status flag** on every Bronze document: "PENDING = please process me".
> 2. **Watermark**: "process everything ingested after 09:00".
> 3. **Hash**: "process it only if the content really changed".

| Approach | How | Strengths | Weaknesses | Verdict |
|---|---|---|---|---|
| **Status-based** (`syncSilver.status`) | Read `PENDING / PARTIAL / FAILED (due)`; write the result back per record | Exact; self-healing (failures are retried automatically); works for late and re-processed records; every record's state is visible | Needs a write-back per record (bulk, doc 03 §9) and a good partial index | ✅ **Primary** |
| Watermark on `lastIngestedAt` | Read `lastIngestedAt > last watermark` | Cheap; no write-back needed | ❌ **Unsafe alone**: several ingestion pods write in parallel with their own clocks; a batch can be committed with an *older* timestamp **after** the reader has moved past it → those records are silently missed. Also can't retry failures | ➕ Only as a **safety net** (§3.3) |
| Watermark on source `updatedAt` | Read `data.updatedAt > watermark` | Business-meaningful | Depends on every source service setting `updatedAt` correctly; deletes have no `updatedAt` | ❌ Not used for selection |
| `recordHash` | Compare content hashes | Detects "nothing really changed" | Not a selection method by itself | ✅ Used to **avoid useless work** (ingestion keeps the status unchanged for no-op updates; Silver skips unchanged rows) |

**Decision: status-based selection + row-hash/version guards for idempotency + a daily safety-net sweep.**

### 3.2 Pseudo-code: Bronze → Silver incremental

```text
function incremental_b2s(job):
    cutoff = now()                                         # run cutoff (doc 03 §2.1)
    plan = ( status IN (PENDING, PARTIAL)
             OR (status = FAILED AND retryCount < maxRetries AND nextRetryAt <= cutoff) )
           AND lastIngestedAt <= cutoff
           AND job.staticFilter
    if not exists(bronze, plan): finish SUCCESS (0 records)          # empty-run short-circuit
    docs = read(bronze, plan, limit = job.maxRecordsPerRun, order by lastIngestedAt)

    rows = split → standardize → dq → dedupe → enrich (docs)
    status_per_doc = classify(rows)                                   # SUCCESS/PARTIAL/FAILED/QUARANTINED/SKIPPED
    if bad_share(status_per_doc) > job.thresholds: ROLLBACK, FAILED (THRESHOLD_BREACHED), stop

    BEGIN TRANSACTION (EPAS)
        stage + merge good rows (version + row-hash guard)
        write quarantine items
    COMMIT
    write_back(bronze, status_per_doc, filter = {_id, sourceVersion = readVersion})   # optimistic
    reconcile(read = success + partial + failed + quarantined + skipped)              # strict
    status = resolve_job_status(counts)                                               # §4
```

### 3.3 Safety-net sweep ➕

> **In simple words:** a nightly "double check" that no Bronze document has slipped through the cracks.

Once a day, a maintenance step counts Bronze documents where `_etl.sourceVersion > _etl.syncSilver.syncedVersion` **and** the status is `SUCCESS`. That should be impossible (a real change resets the status to `PENDING`). If any are found, they are set back to `PENDING` and an alert is raised, because something (a bug, a manual edit) broke the rule. In a healthy system the count is always 0.

### 3.4 Silver → Gold incremental (from doc 04)

```text
function incremental_s2g(job):
    for each source table T of the job:
        bookmark[T] = watermark(job, T)            # NULL on the very first run → read everything
        cutoff[T]   = max(_etl_change_seq) of T     # read once at the start
    keys = union over T of:
              rows of T with bookmark[T] < _etl_change_seq <= cutoff[T]
              expanded to the fact grain for SECONDARY tables (doc 04 §7.2)
           ∪ entries in gold_record_status for this job that are due for retry
    rows = read current Silver rows for those keys
    ... dq → dedupe → measures → stage ...
    BEGIN TRANSACTION
        inferred members, merge (SK lookups "as of"), touched partitions,
        gold_record_status (upsert failures, delete successes),
        Silver summary status, bookmarks := cutoff     # all-or-nothing
    COMMIT
```

### 3.5 Retry backoff for FAILED records ➕

> **In simple words:** if a record failed because its customer has not arrived yet, retrying every hour for days wastes effort. We wait a little longer after each failure: 0 h, 1 h, 4 h, 12 h, 24 h … After `maxRetries`, it goes to quarantine (`RETRIES_EXHAUSTED`).

| Attempt (`retryCount`) | Wait before the next try (default) |
|---|---|
| 1 | next run |
| 2 | 1 hour |
| 3 | 4 hours |
| 4 | 12 hours |
| 5 | 24 hours |
| ≥ `maxRetries` (5) | → `QUARANTINED`, `RETRIES_EXHAUSTED` |

New field: `_etl.syncSilver.nextRetryAt` (Bronze) and `next_retry_at` (`etl_ctl.gold_record_status`), set at write-back. The backoff list is configurable per job (`retry.backoff: [0h, 1h, 4h, 12h, 24h]`). `RERUN_FAILED_RECORDS` ignores the backoff on purpose.

### 3.6 Late-arriving and re-processed records

| Case | What happens |
|---|---|
| A record arrives **late** in Bronze (offline device, slow source) | It arrives as `PENDING` whenever it comes; the status-based selection picks it up in the next run. Its old event date is kept, so Gold puts it in the correct period and recomputes that period's aggregates (doc 04 §7.5) |
| A record arrives **out of order** (older version after newer) | The ingestion version guard ignores the older version (doc 02 §8) |
| A record is **re-processed** (rerun, backfill) | Merge guards: same data → "unchanged"; newer → update; older → ignored |
| A parent arrives **after** its child | The child fails with `REF_PARENT_MISSING` (retryable) → retried with backoff → succeeds when the parent arrives, or after the retry window is loaded with a warning / quarantined (doc 03 §6.2) |

### 3.7 Switching from full load to incremental, and resetting the watermark

| Situation | Bronze → Silver | Silver → Gold |
|---|---|---|
| **Brand-new job** | Bronze documents are all `PENDING` already, so the first incremental run **is** a full load. ➕ The ops UI still recommends a first `FULL_LOAD` with a dry-run before it | No bookmark yet (`NULL`) → the first incremental run reads everything. Same effect |
| **After a FULL_LOAD** | Every document got a status written back (`SUCCESS` …). The next incremental run picks only new `PENDING` ones. Nothing to "switch" | The full load sets the bookmark to its cutoff. The next incremental run continues from there |
| **Reset the watermark** (emergency: e.g. Gold data was lost after the bookmark moved) | Not needed: use `REPLAY` / `BACKFILL` | `PLATFORM_ADMIN` calls `POST /jobs/{job}/watermarks/reset` with a target value, reason and ticket. The old value goes to `watermark_history`. The next incremental run re-reads from there (idempotent thanks to the guards) |
| ➕ **Guard** | — | A normal run can never move a bookmark **backwards** (doc 05 §6.8). Only the reset API can, with an audit trail |

---

## 4. Job Status Rules

### 4.1 The five possible results

> **In simple words**
> - **SUCCESS**: everything went fine (or so few problems that they are within an allowed tolerance).
> - **PARTIAL**: the job finished and saved the good data, but **some records had problems**. They are counted, saved for retry or repair, and nothing is lost.
> - **FAILED**: the job could not finish properly (broken infrastructure, too many errors, counts don't tally). **Nothing was saved** (rolled back).
> - **SKIPPED**: the job didn't run on purpose (already running, disabled, upstream not ready).
> - **CANCELLED**: someone stopped it.

### 4.2 State machine

```
                         ┌────────────────────── cancel ───────────────────────┐
                         │                                                     ▼
   request ──► ┌─────────┴──┐   lock taken, checks OK   ┌─────────┐       ┌───────────┐
               │   QUEUED   │ ────────────────────────► │ RUNNING │──────►│ CANCELLED │
               └─────┬──────┘                           └────┬────┘cancel └───────────┘
                     │ lock held by another run /            │
                     │ job disabled / upstream policy SKIP    │  steps finished
                     ▼                                        ▼
               ┌───────────┐                   ┌──────────────────────────────┐
               │  SKIPPED  │                   │ resolve status (table §4.3)  │
               └───────────┘                   └──────┬────────────┬──────────┘
                                                      │            │
                     ┌────────────────────────────────┘            └─────────────────┐
                     ▼                              ▼                                ▼
               ┌───────────┐                 ┌───────────┐                     ┌───────────┐
               │  SUCCESS  │                 │  PARTIAL  │                     │  FAILED   │
               └───────────┘                 └─────┬─────┘                     └─────┬─────┘
                                                   │ RERUN_FAILED_RECORDS            │ RERUN_JOB / Airflow retry
                                                   ▼ (new run, parent = this)        ▼ (new run, parent = this)
                                                  ...                               ...

   ➕ ZOMBIE check (maintenance, every 5 min): RUNNING + lock expired (no heartbeat for 10 min)
      → FAILED with error_code INFRA_UNKNOWN, failure_reason "abandoned: driver lost"
```

**Allowed transitions:** `QUEUED → RUNNING | SKIPPED | CANCELLED`; `RUNNING → SUCCESS | PARTIAL | FAILED | CANCELLED`. Final states never change. A rerun is always a **new** row.

### 4.3 Status decision table (first matching row wins)

| # | Condition | Status | `error_code` | Committed? |
|---|---|---|---|---|
| 1 | Cancelled by a user / Airflow | `CANCELLED` | — | No (rolled back if mid-run) |
| 2 | Lock held by another live run | `SKIPPED` | `LOCK_NOT_ACQUIRED` | — |
| 3 | Job disabled in the registry | `SKIPPED` | — (reason text) | — |
| 4 | HARD upstream failed and policy = `SKIP`, or `WAIT` timed out (§7) | `SKIPPED` | `DEPENDENCY_FAILED` | — |
| 5 | Config invalid | `FAILED` | `CONFIG_INVALID` | No |
| 6 | Infrastructure exception (DB, Mongo, Spark, timeout) | `FAILED` | `INFRA_*` (classified) | No (rolled back) |
| 7 | Bad records above `maxFailedPct` **or** `maxFailedCount` (circuit breaker) | `FAILED` | `THRESHOLD_BREACHED` | No (rolled back, no write-back) |
| 8 | Reconciliation mismatch (strict checks) | `FAILED` | `RECONCILIATION_MISMATCH` | Already committed ⚠ → alert + investigation (it means a framework bug) |
| 9 | SCD integrity check failed | `FAILED` | `SCD_INTEGRITY_VIOLATION` | Same as 8 |
| 10 | Bad records = 0 | `SUCCESS` | — | Yes |
| 11 | Bad records ≤ `successTolerancePct` **and** ≤ `successToleranceCount` | `SUCCESS` (with `has_record_errors = true`) | top record error code | Yes |
| 12 | Otherwise (bad records > tolerance but ≤ max thresholds) | `PARTIAL` | top record error code | Yes |

**Definitions:**
- **Bad records** = `FAILED + QUARANTINED + PARTIAL` (counted per **source record**: one Bronze document or one Silver driving row).
- **Not bad:** `SKIPPED` (filtered/duplicate on purpose) and `WARN`-only records.
- **Top record error code** ➕ = the most frequent record-level error code of the run (e.g. `REF_PARENT_MISSING`), so the run list shows *why* at a glance. The full breakdown is in `dq_results`.

> Rows 8–9 happen **after** the commit (the checks need the committed data). Their data is not wrong in a normal way, but the framework's own bookkeeping disagreed, so a human must look. They should be extremely rare.

### 4.4 Configurable thresholds (per job)

```yaml
# per job (YAML); defaults in job-defaults.yml; overridable per environment
thresholds:
  successTolerancePct:   0        # ➕ default 0 → even ONE bad record makes the run PARTIAL
  successToleranceCount: 0
  maxFailedPct:          2.0      # above → FAILED (circuit breaker, rollback)
  maxFailedCount:        5000
  minRecordsForPct:      100      # below this many records, only the count limits apply
  maxQuarantinedPct:     1.0      # ➕ optional, stricter limit for data errors
  maxUnknownSkPct:       0.5      # Gold facts only (doc 04)
  onBreach:              ROLLBACK # ROLLBACK | COMMIT_AND_FAIL
```

**Should thresholds be per job, by count and/or %?** **Both, per job:**

| Why % | Why count | Why `minRecordsForPct` |
|---|---|---|
| A big job (2 million records) with 1,000 bad ones = 0.05%: fine | A small reference job with 3 bad records out of 10 = 30%: the % alone would scream | Prevents "1 of 3 = 33%" panics on tiny runs |

Different jobs have different risk: finance facts get strict limits (`maxFailedPct: 0.1`), a clickstream job can be loose (`maxFailedPct: 5`).

### 4.5 How status and reason are stored

| Where | What |
|---|---|
| `job_run_audit.status` | One of the 7 statuses |
| `job_run_audit.error_code` | The **main** reason, always a code from `error_code_catalog` (for PARTIAL: the top record error code) |
| `job_run_audit.failure_reason` | Short human text, no PII: "EPAS connection refused after 3 attempts" |
| `job_run_audit.records_*` | All counts (with the CHECK on the equation, doc 05) |
| `dq_results` | Per rule and table: checked / failed |
| `reconciliation` | Every tally check |
| `record_error_log` | Per record (sampled when huge) |

**Examples of the reason mapping:**

| What happened | status | error_code |
|---|---|---|
| Mandatory field empty in 12 of 50,000 records | PARTIAL | `DQ_NOT_NULL` |
| Source changed `amount` from number to text → 60% fail | FAILED | `THRESHOLD_BREACHED` (dq_results show `DQ_TYPE_CAST`) |
| EPAS down | FAILED | `INFRA_DB_UNAVAILABLE` |
| Merge took longer than the statement timeout | FAILED | `INFRA_TIMEOUT` |
| Customer job failed, policy SKIP | SKIPPED | `DEPENDENCY_FAILED` |
| Same job already running | SKIPPED | `LOCK_NOT_ACQUIRED` |

### 4.6 Status resolver (Java)

```java
// etl-core: pipeline/JobStatusResolver.java
public final class JobStatusResolver {

    public static JobResult resolve(RunContext ctx, PipelineState s) {
        RunCounts c = s.counts();
        Thresholds t = ctx.config().thresholds();
        long bad = c.failed() + c.quarantined() + c.partial();
        long base = c.read() - c.skipped();
        double badPct = base == 0 ? 0 : bad * 100.0 / base;

        if (bad == 0) return JobResult.success(ctx, c);
        boolean withinTolerance = bad <= t.successToleranceCount()
                && (base < t.minRecordsForPct() || badPct <= t.successTolerancePct());
        ErrorCode top = s.topRecordErrorCode();                       // most frequent record-level code
        return withinTolerance ? JobResult.successWithRecordErrors(ctx, c, top)
                               : JobResult.partial(ctx, c, top);
        // FAILED cases never reach here: they are exceptions thrown earlier
        // (ThresholdBreachedException in ClassifyStep, InfraException, ReconciliationMismatchException ...)
    }
}
```

---

## 5. Every Single Failed Record Is Counted

> **In simple words:** the platform must be able to say "this run read **1,000** records: **975** OK, **3** partial, **10** failed, **7** quarantined, **5** skipped", and those numbers must **always** add up to 1,000. Not "about 1,000".

| Mechanism | What it guarantees |
|---|---|
| One status per source record, computed from Spark **aggregations** (never accumulators, doc 03 §11) | Exact counts even when Spark retries tasks |
| `ck_run_counts` CHECK constraint in `job_run_audit` (doc 05) | A finished run whose counts don't add up **cannot be saved** |
| Strict reconciliation equation `read = success + partial + failed + quarantined + skipped` | A mismatch fails the run (`RECONCILIATION_MISMATCH`) |
| Dedupe losers and filtered records are `SKIPPED` **with a reason**, never silently dropped | "Missing" records are always explainable |
| Record-level counts (documents) **and** row-level counts (rows per target table) are kept separately | One document with 3 items = 1 record, 4 rows. Both are visible |
| `successTolerancePct = 0` by default | One bad record → `PARTIAL` (visible), unless the job owner deliberately allows a tolerance |
| Write-back `matched` vs `stale` reported | Even the "document changed during the run" case is counted |

---

## 6. Failed and Sensitive (PII) Data

### 6.1 Flag in the previous layer, quarantine table, or both?

> **In simple words:** we use **both**, but for different purposes:
> - The **status flag** on the record (Bronze `syncSilver`, Silver `_sync_gold_*`) is for the **machine**: "don't treat this as done; retry or wait for a fix".
> - The **quarantine item** is for **people**: a to-do list with what's wrong, who works on it and its approval state.
> - The **actual data stays where it is** (Bronze document / Silver row). The quarantine item only points to it and keeps a **masked** copy. That way sensitive data is **not copied** into yet another place.

| Option | Problem | Verdict |
|---|---|---|
| Only a flag in Bronze/Silver | No workflow, no assignment, no approval, hard to search across 100 jobs | ❌ |
| Only a quarantine table with a full data copy | Copies PII into another table (more places to protect, purge and audit); the pipeline can't see the state on the record | ❌ |
| **Both: flag on the record + quarantine item with pointer + masked snapshot** | — | ✅ **Chosen** |

### 6.2 Quarantine → fix → reprocess flow

```
 ┌──────────────────────────┐
 │ Job run finds a data     │  severity QUARANTINE, non-retryable error,
 │ error (doc 03 §5.2)      │  or RETRIES_EXHAUSTED
 └────────────┬─────────────┘
              ▼
 ┌──────────────────────────┐   Bronze/Silver record: status = QUARANTINED, quarantineRef = id
 │ quarantine_record created│   quarantine item: pointer + errors + MASKED snapshot
 │ status = QUARANTINED     │   alert to the job owner (grouped per error code, not per record) ➕
 └────────────┬─────────────┘
              ▼
 ┌──────────────────────────────────────────────────────────────────────────────────────┐
 │ TRIAGE in the ops UI (data steward / operator)                                       │
 │   items grouped by job + error_code + rule ("312 items: SO-002 DQ_RANGE total_amount")│
 └───────┬──────────────────┬───────────────────────┬──────────────────────┬────────────┘
         │                  │                       │                      │
         ▼                  ▼                       ▼                      ▼
 ┌───────────────┐  ┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐
 │ SOURCE_FIX    │  │ CODE_FIX /       │   │ CORRECTION_      │   │ REJECT           │
 │ fix it in the │  │ RULE_CHANGE      │   │ OVERRIDE ➕       │   │ (test/garbage    │
 │ source app    │  │ deploy fix via   │   │ enter correct    │   │  data), reason   │
 │               │  │ Git, then mark   │   │ values in the UI │   │ required         │
 │               │  │ FIXED (bulk ok)  │   │ (patch)          │   │                  │
 └──────┬────────┘  └────────┬─────────┘   └────────┬─────────┘   └────────┬─────────┘
        │ CDC brings a new   │                      │                      ▼
        │ version → PENDING  ▼                      ▼                 ┌───────────┐
        │             ┌──────────────────────────────────┐            │ REJECTED  │ ✔
        │             │ status = FIXED                    │            └───────────┘
        │             └─────────────────┬────────────────┘
        │                               ▼
        │             ┌──────────────────────────────────┐  prod: approver ≠ fixer (four-eyes)
        │             │ APPROVE → READY_TO_REPROCESS      │  dev/qa: automatic
        │             └─────────────────┬────────────────┘
        │                               ▼
        │             ┌──────────────────────────────────┐  manual, or automatic every N hours
        │             │ REPROCESS_QUARANTINE run          │  when items are ready
        │             └───────┬───────────────────┬──────┘
        │                     ▼                   ▼
        │              ┌─────────────┐   ┌────────────────────────────────┐
        │              │ REPROCESSED │✔  │ fails again → NEW quarantine   │
        │              └─────────────┘   │ item (previous_quarantine_id)  │
        │                                └────────────────────────────────┘
        ▼
 ┌──────────────────────────────────┐
 │ next incremental run processes   │  old quarantine item → SUPERSEDED ✔ (automatic)
 │ the new version                  │
 └──────────────────────────────────┘
```

| Fix type | Best for | Note |
|---|---|---|
| `SOURCE_FIX` | Wrong data entered in the source app | **Preferred.** Source and warehouse stay identical. Needs no reprocess run: CDC does it |
| `CODE_FIX` | Our transformation had a bug | Deploy, then bulk-mark the affected items `FIXED` (filter by error code/rule/date) |
| `RULE_CHANGE` | The DQ rule was too strict | Change the rule in Git, deploy, bulk-mark `FIXED` |
| `CORRECTION_OVERRIDE` | Source can't be fixed (closed system, historic data) | Use rarely: Silver then differs from the source. Always approval + reason. The patch is applied on top of the source data during the reprocess run, and recorded in the audit |

### 6.3 Automatic vs manual

| Error kind | Handling | Human needed? |
|---|---|---|
| Retryable (`REF_PARENT_MISSING`, …) | `FAILED` → automatic retries with backoff (§3.5) | Only after `maxRetries` (then it's quarantined) |
| Non-retryable data error | `QUARANTINED` immediately | Yes (or automatic `SUPERSEDED` when the source sends a new version) |
| Infra error | Whole run `FAILED` → Airflow retries the job (§8.2) | Only if retries keep failing |

### 6.4 PII masking, encryption and who can see what

**Masking of the quarantine snapshot** (rules come from the PII flags in the data contract, doc 02 §12):

| Data kind | Mask | Example |
|---|---|---|
| Email | keep the first letter + domain | `a***@gmail.com` |
| Phone | keep the last 2 digits | `******12` |
| Name | initials | `A. S.` |
| Address | city only | `Ahmedabad` |
| ID numbers (PAN, Aadhaar, passport) | **never stored** in the snapshot, only "present/absent" | `<present>` |
| Amounts, dates, codes (non-PII) | unchanged (they are what the fixer usually needs) | `1499.00` |

**Seeing the real value ("reveal") ➕:** a person with the `PII_VIEWER` role can press "reveal" for one item. The ops API reads the **real value from the source layer** (Bronze/Silver) on demand, shows it once, and writes an audit entry (who, which item, when, why). The real value is never stored in the quarantine table.

**Encryption:**

| Layer | How |
|---|---|
| Data on disk (EPAS) | EPAS TDE (Transparent Data Encryption, available in recent EPAS versions) or encrypted disks/volumes, with keys in a KMS (Key Management Service) / Vault |
| Data on disk (MongoDB) | MongoDB encryption at rest (edition-dependent) or encrypted volumes |
| Very sensitive columns ➕ | Column-level encryption (`pgcrypto`) for fields like ID numbers in Silver, decrypted only by roles that need them |
| Data in transit | TLS (Transport Layer Security) on every connection: Kafka, Mongo, EPAS, APIs |
| Logs and error messages | Never contain field values (doc 06 §13) |

**Who sees what:**

| Role | Quarantine list | Masked snapshot | Real values | Fix/approve |
|---|---|---|---|---|
| VIEWER | counts only | ❌ | ❌ | ❌ |
| OPERATOR | ✅ | ✅ | ❌ | fix (technical) |
| DATA_STEWARD | ✅ | ✅ | ❌ | fix + approve + reject |
| PII_VIEWER (named people) | ✅ | ✅ | ✅ (audited reveal) | per other role |
| BI users | ❌ | ❌ | ❌ | ❌ |

---

## 7. Job Dependencies: Run, Skip or Wait?

> **In simple words**
> Imagine the sales report waits for the customer list. If the customer job failed, should the sales job:
> - **wait** a bit (maybe the customer job is being retried),
> - **skip** this time (try at the next schedule),
> - or **run anyway** (with what it has)?
>
> Good news: thanks to the bookmarks and statuses, running anyway **never corrupts data**. The worst case is slightly stale data, or temporary placeholder ("inferred") customers. So the policy is mainly about **avoiding wasted work and noise**, not about safety.

### 7.1 Decision matrix

| Upstream status | `HARD` dependency (default) | `SOFT` dependency |
|---|---|---|
| `SUCCESS` | Run | Run |
| `PARTIAL` | **Run** (good data was committed; the failed part will follow later). Option `requireUpstream: SUCCESS_ONLY` for strict cases (e.g. finance month-end) | Run |
| `FAILED` | `on_upstream_failure` (per dependency): **`WAIT`** (default) up to `max_wait_minutes` (default 60) for a successful retry → then **SKIPPED** (`DEPENDENCY_FAILED`) | Run (with a warning) |
| `SKIPPED` (e.g. lock held) | Treated as "not finished yet" → `WAIT` | Run |
| `RUNNING` | Wait until it finishes (Airflow sensor) | Run |
| `CANCELLED` | Same as `FAILED` | Run |

### 7.2 Why "SKIPPED" and not "FAILED" when an upstream fails ➕

If one Bronze → Silver job fails and 15 downstream jobs then also report `FAILED`, the on-call person gets **16 alerts for one problem**. Marking the downstream runs `SKIPPED (DEPENDENCY_FAILED)` means:
- one **root-cause alert** (the upstream failure), plus a list of "blocked" jobs in the same alert,
- downstream jobs run normally at the next schedule (or via cascade) once the upstream is fixed.

The policy is still configurable: set `on_upstream_failure: RUN` for jobs that must always run (e.g. a dashboard refresh that should show "data as of yesterday" rather than nothing).

---

## 8. Concurrency, Retries, Timeouts and SLA

### 8.1 Concurrency control

| Situation | Protection |
|---|---|
| Same job twice at the same time (schedule + manual click) | `job_lock` (doc 05 §6.5). The second run is `SKIPPED (LOCK_NOT_ACQUIRED)`. ➕ For manual requests the ops API offers **"queue after the current run"** instead of skipping |
| Two jobs writing the same table | Impossible by design: the single-owner rule (`job_target` unique index, doc 05) |
| A Gold job reading a Silver table while the Silver job writes it | Safe: PostgreSQL MVCC (Multi-Version Concurrency Control) gives the reader a consistent snapshot, and the bookmark cutoff fixes what it reads |
| A fact job inserting inferred members while the dimension job merges | `ON CONFLICT` + a short advisory lock per dimension (doc 04 §6.6) |
| Too many jobs at once overloading EPAS or the Spark cluster | ➕ Airflow **pools** (e.g. `epas_writers: 6 slots`, `spark_cluster: 10 slots`) and priority weights (Prompt 09) |
| A double click on "start run" | ➕ `Idempotency-Key` header in the API (§10): the same key returns the same run instead of creating a second one |

### 8.2 Retries: four levels

> **In simple words:** we retry at the **smallest level that can fix the problem**: a single database statement first, then a Spark task, then the whole job. Bad **data** is never "retried" by repeating the job; it is retried per record with backoff, or quarantined.

| Level | What is retried | Policy (default) | Configured in |
|---|---|---|---|
| 1. Statement | One SQL statement or Mongo bulk write that hit a **transient** error: deadlock (`40P01`), serialization failure (`40001`), connection reset, Mongo "not primary" | 3 attempts, 1 s → 4 s → 16 s | framework constant, per env |
| 2. Spark task | One task of a Spark stage (executor lost, fetch failure) | `spark.task.maxFailures = 4` | Spark config |
| 3. Job | The whole run, by Airflow | Only for exit code **30** (retryable FAILED); **not** for 40 (config/threshold/reconciliation). 3 retries, 5 min → 15 min → 45 min | Airflow task (Prompt 09) |
| 4. Record | One source record with a retryable data error | Backoff list (§3.5), `maxRetries = 5` → `QUARANTINED` | job YAML |

### 8.3 Timeouts

| Timeout | Default | Why |
|---|---|---|
| Airflow `execution_timeout` per task | 3 × `maxDurationMinutes` of the job | Kill truly stuck runs |
| EPAS `statement_timeout` (merge session) | 30 min | A runaway merge can't block the database forever |
| EPAS `lock_timeout` | 30 s | Don't wait endlessly for a table lock; fail fast and retry (level 1) |
| EPAS `idle_in_transaction_session_timeout` | 10 min | A crashed driver can't hold a transaction open |
| Mongo `maxTimeMS` (reads) | 10 min per cursor batch | Same idea for MongoDB |
| ➕ Progress watchdog in the driver | No step progress for 30 min → `INFRA_TIMEOUT` | Catches hangs that no single timeout sees |
| Lock lease | 10 min, heartbeat 60 s | Crashed runs release their lock automatically |

### 8.4 SLA (Service Level Agreement)

> **In simple words:** two different promises:
> 1. **Duration SLA:** "this job finishes within 20 minutes".
> 2. **Freshness SLA:** "the sales dashboard never shows data older than 2 hours" or "yesterday's data is ready by 08:00".
> The second one is what business users actually care about.

```yaml
# job YAML
sla:
  maxDurationMinutes: 20          # duration SLA → alert if a run takes longer
  freshnessMinutes: 120           # freshness SLA → alert if the newest committed data is older than this
  readyBy: "0 8 * * *"            # optional: "data of yesterday complete by 08:00 every day"
```

| SLA | Measured as | Alert |
|---|---|---|
| Duration | `job_run_audit.duration_ms` | Run longer than `maxDurationMinutes` (warning while still running at 100%, alert at 150%) |
| Freshness | `now() − (event time of the last committed source change)`; for Gold: the bookmark's change time | Above `freshnessMinutes` |
| Ready-by | The last successful run covering yesterday finished before the time | Missed → alert to the job owner + the business contact |

This **replaces** the single `sla_minutes` column of doc 05 with three values (`sla_max_duration_minutes`, `sla_freshness_minutes`, `sla_ready_by`), see §12.

---

## 9. Idempotency: Safe to Re-Run Anything

> **In simple words:** **idempotent** means "doing it twice has the same result as doing it once". Like pressing an elevator button many times: the elevator still comes once. Every job in every run mode must behave like that.

### 9.1 The mechanisms, layer by layer

| Layer / step | Mechanism | Doc |
|---|---|---|
| Kafka → Bronze | Version guard on `clusterTime`; upsert by `_id`; hash check for no-op updates | 02 |
| Bronze → Silver merge | `INSERT … ON CONFLICT` with `_src_version` + `_row_hash` guard | 03 |
| Child tables | Same guard + orphan handling only for parents applied in this run | 03 |
| Bronze write-back | Conditional update on `{_id, sourceVersion = readVersion}` | 03 |
| Silver → Gold | Change numbers + bookmarks + retry list; bookmark moved in the same transaction | 04 |
| Dimensions | Surrogate keys never change; SCD actions computed against the current state | 04 |
| Facts | Grain key upsert + row-hash guard; partition-move delete | 04 |
| Aggregates / snapshots | Delete + insert per touched date | 04 |
| Staging | Truncated at the start of each run | 05 |
| Run level | Lock (one run per job), unique `run_id`, lost-lock check before commit | 05 |
| API | `Idempotency-Key` for manual requests | §10 |

### 9.2 "What if it crashes right here?" ➕

| Crash point | State after the crash | What the next run does | Result |
|---|---|---|---|
| During read / transform | Nothing written | Reads the same records again | ✅ correct |
| During staging load | Staging partly filled | Truncates staging at start | ✅ correct |
| During the merge transaction | Transaction rolled back by EPAS | Same as above | ✅ correct |
| **After the Silver commit, before the Bronze write-back** | Silver has the data; Bronze still says `PENDING` | Processes them again → merge says "unchanged" → writes back `SUCCESS` | ✅ correct (a little extra work) |
| During the Bronze write-back | Some documents updated, some not | Not-updated ones processed again → "unchanged" | ✅ correct |
| After the write-back, before the audit row is finished | Data and statuses fine; the run looks `RUNNING` | Zombie check marks the run `FAILED (abandoned)`; the next run finds nothing pending | ✅ correct (audit says FAILED although data is fine: the reconciliation of the next run confirms it) |
| Gold: after the commit | Data + bookmark + statuses together (one transaction) | Continues from the new bookmark | ✅ correct |

The rule that makes this table possible: **commit the data and the bookmark together; write to other systems (Bronze/Mongo) only after the commit, and only in an idempotent way.**

---

## 10. Rerun API Design (`etl-ops-api`)

### 10.1 How a manual run request flows

```
 Operator (ops UI)               etl-ops-api                         Airflow                     Spark job
       │  POST /runs                  │                                 │                            │
       │ ───────────────────────────► │ 1. auth + role check            │                            │
       │                              │ 2. validate params, job enabled  │                            │
       │                              │ 3. Idempotency-Key seen? → return same request
       │                              │ 4. estimate size (optional dry count)
       │                              │ 5. needs approval? → run_request PENDING_APPROVAL ──► notify approver
       │  202 {requestId, status}     │                                 │                            │
       │ ◄─────────────────────────── │                                 │                            │
       │                              │ (after approval)                │                            │
       │                              │ 6. create run_id, job_run_audit QUEUED
       │                              │ 7. POST /api/v1/dags/{dag}/dagRuns {conf: run params}
       │                              │ ──────────────────────────────► │ 8. task: spark-submit       │
       │                              │                                 │    --run-id ... --mode ... │
       │                              │                                 │ ─────────────────────────► │ 9. RUNNING → result
       │  GET /runs/{runId}           │ ◄──────────── reads job_run_audit ──────────────────────────  │
       │ ───────────────────────────► │                                 │                            │
```

### 10.2 New table: `run_request` ➕

> **In simple words:** a request that is waiting for approval is **not yet a run**. Keeping requests separate keeps `job_run_audit` clean (only real runs) and gives approvals their own audit trail.

```sql
-- db/control/V19__run_request.sql
CREATE TABLE etl_ctl.run_request (
    request_id        uuid          PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key   varchar(100)  UNIQUE,                    -- from the Idempotency-Key header
    job_name          varchar(100)  NOT NULL REFERENCES etl_ctl.job_registry (job_name),
    run_mode          varchar(30)   NOT NULL,
    is_dry_run        boolean       NOT NULL DEFAULT false,
    run_params        jsonb         NOT NULL DEFAULT '{}',
    parent_run_id     varchar(100)  REFERENCES etl_ctl.job_run_audit (run_id),
    cascade           jsonb,                                   -- {"enabled":true,"modes":{"s2g_dim_x":"FULL_LOAD"}}
    reason            varchar(1000) NOT NULL,
    request_ticket    varchar(50),
    requested_by      varchar(100)  NOT NULL,
    requested_at      timestamptz   NOT NULL DEFAULT now(),
    approval_status   varchar(20)   NOT NULL CHECK (approval_status IN
                        ('NOT_REQUIRED','PENDING_APPROVAL','APPROVED','REJECTED','EXPIRED')),
    approved_by       varchar(100),
    approved_at       timestamptz,
    reject_reason     varchar(1000),
    estimated_records bigint,                                  -- from the optional preview
    run_id            varchar(100)  REFERENCES etl_ctl.job_run_audit (run_id),   -- set when launched
    CONSTRAINT ck_four_eyes CHECK (approved_by IS NULL OR approved_by <> requested_by)
);
CREATE INDEX ix_run_request_pending ON etl_ctl.run_request (approval_status) WHERE approval_status = 'PENDING_APPROVAL';
```

### 10.3 Endpoints

**Runs**

| Method & path | Purpose | Role |
|---|---|---|
| `POST /api/v1/runs` | Request a run (any mode, dry-run, cascade) | OPERATOR+ (mode-dependent, §2.13) |
| `POST /api/v1/runs/preview` | ➕ Estimate how many records a request would touch (no run) | OPERATOR+ |
| `GET /api/v1/runs?job=&status=&mode=&from=&to=` | List runs | VIEWER+ |
| `GET /api/v1/runs/{runId}` | One run: status, counts, error code, links (Spark UI, Airflow, DQ results) | VIEWER+ |
| `GET /api/v1/runs/{runId}/chain` | The rerun chain (root, parents, children, cascade runs) | VIEWER+ |
| `POST /api/v1/runs/{runId}/rerun` | Shortcut: `{"mode":"RERUN_JOB"}` or `{"mode":"RERUN_FAILED_RECORDS"}` with `parent_run_id` filled in | OPERATOR+ |
| `POST /api/v1/runs/{runId}/cancel` | Cancel a queued/running run | OPERATOR+ |
| `GET /api/v1/run-requests?status=PENDING_APPROVAL` | Approval inbox | OPERATOR+ |
| `POST /api/v1/run-requests/{id}/approve` · `/reject` | Approve / reject (four-eyes) | OPERATOR+ (not the requester) |

**Jobs and bookmarks**

| Method & path | Purpose | Role |
|---|---|---|
| `GET /api/v1/jobs` · `/jobs/{job}` | Registry view, last run, SLA state | VIEWER+ |
| `GET /api/v1/jobs/{job}/downstream` | Cascade preview (which jobs, in which order) | VIEWER+ |
| `GET /api/v1/jobs/{job}/watermarks` | Current bookmarks + history | VIEWER+ |
| `POST /api/v1/jobs/{job}/watermarks/reset` | Move a bookmark (reason + ticket, approval in prod) | PLATFORM_ADMIN |
| `POST /api/v1/jobs/{job}/lock/release` | Emergency lock release (only if the holder is dead) | PLATFORM_ADMIN |

**Quarantine**

| Method & path | Purpose | Role |
|---|---|---|
| `GET /api/v1/quarantine?job=&errorCode=&status=&ruleId=` | List / group items | OPERATOR+ |
| `GET /api/v1/quarantine/{id}` | One item (masked snapshot, errors, history) | OPERATOR+ |
| `POST /api/v1/quarantine/{id}/reveal` | Show real values once (audited) | PII_VIEWER |
| `POST /api/v1/quarantine/{id}/fix` | `{fixType, correctionPatch?, note}` → FIXED | OPERATOR / DATA_STEWARD |
| `POST /api/v1/quarantine/{id}/approve` · `/reject` | → READY_TO_REPROCESS / REJECTED | DATA_STEWARD (≠ fixer in prod) |
| `POST /api/v1/quarantine/bulk` | `{filter: {job, errorCode, ruleId, createdBetween}, action: FIX\|APPROVE\|REJECT, fixType, note}` | DATA_STEWARD |
| `POST /api/v1/quarantine/reprocess` | `{jobName}` → a `REPROCESS_QUARANTINE` run request | OPERATOR / DATA_STEWARD |

**Ingestion DLQ** (doc 02): `GET /api/v1/dlq`, `POST /api/v1/dlq/replay` (filter + ticket), `POST /api/v1/dlq/ignore`.

### 10.4 Request / response examples

```http
POST /api/v1/runs
Idempotency-Key: 6c1e2f0a-ops-ui-asha-0930
Content-Type: application/json

{
  "jobName": "b2s_order",
  "runMode": "BACKFILL",
  "dryRun": false,
  "params": { "from": "2026-10-01T00:00:00Z", "to": "2026-10-08T00:00:00Z" },
  "reason": "order_status_map was wrong for 1–8 Oct, fixed in release 1.14.3",
  "ticket": "INC-1042",
  "cascade": { "enabled": true }
}
```

```http
HTTP/1.1 202 Accepted
{
  "requestId": "0b6f7c2e-5d1a-4c55-9a1e-2f1f0d8b6a11",
  "approvalStatus": "PENDING_APPROVAL",
  "estimatedRecords": 184220,
  "cascadePreview": ["s2g_fact_sales_line", "s2g_fact_order_fulfillment", "s2g_agg_sales_daily_store"],
  "links": { "self": "/api/v1/run-requests/0b6f7c2e-5d1a-4c55-9a1e-2f1f0d8b6a11" }
}
```

| HTTP status | When |
|---|---|
| `202 Accepted` | Request accepted (queued or waiting for approval) |
| `200 OK` | Same `Idempotency-Key` seen before → returns the existing request |
| `400 Bad Request` | Invalid parameters (e.g. BACKFILL without `to`, `from` after `to`) |
| `403 Forbidden` | Role not allowed for this mode/environment |
| `409 Conflict` | Job disabled, or a conflicting request is already pending |
| `422 Unprocessable Entity` | Too big (e.g. more than 100,000 ids) → suggests a backfill instead |

### 10.5 Controller skeleton (Spring Boot)

```java
// etl-ops-api: run/RunController.java
@RestController
@RequestMapping("/api/v1/runs")
@RequiredArgsConstructor
class RunController {

    private final RunRequestService service;

    @PostMapping
    ResponseEntity<RunRequestResponse> request(@RequestHeader(value = "Idempotency-Key", required = false) String key,
                                               @Valid @RequestBody RunRequestDto body,
                                               @AuthenticationPrincipal EtlUser user) {
        RunRequestResult r = service.submit(key, body, user);       // role check, validation, approval, launch
        return ResponseEntity.status(r.isExisting() ? HttpStatus.OK : HttpStatus.ACCEPTED).body(r.toResponse());
    }

    @PostMapping("/{runId}/rerun")
    ResponseEntity<RunRequestResponse> rerun(@PathVariable String runId, @Valid @RequestBody RerunDto body,
                                             @AuthenticationPrincipal EtlUser user) {
        return ResponseEntity.accepted().body(service.rerun(runId, body.mode(), body.reason(), user).toResponse());
    }

    @PostMapping("/{runId}/cancel")
    ResponseEntity<Void> cancel(@PathVariable String runId, @AuthenticationPrincipal EtlUser user) {
        service.cancel(runId, user);                                 // Airflow "mark failed" + audit CANCELLED
        return ResponseEntity.accepted().build();
    }
}

// etl-ops-api: run/RunRequestService.java (core of submit)
RunRequestResult submit(String key, RunRequestDto dto, EtlUser user) {
    if (key != null) { var existing = requests.findByIdempotencyKey(key); if (existing.isPresent()) return existing.get().asExisting(); }
    JobInfo job = registry.require(dto.jobName());                                  // 409 if disabled
    permissions.assertAllowed(user, job, dto.runMode(), env);                        // 403
    RunParamsValidator.validate(dto.runMode(), dto.params(), limits);                // 400 / 422
    long estimate = preview.estimate(job, dto);                                      // optional dry count
    boolean needsApproval = approvalPolicy.requires(env, dto.runMode(), dto.params(), estimate);
    RunRequest req = requests.insert(key, dto, user, needsApproval ? PENDING_APPROVAL : NOT_REQUIRED, estimate);
    return needsApproval ? notifier.askApproval(req) : launcher.launch(req);         // launch = audit QUEUED + Airflow REST
}
```

---

## 11. ➕ Added by Architect (summary)

| # | Addition | Why |
|---|---|---|
| L1 | Every attempt is its own audit row (`-a2`, `-a3`), linked by parent/root | First failures are never overwritten |
| L2 | Trigger coalescing for dependency/event runs | 50 events → at most one extra run |
| L3 | Explicit cascade with preview, in dependency order, as incremental runs | Fixes reach dashboards now, without needless full loads |
| L4 | RBAC matrix per environment + four-eyes approval with expiry | Powerful modes are controlled in prod |
| L5 | Safety-net sweep (`sourceVersion > syncedVersion AND SUCCESS`) | Detects broken invariants early |
| L6 | Retry backoff with `nextRetryAt` | Less wasted work for late-arriving parents |
| L7 | `successTolerance*` defaults to 0; "top record error code" on the run | One bad record is visible; the reason is clear at a glance |
| L8 | Zombie-run detection | No runs stuck as RUNNING forever |
| L9 | Upstream failure → downstream SKIPPED (root-cause alerting) | One alert per real problem |
| L10 | Four retry levels (statement, Spark task, job, record) with clear rules | The right retry at the right level; data errors never loop |
| L11 | Database timeouts (`statement_timeout`, `lock_timeout`, idle-in-transaction) + progress watchdog | Nothing can hang forever |
| L12 | Duration, freshness and ready-by SLAs | Measures what business users care about |
| L13 | Crash-point table | Proof that every crash point is recoverable |
| L14 | `run_request` table, `Idempotency-Key`, size preview, `422` for oversized requests | Clean audit, no double runs, no accidental huge runs |
| L15 | Masking rules per data kind, audited "reveal", column encryption for ID numbers | PII never copied in clear text; access is traceable |
| L16 | Bulk quarantine actions + grouped alerts | 300 items with one cause = one action, one alert |
| L17 | Automatic periodic `REPROCESS_QUARANTINE` when items are ready | Fixed items don't wait for someone to remember |

---

## 12. Changes to Earlier Decisions

| Earlier | Now | Reason |
|---|---|---|
| Doc 02 `_etl.syncSilver` had no retry time | + `_etl.syncSilver.nextRetryAt` (date) | Retry backoff (§3.5) |
| Doc 05 `gold_record_status` | + `next_retry_at timestamptz` | Same, for Gold |
| Doc 05 `job_registry.sla_minutes` | → `sla_max_duration_minutes`, `sla_freshness_minutes`, `sla_ready_by` | Three kinds of SLA (§8.4) |
| Doc 05 had no request table | + `etl_ctl.run_request` (`V19__run_request.sql`) | Approvals and idempotent manual requests |
| Doc 03/05: upstream failure → downstream `FAILED` (implicitly) | Default → `SKIPPED (DEPENDENCY_FAILED)`, configurable | Root-cause alerting |
| Airflow retries reused the same `run_id` | Each attempt gets its own `run_id` (`-a<n>`) | Full history |
| Doc 03 status thresholds | + `successTolerancePct/Count` (default 0) and optional `maxQuarantinedPct` | Clear SUCCESS vs PARTIAL rule |

```sql
-- db/control/V20__lifecycle_additions.sql
ALTER TABLE etl_ctl.gold_record_status ADD COLUMN next_retry_at timestamptz;
CREATE INDEX ix_grs_due ON etl_ctl.gold_record_status (job_name, next_retry_at) WHERE status = 'FAILED';

ALTER TABLE etl_ctl.job_registry
    RENAME COLUMN sla_minutes TO sla_max_duration_minutes;
ALTER TABLE etl_ctl.job_registry
    ADD COLUMN sla_freshness_minutes integer,
    ADD COLUMN sla_ready_by          varchar(50);

ALTER TABLE etl_ctl.job_run_audit
    ADD COLUMN has_record_errors boolean NOT NULL DEFAULT false;   -- SUCCESS within tolerance but not zero
```

---

## 13. Key Decisions

1. **One lifecycle for all runs:** request → checks → `QUEUED` → `RUNNING` → `SUCCESS | PARTIAL | FAILED | SKIPPED | CANCELLED`; final states never change; every rerun and every Airflow attempt is a new `run_id` linked by `parent_run_id` / `root_run_id`; zombie runs are closed automatically.
2. **Run modes** (`run_mode`): `INCREMENTAL`, `FULL_LOAD`, `RERUN_JOB`, `RERUN_FAILED_RECORDS`, `REPROCESS_QUARANTINE`, `BACKFILL`, `REPLAY`, `ID_REPROCESS`; triggers (`trigger_type`) and `is_dry_run` are separate. Only `INCREMENTAL`, `FULL_LOAD` and `RERUN_JOB` move bookmarks.
3. **Cascade:** automatic through change numbers; explicit cascade requests queue downstream jobs (incremental by default) in dependency order.
4. **Incremental B2S** = status-based selection (`PENDING`, `PARTIAL`, `FAILED` due for retry) + run cutoff + guards; `lastIngestedAt` is only a safety net; `recordHash`/row hash avoid useless work; a nightly sweep checks the invariant. **Incremental S2G** = change numbers + bookmarks + retry list.
5. **Retry backoff** for FAILED records (`nextRetryAt`, default 0h/1h/4h/12h/24h, `maxRetries` 5 → `QUARANTINED`).
6. **Status rules** (first match wins): cancelled → skipped cases → config → infra → threshold breach → reconciliation/SCD integrity → 0 bad = SUCCESS → within tolerance = SUCCESS with record errors → otherwise PARTIAL. Thresholds per job: `successTolerancePct/Count` (default 0), `maxFailedPct/Count`, `minRecordsForPct`, optional `maxQuarantinedPct`, `maxUnknownSkPct`. Main reason always an `error_code` from the catalog.
7. **Counting:** per source record, from aggregations; CHECK constraint + strict reconciliation; skipped records always have a reason.
8. **Failed/PII data:** status flag on the record **and** a quarantine item (pointer + masked snapshot); real values only via audited "reveal"; fix types SOURCE_FIX / CODE_FIX / RULE_CHANGE / CORRECTION_OVERRIDE; four-eyes approval in prod; automatic SUPERSEDED and periodic reprocessing.
9. **Dependencies:** upstream PARTIAL → run; FAILED → `WAIT` (60 min) then `SKIPPED (DEPENDENCY_FAILED)` for HARD, run for SOFT; configurable per dependency.
10. **Concurrency/retries/timeouts/SLA:** job lock + single-owner tables + MVCC + Airflow pools; four retry levels (statement, Spark task, job for exit code 30 only, record with backoff); database and Airflow timeouts + a progress watchdog; duration, freshness and ready-by SLAs.
11. **Idempotency:** guards in every layer; data + bookmark in one transaction; writes to other systems only after the commit and idempotent; every crash point recoverable.
12. **Ops API:** `run_request` with approval and `Idempotency-Key`; endpoints for runs, reruns, cancel, approvals, cascade preview, watermark reset, lock release, quarantine (list, reveal, fix, approve, reject, bulk, reprocess) and DLQ.
