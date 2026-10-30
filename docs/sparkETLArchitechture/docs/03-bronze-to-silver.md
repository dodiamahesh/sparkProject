# 03 — Bronze → Silver ETL (Spark Java)

> Builds on [01-architecture-and-structure.md](01-architecture-and-structure.md) and [02-ingestion-bronze.md](02-ingestion-bronze.md).
> Scope: the Spark Bronze → Silver job: reading per run mode, the processing steps, table splitting, Silver-level dimensional modeling, the DQ framework, error handling and quarantine, the Silver merge, Bronze write-back, reconciliation and performance.
> Later prompts finalize: Gold modeling (04), the control/audit table DDL (05), the framework class design (06), status rules and run modes in depth (07).

---

## 0. Summary

| Topic | Decision |
|---|---|
| Job shape | One generic `BronzeToSilverJob`, fully driven by `pipelines/bronze-to-silver/<domain>/<job>.yml`. **One owning B→S job per Bronze collection**; it may write several Silver tables |
| Read | Mongo Spark connector, **explicit schema** (from the data contract), `$match` + `$project` pushed down to Mongo, secondary reads, a **run cutoff** for a stable input set |
| Step order | read → pre-filter → **split** → standardize/cast → **DQ** → dedupe → lookups/referential → delete handling → **stage + merge** → **write-back** → reconcile |
| DQ | Rule engine compiles YAML rules into Spark column expressions. Each row collects an `_errors` array in **one vectorized pass** (no exceptions per row, no UDFs) |
| Record status | `SUCCESS` · `PARTIAL` (some Silver tables failed) · `FAILED` (**retryable only**) · `QUARANTINED` (needs a data/code fix) · `SKIPPED` (filtered/duplicate loser) |
| Silver keys | **Natural keys** from the source (Mongo `_id` as string) + deterministic child keys. Surrogate keys are a Gold concern |
| Silver modeling | Cleansed, integrated **entity tables** (3NF-ish). Reference data → `silver_ref`. History (SCD2) is **captured in Silver** when the source overwrites and the business needs history. Star schema is built in Gold |
| Write | Spark → **UNLOGGED staging table** (COPY/JDBC batch) → **one EPAS transaction**: `INSERT … ON CONFLICT` with a **version + row-hash guard**, child orphan handling, delete propagation |
| Write-back | **After** the Silver commit, never before. Bulk Mongo `updateOne` per record with the optimistic filter `{_id, _etl.sourceVersion: readVersion}` |
| Job failure | Infra error → job FAILED, Silver rolled back, no write-back. Failure rate above threshold → **circuit breaker**: roll back, no write-back, alert |
| Reconciliation | `read = success + partial + failed + quarantined + skipped`, plus per-table insert/update/unchanged counts and optional sum checksums, stored per run |

---

## 1. End-to-End Flow

```
 Airflow ──► spark-submit etl-spark-app.jar  EtlJobRunner --job=b2s_order --mode=INCREMENTAL --env=prod
                │
 ┌──────────────▼───────────────────────────────────────────────────────────────────────────────────┐
 │ 0. START      load job YAML (+ env overrides) · acquire job_lock · insert job_run_audit RUNNING   │
 │               · cutoff = now() · resolve run mode → ReadPlan                                       │
 ├──────────────────────────────────────────────────────────────────────────────────────────────────┤
 │ 1. READ       Bronze (Mongo) with $match (run-mode filter + cutoff) + $project, explicit schema   │
 │               → empty? → finish as SUCCESS (0 records) without starting the heavy steps           │
 ├──────────────────────────────────────────────────────────────────────────────────────────────────┤
 │ 2. PRE-FILTER business/technical filters from config (test tenants, out-of-scope types)           │
 │               rejected rows → status SKIPPED (reason FILTERED)                                    │
 ├──────────────────────────────────────────────────────────────────────────────────────────────────┤
 │ 3. SPLIT      one Bronze doc → parent row + child rows (explode arrays, flatten 1:1 sub-docs)      │
 │               every row keeps _bronze_id + _src_version (link back for write-back)                │
 ├──────────────────────────────────────────────────────────────────────────────────────────────────┤
 │ 4. STANDARDIZE trim/case/null-if-empty · try_cast to target types (cast failure → error, not null)│
 │               · timezone → UTC · code mapping via silver_ref tables · row hash                    │
 ├──────────────────────────────────────────────────────────────────────────────────────────────────┤
 │ 5. VALIDATE   DQ engine: all rules of the target as columns → _errors array per row               │
 ├──────────────────────────────────────────────────────────────────────────────────────────────────┤
 │ 6. DEDUPE     by business key (strategy from config); losers → SKIPPED (DUPLICATE)                │
 ├──────────────────────────────────────────────────────────────────────────────────────────────────┤
 │ 7. ENRICH     lookups (broadcast small refs) · referential checks (missing parent = retryable)    │
 ├──────────────────────────────────────────────────────────────────────────────────────────────────┤
 │ 8. CLASSIFY   per row: OK / WARN / FAILED / QUARANTINE → per Bronze doc status (+ targets[])      │
 │               → threshold check (circuit breaker)                                                  │
 ├──────────────────────────────────────────────────────────────────────────────────────────────────┤
 │ 9. DELETES    isDeleted docs → soft-delete rows (parent + children) per deleteStrategy             │
 ├──────────────────────────────────────────────────────────────────────────────────────────────────┤
 │ 10. WRITE     good rows → staging (COPY) → ONE transaction: merge parents, children, orphans,      │
 │               SCD2 timelines → COMMIT        (dry-run: skipped)                                    │
 ├──────────────────────────────────────────────────────────────────────────────────────────────────┤
 │ 11. QUARANTINE quarantine records (reference + errors, masked) → etl_ctl.quarantine_record          │
 ├──────────────────────────────────────────────────────────────────────────────────────────────────┤
 │ 12. WRITE-BACK Bronze _etl.syncSilver.* via bulk conditional updates (only after step 10 commit)  │
 ├──────────────────────────────────────────────────────────────────────────────────────────────────┤
 │ 13. RECONCILE counts + checksums → etl_ctl.reconciliation, dq_results · decide job status          │
 │               · update job_run_audit · emit lineage + metrics · release lock                       │
 └──────────────────────────────────────────────────────────────────────────────────────────────────┘
        any infra exception in 1–10 → rollback, job FAILED, no write-back, lock released, Airflow retries
```

**Why this order differs a little from "filter → validate → dedupe → standardize":**
- **Split before validate:** rules are defined per Silver table (e.g. `qty > 0` applies to `order_item` rows, not to the order document).
- **Standardize before validate:** `" 123 "` is valid after trimming; status `"confirmed"` is valid after upper-casing. Validating raw values would raise false errors. Cast failures are captured as errors in this step.
- **Dedupe after validate:** a duplicate that is invalid must not "win" over a valid one.
- **Referential checks after lookups:** the lookup result is what the check needs.

---

## 2. Read Bronze

### 2.1 Run mode → Bronze filter

All modes share the same steps; only the **ReadPlan** differs.

| Run mode | Bronze `$match` (in addition to the job's static filter) |
|---|---|
| `INCREMENTAL` (scheduled) | `_etl.syncSilver.status ∈ {PENDING, FAILED, PARTIAL}` AND (`status ≠ FAILED` OR (`retryCount < maxRetries` AND `nextRetryAt ≤ cutoff`)) AND `_etl.lastIngestedAt ≤ cutoff` (retry backoff: doc 07 §3.5) |
| `FULL_LOAD` | all documents (including deleted), `lastIngestedAt ≤ cutoff` |
| `RERUN_JOB` | same as INCREMENTAL. The failed run wrote nothing back, so the same documents are still pending |
| `RERUN_FAILED_RECORDS` | `status ∈ {FAILED, PARTIAL}` (optionally `_etl.syncSilver.runId = parentRunId`) |
| `REPROCESS_QUARANTINE` | `_id ∈` ids from `etl_ctl.quarantine_record` with status `READY_TO_REPROCESS` for this job |
| `BACKFILL` | `_etl.sourceEventTs ∈ [from, to)` |
| `REPLAY` | like FULL_LOAD or BACKFILL; used after Bronze was rebuilt or Silver was lost |
| `ID_REPROCESS` | `_etl.sourceId ∈ [ids]` |
| `DRY_RUN` | any of the above; Silver writes, quarantine writes and write-back are disabled |

➕ **Run cutoff:** `cutoff = run start time`. Documents ingested during the run are left for the next run. Every run then has a fixed, explainable input set, and the reconciliation counts are exact. The optimistic write-back (section 9) keeps it safe even without the cutoff.

➕ **`maxRecordsPerRun`** (optional): during a big backlog (e.g. after an outage), each run takes the oldest N pending documents (sorted by `lastIngestedAt`). Runs stay short and predictable, and the backlog drains over several runs.

➕ **No "claim" step (`IN_PROGRESS`).** Marking documents before processing would double the Mongo writes. The job lock (one run per job) + the optimistic write-back already prevent double processing and races.

### 2.2 Reader

```java
// etl-core: io/reader/BronzeReader.java
public final class BronzeReader {

    public Dataset<Row> read(SparkSession spark, BronzeSource src, ReadPlan plan, StructType dataSchema) {
        List<Document> pipeline = List.of(
                new Document("$match", plan.toMongoMatch()),                 // pushed down, uses ix_sync_pickup
                new Document("$project", ProjectionBuilder.of(src.requiredDataFields())));   // only needed fields

        return spark.read().format("mongodb")
                .option("connection.uri", src.connectionUri())               // resolved from env config + secret
                .option("database", src.database())
                .option("collection", src.collection())
                .option("aggregation.pipeline", Json.toJsonArray(pipeline))
                .option("read.readPreference.name", "secondaryPreferred")   // ➕ keep load off the primary
                .option("partitioner", "com.mongodb.spark.sql.connector.read.partitioner.SamplePartitioner")
                .option("partitioner.options.partition.size", String.valueOf(src.partitionSizeMb())) // e.g. 64
                .schema(BronzeSchemas.withEtl(dataSchema))                   // explicit: {_id, data:<contract schema>, _etl:<fixed>}
                .load();
    }
}
```

| Choice | Why |
|---|---|
| **Explicit schema** from `contracts/<svc>/<coll>.contract.yml` ➕ | Schema inference samples documents and is unstable (types change from run to run, sparse fields get missed). An explicit schema is stable and fast. New fields not yet in the contract go to `_unmapped` (section 4.4) |
| `$match` + `$project` pushdown | Mongo filters with the partial index and sends only the needed fields |
| `secondaryPreferred` | Reads do not load the primary. A slightly stale read is safe because of the optimistic write-back |
| `SamplePartitioner` (64 MB) | Parallel reads. Note: with a selective `$match`, a sample partitioner can create uneven partitions; for small incremental sets the job uses a single partition (see 11) |

---

## 3. Table Division (Splitting Nested Documents)

### 3.1 Splitting rules

| # | Bronze shape | Silver design | Example |
|---|---|---|---|
| R1 | 1:1 sub-document | **Flatten** into the parent with a prefix | `shippingAddress.city` → `ship_addr_city` |
| R2 | 1:1 sub-document reused by many collections (address, money) | Flatten by default; a separate table only if it is queried on its own | — |
| R3 | **Array of objects** (1:N) | **Child table**: PK = parent key + child key | `items[]` → `sales_order_item` |
| R4 | Array inside an array | **Grandchild table** carrying the **full ancestor key path** | `items[].discounts[]` → `sales_order_item_discount (order_id, line_no, discount_seq)` |
| R5 | Array of scalars | Child table `(parent_id, value, position)` if it is filtered/joined; otherwise an `text[]`/`jsonb` column | `tags[]` → `sales_order_tag` |
| R6 | Embedded **copy of a master entity** (customer snapshot inside order) | Keep only the FK (`customer_id`), plus snapshot attributes needed "as of order time" (e.g. `customer_tier_at_order`). The master comes from its own collection/job | `customer: {id, name, tier}` |
| R7 | Free-form map / polymorphic attributes | Key-value child table `(parent_id, attr_key, attr_value)` or a `jsonb` column | `attributes: {color: red}` |
| R8 | Large blobs, binary, raw payloads | **Not in Silver** (stay in Bronze) | `rawGatewayResponse` |
| R9 | Unknown/new fields not in the contract | `_unmapped jsonb` on the parent ➕ | — |

### 3.2 Child key generation

| Strategy | When | Key |
|---|---|---|
| **Natural child key** (preferred) | The array element has a stable id (`lineId`, `sku` unique per order) | `(order_id, line_id)` |
| **Deterministic hash** | No single id field, but a combination is unique | `child_id = sha256(order_id ‖ ':' ‖ sku ‖ ':' ‖ variant)` |
| **Position** (fallback) | No business identity at all | `(order_id, position)` ⚠ if the source re-orders the array, positions shift and rows are "updated" instead of matched. Acceptable only for arrays that are append-only or replaced as a whole |

Keys are always **deterministic**: the same input gives the same key in every run and every run mode. That is what makes reruns idempotent.

### 3.3 Child replacement and orphans ➕

When the parent document changes, its array may have **fewer** elements than before (an item was removed). A plain upsert of the new children would leave the removed child alive in Silver.

**Rule:** for every parent applied in this run, child rows that exist in Silver but are **not** in the new child set are soft-deleted (`_is_deleted = true`, `_delete_reason = 'REMOVED_FROM_PARENT'`). This happens in the same transaction as the merge (section 8.3).

### 3.4 Worked example

**Bronze document** (`bronze_svc_order.orders`, `data` part):

```json
{
  "_id": {"$oid": "65f1a2b3c4d5e6f708091a2b"},
  "orderNo": " ord-1001 ",
  "orderDate": {"$date": "2026-10-29T08:15:00+05:30"},
  "status": "confirmed",
  "currency": "INR",
  "amount": {"$numberDecimal": "1499.00"},
  "customer": {"id": {"$oid": "64aa00000000000000000001"}, "name": "A. Shah", "tier": "GOLD"},
  "shippingAddress": {"line1": "12 MG Road", "city": "ahmedabad", "country": "IN"},
  "items": [
    {"lineId": 1, "sku": "A1", "qty": 2, "price": {"$numberDecimal": "500.00"},
     "discounts": [{"code": "DIWALI", "amount": {"$numberDecimal": "50.00"}}]},
    {"lineId": 2, "sku": "B7", "qty": 1, "price": {"$numberDecimal": "549.00"}, "discounts": []}
  ],
  "payments": [{"paymentId": "PAY-9", "method": "UPI", "amount": {"$numberDecimal": "1499.00"}}],
  "tags": ["festive", "app"]
}
```

**Silver result** (schema `silver_order`):

```
sales_order                    (1 row)   PK order_id
  order_id=65f1a2b3...  order_no=ORD-1001  order_ts=2026-10-29 02:45:00+00 (UTC)
  status_code=CONFIRMED  currency_code=INR  total_amount=1499.00
  customer_id=64aa...01  customer_tier_at_order=GOLD                     ← R6: FK + "as of" attribute
  ship_addr_line1="12 MG Road"  ship_addr_city=Ahmedabad  ship_addr_country=IN   ← R1: flattened

sales_order_item               (2 rows)  PK (order_id, line_id)                  ← R3
  (65f1..., 1, A1, qty 2, unit_price 500.00)
  (65f1..., 2, B7, qty 1, unit_price 549.00)

sales_order_item_discount      (1 row)   PK (order_id, line_id, discount_code)   ← R4: full key path
  (65f1..., 1, DIWALI, 50.00)

sales_order_payment            (1 row)   PK (order_id, payment_id)                ← R3
  (65f1..., PAY-9, UPI, 1499.00)

sales_order_tag                (2 rows)  PK (order_id, tag)                       ← R5
  (65f1..., festive)  (65f1..., app)
```

`customer.name` is **not** copied: the customer master comes from `b2s_customer` (`svc-customer.customers`).

### 3.5 Split config (part of the job YAML)

```yaml
targets:
  - name: sales_order
    table: silver_order.sales_order
    from: root
    key: [order_id]
    columns:
      order_id:               { source: _id,                         type: varchar(24),   transform: [objectIdToString] }
      order_no:               { source: data.orderNo,                type: varchar(30),   transform: [trim, upper] }
      order_ts:               { source: data.orderDate,              type: timestamptz }
      status_code:            { source: data.status,                 type: varchar(20),   transform: [trim, upper],
                                map: { refTable: silver_ref.order_status_map, onUnmapped: ERROR } }
      currency_code:          { source: data.currency,               type: char(3),       transform: [trim, upper] }
      total_amount:           { source: data.amount,                 type: "numeric(18,2)" }
      customer_id:            { source: data.customer.id,            type: varchar(24),   transform: [objectIdToString] }
      customer_tier_at_order: { source: data.customer.tier,          type: varchar(20),   transform: [trim, upper] }
      ship_addr_line1:        { source: data.shippingAddress.line1,  type: varchar(200),  transform: [trim, nullIfEmpty] }
      ship_addr_city:         { source: data.shippingAddress.city,   type: varchar(100),  transform: [trim, initcap] }
      ship_addr_country:      { source: data.shippingAddress.country,type: char(2),       transform: [trim, upper] }
    keepUnmapped: true                       # unknown fields → _unmapped jsonb

  - name: sales_order_item
    table: silver_order.sales_order_item
    from: data.items                         # array of objects → explode
    parentKey: { order_id: _id }
    key: [order_id, line_id]
    childKey: { line_id: { source: lineId, fallback: POSITION } }
    orphanPolicy: SOFT_DELETE                # children removed from the array
    columns:
      sku:        { source: sku,   type: varchar(40), transform: [trim, upper] }
      qty:        { source: qty,   type: integer }
      unit_price: { source: price, type: "numeric(18,2)" }

  - name: sales_order_item_discount
    table: silver_order.sales_order_item_discount
    from: data.items[].discounts             # nested array → grandchild
    parentKey: { order_id: _id, line_id: items.lineId }
    key: [order_id, line_id, discount_code]
    orphanPolicy: SOFT_DELETE
    columns:
      discount_code: { source: code,   type: varchar(30), transform: [trim, upper] }
      amount:        { source: amount, type: "numeric(18,2)" }

  - name: sales_order_tag
    table: silver_order.sales_order_tag
    from: data.tags                          # array of scalars
    scalarArray: { valueColumn: tag, type: varchar(50), transform: [trim, lower] }
    parentKey: { order_id: _id }
    key: [order_id, tag]
    orphanPolicy: HARD_DELETE                # tags have no history value

childFailurePolicy: PARTIAL                  # PARTIAL | FAIL_PARENT (section 7.2)
```

### 3.6 Splitter implementation idea

```java
// etl-core: modeling/TableSplitter.java
public final class TableSplitter {

    /** Builds one Dataset per target. Every row keeps _bronze_id and _src_version for write-back. */
    public Map<String, Dataset<Row>> split(Dataset<Row> bronze, List<TargetSpec> targets) {
        Map<String, Dataset<Row>> out = new LinkedHashMap<>();
        for (TargetSpec t : targets) {
            Dataset<Row> base = bronze.select(
                    col("_id").as("_bronze_id"),
                    col("_etl.sourceVersion").as("_src_version"),
                    col("_etl.sourceEventTs").as("_src_event_ts"),
                    col("_etl.isDeleted").as("_bronze_is_deleted"),
                    col("data"));

            Dataset<Row> rows = switch (t.shape()) {
                case ROOT -> base;
                case ARRAY_OF_OBJECTS, SCALAR_ARRAY -> explodePath(base, t.fromPath()); // posexplode_outer per level
            };
            out.put(t.name(), rows.select(t.selectColumns()));   // raw source columns + parent key path + position
        }
        return out;
    }

    /** data.items[].discounts → posexplode_outer(items) then posexplode_outer(discounts), carrying ancestor keys. */
    private Dataset<Row> explodePath(Dataset<Row> df, ArrayPath path) {
        Dataset<Row> cur = df;
        for (ArrayPath.Level level : path.levels()) {
            cur = cur.select(col("*"), posexplode_outer(col(level.arrayExpr())).as(new String[]{level.posAlias(), level.elemAlias()}))
                     .where(col(level.elemAlias()).isNotNull());          // empty arrays → no child rows
        }
        return cur;
    }
}
```

---

## 4. Standardization, Cleansing and Type Casting

### 4.1 Transform library (config names → built-in Spark functions, no UDFs)

| Transform | Spark expression | Notes |
|---|---|---|
| `trim`, `upper`, `lower`, `initcap` | `trim()`, `upper()`, ... | |
| `nullIfEmpty` | `when(trim(c) === "", null)` | |
| `objectIdToString` | ObjectId struct → `c.oid` | Mongo Spark connector representation |
| `regexReplace(p, r)` | `regexp_replace` | |
| `toUtc(zoneField or default)` | `to_utc_timestamp` | ➕ All Silver timestamps are `timestamptz`, stored in UTC |
| `normalizeEmail` | `lower(trim(c))` | |
| `normalizePhone(defaultCountry)` | regex to E.164 | Complex cases → plugin |
| `round(n)` | `round` | |
| `default(v)` | `coalesce(c, lit(v))` | |
| `map(refTable)` | broadcast join on `silver_ref.*` | Unmapped → error (configurable) |
| `custom(className)` | `CustomTransformer` plugin (`etl-jobs`) | Escape hatch |

### 4.2 Safe casting ➕ (an important trap)

In Spark (non-ANSI mode), `cast("abc" as int)` silently returns **null**. A bad value would then load as NULL with no error. So every typed column is cast with `try_cast`, and a cast failure becomes a DQ error:

```java
Column raw = col(srcCol);
Column typed = expr("try_cast(" + srcCol + " AS " + sparkType + ")");
Column castError = when(raw.isNotNull().and(typed.isNull()),
        DqError.of("DQ_TYPE_CAST", targetCol, Severity.QUARANTINE, "cannot cast to " + sparkType));
```

`spark.sql.ansi.enabled` stays `false` for these jobs (so a bad value never crashes the whole job). `try_cast` gives explicit control instead.

### 4.3 Row hash

`_row_hash = sha2(concat_ws('‖', <business columns in fixed order>), 256)`, computed per Silver row **after** standardization. The merge uses it to skip unchanged rows (section 8.2). Technical columns are excluded.

### 4.4 Unmapped fields ➕

With `keepUnmapped: true`, fields in `data` that are not in the contract are packed into `_unmapped jsonb` on the parent table. New source fields are then never lost. When the contract is updated and the column is added, a `FULL_LOAD` or `BACKFILL` fills the new column from Bronze.

---

## 5. Data Quality Framework

### 5.1 Design

```
 dq-rules/_common/*.yml ─┐
 dq-rules/<domain>/*.yml ├─► RuleLoader ─► RuleFactory ─► List<DqRule> (per target table)
 job YAML (inline rules) ┘                                    │
                                                              ▼
                       Dataset<Row> ──► DqEngine.apply() ──► same Dataset + _errors: array<struct<
                                         (one projection,       code, column, rule_id, severity, message>>
                                          all rules as Columns)            │
                                                              ┌───────────┴───────────┐
                                                              ▼                       ▼
                                                     DqResultCollector         Classifier (step 8)
                                                     per rule: checked/failed  OK / WARN / FAILED /
                                                     → etl_ctl.dq_results      QUARANTINE per row
```

**Principles:**
- **One pass.** Every rule is a Spark `Column` producing either `null` (passed) or an error struct. All of them go into one `array_compact(array(...))`. No per-row exceptions, no UDFs, no separate scan per rule.
- **Rules are data.** YAML files, versioned in Git, validated by JSON Schema in CI, reusable via rule sets.
- **Severity decides the outcome**, not the rule type.

### 5.2 Severities

| Severity | Row outcome | Bronze status | Example |
|---|---|---|---|
| `WARN` | Loaded; warning stored in `_dq_warnings jsonb` | SUCCESS | Phone format unusual |
| `ERROR` + **retryable** error code | Not loaded | `FAILED` (retried next run, `retryCount++`; → `QUARANTINED` when retries are exhausted) | Referenced customer not in Silver yet |
| `QUARANTINE` (or `ERROR` + non-retryable) | Not loaded; quarantine record | `QUARANTINED` | `amount` not numeric, mandatory field null |

➕ **Why "non-retryable → QUARANTINED immediately":** retrying a null mandatory field gives the same result every hour. Only a source change helps, and a source change resets the status to `PENDING` anyway (doc 02, section 6.3). That also **auto-resolves** the quarantine record (status `SUPERSEDED`). So `FAILED` means exactly "this will probably succeed later by itself".

### 5.3 Rule types

| Type | Config example | Compiled to |
|---|---|---|
| `not_null` | `{type: not_null, column: order_no}` | `col.isNull()` |
| `type` | automatic from the column `type` | `try_cast` check (4.2) |
| `regex` | `{type: regex, column: email, pattern: "^[^@]+@[^@]+$"}` | `!rlike` |
| `range` | `{type: range, column: qty, min: 1, max: 10000}` | `col < min OR col > max` |
| `allowed_values` | `{type: allowed_values, column: currency_code, values: [INR, USD, EUR]}` | `!isin` |
| `length` | `{type: length, column: order_no, max: 30}` | `length(col) > max` |
| `expression` | `{type: expression, expr: "total_amount >= 0 OR status_code = 'REFUND'"}` | `!expr(...)` (Spark SQL) |
| `cross_field` | `{type: expression, expr: "ship_addr_country IS NOT NULL OR status_code <> 'SHIPPED'"}` | same |
| `unique_in_batch` | `{type: unique_in_batch, columns: [order_no]}` | window `count(*) over (partition by key) > 1` |
| `referential` | `{type: referential, column: customer_id, refTable: silver_customer.customer, refColumn: customer_id, onMissing: RETRY, retryWindow: 24h}` | left join with the ref keys (section 6.2) |
| `freshness` | `{type: freshness, column: order_ts, maxFutureSkew: 10m}` | `order_ts > now() + skew` |
| `custom` | `{type: custom, class: com.company.etl.jobs.order.GstinCheck}` | `DqRule` plugin |

### 5.4 Rule file example

`dq-rules/order/sales_order.rules.yml`
```yaml
ruleSet: order.sales_order
appliesTo: silver_order.sales_order
include: [_common/keys_not_null]            # reusable shared rules
rules:
  - id: SO-001
    type: not_null
    column: order_no
    severity: QUARANTINE
    errorCode: DQ_NOT_NULL
  - id: SO-002
    type: range
    column: total_amount
    min: 0
    max: 10000000
    severity: QUARANTINE
    errorCode: DQ_RANGE
  - id: SO-003
    type: allowed_values
    column: currency_code
    values: [INR, USD, EUR, GBP]
    severity: QUARANTINE
    errorCode: DQ_ALLOWED_VALUES
  - id: SO-004
    type: referential
    column: customer_id
    refTable: silver_customer.customer
    refColumn: customer_id
    onMissing: RETRY                       # RETRY | WARN | QUARANTINE
    retryWindow: 24h                       # after 24h of retries → onExhausted
    onExhausted: WARN                      # load with a warning (Gold will create an inferred member)
    errorCode: REF_PARENT_MISSING          # retryable=true in error_code_catalog
  - id: SO-005
    type: expression
    expr: "order_ts <= current_timestamp() + INTERVAL 10 MINUTES"
    severity: WARN
    errorCode: DQ_FUTURE_DATE
```

### 5.5 Engine skeleton

```java
// etl-core: dq/DqRule.java
public interface DqRule extends Serializable {
    String id();
    Severity severity();
    String errorCode();
    /** Returns an error struct when the row fails, null when it passes. */
    Column evaluate(DqContext ctx);
}

// etl-core: dq/rules/NotNullRule.java
public record NotNullRule(String id, String column, Severity severity, String errorCode) implements DqRule {
    @Override public Column evaluate(DqContext ctx) {
        return when(col(column).isNull(), DqError.of(errorCode, column, id, severity, column + " is null"));
    }
}

// etl-core: dq/DqError.java
public final class DqError {
    public static Column of(String code, String column, String ruleId, Severity sev, String msg) {
        return struct(lit(code).as("code"), lit(column).as("column"), lit(ruleId).as("rule_id"),
                      lit(sev.name()).as("severity"), lit(msg).as("message"));      // ➕ never include the value (PII)
    }
}

// etl-core: dq/DqEngine.java
public final class DqEngine {
    public Dataset<Row> apply(Dataset<Row> df, List<DqRule> rules, DqContext ctx) {
        Column[] checks = rules.stream().map(r -> r.evaluate(ctx)).toArray(Column[]::new);
        Column existing = df.columns().length > 0 && Arrays.asList(df.columns()).contains("_errors")
                ? col("_errors") : typedLit(new Object[0]);                        // cast/mapping errors from step 4
        return df.withColumn("_errors", concat(existing, array_compact(array(checks))));   // Spark 3.4+
    }
}
```

`DqResultCollector` runs **one aggregation** over the exploded `_errors` (grouped by `rule_id`, `severity`) plus the total row count per table → `etl_ctl.dq_results` (DDL in Prompt 05). These results are also published to OpenMetadata (Prompt 10).

---

## 6. Dedupe, Lookups and Referential Checks

### 6.1 Duplicate detection

Bronze `_id` is unique, so the same document never appears twice in one run. Duplicates here mean **two different source documents with the same business key** (e.g. the same `orderNo` created twice by a bug).

```yaml
dedupe:
  businessKey: [order_no]
  strategy: LATEST_VERSION       # LATEST_VERSION | KEEP_FIRST | QUARANTINE_ALL
  orderBy: [_src_event_ts DESC, _bronze_id DESC]   # deterministic tie-breaker
```

| Strategy | Winner | Losers |
|---|---|---|
| `LATEST_VERSION` | Highest event time | `SKIPPED` with reason `DUPLICATE_BUSINESS_KEY` + winner id |
| `KEEP_FIRST` | Lowest event time | same |
| `QUARANTINE_ALL` | None | All `QUARANTINED`, a human decides |

Duplicates **across runs** (the same business key already in Silver under another `_id`) are found with a unique index on the business key in Silver plus a pre-merge join (the same strategies apply).

No-op duplicates (the same document re-processed with the same data) are not duplicates: the merge's row-hash guard turns them into "unchanged" (section 8.2).

### 6.2 Lookups and referential checks

| Lookup size | Technique |
|---|---|
| Small (< ~50 MB: reference/code tables, currencies, countries) | Read once per run, `broadcast()` join |
| Large (customers: millions) | Read **only the keys needed**: collect the distinct FK values of this run (if < ~100k) and query `WHERE customer_id = ANY(?)`; otherwise a partitioned JDBC read of the key column only, then a sort-merge join |
| Within the same document (child → parent) | Always satisfied; no check needed |

**Late-arriving parents ➕:** `order` and `customer` come from different collections (and jobs). The order may arrive before its customer. Flow:
1. Airflow dependency `b2s_order dependsOn b2s_customer` reduces the window.
2. A missing reference → `REF_PARENT_MISSING` (retryable) → `FAILED` → retried in the next runs.
3. After `retryWindow` (e.g. 24 h, measured from `firstFailedAt`) → `onExhausted`: `WARN` (load anyway, flagged in `_dq_warnings`; Gold creates an inferred member, Prompt 04) or `QUARANTINE`.

Silver has **no enforced FK constraints** ➕: late-arriving data would make inserts fail. Relationships are documented as logical FKs (and in OpenMetadata) and enforced by the referential DQ rules.

---

## 7. Classification: Row → Record → Job

### 7.1 Row level

```
_errors empty                        → OK
only WARN                            → OK (warnings kept in _dq_warnings)
any retryable ERROR (and no QUARANTINE) → FAILED
any QUARANTINE / non-retryable ERROR → QUARANTINE
```

### 7.2 Record (Bronze document) level

A Bronze document produced rows in several targets (parent, children). Its status is derived from them:

| Parent row | Child rows | `childFailurePolicy` | What is written | Bronze status |
|---|---|---|---|---|
| OK | all OK | — | parent + children | `SUCCESS` |
| OK | some bad | `PARTIAL` | parent + good children; bad children quarantined/failed | `PARTIAL` (+ `targets[]`) |
| OK | some bad | `FAIL_PARENT` | nothing | `FAILED` / `QUARANTINED` (worst child) |
| bad | any | — | nothing for this document | `FAILED` / `QUARANTINED` (worst row) |
| filtered (step 2) | — | — | nothing | `SKIPPED` (`FILTERED`) |
| dedupe loser | — | — | nothing | `SKIPPED` (`DUPLICATE_BUSINESS_KEY`) |

Worst-of order: `QUARANTINED` > `FAILED` > `PARTIAL` > `SUCCESS`.

A `PARTIAL` or `FAILED` document is re-processed **as a whole** in `RERUN_FAILED_RECORDS`. The row-hash guard turns the already-written rows into "unchanged", so only the previously failed rows actually change. (This simplifies the doc 02 wording "retries only the failed targets": the effect is the same and the code is simpler.)

### 7.3 Job level (summary; full rules in Prompt 07)

| Condition | Job status | Effect |
|---|---|---|
| Infra exception (Mongo/EPAS down, Spark failure after retries, OOM) | `FAILED` | Silver transaction rolled back, **no write-back**, records stay PENDING, Airflow retries |
| failed% > `maxFailedPct` or count > `maxFailedCount` ➕ **circuit breaker** | `FAILED` (`THRESHOLD_BREACHED`) | **Roll back, no write-back, no quarantine.** A sudden mass failure is almost always systemic (a code bug, a source schema change). Writing back would wrongly increase `retryCount` or quarantine thousands of good records. The DQ results are still stored for diagnosis |
| some records not SUCCESS, below the thresholds | `PARTIAL` | Good data committed, statuses written back |
| everything SUCCESS/SKIPPED | `SUCCESS` | |

```yaml
thresholds:
  maxFailedPct: 2.0          # FAILED + QUARANTINED + PARTIAL, as % of read (minus SKIPPED)
  maxFailedCount: 5000
  minRecordsForPct: 100      # below this, only maxFailedCount applies (avoid 1-of-3 = 33% alarms)
  onBreach: ROLLBACK         # ROLLBACK | COMMIT_AND_FAIL
```

---

## 8. Write to Silver

### 8.1 Pattern: stage → merge in one transaction

Spark's JDBC writer can only insert. Upsert/merge logic therefore runs **inside EPAS** as set-based SQL:

```
Spark (executors)                                   EPAS (driver opens ONE transaction)
─────────────────                                   ──────────────────────────────────────
good rows per target ──COPY / JDBC batch──► stg_silver_order.sales_order        (UNLOGGED)
                                            stg_silver_order.sales_order_item
                                            ...
                                                     BEGIN
                                                       1. merge parents      (INSERT … ON CONFLICT … WHERE guard)
                                                       2. merge children
                                                       3. orphan children    (soft/hard delete)
                                                       4. delete propagation (parents + cascade)
                                                       5. SCD2 timelines     (if configured)
                                                     COMMIT   ← only now is Silver changed
```

| Choice | Reason |
|---|---|
| **UNLOGGED staging** tables (`stg_<schema>.<table>`, same columns + `_etl_run_id`, `_bronze_id`) | Fast, no WAL; it is safe to lose them on a crash because the run is simply repeated |
| Staging **truncated at run start**, kept after the run ➕ | The job lock guarantees one run per job, and the last run's staging is still there for debugging |
| Load via **PostgreSQL `COPY`** (`CopyManager` in `foreachPartition`) ➕ | 5–10x faster than batched INSERTs. Fallback: Spark JDBC with `batchsize=10000` + `reWriteBatchedInserts=true` |
| **One transaction** for all targets of a job | Parent and children are always consistent. Nobody ever sees an order without its items |
| Set-based SQL in EPAS | One statement per target instead of millions of single-row statements |

Staging tables are created automatically by the framework (`CREATE UNLOGGED TABLE IF NOT EXISTS stg_x.t (LIKE x.t INCLUDING DEFAULTS)` + technical columns). They are tracked by Flyway like any other table.

### 8.2 Merge with version + row-hash guard

```sql
-- Parent merge (generated by the framework from the target spec)
INSERT INTO silver_order.sales_order AS t (
    order_id, order_no, order_ts, status_code, currency_code, total_amount, customer_id,
    customer_tier_at_order, ship_addr_line1, ship_addr_city, ship_addr_country,
    _src_service, _src_collection, _src_id, _src_version, _src_event_ts, _row_hash,
    _is_deleted, _deleted_at, _dq_warnings, _unmapped, _etl_job_name, _etl_run_id)
SELECT order_id, order_no, order_ts, status_code, currency_code, total_amount, customer_id,
       customer_tier_at_order, ship_addr_line1, ship_addr_city, ship_addr_country,
       _src_service, _src_collection, _src_id, _src_version, _src_event_ts, _row_hash,
       _is_deleted, _deleted_at, _dq_warnings, _unmapped, _etl_job_name, _etl_run_id
FROM stg_silver_order.sales_order
WHERE _etl_run_id = :runId
ON CONFLICT (order_id) DO UPDATE SET
    order_no = EXCLUDED.order_no,
    order_ts = EXCLUDED.order_ts,
    status_code = EXCLUDED.status_code,
    -- ... all business columns ...
    _src_version = EXCLUDED._src_version,
    _src_event_ts = EXCLUDED._src_event_ts,
    _row_hash = EXCLUDED._row_hash,
    _is_deleted = EXCLUDED._is_deleted,
    _deleted_at = EXCLUDED._deleted_at,
    _dq_warnings = EXCLUDED._dq_warnings,
    _unmapped = EXCLUDED._unmapped,
    _etl_run_id = EXCLUDED._etl_run_id,
    _etl_updated_at = now(),
    _etl_change_seq = nextval('silver_order.etl_change_seq'),   -- new change number → Gold bookmarks (doc 04 §5)
    _sync_gold_status = 'PENDING',              -- tell Gold this row changed
    _sync_gold_error_code = NULL,
    _sync_gold_error_message = NULL,
    _sync_gold_failed_jobs = NULL
WHERE EXCLUDED._src_version > t._src_version                                   -- newer source data
   OR (EXCLUDED._src_version = t._src_version AND EXCLUDED._row_hash <> t._row_hash)  -- same data, new logic
RETURNING (xmax = 0) AS inserted;              -- ➕ count inserts vs updates for reconciliation
```

| Situation | Result |
|---|---|
| New key | Insert |
| Newer `_src_version` | Update |
| Same version, **different** row hash (transformation logic was fixed, `FULL_LOAD`/`REPROCESS`) | Update ➕. Code fixes reach Silver without faking source versions |
| Same version, same hash (rerun, duplicate) | **Unchanged**: no write, no WAL, Gold not triggered |
| Older version (replay of old Bronze state) | Unchanged: Silver never goes backwards |

Rows affected = inserted + updated. Staged − affected = unchanged. All three are stored in reconciliation.

### 8.3 Children, orphans and deletes

```sql
-- Children: same guard pattern, PK (order_id, line_id)
INSERT INTO silver_order.sales_order_item AS t (...) SELECT ... FROM stg_silver_order.sales_order_item WHERE _etl_run_id = :runId
ON CONFLICT (order_id, line_id) DO UPDATE SET ... , _is_deleted = false, _deleted_at = NULL
WHERE EXCLUDED._src_version > t._src_version OR (EXCLUDED._src_version = t._src_version AND EXCLUDED._row_hash <> t._row_hash);

-- Orphans: children of parents applied in THIS run that are no longer in the source array
UPDATE silver_order.sales_order_item t
SET _is_deleted = true, _deleted_at = now(), _delete_reason = 'REMOVED_FROM_PARENT',
    _etl_run_id = :runId, _etl_updated_at = now(), _sync_gold_status = 'PENDING',
    _etl_change_seq = nextval('silver_order.etl_change_seq')
WHERE t._is_deleted = false
  AND t.order_id IN (SELECT p.order_id FROM stg_silver_order.sales_order p
                     JOIN silver_order.sales_order s ON s.order_id = p.order_id
                     WHERE p._etl_run_id = :runId AND s._src_version = p._src_version)   -- parent really applied
  AND NOT EXISTS (SELECT 1 FROM stg_silver_order.sales_order_item c
                  WHERE c._etl_run_id = :runId AND c.order_id = t.order_id AND c.line_id = t.line_id);
```

**Delete propagation** (Bronze `_etl.isDeleted = true`):

| `deleteStrategy` | Silver effect |
|---|---|
| `SOFT` (default) | Parent row staged with `_is_deleted=true`, `_deleted_at`; children cascade-soft-deleted; Gold sees `PENDING` and handles it |
| `HARD` | Rows physically deleted (parent + children). Only for data without audit value |
| `IGNORE` | Delete not propagated (rare: e.g. the source deletes archived data that analytics must keep) |

The GDPR purge (doc 02, section 10) is a separate, audited path that hard-deletes across all layers regardless of this setting.

### 8.4 SCD2 in Silver (only where configured)

Covered in section 10.4 (modeling) and executed here as step 5 of the transaction.

---

## 9. Bronze Write-Back

### 9.1 Order of operations (critical) ➕

```
1. COMMIT Silver transaction
2. Write quarantine records
3. Write back Bronze statuses
4. Write reconciliation + run audit
```

- If step 3 fails (Mongo down after the commit): the documents stay `PENDING`. The next run processes them again; the Silver guard turns them into "unchanged". **No harm, no data loss.**
- The reverse order (write-back first, then Silver) could mark records `SUCCESS` that never reached Silver if the commit fails. **Never do that.**

### 9.2 What is written per record

| Bronze status | `_etl.syncSilver` update |
|---|---|
| `SUCCESS` | `status`, `syncedVersion = readVersion`, `updatedAt`, `runId`, `jobName`; `retryCount = 0`; `errorCode/errorMessage/firstFailedAt/quarantineRef = null`; `targets = []` |
| `PARTIAL` | `status`, `targets[] = [{table, status, errorCode}]`, first error code/message, `runId`, `updatedAt`; `retryCount` += 1 |
| `FAILED` | `status`, `errorCode`, `errorMessage`, `runId`, `updatedAt`; `retryCount` += 1; `firstFailedAt` = existing or now. If `retryCount + 1 ≥ maxRetries` → `QUARANTINED` + `RETRIES_EXHAUSTED` instead |
| `QUARANTINED` | `status`, `errorCode`, `errorMessage`, `quarantineRef`, `runId`, `updatedAt` |
| `SKIPPED` | `status`, `errorCode = FILTERED / DUPLICATE_BUSINESS_KEY`, `syncedVersion = readVersion`, `runId`, `updatedAt` |

Dry-run: no write-back.

### 9.3 Bulk, conditional, parallel

```java
// etl-core: writeback/BronzeWriteBack.java
public final class BronzeWriteBack implements Serializable {

    /** statuses: one row per Bronze doc: _bronze_id, _read_version, status, error_code, error_message,
     *  quarantine_ref, targets (array<struct>) */
    public WriteBackStats apply(Dataset<Row> statuses, BronzeSource src, RunContext ctx) {
        if (ctx.isDryRun()) return WriteBackStats.skipped();

        Dataset<Row> perPartition = statuses
                .repartition(ctx.writeBackParallelism())                           // e.g. 8 → 8 parallel Mongo writers
                .mapPartitions((MapPartitionsFunction<Row, Row>) rows -> {
                    MongoCollection<Document> coll = MongoClientHolder.get(src.connectionUri()) // ➕ one client per executor
                            .getDatabase(src.database()).getCollection(src.collection())
                            .withWriteConcern(WriteConcern.MAJORITY);
                    long sent = 0, matched = 0;
                    List<WriteModel<Document>> buf = new ArrayList<>(1000);
                    while (rows.hasNext()) {
                        buf.add(toUpdate(rows.next(), ctx));
                        if (buf.size() == 1000) { matched += flush(coll, buf); sent += 1000; buf.clear(); }
                    }
                    if (!buf.isEmpty()) { sent += buf.size(); matched += flush(coll, buf); }
                    return List.of(RowFactory.create(sent, matched)).iterator();
                }, Encoders.row(WriteBackStats.SCHEMA));

        return WriteBackStats.from(perPartition.agg(sum("sent"), sum("matched")).first());  // ➕ no accumulators
    }

    private WriteModel<Document> toUpdate(Row r, RunContext ctx) {
        Bson filter = and(eq("_id", r.getAs("_bronze_id")),
                          eq("_etl.sourceVersion", r.<Long>getAs("_read_version")));    // optimistic: unchanged since read
        return new UpdateOneModel<>(filter, SyncSilverUpdates.forStatus(r, ctx));     // $set / $inc per section 9.2
    }

    private long flush(MongoCollection<Document> coll, List<WriteModel<Document>> buf) {
        return coll.bulkWrite(buf, new BulkWriteOptions().ordered(false)).getMatchedCount();
    }
}
```

- `matched < sent` → those documents changed in Bronze during the run (new events). They keep their new `PENDING` status, which is correct. The difference is reported as `writeback_stale` in reconciliation. It is information, not an error.
- Throughput: unordered bulk updates of 1,000, with 8 parallel writers, typically reach tens of thousands of updates per second. Tune `writeBackParallelism` per environment so the Mongo primary is not overloaded.
- Infra errors during write-back are retried (driver retryable writes + a bounded retry loop). If they still fail, the job ends `PARTIAL` with `WRITEBACK_INCOMPLETE`: Silver is correct and the next run heals Bronze.

---

## 10. Dimensional Modeling in Silver

### 10.1 Role of each layer

| Layer | Modeling style | Keys | History |
|---|---|---|---|
| **Silver** | Cleansed, typed, **integrated entities** (close to 3NF): one table per business entity, conformed codes | **Natural keys** (source ids) | SCD1 (current state) by default; **SCD2 captured here when the source overwrites and the business needs history** |
| **Gold** | **Star schema**: facts + dimensions, marts, aggregates | **Surrogate keys** | SCD1/SCD2 dimensions **derived** from Silver |

**Rule of thumb:** Silver answers "what is the clean truth about each entity, and how did it change?" Gold answers "how do I analyze it fast?"

### 10.2 What becomes "dimension-like" in Silver

| Entity type | Silver treatment | Example |
|---|---|---|
| **Reference / code data** | `silver_ref.*` tables: standard codes + source-value mappings, maintained as **seed data** (Flyway repeatable migrations or CSV seeds) ➕ | `order_status_map`, `country`, `currency`, `payment_method` |
| **Master entities** | Clean entity table (SCD1, current), optionally `_hist` (SCD2) | `customer`, `product`, `store`, `employee` |
| **Transactional entities** | Entity tables + child tables, no history (events are immutable or SCD1) | `sales_order`, `payment`, `shipment` |
| **Derived dimensions** (date, junk, mini-dims) | **Gold only** | `dim_date`, `dim_order_flags` |

### 10.3 Natural vs surrogate keys in Silver

| Option | Pros | Cons |
|---|---|---|
| **Natural keys** (chosen): `customer_id` = source `_id` string; compound with `source_service` when entities are integrated | Simple, readable, stable across reruns and rebuilds; no key lookups when loading children | Wider keys (varchar) |
| Sequence surrogate keys in Silver | Narrow integer keys | Keys depend on load order; a rebuild gives different keys; every child load needs a lookup |
| Deterministic hash keys (`xxhash64(source_service, source_id)`) ➕ | Narrow, stable across rebuilds, no lookups | Tiny collision risk (needs a uniqueness check); not human-readable |

**Recommendation:** natural keys in Silver. A deterministic hash key column (`customer_key bigint`) can be added where an integer key helps joins. Sequence-based surrogate keys are created in Gold (Prompt 04).

### 10.4 SCD in Silver

| Use | When |
|---|---|
| **Type 1** (overwrite; default) | Corrections; attributes where only the current value matters; transactional entities |
| **Type 2** (new row per change) | The business needs "as it was" (customer segment/address at order time, product price history, employee department) **and the source system overwrites it**. ➕ History must be captured as early as possible: if Silver overwrites it, Gold can never rebuild it |

**Source of versions for SCD2:** with `keepHistory: true` on the Bronze route (doc 02, section 5.4), Silver builds the SCD2 timeline from **every** source version in `__history`. Without it, Silver sees only the latest state per run, so intermediate changes between two runs are lost. This is documented per entity.

**SCD2 table shape** (`silver_customer.customer_hist`):

```sql
CREATE TABLE silver_customer.customer_hist (
    customer_id      varchar(24)  NOT NULL,
    valid_from       timestamptz  NOT NULL,          -- source event time of the version
    valid_to         timestamptz,                    -- NULL = current
    is_current       boolean      NOT NULL,
    version_no       integer      NOT NULL,
    segment          varchar(20),
    city             varchar(100),
    tier             varchar(20),
    _scd_hash        char(64)     NOT NULL,          -- hash of the TRACKED columns only
    _src_version     bigint       NOT NULL,
    _is_deleted      boolean      NOT NULL DEFAULT false,
    _etl_run_id      varchar(100) NOT NULL,
    _etl_updated_at  timestamptz  NOT NULL DEFAULT now(),
    CONSTRAINT pk_customer_hist PRIMARY KEY (customer_id, valid_from)
);
CREATE UNIQUE INDEX ux_customer_hist_current ON silver_customer.customer_hist (customer_id) WHERE is_current;
```

```yaml
scd2:
  table: silver_customer.customer_hist
  key: [customer_id]
  trackedColumns: [segment, city, tier]     # a change here creates a new version
  type1Columns: [email, phone]              # corrections overwrite the current version only
  validFrom: _src_event_ts
  versionsFrom: BRONZE_HISTORY              # BRONZE_HISTORY | LATEST_STATE
```

**➕ Timeline rebuild for touched keys (handles backdated / out-of-order versions).** Instead of the classic "close the current row, insert a new one" (which breaks when an older version arrives late), the framework recomputes the timeline only for the keys touched in this run:

```sql
-- inside the Silver transaction
CREATE TEMP TABLE tl ON COMMIT DROP AS
WITH versions AS (
    SELECT customer_id, valid_from, segment, city, tier, _scd_hash, _src_version, _is_deleted
    FROM silver_customer.customer_hist
    WHERE customer_id IN (SELECT customer_id FROM stg_silver_customer.customer_hist WHERE _etl_run_id = :runId)
    UNION                                            -- UNION also removes exact duplicates
    SELECT customer_id, valid_from, segment, city, tier, _scd_hash, _src_version, _is_deleted
    FROM stg_silver_customer.customer_hist WHERE _etl_run_id = :runId
),
collapsed AS (                                       -- drop versions whose tracked values did not change
    SELECT *, LAG(_scd_hash) OVER (PARTITION BY customer_id ORDER BY valid_from) AS prev_hash
    FROM versions
)
SELECT customer_id, valid_from,
       LEAD(valid_from) OVER w                      AS valid_to,
       LEAD(valid_from) OVER w IS NULL              AS is_current,
       ROW_NUMBER()     OVER w                      AS version_no,
       segment, city, tier, _scd_hash, _src_version, _is_deleted
FROM collapsed
WHERE prev_hash IS DISTINCT FROM _scd_hash
WINDOW w AS (PARTITION BY customer_id ORDER BY valid_from);

DELETE FROM silver_customer.customer_hist
WHERE customer_id IN (SELECT DISTINCT customer_id FROM tl);
INSERT INTO silver_customer.customer_hist (customer_id, valid_from, valid_to, is_current, version_no,
       segment, city, tier, _scd_hash, _src_version, _is_deleted, _etl_run_id)
SELECT customer_id, valid_from, valid_to, is_current, version_no,
       segment, city, tier, _scd_hash, _src_version, _is_deleted, :runId FROM tl;
```

Type 1 columns are then updated on the current version only. A delete closes the timeline (`valid_to = deleted_at`, `_is_deleted` on the last version).

### 10.5 Conformed / shared entities across sources

When the same business entity comes from several sources (e.g. customer from `svc-customer` and `svc-crm`):

```
silver_mdm.customer_xref         (source_service, source_id) → master_customer_id, match_rule, matched_at
silver_mdm.customer              master_customer_id + "golden record" attributes (survivorship rules)
```

| Item | Rule |
|---|---|
| Matching | Deterministic first (same email/phone/national id after normalization), configured per entity |
| Master id | Deterministic: `xxhash64` of the first matched source key, or a sequence, stored in the crosswalk (stable over time) |
| Survivorship | Per attribute: `MOST_RECENT`, `SOURCE_PRIORITY [svc-customer, svc-crm]`, `MOST_COMPLETE` |
| Unmatched | Its own master id. Manual merge via the ops UI later (phase 2) |

This is **optional, phase 2** ➕. Start with one owning source per entity and add the crosswalk when a second source appears. Conformed **codes** (status, country, currency) are needed from day one, via `silver_ref`.

---

## 11. Performance

| Area | Practice |
|---|---|
| Mongo reads | `$match` + `$project` pushdown; partial index; `secondaryPreferred`; `SamplePartitioner` with 64 MB partitions for full loads; **single partition** for small incremental sets (< ~50k docs) to avoid partitioning overhead ➕ |
| Empty runs ➕ | Count check first (`$match` + `$limit 1`). If nothing is pending: end `SUCCESS (0)` in seconds without the heavy steps |
| Spark startup cost ➕ | For many small, frequent jobs, the runner accepts `--jobs=b2s_order,b2s_customer,...` and runs them **sequentially in one Spark application** (one session, separate locks/audits per job). This saves the 30–60 s start per job |
| Caching | `persist(MEMORY_AND_DISK_SER)` the standardized + validated dataset per target (it is used for staging, quarantine, status and counts); `unpersist()` at the end |
| Joins | `broadcast()` for reference tables; key-restricted reads for big lookups (6.2); AQE on (`spark.sql.adaptive.enabled=true`, skew join on) |
| No UDFs | All transforms and rules are built-in column expressions (Catalyst-optimized, no serialization) |
| JDBC/COPY writes | `repartition(n)` before writing, where n = the allowed DB connections (e.g. 8–16 per job, configured per env). COPY per partition. One merge transaction from the driver |
| Small batches | DB targets have no "small files" problem, but frequent tiny runs waste resources: pick a schedule that gives reasonable batches; job grouping (above); empty-run short-circuit |
| Mongo write-back | Bulk 1,000, unordered, `writeBackParallelism` per env |
| Counts | One aggregation over the cached status dataset. **No accumulators** (their values can be wrong when tasks are retried) ➕ |
| EPAS tuning | Staging UNLOGGED; `work_mem` raised for the merge session; `ANALYZE` the staging table before the merge (the planner needs statistics) ➕; indexes on PK, business keys, `_etl_updated_at`, partial index on `_sync_gold_status <> 'SUCCESS'` |
| Large Silver tables | Range partitioning (e.g. monthly on `order_ts`) only above ~100 M rows. The partition column must then be part of the PK and the conflict target |

**Spark defaults** (`config/environments/base/spark.yml`, overridable per env and per job):
```yaml
spark:
  conf:
    spark.sql.adaptive.enabled: true
    spark.sql.adaptive.coalescePartitions.enabled: true
    spark.sql.adaptive.skewJoin.enabled: true
    spark.sql.autoBroadcastJoinThreshold: 52428800      # 50 MB
    spark.serializer: org.apache.spark.serializer.KryoSerializer
    spark.sql.ansi.enabled: false                        # casts handled with try_cast (4.2)
    spark.sql.session.timeZone: UTC                      # ➕ UTC everywhere
    spark.dynamicAllocation.enabled: true
```

---

## 12. Reconciliation

### 12.1 Equations checked every run

```
(1) bronze_read  = success + partial + failed + quarantined + skipped
(2) per target:   staged = inserted + updated + unchanged
(3) writeback:    sent   = matched + stale          (stale = changed in Bronze during the run)
(4) optional:     Σ amount (Bronze, SUCCESS docs of this run) = Σ total_amount (Silver rows of this run)
```

- (1) and (2) **must** hold. A mismatch means a framework bug → job `FAILED` (`RECONCILIATION_MISMATCH`) + alert.
- (4) is configured per job (`reconcile.sumColumns`) ➕. A mismatch is a `WARN` by default (rounding/transform rules can legitimately differ), and can be made strict.

### 12.2 Config and storage

```yaml
reconcile:
  sumColumns:
    - { bronze: data.amount, silver: silver_order.sales_order.total_amount, tolerance: 0.01, severity: WARN }
  countCheck: STRICT
```

One row per run and target goes to `etl_ctl.reconciliation` (DDL in Prompt 05):

| run_id | job | target | bronze_read | success | partial | failed | quarantined | skipped | staged | inserted | updated | unchanged | wb_sent | wb_matched | wb_stale | sum_src | sum_tgt | status |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|

The same counts go to `job_run_audit` (totals) and to Prometheus.

---

## 13. Quarantine (what this job writes)

Per quarantined row/record → `etl_ctl.quarantine_record` (full DDL and the fix workflow in Prompts 05 and 07):

| Field | Content |
|---|---|
| `quarantine_id` | uuid (also written to Bronze `quarantineRef`) |
| `job_name`, `run_id`, `target_table` | Where it failed |
| `source_service`, `source_collection`, `source_id`, `source_version` | Pointer to the Bronze document (the data itself stays in Bronze) |
| `errors` | `jsonb` array `[{code, column, rule_id, severity, message}]`; **no field values** ➕ |
| `row_snapshot` | Optional, **masked/encrypted** `jsonb` of the failing Silver row (helps the fixer; PII masked per the contract) |
| `status` | `QUARANTINED → FIXED → READY_TO_REPROCESS → REPROCESSED / REJECTED / SUPERSEDED` |
| `created_at`, `updated_at`, `fixed_by`, `reprocess_run_id` | Audit |

➕ **Auto-supersede:** when a new source version of a quarantined document arrives (ingestion resets it to `PENDING`), the next B→S run marks the open quarantine record `SUPERSEDED`. Nobody has to fix outdated data.

---

## 14. Job Config: Complete Example

`pipelines/bronze-to-silver/order/b2s_order.yml`
```yaml
job:
  name: b2s_order
  layer: BRONZE_TO_SILVER
  domain: order
  owner: team-order-data
  enabled: true
  description: "Orders, items, discounts, payments and tags from svc-order"

source:
  datasource: bronze_mongo
  database: bronze_svc_order
  collection: orders
  contract: contracts/svc-order/orders.contract.yml
  staticFilter: { "data.isTestOrder": { $ne: true } }      # pushed into $match
  maxRecordsPerRun: 2000000
  maxRetries: 5

preFilter:
  - { expr: "data.channel IS NULL OR data.channel <> 'INTERNAL_TEST'", reason: TEST_CHANNEL }

targets: [ ... see section 3.5 ... ]
childFailurePolicy: PARTIAL

dq:
  ruleSets: [_common/keys_not_null, order/sales_order, order/sales_order_item]

dedupe:
  businessKey: [order_no]
  strategy: LATEST_VERSION
  orderBy: [_src_event_ts DESC, _bronze_id DESC]

lookups:
  - { name: order_status, table: silver_ref.order_status_map, keys: [source_service, source_value], broadcast: true }

deleteStrategy: SOFT

thresholds: { maxFailedPct: 2.0, maxFailedCount: 5000, minRecordsForPct: 100, onBreach: ROLLBACK }

write:
  method: COPY                     # COPY | JDBC_BATCH
  parallelism: 8
  transactionScope: JOB            # all targets in one transaction

writeBack:
  parallelism: 8
  batchSize: 1000

reconcile:
  countCheck: STRICT
  sumColumns:
    - { bronze: data.amount, silver: silver_order.sales_order.total_amount, tolerance: 0.01, severity: WARN }

schedule: "15 * * * *"             # hourly at :15 (overridable per env)
dependsOn: [b2s_customer]
resources: { executors: 4, executorMemory: 4g, executorCores: 2 }   # env overrides possible
plugins: []
```

---

## 15. Spark Java Skeleton

### 15.1 Packages involved (in `etl-core`)

```
etl-core/src/main/java/com/company/etl/core/
├── runner/EtlJobRunner.java              # main(): args → RunContext → job type → run
├── pipeline/AbstractEtlJob.java          # template method: lifecycle, audit, lock, error handling
├── pipeline/BronzeToSilverJob.java       # the B→S step sequence
├── runmode/ReadPlanFactory.java          # run mode → ReadPlan ($match)
├── io/reader/BronzeReader.java
├── modeling/TableSplitter.java
├── transform/Standardizer.java           # transforms + try_cast + row hash + unmapped
├── dq/DqEngine.java, dq/RuleFactory.java, dq/rules/*
├── transform/Deduplicator.java
├── transform/LookupEnricher.java         # lookups + referential checks
├── pipeline/StatusClassifier.java        # row → record → job thresholds
├── io/writer/StagingWriter.java          # COPY / JDBC batch into stg_*
├── io/writer/SilverMergeExecutor.java    # one transaction: merge, orphans, deletes, SCD2
├── modeling/Scd2TimelineBuilder.java     # SQL generation for 10.4
├── quarantine/QuarantineWriter.java
├── writeback/BronzeWriteBack.java
├── control/Reconciler.java               # equations + reconciliation rows
├── lineage/LineageEmitter.java           # OpenLineage (Prompt 10)
└── metrics/JobMetrics.java
```

### 15.2 Template method (lifecycle shared by all job types)

```java
// pipeline/AbstractEtlJob.java
public abstract class AbstractEtlJob {

    protected final RunContext ctx;           // job config, run mode, env, runId, cutoff, parentRunId, ids, dry-run
    protected final ControlStore control;     // etl-control: audit, lock, watermark, dq, reconciliation

    public final JobResult run() {
        JobLock lock = control.locks().acquire(ctx.jobName(), ctx.runId());      // fails fast if already running
        control.audit().started(ctx);
        try {
            JobResult result = execute();                                           // subclass steps
            control.audit().finished(ctx, result);
            return result;
        } catch (ThresholdBreachedException e) {
            control.audit().failed(ctx, ErrorCode.THRESHOLD_BREACHED, e);
            throw e;
        } catch (Exception e) {                                                     // infra / unexpected
            control.audit().failed(ctx, ErrorCode.classify(e), e);
            throw new JobFailedException(ctx, e);                                   // non-zero exit → Airflow retry
        } finally {
            lock.release();
            ctx.metrics().flush();
        }
    }

    protected abstract JobResult execute();
}
```

### 15.3 Bronze → Silver steps

```java
// pipeline/BronzeToSilverJob.java
public final class BronzeToSilverJob extends AbstractEtlJob {

    @Override
    protected JobResult execute() {
        B2SConfig cfg = ctx.config(B2SConfig.class);
        ReadPlan plan = ReadPlanFactory.forMode(ctx, cfg);

        // 1. READ
        if (reader.isEmpty(cfg.source(), plan)) return JobResult.empty(ctx);
        Dataset<Row> bronze = reader.read(spark, cfg.source(), plan, contracts.schemaFor(cfg.source()));

        // 2. PRE-FILTER
        FilterResult filtered = preFilter.apply(bronze, cfg.preFilter());               // kept + skipped(reason)

        // 3. SPLIT
        Map<String, Dataset<Row>> rows = splitter.split(filtered.kept(), cfg.targets());

        // 4–7. per target: STANDARDIZE → VALIDATE → DEDUPE → ENRICH
        Map<String, Dataset<Row>> checked = new LinkedHashMap<>();
        for (TargetSpec t : cfg.targets()) {
            Dataset<Row> df = standardizer.apply(rows.get(t.name()), t);               // try_cast errors, row hash, _unmapped
            df = dqEngine.apply(df, ruleFactory.rulesFor(t, cfg.dq()), dqCtx(t));
            df = deduplicator.apply(df, t, cfg.dedupe());                              // losers flagged SKIPPED
            df = lookups.apply(df, t, cfg.lookups());                                  // lookups + referential errors
            checked.put(t.name(), df.persist(StorageLevel.MEMORY_AND_DISK_SER()));
        }

        // 8. CLASSIFY: row → record status (+ targets[]), then job thresholds
        Dataset<Row> recordStatus = classifier.recordStatuses(checked, filtered.skipped(), cfg.childFailurePolicy())
                                              .persist(StorageLevel.MEMORY_AND_DISK_SER());
        RunCounts counts = classifier.counts(recordStatus);
        dqResults.store(ctx, checked);                                                 // stored even if we roll back
        classifier.enforceThresholds(counts, cfg.thresholds());                        // throws ThresholdBreachedException

        // 9–10. DELETES + WRITE (skipped in dry-run)
        MergeStats mergeStats = MergeStats.none();
        if (!ctx.isDryRun()) {
            Map<String, Dataset<Row>> writable = classifier.writableRows(checked, recordStatus);   // OK rows of writable docs
            writable = deletes.apply(writable, cfg.deleteStrategy());
            stagingWriter.load(ctx, cfg.targets(), writable, cfg.write());             // TRUNCATE + COPY into stg_*
            mergeStats = mergeExecutor.mergeInOneTransaction(ctx, cfg);                // COMMIT here
        }

        // 11–12. QUARANTINE + WRITE-BACK (only after commit)
        quarantine.write(ctx, checked, recordStatus);
        WriteBackStats wb = writeBack.apply(classifier.writeBackRows(recordStatus), cfg.source(), ctx);

        // 13. RECONCILE + finish
        ReconciliationResult rec = reconciler.check(ctx, counts, mergeStats, wb, cfg.reconcile());
        lineage.emit(ctx, cfg);
        checked.values().forEach(Dataset::unpersist);
        recordStatus.unpersist();
        return JobResult.of(ctx, counts, mergeStats, wb, rec);                         // status rules: Prompt 07
    }
}
```

### 15.4 Classifier core (row → record)

```java
// pipeline/StatusClassifier.java (excerpt)
Column rowOutcome = when(exists(col("_errors"), e -> e.getField("severity").equalTo("QUARANTINE")), lit("QUARANTINE"))
        .when(exists(col("_errors"), e -> e.getField("severity").equalTo("ERROR")
                                           .and(isRetryable(e.getField("code")))), lit("FAILED"))
        .when(exists(col("_errors"), e -> e.getField("severity").equalTo("ERROR")), lit("QUARANTINE"))
        .otherwise(lit("OK"));

// per Bronze doc: worst outcome over parent + children, respecting childFailurePolicy
Dataset<Row> perDoc = unionOfTargets(checked)                     // _bronze_id, _read_version, target, is_parent, row_outcome, first_error
        .groupBy("_bronze_id", "_read_version")
        .agg(max(when(col("is_parent"), rank(col("row_outcome")))).as("parent_rank"),
             max(when(not(col("is_parent")), rank(col("row_outcome")))).as("child_rank"),
             collect_list(struct("target", "row_outcome", "first_error_code")).as("targets"));
// → status = SUCCESS / PARTIAL / FAILED / QUARANTINED per the table in 7.2
```

`isRetryable(code)` comes from `error_code_catalog` (loaded once from the control DB and broadcast as a small map).

---

## 16. Silver DDL Example (with standard technical columns)

```sql
-- db/silver/V3__silver_order_sales_order.sql
CREATE SCHEMA IF NOT EXISTS silver_order;
CREATE SEQUENCE IF NOT EXISTS silver_order.etl_change_seq;     -- one per Silver schema (doc 04 §5.1)

CREATE TABLE silver_order.sales_order (           -- ➕ "order" is a reserved word: avoid it as a table name
    order_id                  varchar(24)    NOT NULL,
    order_no                  varchar(30)    NOT NULL,
    order_ts                  timestamptz    NOT NULL,
    status_code               varchar(20)    NOT NULL,
    currency_code             char(3)        NOT NULL,
    total_amount              numeric(18,2)  NOT NULL,
    customer_id               varchar(24),
    customer_tier_at_order    varchar(20),
    ship_addr_line1           varchar(200),
    ship_addr_city            varchar(100),
    ship_addr_country         char(2),

    -- source lineage
    _src_service              varchar(50)    NOT NULL,
    _src_collection           varchar(100)   NOT NULL,
    _src_id                   varchar(64)    NOT NULL,
    _src_version              bigint         NOT NULL,
    _src_event_ts             timestamptz    NOT NULL,
    _row_hash                 char(64)       NOT NULL,
    -- delete + quality
    _is_deleted               boolean        NOT NULL DEFAULT false,
    _deleted_at               timestamptz,
    _delete_reason            varchar(30),
    _dq_warnings              jsonb,
    _unmapped                 jsonb,
    -- ETL audit
    _etl_job_name             varchar(100)   NOT NULL,
    _etl_run_id               varchar(100)   NOT NULL,
    _etl_inserted_at          timestamptz    NOT NULL DEFAULT now(),
    _etl_updated_at           timestamptz    NOT NULL DEFAULT now(),
    -- Gold sync (design in doc 04 §5): change number for Gold bookmarks + summary status over all Gold consumers
    _etl_change_seq           bigint         NOT NULL DEFAULT nextval('silver_order.etl_change_seq'),
    _sync_gold_status         varchar(15)    NOT NULL DEFAULT 'PENDING',
    _sync_gold_updated_at     timestamptz,
    _sync_gold_run_id         varchar(100),
    _sync_gold_error_code     varchar(50),
    _sync_gold_error_message  varchar(2000),
    _sync_gold_failed_jobs    text[],

    CONSTRAINT pk_sales_order PRIMARY KEY (order_id)
);

CREATE UNIQUE INDEX ux_sales_order_order_no ON silver_order.sales_order (order_no) WHERE NOT _is_deleted;
CREATE INDEX ix_sales_order_customer      ON silver_order.sales_order (customer_id);
CREATE INDEX ix_sales_order_etl_updated   ON silver_order.sales_order (_etl_updated_at);
CREATE INDEX ix_sales_order_change_seq    ON silver_order.sales_order (_etl_change_seq);
CREATE INDEX ix_sales_order_gold_pending  ON silver_order.sales_order (_sync_gold_status) WHERE _sync_gold_status <> 'SUCCESS';

CREATE TABLE silver_order.sales_order_item (
    order_id      varchar(24)    NOT NULL,          -- logical FK → sales_order (not enforced, see 6.2)
    line_id       integer        NOT NULL,
    sku           varchar(40)    NOT NULL,
    qty           integer        NOT NULL,
    unit_price    numeric(18,2)  NOT NULL,
    -- same technical column block as above (_src_*, _row_hash, _is_deleted, _etl_*, _sync_gold_*)
    CONSTRAINT pk_sales_order_item PRIMARY KEY (order_id, line_id)
);
```

**Silver naming and type rules:**

| Rule | Convention |
|---|---|
| Schema | `silver_<domain>` (e.g. `silver_order`, `silver_customer`), plus `silver_ref` and later `silver_mdm` |
| Tables | snake_case, singular business names, no reserved words |
| Technical columns | Prefix `_` (`_src_*`, `_etl_*`, `_sync_gold_*`, `_is_deleted`, ...): identical on every Silver table, added by the framework's DDL template |
| Mongo → EPAS types | ObjectId → `varchar(24)`; Decimal128 → `numeric(p,s)` (**money never as double**); Date → `timestamptz` (UTC); int32 → `integer`; int64 → `bigint`; double → `double precision` (measurements only); bool → `boolean`; free-form → `jsonb` |
| Staging | `stg_<schema>.<table>`, UNLOGGED, auto-created |

---

## 17. ➕ Added by Architect (summary)

| # | Addition | Why |
|---|---|---|
| S1 | Run cutoff + `maxRecordsPerRun` | Fixed input per run, exact reconciliation, controlled backlog draining |
| S2 | Explicit Spark schema from the data contract | No unstable schema inference |
| S3 | `try_cast` + cast-failure errors | Spark silently turns bad values into NULL otherwise |
| S4 | Vectorized `_errors` array (one pass, no UDFs) | Scales to 100+ rules and millions of rows |
| S5 | FAILED = retryable only; non-retryable → QUARANTINED at once; auto-supersede on new source versions | Clear meaning, no useless retries, self-healing quarantine |
| S6 | Retry window for late-arriving parents | Cross-collection timing without false quarantines |
| S7 | No enforced FKs in Silver; DQ referential rules instead | Late-arriving data must not break loads |
| S8 | Child orphan handling | Removed array elements would stay alive otherwise |
| S9 | Version **+ row-hash** guard in the merge | Idempotent reruns, no backwards updates, and code fixes still reach Silver |
| S10 | One transaction for all targets; UNLOGGED staging; COPY | Consistency and speed |
| S11 | Silver commit **before** Bronze write-back | A failure can never mark unwritten data as synced |
| S12 | Circuit breaker on the failure threshold (rollback, no write-back) | A systemic bug must not quarantine thousands of good records |
| S13 | SCD2 captured in Silver from `__history`; timeline rebuild for touched keys | History can't be recovered later; backdated versions are handled correctly |
| S14 | `_unmapped jsonb` | New source fields are never lost |
| S15 | `silver_ref` seed data; MDM crosswalk (phase 2) | Conformed codes and entities |
| S16 | Empty-run short-circuit; job grouping in one Spark app | Saves cluster time for frequent small jobs |
| S17 | No accumulators for counts; `ANALYZE` staging before the merge | Correct counts under task retries; good merge plans |
| S18 | Error messages without values; masked quarantine snapshots | PII safety |

---

## 18. Changes to Earlier Decisions

| Earlier | Now | Reason |
|---|---|---|
| Doc 02: `FAILED` = any Silver error, retried until `maxRetries` | `FAILED` = **retryable** errors only; non-retryable → `QUARANTINED` immediately; retries exhausted → `QUARANTINED` (`RETRIES_EXHAUSTED`) | Retrying deterministic data errors is useless |
| Doc 02 diagram: "rerun-failed retries only the failed targets" | The whole document is re-processed; the row-hash guard makes the already-written rows no-ops | Simpler code, same effect |
| Doc 02: `syncSilver.status` values | + `SUPERSEDED` exists only on the **quarantine record**, not in Bronze | Bronze returns to `PENDING` on a new version |

---

## 19. Open Points for Later Prompts

- ✅ **Resolved in doc 04 §5:** Silver tables are read by several Gold jobs, so Gold sync uses a change number (`_etl_change_seq`) + a bookmark per Gold job, a per-job failure list (`etl_ctl.gold_record_status`) and summary `_sync_gold_*` columns on the Silver row.
- **Prompt 05:** DDL for `quarantine_record`, `dq_results`, `reconciliation`, `job_lock`, `error_code_catalog` (with the `retryable` flag).
- **Prompt 06:** full class design of `etl-core` and the job config JSON Schema.
- **Prompt 07:** complete job status rules and every run mode end to end.

---

## 20. Key Decisions

1. **Generic, config-driven `BronzeToSilverJob`**; one owning B→S job per Bronze collection; several Silver targets per job.
2. **Read:** Mongo Spark connector, explicit contract schema, `$match`/`$project` pushdown, `secondaryPreferred`, run cutoff, optional `maxRecordsPerRun`, empty-run short-circuit. The run mode only changes the ReadPlan.
3. **Steps:** read → pre-filter → split → standardize/`try_cast` → DQ → dedupe → lookups/referential → classify (+ circuit breaker) → deletes → stage + merge → quarantine → write-back → reconcile.
4. **Splitting:** 1:1 sub-docs flattened; arrays → child/grandchild tables with full ancestor key paths; deterministic child keys (natural → hash → position); orphan handling; embedded masters → FK + "as of" attributes; blobs stay in Bronze; unknown fields → `_unmapped`.
5. **DQ:** YAML rule sets → Spark columns → `_errors` array; severities WARN / ERROR (retryable → FAILED) / QUARANTINE; results per rule → `etl_ctl.dq_results`.
6. **Statuses:** `SUCCESS`, `PARTIAL`, `FAILED` (retryable), `QUARANTINED`, `SKIPPED`; `childFailurePolicy` PARTIAL | FAIL_PARENT; quarantine auto-superseded by new source versions.
7. **Write:** UNLOGGED staging via COPY → one EPAS transaction with `INSERT … ON CONFLICT` guarded by `_src_version` and `_row_hash`; orphans; delete strategy SOFT/HARD/IGNORE; SCD2 timeline rebuild where configured.
8. **Write-back:** after the commit only; bulk conditional `updateOne` on `{_id, _etl.sourceVersion}`; stale matches reported, not errors.
9. **Modeling:** Silver = integrated entities with natural keys, `silver_ref` conformed codes, SCD2 captured in Silver when needed; star schema and surrogate keys in Gold; MDM crosswalk in phase 2.
10. **Job failure:** infra error → rollback + FAILED; threshold breach → rollback + FAILED (no write-back); otherwise PARTIAL/SUCCESS.
11. **Reconciliation:** count equations (strict), insert/update/unchanged per target, write-back matched/stale, optional sum checksums → `etl_ctl.reconciliation`.
12. **Performance:** pushdown, partial indexes, broadcast refs, key-restricted big lookups, AQE, no UDFs, cached intermediate datasets, COPY, controlled DB parallelism, job grouping, UTC session time zone.
