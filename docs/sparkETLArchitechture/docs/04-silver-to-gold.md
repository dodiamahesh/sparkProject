# 04 — Silver → Gold ETL (Dimensional Modeling)

> Builds on [01](01-architecture-and-structure.md), [02](02-ingestion-bronze.md) and [03](03-bronze-to-silver.md).
> This document is written in **simple language**. Every new term is explained the first time it is used, and each section starts with an **"In simple words"** box. The technical details (DDL, SQL, Java, YAML) are still complete.

---

## 0. First, the Basic Idea of Gold

> **In simple words**
> Silver is like a **clean, well-organized store room**: every item has its place, it is correct and complete. But when a manager asks "How much did we sell per city per month?", searching the store room every time is slow.
> Gold is the **showroom**: data is arranged in the exact shape that reports and dashboards need, so questions are answered fast. Gold does not invent new data. It **re-arranges Silver data for analysis**.

### 0.1 Mini glossary (read this first)

| Term | Meaning in simple words | Example |
|---|---|---|
| **Fact** | A table of **events or measurements**: things that happened, with numbers you can add up | "Order line: 2 units of product A1 sold for ₹1000" |
| **Measure** | A **number** in a fact that you sum, count or average | `quantity`, `net_amt` |
| **Dimension** | A table that **describes** the who / what / where / when of a fact. You filter and group by it | Customer, Product, Store, Date |
| **Grain** | The exact meaning of **one row** in a fact table. Always decided first | "One row = one order line" |
| **Star schema** | One fact table in the middle, dimension tables around it like the points of a star | See section 3 |
| **Natural key** | The ID that comes from the source system | `customer_id = "64aa…01"` (Mongo `_id`) |
| **Surrogate key (SK)** | A **new number created in Gold** to identify a dimension row | `customer_sk = 1052` |
| **SCD** (Slowly Changing Dimension) | The rule for what to do when a dimension attribute changes (e.g. a customer moves city) | Type 1 / 2 / 3 (section 6) |
| **Conformed dimension** | One shared dimension used by **many** facts, so all reports agree | One `dim_customer` for sales, returns and payments |
| **Degenerate dimension** | An ID kept directly on the fact, with no dimension table of its own | `order_no` on the sales fact |
| **Junk dimension** | A small dimension that groups many **yes/no flags and small codes** | is_gift + is_express + channel |
| **Inferred member** | A **placeholder** dimension row created when a fact arrives before its dimension | "Customer 64aa…99 (details not yet known)" |
| **Watermark / bookmark** | A saved "I have processed up to here" marker | "Last change number processed = 58,201" |
| **Aggregate table** | A pre-computed summary for fast dashboards | Sales per store per day |

---

## 1. Summary of Decisions

| Topic | Decision | One-line reason |
|---|---|---|
| Where Gold lives | **EPAS only.** Mongo only as an optional "serving copy" later | BI tools need SQL, joins and aggregation |
| Schema style | **Star schema** (snowflake only in rare cases) | Simple for users, fast for queries |
| Gold schemas | `gold_core` (shared/conformed dimensions) + one schema per subject area (`gold_sales`, `gold_finance`, …) | Shared things in one place, each business area separate |
| Job types | 4 generic, config-driven job types: **dimension**, **fact**, **snapshot**, **aggregate** | 100+ Gold tables, but only 4 kinds of code |
| How Gold finds new Silver data | **Change number (`_etl_change_seq`) on every Silver row + a bookmark per Gold job** | Works even when many Gold jobs read the same Silver table |
| Write-back to Silver | Per-job failure list (`etl_ctl.gold_record_status`) + **summary status columns on the Silver row** (`_sync_gold_*`) | Correct per job, and still easy to see on the Silver row |
| Surrogate keys | Created **by EPAS** (`GENERATED ALWAYS AS IDENTITY`) during the merge; `-1` = Unknown, `-2` = Not applicable | Safe, unique, never changes on reruns |
| Key lookup for facts | Done **inside EPAS** during the fact merge (SQL join to the dimensions, "as of" the event time) | No need to copy big dimensions into Spark |
| SCD | Per column: Type 1, Type 2 or Type 3, configured in YAML | Each attribute gets the right rule |
| Late dimensions | Inferred members, filled in later | Facts are never lost or blocked |
| Load order | dimensions → facts → snapshots → aggregates | A fact needs its dimensions first |
| Aggregates | SQL **inside EPAS**, recomputing only the **affected dates** ("dirty partitions") | Fast and always consistent with the fact |
| Deletes | Dimensions: never physically deleted (flag/close). Facts: `SOFT` (default), `HARD` or `REVERSAL` | Old facts must still find their dimension rows |

---

## 2. Gold in EPAS, Mongo or Both?

> **In simple words**
> Gold is used by **people asking questions**: "total sales by month", "top 10 products in Gujarat". These questions need **joins** (fact + dimensions) and **group-by** sums. Relational databases like EPAS are built exactly for this. MongoDB is great for storing and fetching **one document at a time**, but weak at big joins and group-bys.

| Need | EPAS | MongoDB |
|---|---|---|
| BI tools (Power BI, Tableau, Superset, Metabase) | Native SQL connectors | Limited, needs special connectors |
| Joins between fact and 6–10 dimensions | Very good (indexes, planner) | Weak (`$lookup` is slow at scale) |
| Group-by over millions of rows | Good (parallel query, partitions) | Possible, but slower and harder |
| Constraints (unique keys, current-version uniqueness) | Yes | Limited |
| OpenMetadata lineage + data quality | Postgres connector (PoC first, doc 01) | Supported, but less useful for star schemas |
| Fast "one entity" lookups for apps | OK | Excellent |

**Recommendation: Gold = EPAS only.**

➕ **Optional later: a Mongo "serving copy".** If a microservice ever needs precomputed data per entity (for example a "customer 360" document shown in an app screen), add a small export job **from Gold EPAS views → MongoDB**. EPAS stays the single source of truth; Mongo is only a fast read copy. Don't build this until a real use case exists.

---

## 3. Sample Star Schema

> **In simple words**
> Put the **fact** (what happened) in the middle. Around it, put the **dimensions** (who, what, where, when). Every line from the fact to a dimension is a **surrogate key** column in the fact.

```
                                   ┌───────────────────────┐
                                   │   gold_core.dim_date   │  (when: order date, ship date)
                                   │   date_key (20261029)  │
                                   └───────────┬───────────┘
                                               │ order_date_key, ship_date_key  (same dim used twice = "role-playing")
 ┌────────────────────────┐                    │                    ┌────────────────────────┐
 │ gold_core.dim_customer │                    │                    │ gold_core.dim_product  │
 │ customer_sk  (SCD2)    │───┐                │                ┌───│ product_sk   (SCD1)    │
 │ customer_id, city,     │   │                │                │   │ sku, name, category    │
 │ segment, tier ...      │   │                ▼                │   └────────────────────────┘
 └────────────────────────┘   │   ┌─────────────────────────────┴──┐
                              └──►│   gold_sales.fact_sales_line   │◄──┐
 ┌────────────────────────┐       │   GRAIN: one row per order line │   │  ┌──────────────────────────────┐
 │ gold_core.dim_store    │──────►│   order_date_key   ship_date_key│   └──│ gold_sales.dim_order_profile │
 │ store_sk, city, region │       │   customer_sk  product_sk       │      │ (junk dim: channel, is_gift, │
 └────────────────────────┘       │   store_sk  order_profile_sk    │      │  is_express, payment_method) │
                                  │   order_no  (degenerate dim)    │      └──────────────────────────────┘
                                  │   quantity, gross_amt,          │
                                  │   discount_amt, net_amt         │  ← measures
                                  └────────────────────────────────┘

 Summaries built from the fact (section 10):
   gold_sales.agg_sales_daily_store        (date × store)
   gold_sales.agg_sales_monthly_category   (month × product category)
```

**Example question:** "Net sales of GOLD-tier customers in Ahmedabad, per month, for 2026"
→ `fact_sales_line` joined to `dim_customer` (tier, city) and `dim_date` (month, year), `SUM(net_amt)`, grouped by month. One simple query.

---

## 4. Star vs Snowflake

> **In simple words**
> - **Star:** each dimension is **one flat table**. `dim_product` contains the category name and the department name directly.
> - **Snowflake:** dimensions are **split further**. `dim_product` points to `dim_category`, which points to `dim_department`. It saves a little space but needs more joins.

| | Star (recommended) | Snowflake |
|---|---|---|
| Queries | Fewer joins, simpler SQL, faster | More joins, more complex |
| Business users | Easy to understand | Confusing |
| Storage | A little duplication (category name repeated per product) | Less duplication |
| Changes to a hierarchy | Update many dimension rows | Update one row |

**Recommendation: star.** Storage is cheap; people's time and query speed are not.

**Allowed exception ("outrigger"):** if a big hierarchy is shared by many dimensions and changes often (e.g. an org/region hierarchy used by `dim_store` and `dim_employee`), it may be a separate table. This must be a documented decision (ADR), not the default.

---

## 5. How a Gold Job Finds New Silver Data, and How Status Goes Back to Silver

> **In simple words**
> In Bronze, each document had **one** owner job, so a status field on the document was enough.
> In Silver this breaks: **one Silver table is read by many Gold jobs**. For example, `silver_order.sales_order` is read by `fact_sales_line` **and** `fact_order_fulfillment`. One status column cannot say "done for job A, not done for job B".
>
> **Solution, like a reading bookmark:**
> 1. Every time a Silver row really changes, it gets a new **change number** (1, 2, 3, … always increasing).
> 2. Each Gold job keeps its own **bookmark**: "I have processed everything up to change number 58,201".
> 3. Next run: "give me all rows with a change number above 58,201". That is the new work.
> 4. Rows that failed are put on that job's **"to retry" list**, and retried next time.
> 5. For humans, the Silver row still shows a **summary status** (`_sync_gold_status`): SUCCESS when all Gold jobs are done with it, FAILED if any of them failed, PENDING otherwise.

### 5.1 Silver metadata columns for Gold sync (final)

This replaces the `_sync_gold_*` block shown in doc 03.

| Column | Type | Set by | Meaning |
|---|---|---|---|
| `_etl_change_seq` | bigint | **B→S merge**, from one DB sequence per Silver schema, on every insert/real update | The **change number**. Gold bookmarks compare against it |
| `_sync_gold_status` | varchar(15) | B→S sets `PENDING` on change; Gold jobs update the summary | `PENDING` / `SUCCESS` / `PARTIAL` / `FAILED` / `QUARANTINED` (summary over all Gold consumers) |
| `_sync_gold_updated_at` | timestamptz | Gold jobs | When the summary last changed (your `syncUpdatedDate`) |
| `_sync_gold_run_id` | varchar(100) | Gold jobs | Last Gold run that touched this row |
| `_sync_gold_error_code` | varchar(50) | Gold jobs | Latest error code (if any consumer failed) |
| `_sync_gold_error_message` | varchar(2000) | Gold jobs | Latest error text (no PII values) |
| `_sync_gold_failed_jobs` | text[] | Gold jobs | ➕ Which Gold jobs failed for this row, e.g. `{s2g_fact_sales_line}` |

**Removed from doc 03:** `_sync_gold_synced_version` and `_sync_gold_retry_count`. The progress is now in the bookmark, and the retry count is kept per job in the failure list (5.3).

```sql
-- db/silver/V10__gold_sync_columns.sql (applied to every Silver table by the framework template)
CREATE SEQUENCE IF NOT EXISTS silver_order.etl_change_seq;
ALTER TABLE silver_order.sales_order
    ADD COLUMN _etl_change_seq        bigint NOT NULL DEFAULT nextval('silver_order.etl_change_seq'),
    ADD COLUMN _sync_gold_failed_jobs text[],
    DROP COLUMN _sync_gold_synced_version,
    DROP COLUMN _sync_gold_retry_count;
CREATE INDEX ix_sales_order_change_seq ON silver_order.sales_order (_etl_change_seq);
```

And in the B→S merge (doc 03, section 8.2) the `DO UPDATE SET` part adds:

```sql
    _etl_change_seq        = nextval('silver_order.etl_change_seq'),
    _sync_gold_status      = 'PENDING',
    _sync_gold_error_code  = NULL,
    _sync_gold_error_message = NULL,
    _sync_gold_failed_jobs = NULL
```

Because the B→S merge only updates rows that **really** changed (version + row-hash guard), the change number moves only on real changes. Gold never re-processes unchanged rows.

➕ **Why the bookmark is safe here (a classic trap).** With a sequence, a row can get number 100, but its transaction commits **after** another transaction with number 101. A reader that already moved its bookmark to 101 would miss row 100 forever. This cannot happen in this design, because **each Silver table has exactly one writer job, a job lock, and one transaction per run** (doc 03). Rule: never let a second process write the same Silver table outside that job (manual fixes go through the job, see Prompt 07).

### 5.2 Bookmark (watermark) per Gold job

Stored in `etl_ctl.watermark` (full DDL in Prompt 05):

| job_name | source_table | last_change_seq | updated_at | run_id |
|---|---|---|---|---|
| s2g_fact_sales_line | silver_order.sales_order_item | 58201 | 2026-10-30 09:00 | … |
| s2g_fact_sales_line | silver_order.sales_order | 41377 | 2026-10-30 09:00 | … |
| s2g_dim_customer | silver_customer.customer | 9933 | 2026-10-30 08:45 | … |

A job can have **several** source tables (a fact built from items **and** order headers), so the bookmark is kept **per job and per source table**.

**Bookmark rule:**
1. At start: `cutoff_seq = max(_etl_change_seq)` of each source table (read once).
2. Read rows with `last_change_seq < _etl_change_seq ≤ cutoff_seq` **plus** this job's retry list.
3. After a successful commit: bookmark = `cutoff_seq`. Rows that failed are on the retry list, so moving the bookmark does not lose them.
4. Job FAILED or circuit breaker: the bookmark does **not** move.

### 5.3 Per-job failure list (the "to retry" list)

```sql
-- sketch; final DDL in Prompt 05
CREATE TABLE etl_ctl.gold_record_status (
    job_name          varchar(100) NOT NULL,
    source_table      varchar(200) NOT NULL,
    row_key           jsonb        NOT NULL,          -- e.g. {"order_id":"65f1…","line_id":1}
    change_seq        bigint       NOT NULL,          -- Silver change number that failed
    status            varchar(15)  NOT NULL,          -- FAILED | QUARANTINED | PARTIAL
    error_code        varchar(50)  NOT NULL,
    error_message     varchar(2000),
    retry_count       integer      NOT NULL DEFAULT 0,
    first_failed_at   timestamptz  NOT NULL,
    quarantine_ref    uuid,
    run_id            varchar(100) NOT NULL,
    updated_at        timestamptz  NOT NULL DEFAULT now(),
    PRIMARY KEY (job_name, source_table, row_key)
);
```

- It holds **only rows that are not OK**. A successful retry deletes the entry, so the table stays small.
- `retry_count`, the retry window and "retries exhausted → QUARANTINED" work exactly as in Bronze → Silver.
- If the Silver row changes again (new `change_seq`), the job reads it through the bookmark anyway, and the old entry is replaced.

### 5.4 Updating the summary status on Silver

After each Gold run, for the Silver rows this job read, the summary is recomputed:

```sql
-- runs in the Gold job's transaction, for the keys read in this run (staged in a temp table "touched")
UPDATE silver_order.sales_order s
SET _sync_gold_status = CASE
        WHEN f.worst = 'QUARANTINED' THEN 'QUARANTINED'
        WHEN f.worst = 'FAILED'      THEN 'FAILED'
        WHEN f.worst = 'PARTIAL'     THEN 'PARTIAL'
        WHEN s._etl_change_seq <= w.min_bookmark THEN 'SUCCESS'   -- every consumer is past this change
        ELSE 'PENDING' END,
    _sync_gold_failed_jobs   = f.failed_jobs,
    _sync_gold_error_code    = f.last_error_code,
    _sync_gold_error_message = f.last_error_message,
    _sync_gold_run_id        = :runId,
    _sync_gold_updated_at    = now()
FROM touched t
CROSS JOIN LATERAL (                                  -- lowest bookmark among ALL Gold jobs that read this table
    SELECT min(wm.last_change_seq) AS min_bookmark
    FROM etl_ctl.watermark wm
    JOIN etl_ctl.job_source js ON js.job_name = wm.job_name AND js.source_table = wm.source_table
    WHERE js.source_table = 'silver_order.sales_order' AND js.active) w
LEFT JOIN LATERAL (                                   -- failures of any consumer for this row
    SELECT max(status_rank(g.status)) AS worst_rank, rank_to_status(max(status_rank(g.status))) AS worst,
           array_agg(g.job_name) AS failed_jobs,
           (array_agg(g.error_code ORDER BY g.updated_at DESC))[1]    AS last_error_code,
           (array_agg(g.error_message ORDER BY g.updated_at DESC))[1] AS last_error_message
    FROM etl_ctl.gold_record_status g
    WHERE g.source_table = 'silver_order.sales_order' AND g.row_key = jsonb_build_object('order_id', s.order_id)) f ON true
WHERE s.order_id = t.order_id;
```

`status_rank()` / `rank_to_status()` are two tiny SQL helper functions (ranking QUARANTINED > FAILED > PARTIAL). The framework generates this statement from the table's key columns.

> **In simple words:** the Silver row says SUCCESS only when **every** Gold job that uses it is done. If any job failed, it shows FAILED and names that job.

---

## 6. Dimensions

### 6.1 What a dimension row looks like

> **In simple words**
> A dimension row is a **description card** for one thing (one customer, one product). The fact table doesn't store the description, only the card number (the surrogate key). Reports then join to the card to show the name, city, segment, etc.

Every dimension has:
- `<name>_sk`: the surrogate key (card number)
- the natural key from Silver (`customer_id`)
- descriptive attributes (name, city, segment …)
- SCD columns when history is kept (`valid_from`, `valid_to`, `is_current`, `version_no`)
- flags: `is_inferred`, `is_deleted`
- technical columns (`_src_change_seq`, hashes, `_etl_*`)

### 6.2 Surrogate keys

> **In simple words**
> Why not just use the source `customer_id`?
> 1. With history (SCD2), **one customer has several rows** (one per version). Each version needs its own key.
> 2. Integer keys make fact tables **smaller and joins faster** than 24-character strings.
> 3. If a second source system with different ids is added later, the facts don't change.

| Option | Verdict | Why |
|---|---|---|
| **EPAS identity column** (`bigint GENERATED ALWAYS AS IDENTITY`) | ✅ **Chosen** | The database guarantees uniqueness, even with retries and parallel runs |
| Spark `monotonically_increasing_id()` / `row_number()` | ❌ | Different numbers on every run; collisions between runs; not safe |
| Hash of the natural key (`xxhash64`) | Only for junk dims (6.7) | Stable and no lookup needed, but no SCD2 versions and a tiny collision risk |

**Special members** (created once by a Flyway migration in every dimension):

| SK | Natural key | Used when |
|---|---|---|
| `-1` | `UNKNOWN` | The fact has **no key at all** (null customer id) |
| `-2` | `N/A` | The dimension **does not apply** (e.g. ship date for a not-yet-shipped order) |

```sql
INSERT INTO gold_core.dim_customer (customer_sk, customer_id, customer_name, valid_from, valid_to, is_current, version_no)
OVERRIDING SYSTEM VALUE VALUES
    (-1, 'UNKNOWN', 'Unknown',        '1900-01-01', '9999-12-31', true, 1),
    (-2, 'N/A',     'Not applicable', '1900-01-01', '9999-12-31', true, 1);
```

**Golden rule ➕:** surrogate keys must **never change**. Never truncate and reload a dimension that facts point to. A full reload of a dimension is done as a **merge** (section 13), which keeps the existing keys.

### 6.3 SCD types, explained with one customer

> **Story:** customer **Asha** lives in **Ahmedabad**, segment **SILVER**. On 1 Oct 2026 she moves to **Surat** and becomes **GOLD**. Her email is corrected from a typo.

**Type 1: overwrite (no history).** Use for corrections and for attributes where only today's value matters.
```
before: | sk=1052 | Asha | email=asha@gmal.com  |
after : | sk=1052 | Asha | email=asha@gmail.com |   ← same row, value replaced
```

**Type 2: new row per change (full history).** Use when reports must show the value **as it was at the time of the fact**.
```
| sk   | customer_id | city      | segment | valid_from | valid_to   | is_current | version |
| 1052 | 64aa…01     | Ahmedabad | SILVER  | 1900-01-01 | 2026-10-01 | false      | 1       |
| 2210 | 64aa…01     | Surat     | GOLD    | 2026-10-01 | 9999-12-31 | true       | 2       |
```
A September sale points to `sk=1052` (Ahmedabad, SILVER). An October sale points to `sk=2210` (Surat, GOLD). "Sales by city" is then historically correct.

**Type 3: "previous value" column (limited history).** Use when the business only needs "current and previous", e.g. to compare before and after a re-segmentation.
```
| sk   | segment | previous_segment | segment_changed_at |
| 2210 | GOLD    | SILVER           | 2026-10-01         |
```

**Mixed (per column), which is what we use:**

```yaml
scd:
  type2Columns: [city, state, segment, tier]     # history needed
  type1Columns: [email_masked, phone_masked, customer_name]   # corrections only
  type3Columns: { segment: previous_segment }    # keep the previous segment as a column
  type1Scope: ALL_VERSIONS                        # corrections fix every version (ALL_VERSIONS | CURRENT_ONLY)
```

| Type | Good for | Cost |
|---|---|---|
| 1 | Typos, contact details, names | Cheapest; history lost |
| 2 | Segment, region, price band, manager, status categories | More rows; facts must look up "as of" the event time |
| 3 | "Before vs after" comparisons | Only one level of history |

**Where does the history come from?**
- If Silver keeps an SCD2 history table (`silver_customer.customer_hist`, doc 03), Gold **copies that timeline** and adds surrogate keys. This is the best case: history is exact.
- If Silver only keeps the current state, Gold builds history from the changes **it sees per run**. Changes between two runs are merged into one. This is acceptable for most attributes and documented per dimension.

### 6.4 SCD merge: how it works step by step

> **In simple words**
> Spark compares each incoming customer with the **current** dimension row and puts a label on it:
> - **NEW**: never seen → insert version 1
> - **SCD2_CHANGE**: a Type 2 column changed → close the old row, insert a new version
> - **TYPE1_ONLY**: only a Type 1 column changed → overwrite in place
> - **INFERRED_FILL**: the row is a placeholder → fill in the real values in place
> - **DELETE**: deleted in Silver → close the current row and mark it deleted
> - **UNCHANGED**: nothing to do → skipped
> Then EPAS applies all labeled rows in **one transaction**.

**Two hashes** make the comparison cheap:
- `_scd2_hash` = hash of the Type 2 columns
- `_type1_hash` = hash of the Type 1 columns

#### Spark Java skeleton (change detection)

```java
// etl-core: modeling/scd/ScdChangeDetector.java
public final class ScdChangeDetector {

    /**
     * incoming: one row per natural key from Silver (deduped to the latest change in this run),
     *           with business columns, effective_ts, is_deleted.
     * Returns the rows to stage, each with an "action" column.
     */
    public Dataset<Row> detect(Dataset<Row> incoming, DimSpec spec, DimReader dimReader) {

        Dataset<Row> in = incoming
                .withColumn("_scd2_hash",  sha2(concat_ws("‖", spec.type2Cols()), 256))
                .withColumn("_type1_hash", sha2(concat_ws("‖", spec.type1Cols()), 256));

        // Read ONLY the current rows of the keys in this batch (key-restricted read, doc 03 §6.2)
        Dataset<Row> current = dimReader.readCurrent(spec, in.select(spec.naturalKeyCols()).distinct())
                .select(spec.naturalKeyColsAs("cur_"),
                        col(spec.skCol()).as("cur_sk"),
                        col("valid_from").as("cur_valid_from"),
                        col("is_inferred").as("cur_is_inferred"),
                        col("_scd2_hash").as("cur_scd2_hash"),
                        col("_type1_hash").as("cur_type1_hash"));

        Dataset<Row> joined = in.join(current, spec.joinOnNaturalKey(in, current), "left");

        Column action =
             when(col("cur_sk").isNull(), when(col("is_deleted"), lit("UNCHANGED")).otherwise(lit("NEW")))
            .when(col("is_deleted"), lit("DELETE"))
            .when(col("cur_is_inferred"), lit("INFERRED_FILL"))
            .when(col("_scd2_hash").notEqual(col("cur_scd2_hash")),
                  when(col("effective_ts").lt(col("cur_valid_from")), lit("BACKDATED"))
                 .otherwise(lit("SCD2_CHANGE")))
            .when(col("_type1_hash").notEqual(col("cur_type1_hash")), lit("TYPE1_ONLY"))
            .otherwise(lit("UNCHANGED"));

        return joined.withColumn("action", action)
                     .where(col("action").notEqual("UNCHANGED"))
                     .withColumn("action", when(col("action").equalTo("BACKDATED"),
                                                spec.backdatedPolicy().asColumn())   // see 6.5
                                           .otherwise(col("action")));
    }
}
```

```java
// etl-core: pipeline/SilverToGoldDimensionJob.java (core of execute())
Dataset<Row> changed  = silverReader.readChanged(ctx, spec.sources());        // bookmark + retry list (§5)
Dataset<Row> mapped   = dimMapper.apply(changed, spec);                       // Silver columns → dim columns
Dataset<Row> checked  = dqEngine.apply(mapped, rules.forDim(spec), dqCtx);    // same DQ framework as doc 03
Dataset<Row> latest   = deduplicator.latestPerKey(okRows(checked), spec);     // one row per natural key
Dataset<Row> actions  = scdDetector.detect(latest, spec, dimReader);
stagingWriter.load(ctx, spec.stagingTable(), actions);                        // COPY into stg_gold_core.dim_customer
mergeExecutor.inTransaction(ctx, tx -> {
    tx.execute(scdSql.inferredFill(spec));
    tx.execute(scdSql.closeChanged(spec));
    tx.execute(scdSql.insertNewVersions(spec));
    tx.execute(scdSql.applyType1(spec));
    tx.execute(scdSql.applyDeletes(spec));
    goldSync.writeBack(tx, ctx, checked);        // failure list + Silver summary (§5.3–5.4)
    goldSync.advanceBookmarks(tx, ctx);          // only reached if everything above succeeded
});
```

#### EPAS SQL (generated from the YAML; one transaction)

```sql
-- staging: stg_gold_core.dim_customer(customer_id, customer_name, email_masked, phone_masked, city, state,
--          segment, tier, effective_ts, _scd2_hash, _type1_hash, _src_change_seq, action, _etl_run_id)

-- 1) INFERRED_FILL: the placeholder becomes the real row (same SK, so facts already point to it)
UPDATE gold_core.dim_customer d
SET customer_name = s.customer_name, email_masked = s.email_masked, phone_masked = s.phone_masked,
    city = s.city, state = s.state, segment = s.segment, tier = s.tier,
    is_inferred = false, _scd2_hash = s._scd2_hash, _type1_hash = s._type1_hash,
    _src_change_seq = s._src_change_seq, _etl_run_id = s._etl_run_id, _etl_updated_at = now()
FROM stg_gold_core.dim_customer s
WHERE s.action = 'INFERRED_FILL' AND d.customer_id = s.customer_id AND d.is_current;

-- 2) SCD2_CHANGE: close the current version
UPDATE gold_core.dim_customer d
SET valid_to = s.effective_ts, is_current = false, _etl_run_id = s._etl_run_id, _etl_updated_at = now()
FROM stg_gold_core.dim_customer s
WHERE s.action = 'SCD2_CHANGE' AND d.customer_id = s.customer_id AND d.is_current;

-- 3) NEW + SCD2_CHANGE: insert the new version (Type 3 column takes the old value)
INSERT INTO gold_core.dim_customer (customer_id, customer_name, email_masked, phone_masked, city, state,
        segment, previous_segment, segment_changed_at, tier,
        valid_from, valid_to, is_current, version_no, is_inferred, is_deleted,
        _scd2_hash, _type1_hash, _src_change_seq, _etl_run_id)
SELECT s.customer_id, s.customer_name, s.email_masked, s.phone_masked, s.city, s.state,
       s.segment,
       CASE WHEN prev.segment IS DISTINCT FROM s.segment THEN prev.segment ELSE prev.previous_segment END,
       CASE WHEN prev.segment IS DISTINCT FROM s.segment THEN s.effective_ts ELSE prev.segment_changed_at END,
       s.tier,
       CASE WHEN s.action = 'NEW' THEN timestamptz '1900-01-01' ELSE s.effective_ts END,  -- ➕ first version covers all older facts
       timestamptz '9999-12-31', true, COALESCE(prev.version_no, 0) + 1, false, false,
       s._scd2_hash, s._type1_hash, s._src_change_seq, s._etl_run_id
FROM stg_gold_core.dim_customer s
LEFT JOIN LATERAL (SELECT * FROM gold_core.dim_customer d
                   WHERE d.customer_id = s.customer_id AND d.valid_to = s.effective_ts   -- the row closed in step 2
                   ORDER BY d.version_no DESC LIMIT 1) prev ON true
WHERE s.action IN ('NEW', 'SCD2_CHANGE');

-- 4) TYPE1_ONLY (and Type 1 columns of SCD2_CHANGE rows): overwrite, on all versions (type1Scope = ALL_VERSIONS)
UPDATE gold_core.dim_customer d
SET customer_name = s.customer_name, email_masked = s.email_masked, phone_masked = s.phone_masked,
    _type1_hash = s._type1_hash, _etl_run_id = s._etl_run_id, _etl_updated_at = now()
FROM stg_gold_core.dim_customer s
WHERE s.action IN ('TYPE1_ONLY', 'SCD2_CHANGE') AND d.customer_id = s.customer_id
  AND d._type1_hash IS DISTINCT FROM s._type1_hash;

-- 5) DELETE: close the current version and flag it (never physically delete, see §11)
UPDATE gold_core.dim_customer d
SET valid_to = s.effective_ts, is_current = false, is_deleted = true,
    _etl_run_id = s._etl_run_id, _etl_updated_at = now()
FROM stg_gold_core.dim_customer s
WHERE s.action = 'DELETE' AND d.customer_id = s.customer_id AND d.is_current;
```

> Note on DELETE: after closing, the customer has **no current row**. If a later fact still arrives for this customer, the fact lookup finds the closed version by its date range, which is correct. If the fact date is after the delete, the lookup falls back to the inferred-member logic (6.6).

### 6.5 Back-dated changes ➕

> **In simple words**
> Sometimes a change arrives **late**, dated **before** the current version started. Example: on 20 Oct we learn that Asha actually moved on 25 Sep, but the current version starts on 1 Oct.

| `backdatedPolicy` | What happens | When to use |
|---|---|---|
| `APPLY_AS_NOW` (default) | Treated as a normal change effective now; a warning is stored | Most dimensions; simple and safe |
| `REBUILD_TIMELINE` | The versions of that one customer are recalculated in the right order (same approach as doc 03 §10.4), and the facts of that customer in the affected date range are **re-keyed** to the correct SK | Dimensions where exact history matters (e.g. finance) |

`REBUILD_TIMELINE` must keep existing SKs where a version still exists, and only re-point the facts of the affected key and date range. Because it is more complex, it is opt-in per dimension.

### 6.6 Late-arriving dimensions (inferred members)

> **In simple words**
> A sale for customer `64aa…99` arrives, but the customer record has not arrived yet (it is still in Kafka, or the customer job runs later).
> We **don't block or drop** the sale. We create a **placeholder** customer row ("details not yet known") and give it a real SK. The sale points to it.
> When the real customer arrives, the dimension job **fills in the same row** (`INFERRED_FILL`). The sale is now automatically correct, because the SK never changed.

```sql
-- executed by the FACT job, inside its transaction, before the fact merge
INSERT INTO gold_core.dim_customer (customer_id, customer_name, valid_from, valid_to, is_current, version_no, is_inferred,
                                    _scd2_hash, _type1_hash, _src_change_seq, _etl_run_id)
SELECT DISTINCT s.customer_id, 'Unknown (pending)', timestamptz '1900-01-01', timestamptz '9999-12-31', true, 1, true,
       'inferred', 'inferred', 0, :runId
FROM stg_gold_sales.fact_sales_line s
WHERE s.customer_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM gold_core.dim_customer d WHERE d.customer_id = s.customer_id)
ON CONFLICT (customer_id) WHERE is_current DO NOTHING;     -- uses the partial unique index "one current row per customer"
```

| Situation | Fact gets |
|---|---|
| Key present, dimension row exists | The real SK (as of the event time) |
| Key present, dimension row missing | An **inferred member** SK |
| Key is NULL / empty | `-1` (Unknown) |
| Dimension does not apply | `-2` (N/A) |

➕ **Two jobs touching one dimension.** The fact job inserts inferred rows and the dimension job merges real rows, possibly at the same time. Both use `ON CONFLICT` and row locks, and the SCD merge takes a short **advisory lock per dimension** (`pg_advisory_xact_lock(hashtext('gold_core.dim_customer'))`), so the two never create two current rows for one customer.

➕ **DQ check:** "inferred members older than N days" is a warning. It means a real customer never arrived, which is usually a source problem.

### 6.7 Conformed, junk, degenerate and date dimensions

**Conformed dimensions** (shared by many facts) live in `gold_core` and have **exactly one owner job**:

| Dimension | Owner job | Used by |
|---|---|---|
| `gold_core.dim_date` | `s2g_dim_date` (generated, yearly) | every fact |
| `gold_core.dim_customer` | `s2g_dim_customer` | sales, returns, payments, support |
| `gold_core.dim_product` | `s2g_dim_product` | sales, returns, inventory |
| `gold_core.dim_store` | `s2g_dim_store` | sales, inventory |

> **In simple words:** if sales and returns each had their own customer table, "GOLD customers" might mean different things in two reports. One shared dimension = one truth.

**Junk dimension** (small flags and codes grouped into one table):

> **In simple words:** instead of putting 5 tiny columns (`channel`, `is_gift`, `is_express`, `payment_method`, `is_first_order`) on a fact with 100 million rows, we store each **combination** once in a small table and keep one key on the fact.

```sql
CREATE TABLE gold_sales.dim_order_profile (
    order_profile_sk  bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    profile_hash      char(64)    NOT NULL UNIQUE,     -- hash of the combination
    channel           varchar(20) NOT NULL,
    payment_method    varchar(20) NOT NULL,
    is_gift           boolean     NOT NULL,
    is_express        boolean     NOT NULL,
    is_first_order    boolean     NOT NULL
);
-- The fact job inserts missing combinations (ON CONFLICT (profile_hash) DO NOTHING) and then joins on profile_hash.
```

**Degenerate dimension:** `order_no` is stored **directly on the fact**. It has no attributes of its own (all order attributes are in other dimensions), but users need it to find "all lines of order ORD-1001".

**Date dimension** (`gold_core.dim_date`), generated once for e.g. 1990–2050 by a small job/migration:

```sql
CREATE TABLE gold_core.dim_date (
    date_key          integer     PRIMARY KEY,         -- 20261029 (readable "smart key": allowed for dates only)
    full_date         date        NOT NULL UNIQUE,
    day_of_week       smallint    NOT NULL,            -- 1 = Monday
    day_name          varchar(10) NOT NULL,
    is_weekend        boolean     NOT NULL,
    week_of_year      smallint    NOT NULL,
    month_no          smallint    NOT NULL,
    month_name        varchar(10) NOT NULL,
    quarter_no        smallint    NOT NULL,
    year_no           smallint    NOT NULL,
    fiscal_year       smallint    NOT NULL,            -- ➕ e.g. Indian FY April–March
    fiscal_quarter    smallint    NOT NULL,
    is_holiday        boolean     NOT NULL DEFAULT false,
    holiday_name      varchar(50)
);
-- special rows: -1 (Unknown date), -2 (Not applicable)
```

- Facts store `*_date_key` integers. One date dimension serves several roles (`order_date_key`, `ship_date_key`, `delivery_date_key`): this is called a **role-playing dimension**; BI tools see it through views like `v_dim_order_date`.
- ➕ An optional `dim_time_of_day` (1,440 rows, one per minute) when reports need "sales by hour".
- Holidays are maintained as seed data (CSV → table), per region if needed.

---

## 7. Facts

### 7.1 The three fact types (+ one bonus)

> **In simple words**
> - **Transactional fact:** one row **per event**. Like a bill book: every sale is written down once.
> - **Periodic snapshot:** one row **per thing per period**, even if nothing happened. Like a **daily photo** of the stock in a godown.
> - **Accumulating snapshot:** one row **per process instance**, updated as it moves through steps. Like a **parcel tracking card**: ordered → paid → shipped → delivered.

| Type | Grain | Example table | Rows change? | Typical measures |
|---|---|---|---|---|
| **Transactional** | one event | `fact_sales_line` (one order line) | Rarely (corrections, deletes) | qty, gross/discount/net amount |
| **Periodic snapshot** | entity × period | `fact_inventory_daily` (product × store × day), `fact_customer_balance_monthly` | No: each period is a new set of rows | stock on hand, balance (semi-additive) |
| **Accumulating snapshot** | one process (order) | `fact_order_fulfillment` | **Yes**: updated at every milestone | `ordered_date_key`, `paid_date_key`, `shipped_date_key`, `delivered_date_key`, `days_to_ship` |
| ➕ **Factless fact** | an event with no numbers | `fact_promotion_coverage` (which product was on promotion in which store on which day) | No | count of rows only |

**Measures come in three kinds ➕** (important for aggregates):

| Kind | Can be summed over… | Example | Rule |
|---|---|---|---|
| Additive | everything | `net_amt`, `quantity` | `SUM` is always right |
| Semi-additive | everything **except time** | `stock_on_hand`, `account_balance` | Over time use the last value or the average, never `SUM` |
| Non-additive | nothing | `margin_pct`, `avg_price` | Store the parts (`margin_amt`, `net_amt`) and compute the ratio at query time |

### 7.2 How a transactional fact job works

> **In simple words**
> 1. Find the Silver rows that changed (bookmark), including order headers that changed for existing items.
> 2. Clean and check them (same DQ framework).
> 3. Build one row per order line with its measures.
> 4. Write them to a staging table.
> 5. In **one EPAS transaction**: create missing placeholder dimensions, look up all surrogate keys "as of" the order date, merge into the fact, and update the Silver statuses and bookmarks.

```
 Silver changes (bookmark per source table)
   sales_order_item (driving table)  ─┐
   sales_order (header changes)       ├─► keys to (re)build = items changed ∪ items of headers changed
                                      ┘
        ▼
 Spark: read the full current rows for those keys (items + header + discounts)
        ▼
 Spark: filter → DQ → dedupe by grain key (order_id, line_id) → compute measures
        → junk-dim profile hash → order_date_key, ship_date_key → _row_hash
        ▼
 Spark: COPY into stg_gold_sales.fact_sales_line
        ▼
 EPAS (ONE transaction):
   1. insert missing inferred dimension members + missing junk combinations
   2. handle grain rows whose date key changed (move between partitions, §7.4)
   3. MERGE into fact with SK lookups "as of" order_ts
   4. record touched date keys (for aggregates, §10)
   5. failure list + Silver summary status + bookmarks (§5)
   COMMIT
```

➕ **"Secondary table" changes.** A fact row is built from several Silver tables. If only the **header** changes (e.g. the order channel), the item rows did not change, but the fact must still be updated. So the job config lists **all** source tables and how they map to the fact grain:

```yaml
sources:
  - { table: silver_order.sales_order_item, role: DRIVING }
  - { table: silver_order.sales_order,      role: SECONDARY, joinKeys: [order_id] }   # header change → rebuild its lines
  - { table: silver_order.sales_order_item_discount, role: SECONDARY, joinKeys: [order_id, line_id] }
```

### 7.3 Surrogate key lookup in EPAS (during the merge)

> **In simple words**
> For each fact row we need the **right card number** from each dimension. For SCD2 dimensions, "right" means **the version that was valid on the order date**. The database is very good at this kind of lookup (indexes on `customer_id, valid_from`), so we do it in SQL instead of loading big dimensions into Spark.

```sql
-- 3) merge into the fact
INSERT INTO gold_sales.fact_sales_line AS f (
    order_date_key, ship_date_key, customer_sk, product_sk, store_sk, order_profile_sk,
    order_no, order_id, line_id, quantity, unit_price_amt, gross_amt, discount_amt, net_amt,
    is_deleted, _src_change_seq, _row_hash, _etl_run_id)
SELECT s.order_date_key,
       s.ship_date_key,
       CASE WHEN s.customer_id IS NULL THEN -1 ELSE c.customer_sk END,
       CASE WHEN s.sku IS NULL         THEN -1 ELSE p.product_sk  END,
       COALESCE(st.store_sk, -1),
       op.order_profile_sk,
       s.order_no, s.order_id, s.line_id, s.quantity, s.unit_price_amt, s.gross_amt, s.discount_amt, s.net_amt,
       s.is_deleted, s._src_change_seq, s._row_hash, s._etl_run_id
FROM stg_gold_sales.fact_sales_line s
LEFT JOIN gold_core.dim_customer c                         -- SCD2: version valid at the order time
       ON c.customer_id = s.customer_id AND s.order_ts >= c.valid_from AND s.order_ts < c.valid_to
LEFT JOIN gold_core.dim_product p                          -- SCD1: current row
       ON p.sku = s.sku AND p.is_current
LEFT JOIN gold_core.dim_store st
       ON st.store_id = s.store_id AND st.is_current
JOIN gold_sales.dim_order_profile op
       ON op.profile_hash = s.profile_hash
WHERE s._etl_run_id = :runId
ON CONFLICT (order_date_key, order_id, line_id) DO UPDATE SET
    ship_date_key = EXCLUDED.ship_date_key, customer_sk = EXCLUDED.customer_sk, product_sk = EXCLUDED.product_sk,
    store_sk = EXCLUDED.store_sk, order_profile_sk = EXCLUDED.order_profile_sk,
    quantity = EXCLUDED.quantity, unit_price_amt = EXCLUDED.unit_price_amt, gross_amt = EXCLUDED.gross_amt,
    discount_amt = EXCLUDED.discount_amt, net_amt = EXCLUDED.net_amt, is_deleted = EXCLUDED.is_deleted,
    _src_change_seq = EXCLUDED._src_change_seq, _row_hash = EXCLUDED._row_hash,
    _etl_run_id = EXCLUDED._etl_run_id, _etl_updated_at = now()
WHERE f._row_hash <> EXCLUDED._row_hash                     -- something really changed
  AND EXCLUDED._src_change_seq >= f._src_change_seq        -- never go back to older Silver data
RETURNING order_date_key, (xmax = 0) AS inserted;           -- touched dates + insert/update counts
```

- After step 1 (inferred members), the customer and product joins always find a row when the key is not null. A `NULL` result anyway means a framework bug: the reconciliation check "no NULL SK" catches it.
- `_src_change_seq` on the fact = the **highest** change number of all Silver rows that built it.

### 7.4 When the date key of an existing fact row changes ➕

> **In simple words**
> The fact table is split into monthly parts by `order_date_key` (section 12). If an order's date is corrected from 31 Oct to 1 Nov, the new row belongs in a **different part**. A plain upsert would insert a second row in November and leave the old one in October, so the sale would be counted twice.

Fix, step 2 of the transaction:

```sql
DELETE FROM gold_sales.fact_sales_line f
USING stg_gold_sales.fact_sales_line s
WHERE s._etl_run_id = :runId
  AND f.order_id = s.order_id AND f.line_id = s.line_id
  AND f.order_date_key <> s.order_date_key
RETURNING f.order_date_key;          -- the OLD date is also "touched" → its aggregates get recomputed
```

### 7.5 Late-arriving facts

> **In simple words**
> A sale from **last week** arrives **today** (an offline store synced late).

| Concern | How the design handles it |
|---|---|
| Which customer version? | Lookup "as of" `order_ts`, not "today", so the correct historical version is used |
| Aggregates for last week are already computed | Its `order_date_key` is recorded as **touched**, so the aggregate job recomputes last week (§10) |
| A closed snapshot period (e.g. a month already reported) | Policy per table: `RESTATE` (recompute the closed period, default) or `ADJUST_IN_CURRENT` (book the difference in the open period; finance often wants this) |

### 7.6 Periodic snapshot job (example: daily inventory)

```sql
-- runs for one snapshot date (:d); fully idempotent: delete + insert for that date
DELETE FROM gold_inventory.fact_inventory_daily WHERE snapshot_date_key = :d;
INSERT INTO gold_inventory.fact_inventory_daily (snapshot_date_key, product_sk, store_sk, qty_on_hand, stock_value_amt, _etl_run_id)
SELECT :d, p.product_sk, st.store_sk, i.qty_on_hand, i.qty_on_hand * p.unit_cost_amt, :runId
FROM silver_inventory.stock_level i
JOIN gold_core.dim_product p ON p.sku = i.sku AND p.is_current
JOIN gold_core.dim_store   st ON st.store_id = i.store_id AND st.is_current
WHERE NOT i._is_deleted;
```

Scheduled once per period (daily). A backfill reruns the same SQL for each date, but only where the source can show past state (otherwise the snapshot simply starts from today).

### 7.7 Accumulating snapshot job (example: order fulfillment)

One row per order. Each milestone fills a date key; the lag measures are recomputed:

```
| order_id | ordered_date_key | paid_date_key | shipped_date_key | delivered_date_key | days_to_pay | days_to_ship | current_stage |
| 65f1…    | 20261029         | 20261029      | 20261031         | -2 (not yet)       | 0           | 2            | SHIPPED       |
```

It uses the same merge pattern as 7.3 (upsert by `order_id`). Rows **are** updated, which is normal for this fact type.

---

## 8. Validation, Dedupe and Error Handling in Gold

> **In simple words**
> Most cleaning already happened in Silver. Gold checks are about **modeling rules**: does every fact row have a valid date, do the numbers add up, is there exactly one current version per customer?

Same framework as doc 03 (YAML rules → `_errors` array → severities). Typical Gold rules:

| Rule | Level | Severity |
|---|---|---|
| Grain key not null and unique in the batch (`order_id, line_id`) | fact | QUARANTINE |
| `net_amt = gross_amt − discount_amt` (± 0.01) | fact | QUARANTINE |
| `order_date_key` exists in `dim_date` | fact | QUARANTINE |
| Share of facts with `-1` (Unknown) SK above X% | job | WARN / circuit breaker |
| Exactly one `is_current` row per natural key | dim (post-merge check) | job FAILED |
| No overlapping `valid_from`/`valid_to` per natural key ➕ | dim (post-merge check) | job FAILED |
| Inferred members older than N days | dim | WARN |
| Aggregate total = fact total for the touched dates | agg | job FAILED |

**Status per Silver row** (written to the failure list and the Silver summary, §5):

| Situation | Status | Retried? |
|---|---|---|
| Row became a fact/dim row | SUCCESS (no list entry) | — |
| Retryable error (e.g. a lock timeout on a single statement, a referenced Silver row still missing) | FAILED | Yes, next runs |
| Data/model error (e.g. amounts don't add up) | QUARANTINED | After a fix |
| A header and its lines: some lines OK, some quarantined | PARTIAL (on the header row) | Failed lines |
| Filtered out on purpose (e.g. test orders) | SKIPPED (no list entry, counted) | — |

**Job level:** exactly as in doc 03: infra error → rollback, FAILED, bookmark not moved; failure share above the threshold → circuit breaker (rollback, nothing written back); otherwise SUCCESS / PARTIAL.

**Dedupe:** per fact grain key (one row per `order_id, line_id`) and per dimension natural key (latest change wins). Same strategies as doc 03 (`LATEST_VERSION`, …).

---

## 9. Run Modes in Gold

> **In simple words:** the steps never change. Only **which Silver rows are read** changes.

| Run mode | Which Silver rows |
|---|---|
| `INCREMENTAL` | `_etl_change_seq` above the bookmark + this job's retry list |
| `FULL_LOAD` | all rows. **Dimensions: merge** (keeps SKs). **Facts: rebuild** (below) |
| `RERUN_JOB` | same as INCREMENTAL (the bookmark did not move) |
| `RERUN_FAILED_RECORDS` | only this job's retry list |
| `REPROCESS_QUARANTINE` | list entries with status `READY_TO_REPROCESS` |
| `BACKFILL` | Silver rows whose event date is in `[from, to)` |
| `ID_REPROCESS` | the given natural keys |
| `CASCADE` | nothing special: when Silver was re-run, changed rows got new change numbers and Gold picks them up automatically ➕ |
| `DRY_RUN` | any of the above, nothing written |

➕ **Full rebuild of a fact without disturbing dashboards ("shadow table swap"):**
1. Build `gold_sales.fact_sales_line__rebuild` (same structure) from all of Silver.
2. Run the reconciliation checks on it.
3. In one short transaction: rename the live table to `__old` and `__rebuild` to the live name. Dashboards see the switch instantly.
4. Drop `__old` after a safety period.

Dimensions are **never** rebuilt this way (their SKs would change). A dimension full load is always a merge.

---

## 10. Aggregates and KPIs

> **In simple words**
> A dashboard showing "sales per store per day for the last 2 years" should not add up 100 million fact rows every time someone opens it. We pre-compute small **summary tables**. When facts change, we recompute **only the days that changed** (the "dirty" days), not everything.

### 10.1 How "dirty days" work

```
 fact merge RETURNING order_date_key  ─►  etl_ctl.gold_touched_partition (table, date_key, run_id)
                                                    │
 aggregate job (bookmark on this table)  ◄──────────┘
   for each touched date_key:
     DELETE the aggregate rows for that day
     INSERT the recomputed rows from the fact
   (one transaction) → bookmark moves → reconciliation: aggregate total = fact total for those days
```

```sql
-- generated from the aggregate YAML
DELETE FROM gold_sales.agg_sales_daily_store a
WHERE a.date_key IN (SELECT date_key FROM tmp_touched_dates);

INSERT INTO gold_sales.agg_sales_daily_store (date_key, store_sk, order_cnt, line_cnt, qty, gross_amt, discount_amt, net_amt, _etl_run_id)
SELECT f.order_date_key, f.store_sk,
       COUNT(DISTINCT f.order_no), COUNT(*), SUM(f.quantity),
       SUM(f.gross_amt), SUM(f.discount_amt), SUM(f.net_amt), :runId
FROM gold_sales.fact_sales_line f
WHERE NOT f.is_deleted
  AND f.order_date_key IN (SELECT date_key FROM tmp_touched_dates)
GROUP BY f.order_date_key, f.store_sk;
```

**Why SQL inside EPAS and not Spark here ➕:** the fact is already in EPAS. Moving millions of rows out to Spark and back only to `GROUP BY` them is slower than letting the database do it. The framework runs it as an **SQL job type**, with the same lock, audit, bookmark and reconciliation. Spark is still used when an aggregate needs heavy logic or data from several systems.

### 10.2 Aggregate config

```yaml
# pipelines/silver-to-gold/sales/s2g_agg_sales_daily_store.yml
job:
  name: s2g_agg_sales_daily_store
  type: S2G_AGGREGATE
  domain: sales
source:
  fact: gold_sales.fact_sales_line
  dirtyPartitionColumn: order_date_key
  filter: "NOT is_deleted"
target:
  table: gold_sales.agg_sales_daily_store
  grain: [date_key: order_date_key, store_sk]
measures:
  order_cnt:    "COUNT(DISTINCT order_no)"     # non-additive across days → document it (see 10.3)
  line_cnt:     "COUNT(*)"
  qty:          "SUM(quantity)"
  gross_amt:    "SUM(gross_amt)"
  discount_amt: "SUM(discount_amt)"
  net_amt:      "SUM(net_amt)"
reconcile:
  - { measure: net_amt, against: "SUM(net_amt) FROM gold_sales.fact_sales_line WHERE NOT is_deleted", tolerance: 0 }
dependsOn: [s2g_fact_sales_line]
```

### 10.3 KPI definitions ➕

> **In simple words:** "Net revenue" must mean the same thing in every report. So KPIs are **written down once**, in a file, with an owner.

```yaml
# pipelines/silver-to-gold/sales/kpis.yml
kpis:
  - name: net_revenue
    description: "Revenue after discounts, excluding cancelled/deleted lines"
    formula: "SUM(net_amt)"
    source: gold_sales.fact_sales_line
    filter: "NOT is_deleted"
    additivity: ADDITIVE
    owner: finance-analytics
  - name: avg_order_value
    formula: "SUM(net_amt) / NULLIF(COUNT(DISTINCT order_no), 0)"
    additivity: NON_ADDITIVE        # never sum this across rows; compute from the parts
  - name: unique_customers
    formula: "COUNT(DISTINCT customer_id)"
    additivity: NON_ADDITIVE        # a daily count cannot be added up into a monthly count
```

These definitions are published to the OpenMetadata **glossary** (Prompt 10), and the aggregate jobs reference them, so the docs and the numbers never drift apart.

**Distinct counts** (customers, orders) cannot be added across days. Options: compute them directly at each needed grain (daily, monthly) or, if very large, store an approximate sketch (HyperLogLog) column. Start with direct computation.

**Refresh options per aggregate:**

| Option | When |
|---|---|
| Dirty-partition recompute (default) | Most aggregates |
| Full recompute | Small aggregates (< a few million fact rows) |
| ➕ EPAS materialized view + `REFRESH MATERIALIZED VIEW CONCURRENTLY` | Simple, small summaries without the need for audit/reconciliation |

---

## 11. Soft Deletes in Gold

> **In simple words**
> - **Dimensions are never physically deleted.** Old facts still point to them. If we deleted the customer card, last year's sales would show "unknown customer".
> - **Facts** usually get a **flag**, and the reporting views hide flagged rows.

| Table type | When Silver says "deleted" | Configurable |
|---|---|---|
| Dimension SCD1 | `is_deleted = true` on the row; it stays | No (always kept) |
| Dimension SCD2 | Current version closed (`valid_to = deleted_at`), `is_deleted = true` | No |
| Transactional fact | `SOFT` (default): `is_deleted = true`. `HARD`: row removed. `REVERSAL` ➕: a **negative copy** of the row is inserted (finance/accounting style: nothing is ever erased) | `factDeleteStrategy: SOFT \| HARD \| REVERSAL` |
| Periodic snapshot | Deleted entities simply don't appear in the next snapshot; past snapshots are not changed | — |
| Accumulating snapshot | `is_deleted = true`, `current_stage = 'CANCELLED'` (if that is the business meaning) | — |
| Aggregates | The touched dates are recomputed **excluding** deleted fact rows | — |

➕ **Views for BI users:** BI tools read **views**, not the tables: `gold_sales.v_fact_sales_line` (= `WHERE NOT is_deleted`), `gold_core.v_dim_customer_current` (= `WHERE is_current AND NOT is_deleted`). Users can then never forget the filter, and the tables can change behind the views without breaking reports.

The GDPR/DPDP purge (doc 02 §10) is a separate, audited process that anonymizes or removes personal data in dimensions (e.g. sets the name/email to `REDACTED`) while keeping the SKs, so the facts stay valid.

---

## 12. Naming, Keys, Partitioning and Indexing

### 12.1 Naming conventions

| Object | Pattern | Example |
|---|---|---|
| Schema (shared) | `gold_core` | `gold_core.dim_customer` |
| Schema (subject area) | `gold_<area>` | `gold_sales`, `gold_finance`, `gold_inventory` |
| Dimension | `dim_<entity>` | `dim_customer`, `dim_product`, `dim_date` |
| Junk dimension | `dim_<fact>_profile` | `dim_order_profile` |
| Transactional fact | `fact_<process>` (+ grain hint if needed) | `fact_sales_line`, `fact_payment` |
| Periodic snapshot | `fact_<subject>_<period>` | `fact_inventory_daily`, `fact_customer_balance_monthly` |
| Accumulating snapshot | `fact_<process>_fulfillment` / `_lifecycle` | `fact_order_fulfillment` |
| Factless fact | `fact_<event>_coverage` / `_event` | `fact_promotion_coverage` |
| Bridge (many-to-many) | `bridge_<a>_<b>` | `bridge_customer_segment` |
| Aggregate | `agg_<subject>_<grain>` | `agg_sales_daily_store`, `agg_sales_monthly_category` |
| View for BI | `v_<table>` | `v_fact_sales_line`, `v_dim_customer_current` |
| Role-playing date view | `v_dim_<role>_date` | `v_dim_order_date`, `v_dim_ship_date` |
| Staging | `stg_<schema>.<table>` | `stg_gold_sales.fact_sales_line` |
| Surrogate key | `<entity>_sk` | `customer_sk` |
| Natural key | same name as in Silver | `customer_id`, `sku` |
| Date key | `<role>_date_key` (int `yyyymmdd`) | `order_date_key` |
| Amount / quantity / count | `_amt`, `_qty`, `_cnt` suffix (currency in the column comment, or `_inr` if multi-currency) | `net_amt`, `order_cnt` |
| Flags | `is_<something>` | `is_current`, `is_inferred`, `is_deleted` |
| Technical | `_src_*`, `_etl_*`, `_scd2_hash`, `_type1_hash`, `_row_hash` | |
| Job names | `s2g_dim_<x>`, `s2g_fact_<x>`, `s2g_snap_<x>`, `s2g_agg_<x>` | `s2g_fact_sales_line` |

### 12.2 Keys

| Table | Primary key | Other unique keys |
|---|---|---|
| Dimension (SCD2) | `<entity>_sk` | `(natural key, valid_from)`; partial unique `(natural key) WHERE is_current` |
| Dimension (SCD1) | `<entity>_sk` | `(natural key)` |
| Junk dimension | `<name>_sk` | `profile_hash` |
| Transactional fact | **grain natural key + partition key**: `(order_date_key, order_id, line_id)` | — (no separate fact SK needed) |
| Periodic snapshot | `(snapshot_date_key, <dim SKs of the grain>)` | — |
| Accumulating snapshot | `(order_id)` | — |
| Aggregate | the grain columns | — |

### 12.3 Partitioning (EPAS declarative partitioning)

> **In simple words:** a huge fact table is split into **monthly drawers**. A query for October only opens the October drawer. Old drawers can be archived or dropped easily.

| Table | Partitioning | Why |
|---|---|---|
| Transactional facts > ~50 M rows | `PARTITION BY RANGE (order_date_key)`, **monthly** | Partition pruning for date filters; fast archiving; smaller indexes |
| Periodic snapshots | Range on `snapshot_date_key`, monthly | Same; delete+insert per day stays local |
| Aggregates | Usually none (small); yearly if large | |
| Dimensions | None | Small enough; lookups by key |

- ➕ Partitions are created **ahead of time** by a maintenance DAG (e.g. 3 months in advance), or with `pg_partman` if it is available in your EPAS installation. A `DEFAULT` partition catches unexpected dates and raises an alert.
- The partition key must be part of the primary key; that is why the fact PK includes `order_date_key` (and why §7.4 exists).

### 12.4 Indexing

| Table | Indexes |
|---|---|
| Fact | PK; one B-tree per **dimension SK** column that is often filtered (`customer_sk`, `product_sk`, `store_sk`); `(order_id)` for lookups and §7.4; ➕ BRIN on the date key for very large append-mostly tables |
| SCD2 dimension | PK; `(natural key, valid_from)` (as-of lookup); partial unique `(natural key) WHERE is_current`; columns users filter by (`city`, `segment`) if the dimension is large |
| SCD1 dimension | PK; unique natural key |
| Aggregate | PK on the grain; an index on the most common filter (`date_key`) |
| Staging | none (bulk loaded, read once); `ANALYZE` before the merge |

**Mongo in Gold:** not used (§2). If a serving copy is added later, index it on the lookup key used by the app (e.g. `customer_id`).

---

## 13. Load Order and Dependencies

> **In simple words**
> You can't print the sales report card before the customer cards exist. So: **dimensions first, then facts, then summaries**. Inferred members are the safety net when something arrives out of order, but the normal order should still be followed.

```
 BRONZE → SILVER (doc 03)                GOLD DIMENSIONS               GOLD FACTS                 SNAPSHOTS / AGGREGATES
 ─────────────────────────               ───────────────               ──────────                 ──────────────────────
 b2s_customer ─────────────────────────► s2g_dim_customer ─┐
 b2s_product ──────────────────────────► s2g_dim_product ──┤
 b2s_store ────────────────────────────► s2g_dim_store ────┤
 (generated yearly) ───────────────────► s2g_dim_date ─────┼──► s2g_fact_sales_line ───► s2g_agg_sales_daily_store
 b2s_order ───────────────────────────────────────────────┤        │                   ► s2g_agg_sales_monthly_category
                                                          │        └─► (touched dates)
                                                          └──► s2g_fact_order_fulfillment
 b2s_inventory ───────────────────────────────────────────────► s2g_snap_inventory_daily (daily, after dims)
```

| Rule | How it is implemented |
|---|---|
| A dimension job runs after its Silver job | `dependsOn: [b2s_customer]` in the YAML → Airflow dependency (Prompt 09) |
| A fact job runs after its dimension jobs **and** its Silver jobs | `dependsOn: [s2g_dim_customer, s2g_dim_product, s2g_dim_store, b2s_order]` |
| An aggregate runs after its fact | `dependsOn: [s2g_fact_sales_line]` |
| A dimension job failed | The fact job **still runs** by default (inferred members keep it correct), but a warning is raised; configurable `onUpstreamFailure: RUN \| SKIP \| WAIT` (Prompt 07) |
| Shared dimensions | Exactly **one** owner job; facts never update dimension attributes (they only insert inferred members) |

---

## 14. DDL: One Fact + Its Dimensions

```sql
-- db/gold/V1__gold_core_dimensions.sql
CREATE SCHEMA IF NOT EXISTS gold_core;
CREATE SCHEMA IF NOT EXISTS gold_sales;

-- ── dim_customer (SCD2 + Type 1 + Type 3) ─────────────────────────────────────────
CREATE TABLE gold_core.dim_customer (
    customer_sk         bigint        GENERATED ALWAYS AS IDENTITY,
    customer_id         varchar(24)   NOT NULL,                  -- natural key from Silver
    customer_name       varchar(200),                            -- Type 1
    email_masked        varchar(200),                            -- Type 1 (masked: ➕ minimal PII in Gold)
    phone_masked        varchar(30),                             -- Type 1
    city                varchar(100),                            -- Type 2
    state               varchar(100),                            -- Type 2
    segment             varchar(20),                             -- Type 2
    previous_segment    varchar(20),                             -- Type 3
    segment_changed_at  timestamptz,                             -- Type 3
    tier                varchar(20),                             -- Type 2
    valid_from          timestamptz   NOT NULL,
    valid_to            timestamptz   NOT NULL DEFAULT '9999-12-31',   -- ➕ far-future instead of NULL: simpler "between" joins
    is_current          boolean       NOT NULL,
    version_no          integer       NOT NULL,
    is_inferred         boolean       NOT NULL DEFAULT false,
    is_deleted          boolean       NOT NULL DEFAULT false,
    _scd2_hash          varchar(64)   NOT NULL,
    _type1_hash         varchar(64)   NOT NULL,
    _src_change_seq     bigint        NOT NULL,
    _etl_run_id         varchar(100)  NOT NULL,
    _etl_inserted_at    timestamptz   NOT NULL DEFAULT now(),
    _etl_updated_at     timestamptz   NOT NULL DEFAULT now(),
    CONSTRAINT pk_dim_customer PRIMARY KEY (customer_sk)
);
CREATE UNIQUE INDEX ux_dim_customer_nk_from ON gold_core.dim_customer (customer_id, valid_from);
CREATE UNIQUE INDEX ux_dim_customer_current ON gold_core.dim_customer (customer_id) WHERE is_current;
CREATE INDEX ix_dim_customer_asof          ON gold_core.dim_customer (customer_id, valid_from, valid_to);

-- ── dim_product (SCD1) ────────────────────────────────────────────────────────────
CREATE TABLE gold_core.dim_product (
    product_sk          bigint        GENERATED ALWAYS AS IDENTITY,
    sku                 varchar(40)   NOT NULL,
    product_name        varchar(200),
    category            varchar(100),
    sub_category        varchar(100),
    brand               varchar(100),
    unit_cost_amt       numeric(18,2),
    is_current          boolean       NOT NULL DEFAULT true,     -- always true for SCD1 (kept for a uniform lookup pattern)
    is_inferred         boolean       NOT NULL DEFAULT false,
    is_deleted          boolean       NOT NULL DEFAULT false,
    _type1_hash         varchar(64)   NOT NULL,
    _src_change_seq     bigint        NOT NULL,
    _etl_run_id         varchar(100)  NOT NULL,
    _etl_inserted_at    timestamptz   NOT NULL DEFAULT now(),
    _etl_updated_at     timestamptz   NOT NULL DEFAULT now(),
    CONSTRAINT pk_dim_product PRIMARY KEY (product_sk),
    CONSTRAINT ux_dim_product_sku UNIQUE (sku)
);

-- ── dim_store (SCD1) ─ same pattern as dim_product (store_sk, store_id, store_name, city, region, ...)
-- ── dim_date ─ see §6.7
-- ── gold_sales.dim_order_profile (junk) ─ see §6.7

-- Special members (-1 Unknown, -2 Not applicable) in every dimension: see §6.2

-- db/gold/V2__gold_sales_fact_sales_line.sql
-- ── fact_sales_line (transactional, grain = one order line) ──────────────────────
CREATE TABLE gold_sales.fact_sales_line (
    order_date_key      integer       NOT NULL,                  -- → dim_date (partition key)
    ship_date_key       integer       NOT NULL DEFAULT -2,       -- → dim_date (role-playing), -2 = not shipped yet
    customer_sk         bigint        NOT NULL,                  -- → dim_customer (as of order time)
    product_sk          bigint        NOT NULL,                  -- → dim_product
    store_sk            bigint        NOT NULL,                  -- → dim_store
    order_profile_sk    bigint        NOT NULL,                  -- → dim_order_profile (junk)
    order_no            varchar(30)   NOT NULL,                  -- degenerate dimension
    order_id            varchar(24)   NOT NULL,                  -- grain natural key (part 1)
    line_id             integer       NOT NULL,                  -- grain natural key (part 2)
    quantity            integer       NOT NULL,                  -- additive
    unit_price_amt      numeric(18,2) NOT NULL,                  -- non-additive (price per unit)
    gross_amt           numeric(18,2) NOT NULL,                  -- additive
    discount_amt        numeric(18,2) NOT NULL,                  -- additive
    net_amt             numeric(18,2) NOT NULL,                  -- additive
    is_deleted          boolean       NOT NULL DEFAULT false,
    _src_change_seq     bigint        NOT NULL,
    _row_hash           varchar(64)   NOT NULL,
    _etl_run_id         varchar(100)  NOT NULL,
    _etl_inserted_at    timestamptz   NOT NULL DEFAULT now(),
    _etl_updated_at     timestamptz   NOT NULL DEFAULT now(),
    CONSTRAINT pk_fact_sales_line PRIMARY KEY (order_date_key, order_id, line_id)
) PARTITION BY RANGE (order_date_key);

CREATE TABLE gold_sales.fact_sales_line_2026_10 PARTITION OF gold_sales.fact_sales_line FOR VALUES FROM (20261001) TO (20261101);
CREATE TABLE gold_sales.fact_sales_line_2026_11 PARTITION OF gold_sales.fact_sales_line FOR VALUES FROM (20261101) TO (20261201);
CREATE TABLE gold_sales.fact_sales_line_default PARTITION OF gold_sales.fact_sales_line DEFAULT;   -- alert if not empty

CREATE INDEX ix_fsl_customer ON gold_sales.fact_sales_line (customer_sk);
CREATE INDEX ix_fsl_product  ON gold_sales.fact_sales_line (product_sk);
CREATE INDEX ix_fsl_store    ON gold_sales.fact_sales_line (store_sk);
CREATE INDEX ix_fsl_order    ON gold_sales.fact_sales_line (order_id, line_id);

-- Foreign keys: ➕ declared as NOT VALID documentation-only constraints, or left out. The load process guarantees
-- them (inferred members, -1/-2), and enforced FKs on huge partitioned facts slow down loads.

-- BI views
CREATE VIEW gold_sales.v_fact_sales_line AS
    SELECT * FROM gold_sales.fact_sales_line WHERE NOT is_deleted;
CREATE VIEW gold_core.v_dim_customer_current AS
    SELECT * FROM gold_core.dim_customer WHERE is_current AND NOT is_deleted;
```

---

## 15. Job Configs (YAML)

### 15.1 Dimension job

```yaml
# pipelines/silver-to-gold/core/s2g_dim_customer.yml
job:
  name: s2g_dim_customer
  type: S2G_DIMENSION
  domain: core
  owner: team-customer-data
sources:
  - { table: silver_customer.customer_hist, role: DRIVING, historyMode: SILVER_TIMELINE }   # copy Silver SCD2 timeline
target:
  table: gold_core.dim_customer
  surrogateKey: customer_sk
  naturalKey: [customer_id]
  specialMembers: [UNKNOWN, NOT_APPLICABLE]
columns:
  customer_name: { source: customer_name }
  email_masked:  { source: email, transform: [maskEmail] }      # ➕ mask PII in Gold
  phone_masked:  { source: phone, transform: [maskPhone] }
  city:          { source: city }
  state:         { source: state }
  segment:       { source: segment }
  tier:          { source: tier }
scd:
  type2Columns: [city, state, segment, tier]
  type1Columns: [customer_name, email_masked, phone_masked]
  type3Columns: { segment: previous_segment }
  type1Scope: ALL_VERSIONS
  effectiveFrom: valid_from            # from the Silver timeline
  backdatedPolicy: APPLY_AS_NOW
dq:
  ruleSets: [gold/_common/dim_rules, gold/core/dim_customer]
thresholds: { maxFailedPct: 1.0, maxFailedCount: 500, onBreach: ROLLBACK }
schedule: "40 * * * *"
dependsOn: [b2s_customer]
```

### 15.2 Fact job

```yaml
# pipelines/silver-to-gold/sales/s2g_fact_sales_line.yml
job:
  name: s2g_fact_sales_line
  type: S2G_FACT
  domain: sales
  owner: team-sales-analytics
sources:
  - { table: silver_order.sales_order_item,          role: DRIVING }
  - { table: silver_order.sales_order,               role: SECONDARY, joinKeys: [order_id] }
  - { table: silver_order.sales_order_item_discount, role: SECONDARY, joinKeys: [order_id, line_id] }
target:
  table: gold_sales.fact_sales_line
  factType: TRANSACTIONAL
  grain: [order_id, line_id]
  partitionKey: order_date_key
eventTime: order_ts                      # used for "as of" SCD2 lookups
dimensions:
  - { name: customer, table: gold_core.dim_customer, naturalKey: customer_id, sk: customer_sk, scd: TYPE2,
      onMissing: INFERRED, onNullKey: UNKNOWN }
  - { name: product,  table: gold_core.dim_product,  naturalKey: sku,         sk: product_sk,  scd: TYPE1,
      onMissing: INFERRED, onNullKey: UNKNOWN }
  - { name: store,    table: gold_core.dim_store,    naturalKey: store_id,    sk: store_sk,    scd: TYPE1,
      onMissing: UNKNOWN,  onNullKey: UNKNOWN }
  - { name: order_profile, table: gold_sales.dim_order_profile, type: JUNK, sk: order_profile_sk,
      columns: [channel, payment_method, is_gift, is_express, is_first_order] }
dateKeys:
  order_date_key: { from: order_ts }
  ship_date_key:  { from: shipped_ts, onNull: NOT_APPLICABLE }
degenerate: [order_no]
measures:
  quantity:       "qty"
  unit_price_amt: "unit_price"
  gross_amt:      "qty * unit_price"
  discount_amt:   "COALESCE(line_discount_total, 0)"
  net_amt:        "qty * unit_price - COALESCE(line_discount_total, 0)"
factDeleteStrategy: SOFT
lateFactPolicy: RESTATE
dq:
  ruleSets: [gold/_common/fact_rules, gold/sales/fact_sales_line]
thresholds: { maxFailedPct: 1.0, maxFailedCount: 1000, maxUnknownSkPct: 0.5, onBreach: ROLLBACK }
schedule: "50 * * * *"
dependsOn: [b2s_order, s2g_dim_customer, s2g_dim_product, s2g_dim_store]
```

### 15.3 Folder layout for Gold configs

```
pipelines/silver-to-gold/
├── core/                         # conformed dimensions (gold_core)
│   ├── s2g_dim_date.yml
│   ├── s2g_dim_customer.yml
│   ├── s2g_dim_product.yml
│   └── s2g_dim_store.yml
├── sales/
│   ├── s2g_fact_sales_line.yml
│   ├── s2g_fact_order_fulfillment.yml
│   ├── s2g_agg_sales_daily_store.yml
│   ├── s2g_agg_sales_monthly_category.yml
│   └── kpis.yml
└── inventory/
    └── s2g_snap_inventory_daily.yml

dq-rules/gold/
├── _common/{dim_rules.yml, fact_rules.yml}
├── core/dim_customer.yml
└── sales/fact_sales_line.yml

db/gold/                          # Flyway: dims, facts, partitions, views, special members, dim_date seed
```

### 15.4 New code in `etl-core`

```
etl-core/src/main/java/com/company/etl/core/
├── pipeline/SilverToGoldDimensionJob.java
├── pipeline/SilverToGoldFactJob.java
├── pipeline/SilverToGoldSnapshotJob.java
├── pipeline/SqlAggregateJob.java              # SQL-in-EPAS job type (§10)
├── io/reader/SilverChangeReader.java          # bookmark + retry list + secondary-table key expansion
├── modeling/scd/ScdChangeDetector.java        # §6.4
├── modeling/scd/ScdSqlGenerator.java          # §6.4 SQL from the YAML
├── modeling/keys/InferredMemberSql.java       # §6.6
├── modeling/keys/FactMergeSqlGenerator.java   # §7.3–7.4
├── modeling/junk/JunkDimensionSql.java
├── control/GoldSyncTracker.java               # bookmarks, gold_record_status, Silver summary (§5)
└── control/TouchedPartitionRecorder.java      # dirty dates for aggregates (§10)
```

---

## 16. Reconciliation in Gold

| Check | Equation | On mismatch |
|---|---|---|
| Record counts | `silver_rows_read = success + failed + quarantined + skipped` | job FAILED |
| Merge counts | `staged = inserted + updated + unchanged` | job FAILED |
| No missing SKs | `count(fact rows with NULL SK) = 0` | job FAILED |
| Unknown share | `% of fact rows with SK -1` ≤ `maxUnknownSkPct` | WARN / circuit breaker |
| Measure totals | `Σ net_amt` of the Silver lines processed = `Σ net_amt` of the fact rows written in this run (± tolerance) | WARN (configurable strict) |
| Aggregate vs fact | for the touched dates: `Σ agg.net_amt = Σ fact.net_amt` | job FAILED |
| SCD integrity | one current row per natural key; no overlapping intervals | job FAILED |

Stored in `etl_ctl.reconciliation`, the same as for Bronze → Silver.

---

## 17. ➕ Added by Architect (summary)

| # | Addition | Why |
|---|---|---|
| G1 | Change number (`_etl_change_seq`) + bookmark per Gold job and source table | One Silver table, many Gold readers |
| G2 | Per-job failure list + Silver summary status with `_sync_gold_failed_jobs` | Correct retries per job and a clear view per Silver row |
| G3 | Single-writer rule per Silver table | Keeps the sequence-based bookmark safe |
| G4 | SK lookups and inferred members inside the EPAS transaction | No big dimension copies in Spark; facts and placeholders are always consistent |
| G5 | First SCD2 version starts at 1900-01-01; `valid_to` = 9999-12-31 | Older facts always find a version; simple range joins |
| G6 | Back-dated change policy (`APPLY_AS_NOW` / `REBUILD_TIMELINE` + fact re-keying) | Late corrections handled deliberately |
| G7 | Advisory lock per dimension during the SCD merge | Fact jobs and dimension jobs never create two current rows |
| G8 | Secondary-table change expansion for facts | Header changes reach the line-level fact |
| G9 | Partition-move handling for changed date keys | No double-counted facts |
| G10 | Dirty-partition aggregates in SQL inside EPAS | Fast, consistent summaries |
| G11 | KPI definitions file → OpenMetadata glossary | One meaning per KPI |
| G12 | Additive / semi-additive / non-additive measure rules | Prevents wrong sums in dashboards |
| G13 | `REVERSAL` delete strategy for finance facts | Accounting-style "never erase" |
| G14 | BI views (`v_*`) as the user contract | Users never forget filters; tables can evolve |
| G15 | Shadow-table swap for full fact rebuilds; never rebuild dimensions | No dashboard downtime; SKs stay stable |
| G16 | Masked PII in Gold dimensions; GDPR purge keeps SKs | Privacy without breaking facts |
| G17 | Partitions created ahead + DEFAULT partition alert | No failed loads at month start |
| G18 | Post-merge SCD integrity checks | Catches modeling bugs immediately |
| G19 | Optional Mongo serving copy from Gold views | Only when an app really needs it |

---

## 18. Changes to Earlier Decisions

| Earlier (doc 03) | Now | Reason |
|---|---|---|
| Silver had `_sync_gold_synced_version` and `_sync_gold_retry_count` | Replaced by `_etl_change_seq` (bookmark) + `etl_ctl.gold_record_status` (per-job retries); `_sync_gold_failed_jobs` added | Many Gold jobs read one Silver table |
| Doc 03 §19 open point "how to track Gold sync" | Resolved (§5) | — |
| The B→S merge set `_sync_gold_status = 'PENDING'` and `_sync_gold_retry_count = 0` | It sets `_etl_change_seq = nextval(...)`, `_sync_gold_status = 'PENDING'` and clears the error columns | New columns |
| SCD2 `valid_to` NULL for the current row (Silver `_hist` tables) | Gold dimensions use `9999-12-31`; Silver `_hist` tables keep NULL | Simpler BI joins in Gold; Silver stays as designed |

---

## 19. Key Decisions

1. **Gold = EPAS only** (`gold_core` for conformed dimensions + `gold_<area>` per subject area). A Mongo serving copy only on a real need.
2. **Star schema**; snowflake/outriggers only as a documented exception.
3. **Four generic job types:** `S2G_DIMENSION`, `S2G_FACT`, `S2G_SNAPSHOT`, `S2G_AGGREGATE` (SQL in EPAS), all YAML-driven.
4. **Incremental from Silver:** `_etl_change_seq` per Silver row + a bookmark per Gold job and source table in `etl_ctl.watermark`; secondary-table changes expand to the fact grain.
5. **Write-back to Silver:** per-job `etl_ctl.gold_record_status` for non-OK rows + summary columns `_sync_gold_status/updated_at/run_id/error_code/error_message/failed_jobs` on the Silver row.
6. **Surrogate keys** from EPAS identity columns; `-1` Unknown, `-2` N/A; SKs never change; dimensions are never truncated.
7. **SCD per column** (Type 1/2/3 in YAML); change detection in Spark with two hashes; apply in one EPAS transaction; first version from 1900-01-01, `valid_to` 9999-12-31; back-dated policy per dimension.
8. **Late-arriving dimensions** → inferred members created by the fact job, filled by the dimension job (same SK). Null keys → `-1`.
9. **Facts:** transactional, periodic snapshot, accumulating snapshot (+ factless); SK lookups "as of" the event time inside the merge; version + row-hash guard; partition-move handling; late facts restate the touched periods.
10. **Load order:** dimensions → facts → snapshots → aggregates, via `dependsOn`.
11. **Aggregates:** dirty-partition recompute in SQL; KPI definitions in `kpis.yml`; additivity rules respected.
12. **Deletes:** dimensions flagged/closed, never removed; facts `SOFT` (default) / `HARD` / `REVERSAL`; BI reads `v_*` views.
13. **Physical design:** monthly range partitions on the date key for large facts (created ahead + DEFAULT partition alert); indexes on SK columns, as-of index on SCD2 dimensions; no enforced FKs on big facts.
14. **Reconciliation:** counts, no NULL SKs, unknown-SK share, measure totals, aggregate = fact for touched dates, SCD integrity.
