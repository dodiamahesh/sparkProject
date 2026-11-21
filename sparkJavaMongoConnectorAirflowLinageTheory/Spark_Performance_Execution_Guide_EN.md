# Spark Execution, Data Load & Performance – Full Summary

> This document covers our whole **performance-related discussion**, in order:
> how Spark builds its plan, when execution starts, how data is loaded from MongoDB,
> what stays in memory, which methods to avoid, and best practices.
> At the end are **all your questions with short answers**.
>
> Scenario: **20M active users**, **5M orders / hour**, job runs **every 4 hours**,
> cluster = **1 master + 2 workers** (each: 8 GB RAM, i5, 50 GB SSD).

---

## Table of Contents

1. [Main Parts of Spark (Driver, Executor, Core, Partition, Task)](#1-main-parts-of-spark)
2. [Lazy Evaluation – Transformation vs Action](#2-lazy-evaluation--transformation-vs-action)
3. [How Spark Builds the Plan (Job → Stage → Task)](#3-how-spark-builds-the-plan)
4. [How Data Is Loaded from MongoDB](#4-how-data-is-loaded-from-mongodb)
5. [Incremental Load – Only New Data](#5-incremental-load--only-new-data)
6. [What Is in Memory and When?](#6-what-is-in-memory-and-when)
7. [Methods That Start Execution (Actions) – Full List](#7-methods-that-start-execution-actions)
8. [Danger List – Methods That Pull All Data into One Place (AVOID)](#8-danger-list--avoid-for-big-data)
9. [Calculation for Your Cluster](#9-calculation-for-your-cluster)
10. [Recommended Configuration](#10-recommended-configuration)
11. [Performance Best Practices](#11-performance-best-practices)
12. [Data Validation (DQ) & Rejected Records](#12-data-validation-dq--rejected-records)
13. [Monitoring – Spark UI](#13-monitoring--spark-ui)
14. [Final Checklist](#14-final-checklist)
15. [Your Questions – Short Answers](#15-your-questions--short-answers)
16. [Appendix – What Is Parquet?](#appendix--what-is-parquet)

---

## 1. Main Parts of Spark

**Example:** Picture a warehouse holding 2 million boxes. There is one manager and 2 workers, and each worker has 4 hands.

| In Spark | In the example | Job |
|---|---|---|
| **Driver (master)** | Manager | Builds the plan, hands out work, keeps track. **Never lifts data itself.** |
| **Executor (worker)** | Worker | Actually reads, processes and writes the data. |
| **Core** | A worker's hand | One core runs one task at a time. |
| **Partition** | A small pile of boxes | One chunk of data (default ~64 MB in the Mongo connector). |
| **Task** | The job of lifting one pile | 1 task per partition. |
| **Cluster Manager** | HR / supervisor | Gives resources (RAM, cores) on the workers (Standalone / YARN / K8s). |

```
                        ┌──────────────────────┐
                        │   DRIVER (master)    │
                        │  - builds the plan   │
                        │  - hands out tasks   │
                        │  - tracks status     │
                        └──────────┬───────────┘
                     instructions  │  (no data, only work)
                 ┌─────────────────┴─────────────────┐
        ┌────────▼────────┐                 ┌────────▼────────┐
        │   WORKER-1      │                 │   WORKER-2      │
        │   Executor      │                 │   Executor      │
        │ [c1][c2][c3][c4]│                 │ [c1][c2][c3][c4]│
        └────────┬────────┘                 └────────┬────────┘
                 │      data flows DIRECTLY in/out   │
                 └──────────────► MongoDB / Storage ◄┘
```

---

## 2. Lazy Evaluation – Transformation vs Action

Spark is **lazy**. When you write code, it only **takes notes**; it does no work.
All the work starts together only when an **action** is called.

| Type | What happens? | Example |
|---|---|---|
| **Transformation** | Only a new step is added to the plan. **No data is read.** | `select`, `filter`, `withColumn`, `join`, `groupBy`, `explode` |
| **Action** | The plan executes: data is read, processed, and a result is produced | `write`, `count`, `show`, `collect` |

```java
Dataset<Row> orders = spark.read().format("mongodb")...load();   // plan (only a small sample for schema)
Dataset<Row> items  = orders.select(explode(col("items")));       // plan
Dataset<Row> big    = items.filter(col("qty").gt(1));              // plan
big.write().parquet("/curated/fact_sales");                        // ← ACTION: now everything runs
```

> ⚠️ A small detail: at `load()` time, if the **schema** is unknown, the Mongo connector samples ~1000 docs to infer it (`sampleSize`).
> This is a small read, not all 20M. If you provide an **explicit schema**, even this read is avoided.

### 2 Kinds of Transformations

| Narrow (fast, no shuffle) | Wide (shuffle – expensive) |
|---|---|
| `select`, `filter`, `withColumn` | `join` (except broadcast) |
| `explode`, `map`, `flatMap` | `groupBy().agg()` |
| `union` / `unionByName` | `distinct`, `dropDuplicates` |
| `coalesce` (to reduce) | `orderBy` / `sort` |
| | `repartition`, Window functions |

> **Shuffle** = sending data over the network from one worker to another (to bring rows with the same key together).
> Shuffle = disk + network + time. **The most expensive operation.**

---

## 3. How Spark Builds the Plan

```
 Your Code
     │
     ▼
 ① Logical Plan          "what to do"  (read → explode → filter → join → write)
     │
     ▼
 ② Optimized Plan        Catalyst optimizer:
     │                    - moves filters earlier (filter first, then work)
     │                    - reads only the needed columns
     │                    - sends the filter to Mongo itself (pushdown)
     ▼
 ③ Physical Plan         "how to do it" – which join (Broadcast / SortMerge), how many chunks
     │
     ▼
 ④ Job → Stages → Tasks
       - Each ACTION = 1 Job
       - Each SHUFFLE starts a new Stage in the Job
       - In each Stage, each partition = 1 Task
```

### Example: Our fact_sales Job

```
 JOB (write action)
 │
 ├── STAGE 1 ──  read orders (320 tasks) → explode items → join dim_customer (broadcast, no shuffle)
 │                                                       → join dim_product (broadcast)
 │                       │
 │                ════ SHUFFLE (on order_id) ════   ← for the join with payments
 │                       │
 ├── STAGE 2 ──  read payments (N tasks)
 │                       │
 │                ════ SHUFFLE ════
 │                       │
 └── STAGE 3 ──  join orders+payments (shuffle.partitions = 160 tasks) → write parquet
```

To see the plan:
```java
factSales.explain("formatted");   // shows physical plan, join type, pushed filters
```

---

## 4. How Data Is Loaded from MongoDB

### 4.1 Who decides which core reads which data?

A core does not know anything on its own. **The Driver decides all the chunks up front.**
Think of a cake: it is cut before serving, and everyone gets a piece with their name on it.

| Who | Job |
|---|---|
| **Driver** | Asks Mongo: how many docs? avg size? sample `_id`s? |
| **Partitioner** (Mongo connector, default `SamplePartitioner`) | Computes `_id` ranges: P0 = A→B, P1 = B→C … |
| **Task Scheduler** (in the Driver) | Gives the next task from the queue to whichever core becomes free |
| **Executor core** | Runs its task's query, fetches the data, processes it |

### 4.2 Full Visualization – 10 AM Run

```
 10:00 AM  Airflow job start
           window = [06:00, 10:00)   ← upper bound FIXED

 ┌───────────────────────── STEP 1: PLANNING (Driver) ─────────────────────────┐
 │ Driver ──► MongoDB : "how many docs with created_at 06–10? avg size? sample?"│
 │ MongoDB ──► Driver : ~20M docs, ~1 KB avg, sample _id list   (just a few KB) │
 │                                                                              │
 │ Partitioner : 20 GB / 64 MB ≈ 320 chunks                                     │
 │   64 MB / 1 KB ≈ 65,000 docs per chunk → sort sample _ids → boundaries       │
 │                                                                              │
 │ TASK LIST (fixed):                                                           │
 │   P0   : _id >= 65a0..000 AND _id < 65a0..9f3  + created_at filter           │
 │   P1   : _id >= 65a0..9f3 AND _id < 65a1..1c2  + created_at filter           │
 │   ...                                                                        │
 │   P319 : _id >= 65b3..e11 AND _id <= 65b3..fff + created_at filter           │
 └──────────────────────────────────────────────────────────────────────────────┘

 ┌───────────────────────── STEP 2: EXECUTION ─────────────────────────────────┐
 │ Task Queue: [P0 … P7 | P8 P9 … P319]                                         │
 │                                                                              │
 │ Worker-1  c1◄P0  c2◄P1  c3◄P2  c4◄P3                                         │
 │ Worker-2  c1◄P4  c2◄P5  c3◄P6  c4◄P7        ← 8 tasks at once                │
 │                                                                              │
 │ Worker-2 c3 free first  ──► Scheduler: "take P8"                             │
 │ Worker-1 c1 free        ──► Scheduler: "take P9"   (no fixed rounds,         │
 │ ...                                                 whoever is free takes)   │
 │                                                                              │
 │ Each task sends ITS OWN query to MongoDB:                                    │
 │  find({ _id:{$gte:X,$lt:Y}, created_at:{$gte:06:00,$lt:10:00} })             │
 └──────────────────────────────────────────────────────────────────────────────┘

 ┌───────────────────────── STEP 3: FAILURE ───────────────────────────────────┐
 │ P42 was running and Worker-2 crashed                                         │
 │ Driver: "P42 not finished" ──► P42 again on another core                     │
 │ Range is fixed → exactly the same data again (no duplicates)                 │
 └──────────────────────────────────────────────────────────────────────────────┘
```

### 4.3 Does data go to the Driver? → **NO**

```
 ❌ THIS DOES NOT HAPPEN                  ✅ THIS HAPPENS

 MongoDB                                  MongoDB
    │ 20M (one call)                        ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲   ← 8 separate queries at once
    ▼                                       │ │ │ │ │ │ │ │
 Driver (20 GB in memory)                ┌──┴─┴─┴─┴┐ ┌┴─┴─┴─┴──┐
    │                                    │Worker-1 │ │Worker-2 │
    ▼                                    └─────────┘ └─────────┘
 Workers
```

| Who | Calls to Mongo | How much data |
|---|---|---|
| Driver | 1–2 small planning calls | A few KB (boundaries) |
| Each task | 1 query for its own range | Its own chunk (~64 MB) |

- 320 chunks = **320 separate queries on MongoDB**, but only **8** at a time (your cores).
- Each task also brings its chunk in **cursor batches**: a few docs arrive, get processed, then the next ones.
- So Mongo sees the load of ~8 connections at a time.

### 4.4 Why `_id`?
An `ObjectId` has the **creation time embedded** in it and always increases. That makes it the best field for building ranges, and it is already indexed.

---

## 5. Incremental Load – Only New Data

### Problem
```java
spark.read().format("mongodb").option("collection","orders").load();   // ❌ the WHOLE collection
```
Spark does not know by itself what "new data" is. Without a filter it will read **all the old data** too.

### Solution – fixed time window

```
  02:00 ──────── 06:00 ──────── 10:00 ──────── 14:00
          Run@6AM        Run@10AM       Run@2PM
        [02:00,06:00)  [06:00,10:00)  [10:00,14:00)

  A new order arriving after 10:00 while the job runs → goes into the 2 PM run ✅
```

| Rule | Why? |
|---|---|
| **Give both Start + End** (`>= start AND < end`) | New data arriving during the job does not mix into this run |
| Take the window from **Airflow** (`{{ data_interval_start }}`, `{{ data_interval_end }}`) | A re-run gets exactly the same data |
| Or a **control table** `etl_watermark` (`last_success_end`) | Updated only after the job **succeeds** |
| Put the filter in **`aggregation.pipeline`** | The filter runs inside Mongo and the partitioner uses the same filter |
| **Index** on `created_at` in Mongo | Otherwise Mongo scans the whole collection |
| A small **overlap** for late data (e.g. 10 min) + MERGE / dedupe | Sometimes an order is inserted late with an older timestamp |

```java
String pipeline = "[{ $match: { created_at: { $gte: ISODate('" + start + "'), $lt: ISODate('" + end + "') } } }]";
```

### 5.1 For a summary table (aggregated table like Customer 360)
- Rahul places a new order → **his existing row is updated** (grain = 1 row per customer)
- New customer → **new row inserted** (through the `dim_customer` LEFT JOIN)
- ⚠️ Don't just do "+1" on the old numbers. On a refund, `net_spent` must go down, so recompute the affected customer's whole summary from the **full history of the fact table**.
- The method depends on the target:

| Target | Method |
|---|---|
| **EPAS / Postgres** (ours) | Staging table + `INSERT ... ON CONFLICT DO UPDATE` (upsert). See 5.2. |
| Data lake – plain Parquet | Rows cannot be updated, so rebuild the whole table **from full history** + overwrite |
| Data lake – Delta / Iceberg / Hudi | `MERGE INTO` (see the [Appendix](#appendix--what-is-parquet)) |

### 5.2 Loading into EPAS (EDB Postgres) – Upsert, Not Overwrite

#### ❌ Danger – overwrite with an incremental batch

```java
newBatch.write().format("jdbc").mode("overwrite")...   // ❌ only the 20M from 06:00–10:00
```

```
 Before: fact_sales = 6 months of data (crores of rows)
 After:  fact_sales = only this run's 20M rows   ← old data GONE ❌
```

- Spark JDBC `overwrite` by default **DROPs the table and creates a new one**, so indexes, constraints and grants are lost too.
- **Rule:** **Never overwrite** a curated table with an incremental batch.

#### ✅ EPAS can update rows

Parquet's "rows cannot be updated" limitation **does not exist in EPAS / Postgres**. `UPDATE`, `INSERT ... ON CONFLICT` and `MERGE` all work.
But **Spark JDBC does not upsert by itself**; it only does `append` or `overwrite`. So use a **staging table** in between:

```
 Spark ──(overwrite + truncate)──► staging.stg_*  (only this run's data)
                                        │
                                        ▼  SQL: INSERT ... ON CONFLICT DO UPDATE   (inside EPAS)
                                  curated.* table  (old data SAFE + new added/updated)
```

#### Which method for which table?

| Table | Method | Why |
|---|---|---|
| `fact_sales`, `fact_payment` | **Append** + `ON CONFLICT DO NOTHING` | Orders are only ever new. A re-run creates no duplicates. |
| `dim_customer`, `dim_product` | **Upsert** (`ON CONFLICT DO UPDATE`) | New customers get inserted; a changed city or tier gets updated |
| `customer_360_summary` | Upsert only **changed customers** | No full rebuild of 20M users |
| `staging.stg_*` | `overwrite` + `truncate=true` | It should hold only the current run's data |

#### (a) Spark → staging table
```java
batch.write().format("jdbc")
    .option("url", "jdbc:postgresql://epas-host:5444/dwh?reWriteBatchedInserts=true")
    .option("dbtable", "staging.stg_customer")
    .option("user", user).option("password", pwd)
    .option("truncate", "true")          // no DROP, only TRUNCATE (structure kept)
    .option("batchsize", "10000")        // default 1000 → faster inserts
    .option("numPartitions", "8")        // max 8 connections to EPAS at once
    .mode("overwrite")                   // ONLY for STAGING
    .save();
```

#### (b) Staging → curated: dim upsert
```sql
INSERT INTO curated.dim_customer (customer_id, customer_name, city, state, customer_tier, updated_at)
SELECT customer_id, customer_name, city, state, customer_tier, now()
FROM staging.stg_customer
ON CONFLICT (customer_id) DO UPDATE
SET customer_name = EXCLUDED.customer_name,
    city          = EXCLUDED.city,
    state         = EXCLUDED.state,
    customer_tier = EXCLUDED.customer_tier,
    updated_at    = now();
```

#### (c) Fact: append, re-run safe
```sql
INSERT INTO curated.fact_sales (order_id, product_key, customer_key, date_key, quantity, unit_price, gross_amount, net_amount)
SELECT s.order_id, p.product_key, c.customer_key, s.date_key, s.quantity, s.unit_price, s.gross_amount, s.net_amount
FROM staging.stg_fact_sales s
JOIN curated.dim_customer c ON c.customer_id = s.customer_id
JOIN curated.dim_product  p ON p.product_id  = s.product_id
ON CONFLICT (order_id, product_key) DO NOTHING;   -- re-running the same window creates no duplicates
```

#### (d) Customer 360: only changed customers, from full history
```sql
INSERT INTO gold.customer_360_summary (customer_id, total_orders, net_spent, last_order_date, refreshed_at)
SELECT c.customer_id, COUNT(DISTINCT f.order_id), COALESCE(SUM(f.net_amount), 0), MAX(d.full_date), now()
FROM curated.dim_customer c
LEFT JOIN curated.fact_sales f ON f.customer_key = c.customer_key
LEFT JOIN curated.dim_date  d ON d.date_key     = f.date_key
WHERE c.customer_id IN (SELECT DISTINCT customer_id FROM staging.stg_changed_customers)
GROUP BY c.customer_id
ON CONFLICT (customer_id) DO UPDATE
SET total_orders    = EXCLUDED.total_orders,
    net_spent       = EXCLUDED.net_spent,
    last_order_date = EXCLUDED.last_order_date,
    refreshed_at    = now();
```

> The summary is built from the **full history of `fact_sales`**, not just today's batch. So Rahul's 2 old + 1 new = the correct **3 orders**.
>
> Run the SQL for steps (b), (c) and (d) in **Airflow** with `SQLExecuteQueryOperator`, or as a single JDBC statement from the Spark driver. It is just one command; no data comes to the driver.

#### Things to watch for EPAS

| Point | Why |
|---|---|
| **Generate the surrogate key in the DB** (`GENERATED ALWAYS AS IDENTITY`) | Spark's `row_number()` restarts at 1 every run, so keys would collide |
| **Unique index / constraint**: `customer_id`, `(order_id, product_key)` | Required for `ON CONFLICT` |
| **Partition the fact table by date** (Postgres declarative partitioning) | ~120M rows/day, so queries and deleting old data become easier |
| Keep JDBC `numPartitions` low (e.g. 8) | 320 Spark tasks opening connections at once would overload the DB |
| Upsert inside a **transaction** | No half-finished load is visible |
| For very large loads use **`COPY`** instead of JDBC | COPY is the fastest bulk load in Postgres |
| **Dim upsert** before the fact load | The dim must be ready for the fact's `customer_key` lookup |

---

## 6. What Is in Memory and When?

| Moment | Driver memory | Executor memory |
|---|---|---|
| `read()...load()` | Plan + schema | Nothing |
| Transformations written | Only the plan | Nothing |
| `write()` running | Task list + status | ~8 chunks at a time (each ~64 MB, 2–3× bigger in the JVM) |
| Shuffle | Nothing | Shuffle buffers. If no room → **spill to disk** |
| `count()` | Only 320 numbers | Each task counts its own rows |
| `collect()` | ❌ **All 20M rows** → OOM | — |

**Spill** = when memory runs short, Spark puts data on `spark.local.dir` (SSD). The job gets **slower but does not crash**. The Spark UI shows "Spill (Disk)".

---

## 7. Methods That Start Execution (Actions)

A Spark job runs **only when** one of these methods is called.

### 7.1 Safe actions (data stays on executors or only a small result comes back)

| Method | What it does | What reaches the driver |
|---|---|---|
| `write().parquet() / save() / saveAsTable() / insertInto()` | Data goes straight to storage | Nothing (only status) |
| `write().format("mongodb").save()` | Data goes straight to Mongo | Nothing |
| `count()` | Counts rows | 1 number |
| `show()` / `show(20)` | Shows a few rows | 20 rows |
| `take(10)`, `head(10)`, `first()`, `limit(10).collect()` | A few rows | 10 rows |
| `isEmpty()` | Is there any data? | true/false |
| `foreach()`, `foreachPartition()` | Work on each row/partition **on the executor** | Nothing |
| `agg(sum, max…).collect()` | Aggregated result | 1 row (small) |

### 7.2 Hidden triggers – a job runs without you noticing

| Method | Why a job runs |
|---|---|
| `load()` without a schema | Samples ~1000 docs for the schema |
| `orderBy()` / `sort()` | Sample job to find ranges |
| `pivot("col")` without values | Job to find distinct values |
| `checkpoint()` | Eager by default – writes immediately |
| `cache()` + action | `cache()` itself is lazy, but on the first action all the data goes into memory |

---

## 8. Danger List – Avoid for Big Data

These methods **bring all the data into one place (mostly the Driver's memory)**. On 20M rows they will throw **`OutOfMemoryError`**.

### 8.1 Pull everything into Driver memory ❌

| Method | Problem | Safe alternative |
|---|---|---|
| `collect()` / `collectAsList()` | **All rows go to the driver** | Use `write()`, or `limit(n).collect()` |
| `take(n)` / `head(n)` with a big `n` | n rows on the driver | Keep n small |
| `show(1000000)` | That many rows on the driver | `show(20)` |
| `toLocalIterator()` | One partition at a time, but **everything passes through the driver** – very slow | `foreachPartition()` |
| `toPandas()` (PySpark) | Whole dataset on the driver | Only after aggregating |
| `rdd.collectAsMap()` | Whole map on the driver | Use a join |
| `rdd.countByKey()` / `countByValue()` | With many keys, a huge map on the driver | `groupBy().count().write()` |
| `broadcast(bigTable)` | First collected on the driver, then sent to all workers | Broadcast only small dims (< 10–100 MB) |
| Filling a driver-side `List` / `ArrayList` with rows | Driver OOM | Keep it in a DataFrame |

### 8.2 Pile everything onto one place in an executor ⚠️

| Method | Problem | Safe alternative |
|---|---|---|
| `coalesce(1)` / `repartition(1)` | All data in **1 task** – only 1 core works | Write multiple files, or `coalesce(n)` |
| `rdd.groupByKey()` | All values of a key in one task | `reduceByKey` / DataFrame `groupBy().agg()` |
| `collect_list()` / `collect_set()` on a big group | Lakhs of values in one row | Only when really needed |
| `cache()` / `persist()` on big data | Fills executor memory | Only if reused 2+ times. `unpersist()` afterwards |
| `Window.orderBy()` without `partitionBy` | All data in 1 partition | Always give `partitionBy(...)` |
| Skewed join (crores of rows on one key) | One task takes hours | AQE skew join, salting |

> **Golden Rule:** For big data, **write the result to storage (`write`)**. Bring only **small numbers or summaries** to the driver.

---

## 9. Calculation for Your Cluster

| Item | Estimate |
|---|---|
| Orders / run | 5M × 4 hours = **20M** |
| Size of 1 order | ~1 KB (assumed) |
| Raw data / run | **~20 GB** |
| After exploding items | ~50–60M rows |
| Partitions (64 MB) | ~320 tasks |
| Total cores | 2 × 4 = **8 at once** |
| Rounds | 320 / 8 ≈ **40 waves** |
| Executor memory | ~**5 GB** of 8 GB RAM (~3 GB for the OS) |
| Memory per task | ~1 GB |
| Disk | 50 GB SSD (OS + shuffle spill + temp) |
| Runs / day | 6 × 20 GB = **~120 GB / day** |

### Main Risks

1. **Disk is the biggest problem** – if you write output to worker disks they will fill up within a few days. Write output to **EPAS** (staging → upsert) or to separate storage like **HDFS / S3 / MinIO**.
2. **Shuffles (join, groupBy) are expensive** – with 8 GB RAM there will be spill, so the job will be slow.
3. **Customer 360 for 20M users** – don't recompute everyone every run. Only **changed customers**.
4. **Load on production Mongo** – read with `secondaryPreferred`.
5. **5M orders/hour ≈ 1400 orders/sec** – this is a very large number. Confirm whether it is really per hour or per day. The whole hardware plan depends on it.

> In short: the job can run, but **slowly and with tight disk**. This cluster is small for production scale.

---

## 10. Recommended Configuration

### 10.1 Mongo read
```java
String start = args[0];   // {{ data_interval_start }}
String end   = args[1];   // {{ data_interval_end }}

String pipeline = "[{ $match: { created_at: { $gte: ISODate('" + start + "'), $lt: ISODate('" + end + "') } } }]";

Dataset<Row> orders = spark.read().format("mongodb")
    .schema(ordersSchema)                                            // avoids sampling
    .option("connection.uri", "mongodb://host:27017/?readPreference=secondaryPreferred")
    .option("database", "shop")
    .option("collection", "orders")
    .option("aggregation.pipeline", pipeline)                        // incremental filter
    .option("partitioner", "com.mongodb.spark.sql.connector.read.partitioner.SamplePartitioner")
    .option("partitioner.options.partition.field", "_id")
    .option("partitioner.options.partition.size", "64")              // MB per chunk (64–128)
    .option("partitioner.options.samples.per.partition", "10")
    .load()
    .select("order_id", "order_date", "customer", "items", "payment_mode", "status");  // only needed columns
```

| Setting | Purpose | Value |
|---|---|---|
| `schema(...)` | Avoids schema sampling, fixes types | Explicit `StructType` |
| `aggregation.pipeline` | Only the window's data from Mongo | `$match` start + end |
| `partitioner` | How chunks are built | `SamplePartitioner` |
| `partition.field` | Which field the ranges use | `_id` |
| `partition.size` | How many MB per chunk | 64–128 |
| `readPreference` | Protects the app's primary | `secondaryPreferred` |
| Mongo index | Fast filter | `createIndex({created_at: 1})` |

### 10.2 spark-submit
```bash
spark-submit \
  --master spark://master:7077 \
  --driver-memory 2g \
  --executor-memory 5g \
  --executor-cores 4 \
  --total-executor-cores 8 \
  --conf spark.sql.shuffle.partitions=160 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.sql.adaptive.skewJoin.enabled=true \
  --conf spark.sql.autoBroadcastJoinThreshold=50m \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.local.dir=/data/spark-tmp \
  orders-etl.jar "$START" "$END"
```

| Setting | Meaning |
|---|---|
| `driver-memory 2g` | No data reaches the driver, so 2 GB is enough |
| `executor-memory 5g` | Leaves room for the OS on an 8 GB machine |
| `executor-cores 4` | 4 tasks at once on each worker |
| `total-executor-cores 8` | 8 tasks across the whole cluster |
| `shuffle.partitions 160` | How many chunks after a join / groupBy (default 200) |
| `adaptive.enabled` (AQE) | At runtime merges small chunks, handles skew, changes join type |
| `autoBroadcastJoinThreshold` | Tables smaller than this are broadcast automatically (dim tables) |
| `KryoSerializer` | Faster and smaller serialization |
| `spark.local.dir` | SSD folder for spill / shuffle |

### 10.3 Write
```java
factSales
    .write()
    .mode("append")
    .partitionBy("date_key")                 // queries read only the needed days
    .parquet("s3a://datalake/curated/fact_sales");   // not the worker disk, separate storage
```

### 10.4 Write – EPAS (our target)
```java
stgFactSales.write().format("jdbc")
    .option("url", "jdbc:postgresql://epas-host:5444/dwh?reWriteBatchedInserts=true")
    .option("dbtable", "staging.stg_fact_sales")
    .option("truncate", "true")
    .option("batchsize", "10000")
    .option("numPartitions", "8")
    .mode("overwrite")                // ONLY staging; curated uses SQL upsert (Section 5.2)
    .save();
```

| Setting | Meaning |
|---|---|
| `truncate=true` | On overwrite, TRUNCATE the table instead of DROP |
| `batchsize` | How many rows per insert round-trip (default 1000) |
| `numPartitions` | How many connections to EPAS at once |
| `reWriteBatchedInserts=true` | Rewrites batched inserts into multi-row INSERTs – faster |

---

## 11. Performance Best Practices

### Read
1. ✅ **Incremental filter** (start + end) – never the whole collection
2. ✅ Filter **inside Mongo** (`aggregation.pipeline`) + **index**
3. ✅ `select` **only the needed columns** (less data over the network)
4. ✅ Provide an **explicit schema**
5. ✅ `readPreference=secondaryPreferred`

### Transform
6. ✅ **Filter first, join later** (fewer rows go into the join)
7. ✅ **Broadcast** small dims (`dim_customer` if small, `dim_product`, `dim_date`) to avoid a shuffle
8. ✅ Reduce shuffles – no unnecessary `distinct`, `orderBy`, `repartition`
9. ✅ Keep **AQE** on
10. ✅ Fewer UDFs – use Spark built-in functions (`when`, `explode`, `date_format`)
11. ✅ `cache()` only if reused 2+ times, then `unpersist()`
12. ✅ Watch out for **skew** (too much data on one customer or one date)

### Write
13. ✅ `partitionBy("date_key")` – but not too many partitions
14. ✅ Avoid many tiny files – AQE coalesce / `coalesce(n)` (not 1)
15. ✅ Output to **EPAS** or **HDFS / S3 / MinIO**, not the worker's local disk
16. ✅ **Never overwrite** a curated table with an incremental batch. In EPAS use **staging + `ON CONFLICT` upsert**; in a data lake use **Delta / Iceberg `MERGE`**

### Design
17. ✅ **Dims first, facts after** (Airflow DAG dependency)
18. ✅ Aggregated tables (Customer 360) **incremental** – only changed keys
19. ✅ Old data cleanup / retention policy
20. ✅ **Never** `collect()` big data on the driver

---

## 12. Data Validation (DQ) & Rejected Records

> Question: 1M of the 20M records are corrupt or fail the validation rules.
> They must go to a **separate (rejected) table with an error message**.
> Will this happen within the same plan and the same execution flow?

**Answer:** Yes, validation rules also run **in the same plan and the same pass**.
But **if you write valid and rejected separately, Mongo is read twice**, so you need to use the right method.

### 12.1 How do rules become part of the plan?

Validation rules are also just **transformations** (`when`, `withColumn`, `filter`).
So Spark adds them to the plan, and each task applies the rules **on its own chunk** while reading.

```
 Task P8:  row arrives from Mongo ─► explode ─► RULES CHECK ─► set error_message ─► set dq_status ─► write
           (all in one pass, row-by-row – no separate read for validation)
```

### 12.2 Where does validation fit in the pipeline?

```
 MongoDB (window 06:00–10:00, 20M)
        │
        ▼
 ① READ  (risky fields as String)
        │
        ▼
 ② FLATTEN / EXPLODE
        │
        ▼
 ③ VALIDATE  (row-level + lookup rules → errors[], dq_status)
        │
        ▼
 ④ SINGLE WRITE  partitionBy("dq_status")       ← Mongo read only once
        │
        ├──► staging/orders_checked/dq_status=VALID/     (19M)
        │            │
        │            ▼
        │      ⑥ dim_* / fact_*  (curated)
        │
        └──► staging/orders_checked/dq_status=REJECTED/  (1M)
                     │
                     ▼
               ⑤ rejected_orders table (with error_message)
                     │
                     ▼
               ⑦ THRESHOLD CHECK  (rejected > 5% → job FAIL + Airflow alert)
```

### 12.3 Validation code (row-level rules)

```java
Dataset<Row> checked = items
    .withColumn("errors", array_remove(array(
        when(col("order_id").isNull(),                                   lit("order_id missing")),
        when(col("customer_id").isNull(),                                lit("customer_id missing")),
        when(col("quantity_raw").isNotNull()
             .and(col("quantity_raw").cast("int").isNull()),             lit("quantity not numeric")),
        when(col("quantity_raw").cast("int").leq(0),                     lit("quantity <= 0")),
        when(col("unit_price").isNull(),                                 lit("unit_price missing")),
        when(not(col("payment_mode").isin("UPI", "CARD", "COD")),        lit("invalid payment_mode")),
        when(col("order_date").isNull(),                                 lit("order_date missing/invalid"))
    ), null))                                                  // remove nulls → only failed rules remain
    .withColumn("dq_status",     when(size(col("errors")).gt(0), "REJECTED").otherwise("VALID"))
    .withColumn("error_message", concat_ws("; ", col("errors")))
    .withColumn("run_id",        lit(runId))
    .withColumn("checked_at",    current_timestamp());
```

> Each row gets **all the rules it failed** together (not just the first). That makes debugging easier.

### 12.4 ⚠️ Main Trap – 2 writes = 2 reads from Mongo

```java
// ❌ WRONG
checked.filter("dq_status = 'VALID'").write()...      // Action 1 → reads 20M from Mongo
checked.filter("dq_status = 'REJECTED'").write()...   // Action 2 → reads 20M from Mongo AGAIN
```

Each action runs **the whole plan again**: Mongo read, explode and validate, all a second time.
So time doubles, Mongo load doubles, and if late data arrives in between, the two runs may even see different data.

| Method | How | For your cluster |
|---|---|---|
| **A. One write + `partitionBy("dq_status")`** | A single action writes VALID and REJECTED into separate folders | ✅ **Best** – only 1 read from Mongo |
| B. `persist()` + 2 writes | Cache the validated data, then 2 writes | ⚠️ 20 GB vs 8 GB RAM, heavy spill |
| C. Staging table | Write all checked data once, then read from it | ✅ Safe, and good for audit / re-run (A is also staging) |

```java
// ✅ CORRECT – Option A
String stagingPath = "s3a://datalake/staging/orders_checked/run_id=" + runId;

checked.write()
    .mode("overwrite")
    .partitionBy("dq_status")
    .parquet(stagingPath);

// Next: VALID → curated
Dataset<Row> valid = spark.read().parquet(stagingPath + "/dq_status=VALID");
// ... dim / fact build

// REJECTED → rejected table
spark.read().parquet(stagingPath + "/dq_status=REJECTED")
    .select("run_id", "order_id", "customer_id", "product_id", "quantity_raw",
            "unit_price", "payment_mode", "order_date", "error_message", "checked_at")
    .withColumn("source_collection", lit("orders"))
    .write().mode("append").partitionBy("run_id")
    .parquet("s3a://datalake/curated/rejected_orders");
```

> **If the target is EPAS:** JDBC has no `partitionBy`. So write all of `checked` with **a single write** into `staging.stg_orders_checked` (with a `dq_status` column).
> Then, inside EPAS, use SQL to move `dq_status='VALID'` → curated and `dq_status='REJECTED'` → `rejected_orders`. Mongo is still read only once.

### 12.5 Rejected table – sample output

**`rejected_orders`**

| run_id | source_collection | order_id | customer_id | product_id | quantity_raw | payment_mode | error_message | checked_at |
|---|---|---|---|---|---|---|---|---|
| 20261120_10 | orders | O2001 | null | P003 | 1 | UPI | customer_id missing | 2026-11-20 10:42 |
| 20261120_10 | orders | O2002 | C004 | P005 | abc | CARD | quantity not numeric | 2026-11-20 10:42 |
| 20261120_10 | orders | O2003 | C007 | P009 | -2 | PAYTM | quantity <= 0; invalid payment_mode | 2026-11-20 10:42 |
| 20261120_10 | orders | O2004 | C010 | P099 | 1 | COD | product_id not in dim_product | 2026-11-20 10:42 |

> Tip: For debugging you can also keep the whole **original document** as a JSON string (`raw_payload`). Use `to_json(struct(col("*")))`.

### 12.6 Not all rules run in one pass – 3 types

| Rule type | Example | In one pass? | How |
|---|---|---|---|
| **Row-level** | null, range (`qty > 0`), format (email), allowed values | ✅ Yes | `when(...)` – no shuffle |
| **Lookup / FK** | Does `product_id` exist in `dim_product`? | ✅ Yes, if the dim is small | **Broadcast left join** + `when(col("product_key").isNull(), "product_id not in dim_product")` |
| **Dataset-level** | Duplicate `order_id`, total count, "rejected > 5%" | ❌ No | `groupBy` / `Window` (shuffle) or a separate `count()` |

**Lookup rule:**
```java
Dataset<Row> withLookup = items
    .join(broadcast(dimProduct.select("product_id", "product_key")), col("product_id"), "left");
// product_key null → "product_id not in dim_product"
```

**Duplicate rule (causes a shuffle):**
```java
WindowSpec w = Window.partitionBy("order_id", "product_id").orderBy(col("updated_at").desc());
.withColumn("rn", row_number().over(w))
// rn > 1 → "duplicate order line"
```

### 12.7 Records with corrupt types

For example `quantity = "abc"`, or `order_date = "31-31-2026"`.

| Method | What happens |
|---|---|
| ❌ Read with a strict schema (`IntegerType`) | The **job can fail** during type conversion and never reach validation |
| ✅ Read risky fields as **`StringType`**, then `cast` | A failed cast gives `null`, and the row is **rejected** with an error. The job keeps running. |

```java
.withColumn("quantity", col("quantity_raw").cast("int"))
.withColumn("order_date", to_timestamp(col("order_date_raw")))
// null after cast, but raw is not null → corrupt value
```

### 12.8 Threshold check – when should the job fail?

**Don't run the count on Mongo again.** Count on the staging output you already wrote; it costs very little.

```java
long total    = spark.read().parquet(stagingPath).count();
long rejected = spark.read().parquet(stagingPath + "/dq_status=REJECTED").count();
double pct    = rejected * 100.0 / total;

if (pct > 5.0) {
    throw new RuntimeException("DQ FAIL: " + rejected + " / " + total + " (" + pct + "%) rejected");
    // Airflow task fails → alert, curated load does not happen
}
```

Example: 1M rejected out of 20M = **5%**. With a 5% limit it is right on the border; with a 2% limit the job fails.

### 12.9 DQ summary table (optional, for monitoring)

**`dq_run_summary`**

| run_id | source_collection | total_records | valid_records | rejected_records | rejected_pct | status |
|---|---|---|---|---|---|---|
| 20261120_06 | orders | 20,000,000 | 19,950,000 | 50,000 | 0.25 | PASS |
| 20261120_10 | orders | 20,000,000 | 19,000,000 | 1,000,000 | 5.00 | WARN |

A dashboard on this table shows which run had poor data quality.

### 12.10 Validation in the Airflow DAG

```
 extract_window
      │
      ▼
 validate_and_stage        (single write, partitionBy dq_status)
      │
      ▼
 dq_threshold_check        (fail → alert, nothing downstream runs)
      │
      ├──────────────► load_rejected_orders
      ▼
 load_dims ──► load_facts ──► build_gold
```

### 12.11 Summary

- ✅ Row-level and lookup validation run **in the same plan and the same pass**, with no extra Mongo read.
- ⚠️ VALID and REJECTED as **2 separate writes = 2 Mongo reads**. Use **one write + `partitionBy("dq_status")`**.
- ⚠️ Rules like duplicates and thresholds need a shuffle or another action. Count on the **staging output**.
- ✅ Read risky fields as `String`, `cast` them, and reject on failure instead of crashing.
- ✅ Every rejected row has: `run_id`, `error_message` (all rules), and `raw_payload` if needed.
- ✅ If the rejected % exceeds the limit, fail the job + alert, and stop the curated load.

---

## 13. Monitoring – Spark UI

While the job runs: `http://<master>:4040` (for history: Spark History Server)

| Tab | What to look at |
|---|---|
| **Jobs** | How many jobs (each action = 1 job) |
| **Stages** | How many tasks, how many finished, **Shuffle Read/Write**, **Spill (Memory/Disk)** |
| **Stage → Tasks** | Is one task taking much longer than the others? → **skew** |
| **Executors** | Tasks per worker, GC time, memory used, failed tasks |
| **SQL** | Query plan graph, which join (Broadcast / SortMerge), pushed filters |

**Red flags:**
- Spill (Disk) in GB → too little memory or too few partitions
- One task 10× slower → skew
- GC time > 10% → check executor memory / partition size
- Only 1 task running → `coalesce(1)` or a `Window` without `partitionBy`

---

## 14. Final Checklist

```
 READ
 [ ] Incremental window: start + end (Airflow data_interval)
 [ ] Filter in aggregation.pipeline
 [ ] Mongo index on created_at
 [ ] readPreference = secondaryPreferred
 [ ] Explicit schema + only needed columns

 TRANSFORM
 [ ] Filter first, join later
 [ ] Small dims broadcast
 [ ] AQE ON, shuffle.partitions set
 [ ] No collect() / collectAsList() on big data

 DATA VALIDATION
 [ ] Risky fields read as String, then cast
 [ ] All rules in errors[], with error_message
 [ ] Single write + partitionBy("dq_status") (not 2 writes)
 [ ] Rejected table has run_id + error_message (+ raw_payload)
 [ ] Threshold check on staging output, fail → alert

 WRITE (EPAS)
 [ ] NEVER overwrite a curated table
 [ ] Spark → staging.stg_* (overwrite + truncate=true)
 [ ] Staging → curated: INSERT ... ON CONFLICT (upsert / do nothing)
 [ ] Surrogate key as DB identity, unique constraints set
 [ ] Fact table partitioned by date, low JDBC numPartitions

 WRITE (if data lake)
 [ ] Output to separate storage (HDFS / S3 / MinIO)
 [ ] partitionBy(date_key)
 [ ] Delta/Iceberg MERGE for upserts

 OPS
 [ ] Dims → Facts → Gold (DAG order)
 [ ] Check spill / skew in Spark UI
 [ ] Disk retention / cleanup
```

---

## 15. Your Questions – Short Answers

### ETL Design

**Q1. Why do we split data from Raw into Curated? What is the purpose?**
→ MongoDB data is built **for the app** (nested, repeated). Splitting it into **Dim + Fact** makes it correct, fast and easy to understand **for analytics**: fewer duplicates, updates in one place, and ready for BI tools.

**Q2. Case 1 – Single collection → multiple Dims + Facts?**
→ Break one embedded collection (`orders`) apart with `explode` + `dropDuplicates`. It is simple, but the dims stay incomplete: snapshot data, and customers without orders are missing.

**Q3. Case 2 – Multiple collections → multiple Dims + Facts?**
→ **Full dims** from master collections (customers, products), and **facts** from transaction collections (orders, payments) + key lookup. The most common approach for an enterprise DWH.

**Q4. Case 3 – Multiple collections → a single table, is that possible?**
→ **Yes.** JOIN merge (Customer 360, flat report) or UNION merge (website + app + store orders). Mostly in the **Gold layer**, for dashboards / ML / exports.

### Incremental / Update

**Q5. If Rahul places a 3rd order a few days later, will his row in `customer_360_summary` be updated?**
→ **Yes**, no new row; **the same row is updated** (grain = 1 row per customer). In EPAS, via staging + `INSERT ... ON CONFLICT DO UPDATE` (Section 5.2).

**Q6. Will a new customer be inserted?**
→ **Yes**, through the LEFT JOIN. With no orders, they appear with `0` / `Never Ordered`.

### Spark Execution

**Q7. Will Spark load all 20M records from 4 hours into memory at once?**
→ **No.** It is true that 20M will be processed in this run, but Spark breaks them into ~64 MB chunks and processes only 8 chunks (8 cores) at a time.

**Q8. When `write()` or `count()` is called, do all 20M come into memory?**
→ **No.** `write()` reads and writes chunk by chunk. `count()` sends only numbers to the driver. **With `collect()` / `collectAsList()`, yes**: everything comes to the driver and causes OOM.

**Q9. How does a core know how far it has read and where to read from next?**
→ The core doesn't need to track that. **The Driver builds all the chunks (task list) with `_id` ranges up front.** A free core takes the next task from the queue.

**Q10. Who decides, and how, who reads how much?**
→ **The Driver + the Mongo connector's Partitioner**: it gets a count + sample `_id`s from Mongo, computes boundaries from the partition size, and the **Task Scheduler** hands tasks to free cores.

**Q11. What configuration is needed for this?**
→ `aggregation.pipeline` (incremental filter), `partitioner`, `partition.field=_id`, `partition.size`, `readPreference`, the Mongo index, and in spark-submit `executor-memory`, `executor-cores`, `shuffle.partitions`. See [Section 10](#10-recommended-configuration).

**Q12. For 320 chunks, is Mongo called separately each time, or does the master fetch the data once and keep it in memory?**
→ **Each chunk makes its own call**, and data flows **directly from Mongo to the executor**. Data **never** reaches the Driver; it only provides the plan and instructions.

**Q13. What if new data arrives while the job is running?**
→ The window's upper bound (`< 10:00`) is fixed. So that data will arrive in **the next run (2 PM)**.

**Q14. Partition size 128 MB or 64 MB?**
→ **128 MB is the default for file reading (Parquet/CSV). The Mongo connector's default is 64 MB.** It can be changed with `partition.size`.

### General

**Q15. Which methods should be avoided for big data?**
→ `collect()`, `collectAsList()`, large `take(n)` / `show(n)`, `toPandas()`, `collectAsMap()`, `countByKey()`, `broadcast()` of a big table, `coalesce(1)`, `groupByKey()`, `Window` without `partitionBy`. See [Section 8](#8-danger-list--avoid-for-big-data).

**Q16. Will my cluster (2 × 8 GB) handle this load?**
→ It can, but **slowly**. **Disk (50 GB) is the biggest problem**, so write output to separate storage. Also confirm the 5M/hour number.

**Q17. How should I discuss this kind of topic?**
→ Provide these 5 things:
```
1. Data:     record count, size of one record (sample JSON), collections
2. Work:     what the job does (explode / join / groupBy / which table)
3. Cluster:  nodes, RAM, cores, disk, Spark version, spark-submit command
4. Schedule: how often it runs, full or incremental
5. Problem:  error / job time / Spark UI screenshot
```

### Data Validation

**Q18. Are validation rules also managed within the plan, with data validated during execution?**
→ **Yes.** Rules are transformations, so they become part of the same plan and run on each chunk in the same pass while reading. Dataset-level rules like duplicates need a shuffle or another action.

**Q19. Will 1M corrupt / invalid records out of 20M go to a separate table with error messages, in the same flow?**
→ **Yes**, with **a single write + `partitionBy("dq_status")`**. Two separate writes for VALID and REJECTED would read Mongo twice. 19M go to curated and 1M go to `rejected_orders` with `error_message`. See [Section 12](#12-data-validation-dq--rejected-records).

**Q20. Will a corrupt type (e.g. `quantity = "abc"`) crash the job?**
→ It can with a strict schema. So read risky fields as `String`, `cast` them, and reject if you get `null`.

### Target DB (EPAS) & Parquet

**Q21. I use EPAS and load incrementally. If I do a full rebuild + overwrite of the table, will old data be lost?**
→ **Yes, it will.** After an overwrite the table holds only this run's data, and Spark JDBC even DROPs the table. So **never overwrite** a curated table. Write to **staging** with Spark, then **upsert** with SQL (Section 5.2).

**Q22. Can EPAS update rows?**
→ **Yes.** `UPDATE`, `INSERT ... ON CONFLICT`, `MERGE` all work. Parquet's limitation does not apply to EPAS. Spark JDBC just doesn't upsert, so you need staging + SQL.

**Q23. What is Parquet?**
→ **A column-wise, compressed file format.** Fast for analytics, but once a file is written it cannot be changed (immutable). Use Delta / Iceberg for updates. See the [Appendix](#appendix--what-is-parquet).

---

## Remember in One Line Each 🧠

- **Driver = plan, Executor = work.** Data never reaches the driver (except with `collect`).
- **Transformation = notes, Action = work starts.**
- **Partition = chunk, Task = work on 1 chunk, Core = how many chunks at once.**
- **Shuffle = the most expensive** – minimize it.
- **Incremental = fixed start + end window + Mongo filter + index.**
- **Big data results = `write()`, not `collect()`.**
- **Validation = in the same pass; VALID + REJECTED = one single write (`partitionBy dq_status`).**
- **EPAS: never overwrite curated → staging + `ON CONFLICT` upsert.**

---

## Appendix – What Is Parquet?

**Parquet is a file format**, just like CSV, JSON or Excel are file formats. It is the most widely used format for storing data in the big data world.
On a data lake (HDFS / S3 / MinIO), Spark tables are mostly saved as `.parquet` files.

> Our curated target is **EPAS**, so our tables live in the DB, not in Parquet.
> But Parquet is very useful for **raw archive, staging, or a data lake**, so it is worth understanding.

### A.1 CSV vs Parquet – how is data stored?

Our data:

| order_id | customer | city | amount |
|---|---|---|---|
| O1001 | Rahul | Ahmedabad | 29000 |
| O1002 | Priya | Surat | 2400 |
| O1003 | Amit | Mumbai | 55000 |

**CSV / JSON (row-wise):** one whole row per line.
```
O1001,Rahul,Ahmedabad,29000
O1002,Priya,Surat,2400
O1003,Amit,Mumbai,55000
```

**Parquet (column-wise):** all values of one column stored together.
```
order_id : O1001 | O1002 | O1003
customer : Rahul | Priya | Amit
city     : Ahmedabad | Surat | Mumbai
amount   : 29000 | 2400  | 55000     (+ stats like min=2400, max=55000)
```

### A.2 What is inside a Parquet file?

```
 orders.parquet
 ├── Row Group 1  (~128 MB of rows)
 │     ├── Column chunk: order_id   (compressed + min/max)
 │     ├── Column chunk: customer   (compressed + min/max)
 │     ├── Column chunk: city       (compressed + min/max)
 │     └── Column chunk: amount     (compressed + min/max)
 ├── Row Group 2
 │     └── ...
 └── Footer (metadata): schema, column types, min/max of each row group, row count
```

Spark reads the **footer** first. It then decides which row groups and which columns need to be read, and skips the rest.

### A.3 Why use Parquet? (Benefits)

| Benefit | Meaning |
|---|---|
| **Reads only the needed columns** (column pruning) | For `SUM(amount)`, only the `amount` column is read. Only 1 of 20 columns needs reading. |
| **Compression** | A column holds values of the same type (e.g. "Gujarat" repeated), so it compresses very well. 20 GB of JSON often becomes **3–5 GB of Parquet**. |
| **Skipping (predicate pushdown)** | Ask for `amount > 100000` and if a row group's max is 55000, the whole block is skipped without reading. |
| **Comes with a schema** | Column names and types (int, string, date) are stored in the file itself. |
| **Readable by all tools** | Spark, Hive, Trino, Athena, Databricks, DuckDB, Pandas |
| **Splittable** | Spark can split even a large file across many tasks and read it in parallel |

### A.4 Limitation – rows cannot be updated

Once written, a Parquet file **cannot be changed (immutable)**. The data is stored column-wise and compressed, so there is no way to change a single value in the middle.

```
 Change Rahul's net_spent from 29000 → 31000

 CSV:      find the line and change it      (possible, but slow)
 EPAS:     UPDATE ... WHERE customer_id='C001'   ✅ direct
 Parquet:  ❌ a single value cannot be changed
           ✅ the whole file (or whole table) must be rewritten   → overwrite
```

Other limitations:
- **Small files problem** – if each run creates thousands of tiny files, reads get slow. Use `coalesce(n)` / AQE.
- **Delete / dedupe is hard** – the whole file must be rewritten.
- **Conflicts when two jobs write at once** – there are no transactions.

### A.5 Delta Lake / Apache Iceberg / Apache Hudi

These are not new file formats. They are **a layer on top of Parquet**:

```
 Delta / Iceberg table
 ├── data files      → still Parquet (part-0001.parquet, part-0002.parquet …)
 └── transaction log → record of "which files are current, which are old"
```

- On `MERGE` / `UPDATE`, it rewrites **only the file that contains Rahul**, and marks the old file as "removed" in the log.
- To you it looks like one row was updated, but underneath the file is replaced.
- Bonus: **ACID** (no half-written data is visible), **time travel** (you can see yesterday's data), and schema evolution.

```sql
MERGE INTO customer_360_summary t
USING changed_customers s ON t.customer_id = s.customer_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

### A.6 Comparison – CSV vs JSON vs Parquet vs Delta/Iceberg vs EPAS

| Point | CSV | JSON | Parquet | Delta / Iceberg | EPAS (Postgres) |
|---|---|---|---|---|---|
| Storage style | Row | Row (nested) | **Column** | Column (Parquet) | Row (table) |
| Compression | ❌ | ❌ | ✅ Very good | ✅ | Some |
| Schema | ❌ | Flexible | ✅ | ✅ + evolution | ✅ Strict |
| Row update | Hard | Hard | ❌ | ✅ MERGE | ✅ UPDATE / UPSERT |
| Transactions (ACID) | ❌ | ❌ | ❌ | ✅ | ✅ |
| Analytics (scanning crores of rows) | Slow | Slow | ✅ Fast | ✅ Fast | Medium (depends on index / partitioning) |
| Finding one row (lookup) | Slow | Slow | Medium | Medium | ✅ Fast (index) |
| Best use | Export / Excel | API / Mongo | Data lake, archive | Lakehouse | App + DWH, BI, upsert |

### A.7 Where does Parquet help in our project?

| Place | Why |
|---|---|
| **Raw archive** (`raw_orders`, as-is copy of Mongo) | Cheap storage, compressed, can be re-processed. Doesn't fill up EPAS. |
| **Staging / DQ output** (`dq_status=VALID/REJECTED`) | Both outputs from a single write, then load into EPAS |
| **Old data (history)** | Archive EPAS partitions older than 1 year to Parquet and keep the DB small |

### A.8 Reading / writing Parquet in Spark

```java
// Write – creates folders by date: .../order_date=2026-11-20/part-0000.parquet
df.write()
  .mode("append")
  .partitionBy("order_date")
  .option("compression", "snappy")          // default; zstd compresses more
  .parquet("s3a://datalake/raw/orders");

// Read – only the needed day and columns are read (partition pruning + column pruning)
Dataset<Row> oneDay = spark.read().parquet("s3a://datalake/raw/orders")
    .filter(col("order_date").equalTo("2026-11-20"))
    .select("order_id", "amount");
```

Folder structure:
```
 s3a://datalake/raw/orders/
 ├── order_date=2026-11-19/
 │     ├── part-00000.snappy.parquet
 │     └── part-00001.snappy.parquet
 └── order_date=2026-11-20/
       └── part-00000.snappy.parquet
```

### A.9 In Short

- **Parquet** = column-wise, compressed file. **Fast for reads and analytics**, but **cannot be updated**.
- **Delta / Iceberg** = Parquet + transaction log. They support `UPDATE` / `MERGE`.
- **EPAS** = a database. Row `UPDATE` / upsert works directly, so Parquet's limitation does not apply to our curated layer.
