# Spark Performance Tuning – Using the Full Power of a Multi-Node Cluster

> Learning notes for the **Weather ETL** project (Spring Boot + Spark 3.5.1 → MySQL).
>
> 📝 **Note:** the code examples below come from the old file→MySQL job (`WeatherEtlJob`), which has been replaced by the medallion pipeline. The same lessons are used in the new code: `pipeline.partitions` (repartition), `rewriteBatchedStatements` + `batchsize` (`PipelineProperties`), and explicit schemas (`BronzeSchemas`). See [MEDALLION_CODE_GUIDE_GU.md](MEDALLION_CODE_GUIDE_GU.md).
> They record one question, the problems we found, and how each one was fixed, with before/after code.

---

## Table of Contents
1. [The Question](#1-the-question)
2. [Short Answer](#2-short-answer)
3. [Background: How Spark Runs Work in Parallel](#3-background-how-spark-runs-work-in-parallel)
4. [What Was Already Good](#4-what-was-already-good)
5. [Problems and Solutions](#5-problems-and-solutions)
   - [P1 – Only 1 partition: one executor does all the work](#p1--only-1-partition-one-executor-does-all-the-work)
   - [P2 – JDBC write uses a single connection](#p2--jdbc-write-uses-a-single-connection)
   - [P3 – MySQL inserts are not batched](#p3--mysql-inserts-are-not-batched)
   - [P4 – Existing-id check reads the whole table over 1 connection](#p4--existing-id-check-reads-the-whole-table-over-1-connection)
   - [P5 – Schema inference reads the data twice](#p5--schema-inference-reads-the-data-twice)
   - [P6 – Input file is on a local disk (not fixed in code)](#p6--input-file-is-on-a-local-disk-not-fixed-in-code)
   - [P7 – Table writes run one after another (kept on purpose)](#p7--table-writes-run-one-after-another-kept-on-purpose)
6. [Tuning Settings (Cheat Sheet)](#6-tuning-settings-cheat-sheet)
7. [How to Check Parallelism in the Spark UI](#7-how-to-check-parallelism-in-the-spark-ui)
8. [When a Cluster Helps and When It Doesn't](#8-when-a-cluster-helps-and-when-it-doesnt)
9. [Test Results After the Changes](#9-test-results-after-the-changes)
10. [Future Improvements](#10-future-improvements)
11. [Key Lessons](#11-key-lessons)

---

## 1. The Question

> *"Is the code able to use the multi-node environment's full potential?"*

## 2. Short Answer

**Before the changes: only partly.** The code was *cluster-safe*: it ran correctly on many nodes. But it was not *cluster-efficient*. Because of how the data was partitioned, **only one executor did real work** and the second worker sat idle.

**After the changes:** the data is spread over all executor cores, MySQL reads and writes run over several connections at once, inserts are batched, and the file is read once instead of twice.

---

## 3. Background: How Spark Runs Work in Parallel

```
 Data ──► split into PARTITIONS ──► 1 partition = 1 TASK ──► 1 task runs on 1 CORE of an EXECUTOR

 Example: 4 partitions, 2 workers with 1 core each
   Worker-1 / Executor-1 (1 core):  task-1 → task-3
   Worker-2 / Executor-2 (1 core):  task-2 → task-4
```

| Term | Meaning |
|---|---|
| **Driver** | Our Spring Boot app. It builds the plan and sends tasks to the executors. It does **not** process the data itself. |
| **Executor** | A JVM on a worker that runs the tasks (reads the file, transforms rows, writes to MySQL). |
| **Partition** | A slice of the data. Spark's unit of parallel work. |
| **Task** | The work for one partition in one stage. |
| **Max parallelism** | `min(number of partitions, total executor cores)` |

**The golden rule:** *you only get parallel work if you have **enough partitions**.*
With 1 partition, 10 workers are no better than 1.

---

## 4. What Was Already Good

| ✅ Good practice | Why it matters |
|---|---|
| No `collect()` of the data to the driver | The data stays on the executors, so the driver doesn't run out of memory. |
| Only built-in Spark functions (no UDFs) | Spark can optimize everything, and executors don't need our classes. |
| `cache()` on the source DataFrame | The 6 table writes reuse it instead of re-reading the file 6 times. |
| Cluster settings come from config (profiles) | The same code runs on `local[*]`, Windows standalone and Docker. |

---

## 5. Problems and Solutions

### P1 – Only 1 partition: one executor does all the work

**Problem**
Spark splits input files into chunks of `spark.sql.files.maxPartitionBytes` (default **128 MB**). Our input file is only a few MB, so it became **1 partition**, which means **1 task**. That task ran on one executor while the other was idle.

**How to spot it:** Spark UI → Stages → *Tasks: 1/1*. Only one executor shows any activity.

**Solution**
Call `repartition(N)` after filtering, so every later step (transforms and JDBC writes) runs as N parallel tasks.

```java
// BEFORE
Dataset<Row> src = withId.join(existing, ..., "left_anti").cache();

// AFTER
Dataset<Row> src = withId.join(existing, ..., "left_anti")
        .repartition(partitions)   // etl.partitions
        .cache();
```

```properties
# application.properties
etl.partitions=4      # rule of thumb: 2-3 x total executor cores
```

**Why 2–3 × the cores?** If some tasks finish early, the cores that free up can pick up the remaining tasks, so no core sits idle.
**Don't overdo it:** a very large N on small data creates many tiny tasks, and the overhead of starting each one outweighs the gain.

> Note: `repartition(N)` triggers a shuffle (data moves between executors). That cost pays off when the later steps are heavy. Here the later steps are 6 JDBC writes, so it pays off.

---

### P2 – JDBC write uses a single connection

**Problem**
`df.write().jdbc(...)` opens **one MySQL connection per partition**. With 1 partition there was only 1 connection, so every row was inserted one after another.

**Solution**
This is fixed automatically by **P1**. With `etl.partitions=4`, each table is written over **4 connections at once**, spread across the executors.

```
Before:  partition-1 ──► 1 connection ──► MySQL
After:   partition-1 ──► conn-1 ─┐
         partition-2 ──► conn-2 ─┼──► MySQL
         partition-3 ──► conn-3 ─┤
         partition-4 ──► conn-4 ─┘
```

> ⚠️ More partitions also means more MySQL connections. Keep `etl.partitions` well below MySQL's `max_connections` (default 151).

---

### P3 – MySQL inserts are not batched

**Problem**
Spark groups rows into JDBC batches, but **by default the MySQL driver still sends each row as a separate `INSERT`**. That means 10,000 rows took 10,000 network round trips.

**Solution**
Two settings, applied in code to the URL used by Spark only (the Spring/Hibernate datasource is unchanged):

```java
// WeatherEtlJob constructor
String u = url.trim();
this.jdbcUrl = u + (u.contains("?") ? "&" : "?") + "rewriteBatchedStatements=true";
jdbcProps.put("batchsize", batchSize);   // etl.jdbc-batch-size
```

```properties
etl.jdbc-batch-size=5000
```

| Setting | Effect |
|---|---|
| `rewriteBatchedStatements=true` | The driver rewrites a batch into one multi-row statement: `INSERT ... VALUES (..),(..),(..)` |
| `batchsize=5000` | Up to 5000 rows per batch (Spark's default is 1000) |

This is often a **5–10× speed-up** on MySQL inserts.

---

### P4 – Existing-id check reads the whole table over 1 connection

**Problem**
To skip records already in MySQL, the job reads all existing ids:

```java
// BEFORE: 1 query, 1 connection, 1 partition, and it grows every run
spark.read().jdbc(jdbcUrl, "(SELECT id FROM weather_observation) t", jdbcProps);
```

A plain JDBC read is always **1 partition**, whatever the cluster size.

**Solution**
Use the `predicates` form of `jdbc()`. **Each predicate becomes one partition and one parallel connection.** Our ids are Mongo ObjectIds (hex strings), so we split by the **last hex character**, which is evenly spread:

```java
// AFTER
Dataset<Row> existing = spark.read()
        .jdbc(jdbcUrl, "weather_observation", idPredicates(), jdbcProps)
        .select("id");

// etl.jdbc-read-partitions=4 produces:
//   RIGHT(id, 1) IN ('0','4','8','c')
//   RIGHT(id, 1) IN ('1','5','9','d')
//   RIGHT(id, 1) IN ('2','6','a','e')
//   RIGHT(id, 1) IN ('3','7','b','f')
```

```properties
etl.jdbc-read-partitions=4   # max 16 (one per hex char)
```

**Why the last character and not the first?** The first 8 hex characters of an ObjectId are a **timestamp**, so the first character is almost the same for every record (heavy skew). The last characters come from a counter, so they are evenly spread.

> **Other ways to read JDBC in parallel:**
> `partitionColumn` + `lowerBound` + `upperBound` + `numPartitions`. This only works with a **numeric, date or timestamp** column, so it doesn't fit our string id.

---

### P5 – Schema inference reads the data twice

**Problem**
`spark.read().json(path)` without a schema makes Spark **scan the whole file first** to guess the columns and types, and then scan it **again** to load it. On big data that doubles the read time.

**Solution**
Give Spark an **explicit schema**. The new class `WeatherSchema` holds it as a Spark DDL string:

```java
// BEFORE
spark.read().json(path);

// AFTER
spark.read().schema(WeatherSchema.DDL).json(path);
```

```java
// WeatherSchema.java (part)
private static final String NUM =
    "STRUCT<`$numberDouble`: STRING, `$numberInt`: STRING, `$numberLong`: STRING>";
...
"airTemperature " + vq(NUM),
"sections ARRAY<STRING>",
```

**Extra benefits**
- Types are always the same on every run. With inference, a field could be a string on one run and something else on another.
- Optional fields that are missing from a file become `NULL`. Before, they made the column disappear, which is why the code needed `if (j.has(...))` checks. Those checks have been removed from `run()`.

**Trade-off:** if the source JSON gets a new field, you must add it to `WeatherSchema`. Until you do, the field is ignored.

---

### P6 – Input file is on a local disk (not fixed in code)

**Problem**
On the cluster, **executors read the file**, not the driver. A path like `D:/.../data1.json` only works when every executor can see that same path:

| Setup | Works? | Why |
|---|---|---|
| `local[*]` | ✅ | One JVM |
| dev (Windows standalone) | ✅ | All workers are on the same PC |
| prod (Docker) | ✅ | `./inputData` is mounted as `/data` in every container |
| Real multi-machine cluster | ❌ | Each machine has its own disk |

**Solution (infrastructure, not code)**
Put the input on **shared storage** that every node can reach:
- HDFS → `hdfs://namenode:9000/weather/data1.json`
- S3 / MinIO → `s3a://bucket/weather/data1.json` (needs the `hadoop-aws` jar)
- A network share (NFS/SMB) mounted at the **same path** on every node

Then also replace the driver-side `Files.exists` / `Files.copy` with Hadoop `FileSystem` calls, so the archive step works on that storage too.

---

### P7 – Table writes run one after another (kept on purpose)

The 6 tables are written one after another. This is **intentional**:
- `weather_observation` **must** be written first, because the child tables have a **foreign key** to it.
- The 5 child tables could be written at the same time from separate threads (Spark accepts jobs from several threads at once). For our data size the gain is small and the code gets more complex, so this is listed under [future improvements](#10-future-improvements).

Each write is still parallel **inside** itself (P1 + P2).

---

## 6. Tuning Settings (Cheat Sheet)

**Project settings** (`application.properties`, can be overridden per profile):

| Property | Default | What it controls |
|---|---|---|
| `etl.partitions` | 4 | Data partitions = parallel tasks = parallel write connections |
| `etl.jdbc-read-partitions` | 4 | Parallel connections when reading existing ids (max 16) |
| `etl.jdbc-batch-size` | 5000 | Rows per MySQL INSERT batch |

**Cluster settings** (profile files, `spark.conf.[key]=value`):

| Spark key | dev / prod value | What it controls |
|---|---|---|
| `spark.executor.cores` | 1 | Cores per executor |
| `spark.executor.memory` | 1g | Heap per executor |
| `spark.cores.max` | 2 | Total cores the app may use across the cluster |
| `spark.sql.shuffle.partitions` | 200 (Spark default) | Partitions after joins and aggregations. AQE shrinks this automatically for small data. |
| `spark.sql.files.maxPartitionBytes` | 128 MB | Input split size. Lower it to get more partitions when reading. |

**Choosing `etl.partitions`:**
```
total cores = spark.cores.max            (dev/prod: 2)
etl.partitions ≈ 2 to 3 × total cores    (→ 4 to 6)
and keep it well below MySQL max_connections
```

---

## 7. How to Check Parallelism in the Spark UI

Open **http://localhost:4040** while the job runs.

| Tab | What to check | Good sign |
|---|---|---|
| **Jobs** | One job per action (`count`, each table write) | Jobs finish without failures |
| **Stages** | The *Tasks* column | `4/4`, not `1/1` |
| **Stages → stage detail** | *Event Timeline* and the *Executor ID* of each task | Tasks spread over **both** executors |
| **Executors** | *Active / Complete Tasks* per executor | Both executors have similar task counts |
| **SQL / DataFrame** | The query plan | `Exchange RoundRobinPartitioning(4)` (from `repartition`) and `Scan JDBCRelation ... [numPartitions=4]` |

Master UI **http://localhost:8080** shows which workers host your executors.

---

## 8. When a Cluster Helps and When It Doesn't

| Data size | Best mode | Why |
|---|---|---|
| KB – tens of MB | `local[*]` | Starting executors and moving data over the network costs more than the work itself |
| Hundreds of MB – GB | Standalone cluster | The work is split across executors and the network cost is paid back |
| Many GB – TB | Cluster + shared storage (HDFS/S3) + tuning | Only a cluster can process it at all |

**Also remember:** the destination is **one MySQL server**. Beyond a certain point, more executors won't make writes faster, because MySQL itself (disk, indexes, foreign key checks) becomes the bottleneck.

---

## 9. Test Results After the Changes

Tested on `local[*]` against a scratch database (`etl_test`, dropped afterwards):

| Check | Result |
|---|---|
| Build | ✅ success |
| 1st run | ✅ **10,000** new observations loaded into all 6 tables |
| 2nd run (same file) | ✅ `All records already loaded`, so the parallel id check works |
| Values parsed with the explicit schema | ✅ ts, longitude, temperature, wind, elevation, sections (68,384 rows), past/present weather, sky condition and waves all filled |

---

## 10. Future Improvements

| Idea | Benefit |
|---|---|
| **Check only the time range of the file's records**: compute min/max `ts` of the file and push `WHERE ts BETWEEN ...` into the id query | The id check stays fast even when the table holds millions of rows |
| **Write the 5 child tables at the same time** (P7) | Shorter total run time on big data |
| **Write into staging tables, then `INSERT ... SELECT` into the final tables in one transaction** | All-or-nothing loads. Today, if a child write fails, those records are skipped on the next run. |
| **Keep a list of processed files** (name + checksum) in a table | Avoids reading the same file every 2 minutes |
| **Read a folder of files** instead of one file (`.json("/data/originalFile/*.json")`) | Many files mean natural parallelism without `repartition` |
| **Shared storage (HDFS / MinIO)** (P6) | A real multi-machine cluster |
| **Kryo serializer**: `spark.serializer=org.apache.spark.serializer.KryoSerializer` | Faster shuffles |

---

## 11. Key Lessons

1. **Partitions decide parallelism**, not the number of workers. Always check *Tasks: N/N* in the Spark UI.
2. **A JDBC read is 1 partition by default.** Use `predicates` or `partitionColumn` to read in parallel.
3. **A JDBC write uses one connection per partition.** Repartition before writing, and add `rewriteBatchedStatements=true` for MySQL.
4. **Give Spark an explicit schema** for JSON/CSV in production: one read instead of two, and stable types.
5. **Executors read the input, not the driver.** On a real cluster the input must be on shared storage.
6. **A cluster is for big data.** For small data, `local[*]` is faster.
