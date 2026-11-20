# Spark Execution, Data Load & Performance – Full Summary

> Aa document ma aapdi badhi **performance related discussion** kramasar che:
> Spark kevi rite plan banave, kyare execution sharu thay, MongoDB mathi data kevi rite load thay,
> memory ma shu rahe, kai methods avoid karvi, ane best practices.
> Chhelle **tamara badha sawal + short jawab** che.
>
> Scenario: **20M active users**, **5M orders / hour**, job **darek 4 kalake**,
> cluster = **1 master + 2 workers** (darek: 8 GB RAM, i5, 50 GB SSD).

---

## Table of Contents

1. [Spark na main parts (Driver, Executor, Core, Partition, Task)](#1-spark-na-main-parts)
2. [Lazy Evaluation – Transformation vs Action](#2-lazy-evaluation--transformation-vs-action)
3. [Spark plan kevi rite banave che (Job → Stage → Task)](#3-spark-plan-kevi-rite-banave-che)
4. [MongoDB mathi data kevi rite load thay che](#4-mongodb-mathi-data-kevi-rite-load-thay-che)
5. [Incremental Load – fakt navo data](#5-incremental-load--fakt-navo-data)
6. [Memory ma kyare shu hoy che?](#6-memory-ma-kyare-shu-hoy-che)
7. [Execution sharu karti methods (Actions) – full list](#7-execution-sharu-karti-methods-actions)
8. [Danger list – badho data ek jagyae memory ma lavti methods (AVOID)](#8-danger-list--avoid-for-big-data)
9. [Tamara cluster par calculation](#9-tamara-cluster-par-calculation)
10. [Recommended Configuration](#10-recommended-configuration)
11. [Performance Best Practices](#11-performance-best-practices)
12. [Monitoring – Spark UI](#12-monitoring--spark-ui)
13. [Final Checklist](#13-final-checklist)
14. [Tamara Sawal – Short Jawab](#14-tamara-sawal--short-jawab)

---

## 1. Spark na main parts

**Example:** Ek godown che jema 20 lakh boxes che. Ek manager che ane 2 workers che, ane darek worker pase 4 haath che.

| Spark ma | Example ma | Kaam |
|---|---|---|
| **Driver (master)** | Manager | Plan banave, kaam vahenche, track rakhe. **Pote data nathi uchakto.** |
| **Executor (worker)** | Worker | Kharekhar data vanche, process kare, lakhe. |
| **Core** | Worker na haath | Ek core ek vaar ma ek task chalave. |
| **Partition** | Boxes no nano dhagalo | Data no ek tukdo (Mongo connector ma default ~64 MB). |
| **Task** | Ek dhagalo uchakvanu kaam | 1 partition par 1 task. |
| **Cluster Manager** | HR / supervisor | Workers par resources (RAM, cores) aape (Standalone / YARN / K8s). |

```
                        ┌──────────────────────┐
                        │   DRIVER (master)    │
                        │  - plan banave       │
                        │  - tasks vahenche    │
                        │  - status track kare │
                        └──────────┬───────────┘
                     instructions  │  (data nahi, fakt kaam)
                 ┌─────────────────┴─────────────────┐
        ┌────────▼────────┐                 ┌────────▼────────┐
        │   WORKER-1      │                 │   WORKER-2      │
        │   Executor      │                 │   Executor      │
        │ [c1][c2][c3][c4]│                 │ [c1][c2][c3][c4]│
        └────────┬────────┘                 └────────┬────────┘
                 │        data SIDHO aave/jay        │
                 └──────────────► MongoDB / Storage ◄┘
```

---

## 2. Lazy Evaluation – Transformation vs Action

Spark **aalsu (lazy)** che. Tame code lakho tyare e fakt **notes banave che**, kaam nathi kartu.
**Action** aave tyare j badhu kaam ek sathe sharu thay.

| Type | Shu thay? | Example |
|---|---|---|
| **Transformation** | Fakt plan ma navu step umerai. **Koi data nathi vanchato.** | `select`, `filter`, `withColumn`, `join`, `groupBy`, `explode` |
| **Action** | Plan execute thay: data vanchay, process thay, result male | `write`, `count`, `show`, `collect` |

```java
Dataset<Row> orders = spark.read().format("mongodb")...load();   // plan (schema mate nano sample j)
Dataset<Row> items  = orders.select(explode(col("items")));       // plan
Dataset<Row> big    = items.filter(col("qty").gt(1));              // plan
big.write().parquet("/curated/fact_sales");                        // ← ACTION: have badhu chale
```

> ⚠️ Ek nani vaat: `load()` vakhte **schema** khabar na hoy to Mongo connector ~1000 docs sample kari ne schema shodhe che (`sampleSize`).
> Aa nano read che, pura 20M nahi. Tame **explicit schema** aapo to aa read pan bachi jay.

### Transformations na 2 prakar

| Narrow (fast, shuffle nahi) | Wide (shuffle thay – bhaare) |
|---|---|
| `select`, `filter`, `withColumn` | `join` (broadcast sivay) |
| `explode`, `map`, `flatMap` | `groupBy().agg()` |
| `union` / `unionByName` | `distinct`, `dropDuplicates` |
| `coalesce` (ochha karva) | `orderBy` / `sort` |
| | `repartition`, Window functions |

> **Shuffle** = data ne network par ek worker thi bija worker par mokalvo (same key vala rows bhega karva).
> Shuffle = disk + network + time. **Sauthi bhaare operation.**

---

## 3. Spark plan kevi rite banave che

```
 Tamaro Code
     │
     ▼
 ① Logical Plan          "shu karvu che"  (read → explode → filter → join → write)
     │
     ▼
 ② Optimized Plan        Catalyst optimizer:
     │                    - filter ne aagal khase (pehla filter, pachhi kaam)
     │                    - fakt jaruri columns j vanche
     │                    - filter Mongo ne j moklave (pushdown)
     ▼
 ③ Physical Plan         "kevi rite karvu" – kayo join (Broadcast / SortMerge), ketla tukda
     │
     ▼
 ④ Job → Stages → Tasks
       - Darek ACTION = 1 Job
       - Darek SHUFFLE par Job ma navo Stage bane
       - Darek Stage ma darek partition = 1 Task
```

### Aapda fact_sales job nu example

```
 JOB (write action)
 │
 ├── STAGE 1 ──  read orders (320 tasks) → explode items → join dim_customer (broadcast, shuffle nahi)
 │                                                       → join dim_product (broadcast)
 │                       │
 │                ════ SHUFFLE (order_id par) ════   ← payments sathe join mate
 │                       │
 ├── STAGE 2 ──  read payments (N tasks)
 │                       │
 │                ════ SHUFFLE ════
 │                       │
 └── STAGE 3 ──  join orders+payments (shuffle.partitions = 160 tasks) → write parquet
```

Plan jova mate:
```java
factSales.explain("formatted");   // physical plan, join type, pushed filters dekhay
```

---

## 4. MongoDB mathi data kevi rite load thay che

### 4.1 Kon decide kare ke kayo core kayo data vanchshe?

Core ne potani mele kai khabar nathi hoti. **Driver pehlethi j badha tukda nakki kari de che.**
Ek cake vicharo: kapvanu kaam serve karta pehla thai jay, ane darek ne ek naam lakhelo piece male che.

| Kon | Kaam |
|---|---|
| **Driver** | Mongo ne puchhe: docs ketla? avg size? sample `_id`? |
| **Partitioner** (Mongo connector, default `SamplePartitioner`) | `_id` na range ganee: P0 = A→B, P1 = B→C … |
| **Task Scheduler** (Driver ma) | Je core free thay ene queue mathi aagal no task aape |
| **Executor core** | Potana task ni query chalave, data lave, process kare |

### 4.2 Full Visualization – 10 AM run

```
 10:00 AM  Airflow job start
           window = [06:00, 10:00)   ← upper bound FIX

 ┌───────────────────────── STEP 1: PLANNING (Driver) ─────────────────────────┐
 │ Driver ──► MongoDB : "created_at 06–10 vala docs ketla? avg size? sample?"   │
 │ MongoDB ──► Driver : ~20M docs, ~1 KB avg, sample _id list   (thoda KB j)    │
 │                                                                              │
 │ Partitioner : 20 GB / 64 MB ≈ 320 tukda                                      │
 │   64 MB / 1 KB ≈ 65,000 docs per tukdo → sample _id sort → boundaries        │
 │                                                                              │
 │ TASK LIST (fix):                                                             │
 │   P0   : _id >= 65a0..000 AND _id < 65a0..9f3  + created_at filter           │
 │   P1   : _id >= 65a0..9f3 AND _id < 65a1..1c2  + created_at filter           │
 │   ...                                                                        │
 │   P319 : _id >= 65b3..e11 AND _id <= 65b3..fff + created_at filter           │
 └──────────────────────────────────────────────────────────────────────────────┘

 ┌───────────────────────── STEP 2: EXECUTION ─────────────────────────────────┐
 │ Task Queue: [P0 … P7 | P8 P9 … P319]                                         │
 │                                                                              │
 │ Worker-1  c1◄P0  c2◄P1  c3◄P2  c4◄P3                                         │
 │ Worker-2  c1◄P4  c2◄P5  c3◄P6  c4◄P7        ← ek sathe 8 tasks               │
 │                                                                              │
 │ Worker-2 c3 vahelo free ──► Scheduler: "le, P8"                              │
 │ Worker-1 c1 free        ──► Scheduler: "le, P9"   (fixed rounds nathi,       │
 │ ...                                                je free thay e le)        │
 │                                                                              │
 │ Darek task POTANI query MongoDB ne mokle:                                    │
 │  find({ _id:{$gte:X,$lt:Y}, created_at:{$gte:06:00,$lt:10:00} })             │
 └──────────────────────────────────────────────────────────────────────────────┘

 ┌───────────────────────── STEP 3: FAILURE ───────────────────────────────────┐
 │ P42 chalto hato ane Worker-2 crash thayo                                     │
 │ Driver: "P42 puro nathi" ──► P42 farithi bija core ne                        │
 │ Range fix che → exact same data farithi (duplicate nahi)                     │
 └──────────────────────────────────────────────────────────────────────────────┘
```

### 4.3 Data Driver ma jay che? → **NA**

```
 ❌ AAVU NATHI THATU                      ✅ AAVU THAY CHE

 MongoDB                                  MongoDB
    │ 20M (ek call)                         ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲   ← 8 alag queries ek sathe
    ▼                                       │ │ │ │ │ │ │ │
 Driver (20 GB memory ma)                ┌──┴─┴─┴─┴┐ ┌┴─┴─┴─┴──┐
    │                                    │Worker-1 │ │Worker-2 │
    ▼                                    └─────────┘ └─────────┘
 Workers
```

| Kon | Mongo ne call | Ketlo data |
|---|---|---|
| Driver | Planning ni 1–2 nani calls | Thoda KB (boundaries) |
| Darek task | Potani range ni 1 query | Potano tukdo (~64 MB) |

- 320 tukda = **MongoDB par 320 alag queries**, pan ek sathe fakt **8** (tamara cores).
- Darek task potano tukdo pan **cursor batches** ma lave: thoda docs aave, process thay, pachhi aagla.
- Etle Mongo par ek vaar ma ~8 connections nu load aave che.

### 4.4 `_id` j kem?
`ObjectId` ma **create time chhupayelo** che ane e hamesha vadhto j jay che. Etle range banavva mate e best che, ane eno index pan pehlethi j hoy che.

---

## 5. Incremental Load – fakt navo data

### Problem
```java
spark.read().format("mongodb").option("collection","orders").load();   // ❌ AAKHI collection
```
Spark potani mele nathi samajtu ke "navo data" kayo che. Filter vagar e **badho juno data** pan vanchshe.

### Solution – fixed time window

```
  02:00 ──────── 06:00 ──────── 10:00 ──────── 14:00
          Run@6AM        Run@10AM       Run@2PM
        [02:00,06:00)  [06:00,10:00)  [10:00,14:00)

  10:00 pachhi job chale tyare navo order aave → e 2 PM vala run ma jashe ✅
```

| Rule | Kem? |
|---|---|
| **Start + End banne aapo** (`>= start AND < end`) | Job chale tyare aavto navo data aa run ma mix na thay |
| Window **Airflow** mathi lo (`{{ data_interval_start }}`, `{{ data_interval_end }}`) | Re-run karo to pan exact same data |
| Athva **control table** `etl_watermark` (`last_success_end`) | Job **success** thay pachhi j update |
| Filter **`aggregation.pipeline`** ma muko | Filter Mongo ma j chale ane partitioner pan e j filter vapare |
| Mongo ma **index** `created_at` par | Nahi to Mongo aakhi collection scan karshe |
| Late data mate thodo **overlap** (e.g. 10 min) + MERGE / dedupe | Kyarek order juna time sathe moda insert thay |

```java
String pipeline = "[{ $match: { created_at: { $gte: ISODate('" + start + "'), $lt: ISODate('" + end + "') } } }]";
```

### Summary table (Customer 360 jevi aggregated table) mate
- Rahul no navo order aave → **eni j row update** (grain = 1 row per customer)
- Navo customer → **navi row insert** (`dim_customer` LEFT JOIN thi)
- Rit:
  - **Nano data** → aakhu table **full rebuild + overwrite**
  - **Moto data** → fakt **badlayela customers** recompute + **MERGE** (Delta / Iceberg / Hudi)
- ⚠️ Plain Parquet ma ek row update na thay.
- ⚠️ Juna number ma fakt "+1" na karo. Refund thay to `net_spent` ochho thavo joie, etle affected customer nu aakhu summary farithi compute karo.

```sql
MERGE INTO customer_360_summary t
USING changed_customers s ON t.customer_id = s.customer_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

---

## 6. Memory ma kyare shu hoy che?

| Samay | Driver memory | Executor memory |
|---|---|---|
| `read()...load()` | Plan + schema | Kai nahi |
| Transformations lakhya | Plan j | Kai nahi |
| `write()` chale | Task list + status | Ek vaar ma ~8 tukda (darek ~64 MB, JVM ma 2–3× motu) |
| Shuffle | Kai nahi | Shuffle buffers. Jagya na hoy to **disk par spill** |
| `count()` | Fakt 320 numbers | Darek task potana rows gane |
| `collect()` | ❌ **Badha 20M rows** → OOM | — |

**Spill** = memory ochhi pade tyare Spark data `spark.local.dir` (SSD) par muki de che. Job **slow thay pan crash nahi**. Spark UI ma "Spill (Disk)" dekhay.

---

## 7. Execution sharu karti methods (Actions)

Aa method call thay **tyare j** Spark job chale che.

### 7.1 Safe actions (data executor par j rahe ke nanu result aave)

| Method | Shu kare | Driver par shu aave |
|---|---|---|
| `write().parquet() / save() / saveAsTable() / insertInto()` | Data sidho storage ma | Kai nahi (fakt status) |
| `write().format("mongodb").save()` | Data sidho Mongo ma | Kai nahi |
| `count()` | Rows gane | 1 number |
| `show()` / `show(20)` | Thoda rows dekhade | 20 rows |
| `take(10)`, `head(10)`, `first()`, `limit(10).collect()` | Thoda rows | 10 rows |
| `isEmpty()` | Data che ke nahi | true/false |
| `foreach()`, `foreachPartition()` | Darek row/partition par kaam **executor par** | Kai nahi |
| `agg(sum, max…).collect()` | Aggregated result | 1 row (nanu) |

### 7.2 Chhupa (hidden) triggers – khabar na pade pan job chale

| Method | Kem job chale |
|---|---|
| `load()` schema vagar | Schema mate ~1000 docs sample |
| `orderBy()` / `sort()` | Range shodhva sample job |
| `pivot("col")` values vagar | Distinct values shodhva job |
| `checkpoint()` | Default ma eager – turant lakhe |
| `cache()` + action | `cache()` pote lazy che, pan pehli action par aakho data memory ma |

---

## 8. Danger List – Avoid for Big Data

Aa methods **badho data ek jagyae (mostly Driver) memory ma lave che**. 20M rows par aa **`OutOfMemoryError`** aapshe.

### 8.1 Driver memory ma badhu lave ❌

| Method | Problem | Safe vikalp |
|---|---|---|
| `collect()` / `collectAsList()` | **Badha rows driver ma** | `write()` karo, athva `limit(n).collect()` |
| `take(n)` / `head(n)` mota `n` sathe | n rows driver ma | Nano n j rakho |
| `show(1000000)` | Etla rows driver ma | `show(20)` |
| `toLocalIterator()` | Ek partition ek vaar, pan **badhu driver par thi pasar thay** – bahu slow | `foreachPartition()` |
| `toPandas()` (PySpark) | Aakho data driver ma | Aggregate kari ne j |
| `rdd.collectAsMap()` | Aakho map driver ma | Join vaparo |
| `rdd.countByKey()` / `countByValue()` | Ghani keys hoy to moto map driver ma | `groupBy().count().write()` |
| `broadcast(bigTable)` | Pehla driver ma collect thay, pachhi badha workers ne | Fakt nana dim (< 10–100 MB) j broadcast |
| Driver side `List` / `ArrayList` ma rows bharva | Driver OOM | DataFrame ma j rakho |

### 8.2 Executor par ek jagyae bhegu kare ⚠️

| Method | Problem | Safe vikalp |
|---|---|---|
| `coalesce(1)` / `repartition(1)` | Aakho data **1 task** ma – 1 core j kaam kare | Ghana files lakho, ke `coalesce(n)` |
| `rdd.groupByKey()` | Ek key na badha values ek task ma | `reduceByKey` / DataFrame `groupBy().agg()` |
| `collect_list()` / `collect_set()` mota group par | Ek row ma lakho values | Jaruri hoy tyare j |
| `cache()` / `persist()` mota data par | Executor memory bharai jay | Fakt 2+ vaar reuse thato hoy to j. Kaam pachhi `unpersist()` |
| `Window.orderBy()` `partitionBy` vagar | Badho data 1 partition ma | Hamesha `partitionBy(...)` aapo |
| Skewed join (ek key par karodo rows) | Ek task kalako le | AQE skew join, salting |

> **Golden Rule:** Mota data par result **storage ma lakho (`write`)**. Driver par fakt **nana numbers ke summary** j lavo.

---

## 9. Tamara Cluster par Calculation

| Vaat | Andaj |
|---|---|
| Orders / run | 5M × 4 kalak = **20M** |
| 1 order size | ~1 KB (assume) |
| Raw data / run | **~20 GB** |
| Items explode pachhi | ~50–60M rows |
| Partitions (64 MB) | ~320 tasks |
| Total cores | 2 × 4 = **8 ek sathe** |
| Rounds | 320 / 8 ≈ **40 waves** |
| Executor memory | 8 GB RAM mathi ~**5 GB** (OS mate ~3 GB) |
| Per task memory | ~1 GB |
| Disk | 50 GB SSD (OS + shuffle spill + temp) |
| Runs / divas | 6 × 20 GB = **~120 GB / divas** |

### Mukhya Risk

1. **Disk sauthi moti problem** – output worker disk par lakhso to thoda j divas ma bharai jashe. Output **HDFS / S3 / MinIO** jeva alag storage par lakho.
2. **Shuffle (join, groupBy) bhaare** – 8 GB RAM sathe spill thashe, etle job slow chalshe.
3. **Customer 360 mate 20M users** ne darek run ma farithi compute na karo. Fakt **changed customers**.
4. **Production Mongo par load** – read `secondaryPreferred` thi.
5. **5M orders/hour ≈ 1400 orders/sec** – aa bahu moto number che. Kharekhar per hour che ke per day, e confirm karo. Hardware no aakho plan aa number par depend che.

> Short ma: job chali shake, pan **dheemi ane disk tight**. Production scale mate aa cluster nano che.

---

## 10. Recommended Configuration

### 10.1 Mongo read
```java
String start = args[0];   // {{ data_interval_start }}
String end   = args[1];   // {{ data_interval_end }}

String pipeline = "[{ $match: { created_at: { $gte: ISODate('" + start + "'), $lt: ISODate('" + end + "') } } }]";

Dataset<Row> orders = spark.read().format("mongodb")
    .schema(ordersSchema)                                            // sampling bache
    .option("connection.uri", "mongodb://host:27017/?readPreference=secondaryPreferred")
    .option("database", "shop")
    .option("collection", "orders")
    .option("aggregation.pipeline", pipeline)                        // incremental filter
    .option("partitioner", "com.mongodb.spark.sql.connector.read.partitioner.SamplePartitioner")
    .option("partitioner.options.partition.field", "_id")
    .option("partitioner.options.partition.size", "64")              // MB per tukdo (64–128)
    .option("partitioner.options.samples.per.partition", "10")
    .load()
    .select("order_id", "order_date", "customer", "items", "payment_mode", "status");  // fakt jaruri columns
```

| Setting | Kaam | Value |
|---|---|---|
| `schema(...)` | Schema sampling bache, types fix | Explicit `StructType` |
| `aggregation.pipeline` | Fakt window no data Mongo mathi | `$match` start + end |
| `partitioner` | Tukda kevi rite banavva | `SamplePartitioner` |
| `partition.field` | Range kaya field par | `_id` |
| `partition.size` | Ek tukdo ketla MB | 64–128 |
| `readPreference` | App no primary bachavo | `secondaryPreferred` |
| Mongo index | Filter fast | `createIndex({created_at: 1})` |

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

| Setting | Matlab |
|---|---|
| `driver-memory 2g` | Driver ma data nathi aavto, etle 2 GB puratu |
| `executor-memory 5g` | 8 GB machine par OS mate jagya chhodo |
| `executor-cores 4` | Darek worker par 4 tasks ek sathe |
| `total-executor-cores 8` | Aakha cluster ma 8 tasks |
| `shuffle.partitions 160` | Join / groupBy pachhi ketla tukda (default 200) |
| `adaptive.enabled` (AQE) | Runtime par nana tukda bhega kare, skew handle kare, join type badle |
| `autoBroadcastJoinThreshold` | Aa size thi nani table automatic broadcast (dim tables) |
| `KryoSerializer` | Fast ane nanu serialization |
| `spark.local.dir` | Spill / shuffle mate SSD folder |

### 10.3 Write
```java
factSales
    .write()
    .mode("append")
    .partitionBy("date_key")                 // query vakhte fakt jaruri divas vanchay
    .parquet("s3a://datalake/curated/fact_sales");   // worker disk nahi, alag storage
```

---

## 11. Performance Best Practices

### Read
1. ✅ **Incremental filter** (start + end) – aakhi collection kyarey nahi
2. ✅ Filter **Mongo ma j** (`aggregation.pipeline`) + **index**
3. ✅ **Fakt jaruri columns** `select` karo (ochho data network par)
4. ✅ **Explicit schema** aapo
5. ✅ `readPreference=secondaryPreferred`

### Transform
6. ✅ **Filter pehla, join pachhi** (ochha rows join ma jay)
7. ✅ Nana dims (`dim_customer` ke jem nanu hoy, `dim_product`, `dim_date`) ne **broadcast** karo, etle shuffle nahi thay
8. ✅ Shuffle ochha karo – bina jarur `distinct`, `orderBy`, `repartition` nahi
9. ✅ **AQE** chalu rakho
10. ✅ UDF ochha – Spark built-in functions (`when`, `explode`, `date_format`) vaparo
11. ✅ `cache()` fakt 2+ vaar reuse thato hoy to j, pachhi `unpersist()`
12. ✅ **Skew** dhyan ma rakho (ek customer ke ek date par vadhare data)

### Write
13. ✅ `partitionBy("date_key")` – pan bahu vadhare partitions nahi
14. ✅ Nani-nani files na bane – AQE coalesce / `coalesce(n)` (1 nahi)
15. ✅ Output **HDFS / S3 / MinIO** par, worker local disk par nahi
16. ✅ Update / upsert jaruri hoy to **Delta / Iceberg** + `MERGE`

### Design
17. ✅ **Dims pehla, facts pachhi** (Airflow DAG dependency)
18. ✅ Aggregated tables (Customer 360) **incremental** – fakt badlayela keys
19. ✅ Juno data cleanup / retention policy
20. ✅ Driver par **kyarey** `collect()` mota data par nahi

---

## 12. Monitoring – Spark UI

Job chale tyare: `http://<master>:4040` (history mate: Spark History Server)

| Tab | Shu jovu |
|---|---|
| **Jobs** | Ketla jobs (darek action = 1 job) |
| **Stages** | Ketla tasks, ketla puri thaya, **Shuffle Read/Write**, **Spill (Memory/Disk)** |
| **Stage → Tasks** | Ek task bija karta bahu vadhare time le? → **skew** |
| **Executors** | Darek worker na tasks, GC time, memory used, failed tasks |
| **SQL** | Query plan graph, kayo join (Broadcast / SortMerge), pushed filters |

**Red flags:**
- Spill (Disk) GB ma → memory ochhi ke partitions ochha
- Ek task 10× slow → skew
- GC time > 10% → executor memory / partition size check karo
- 1 j task chale che → `coalesce(1)` ke `Window` `partitionBy` vagar

---

## 13. Final Checklist

```
 READ
 [ ] Incremental window: start + end (Airflow data_interval)
 [ ] Filter aggregation.pipeline ma
 [ ] Mongo index created_at par
 [ ] readPreference = secondaryPreferred
 [ ] Explicit schema + jaruri columns j

 TRANSFORM
 [ ] Filter pehla, join pachhi
 [ ] Nana dims broadcast
 [ ] AQE ON, shuffle.partitions set
 [ ] Koi collect() / collectAsList() mota data par nathi

 WRITE
 [ ] Output alag storage (HDFS / S3 / MinIO)
 [ ] partitionBy(date_key)
 [ ] Upsert mate Delta/Iceberg MERGE

 OPS
 [ ] Dims → Facts → Gold (DAG order)
 [ ] Spark UI ma spill / skew check
 [ ] Disk retention / cleanup
```

---

## 14. Tamara Sawal – Short Jawab

### ETL Design

**Q1. Raw thi Curated par data split kem kariye chiye? Purpose shu?**
→ MongoDB no data **app mate** banavelo che (nested, repeat). Split kari ne **Dim + Fact** banaviye to data **analytics mate** sacho, fast ane samjay evo bane: duplicate ochha, update ek jagyae, ane BI tools mate ready.

**Q2. Case 1 – Single collection → ghana Dim + Fact?**
→ Ek embedded collection (`orders`) ne `explode` + `dropDuplicates` thi todo. Simple che, pan dim adhuro rahe: snapshot data, ane order vagar na customer nathi aavta.

**Q3. Case 2 – Ghani collections → ghana Dim + Fact?**
→ Master collections (customers, products) mathi **full dims**, ane transaction collections (orders, payments) mathi **facts** + key lookup. Enterprise DWH mate sauthi common.

**Q4. Case 3 – Ghani collections → ek j table possible che?**
→ **Ha.** JOIN merge (Customer 360, flat report) athva UNION merge (website + app + store orders). Mostly **Gold layer** ma, dashboard / ML / export mate.

### Incremental / Update

**Q5. Rahul thoda divas pachhi 3jo order kare to `customer_360_summary` ma eni row update thase?**
→ **Ha**, navi row nahi, **e j row update** (grain = 1 row per customer). Full rebuild (overwrite) ke `MERGE` thi.

**Q6. Navo customer aave to insert thase?**
→ **Ha**, LEFT JOIN thi. Order na hoy to `0` / `Never Ordered` sathe.

### Spark Execution

**Q7. 4 kalak na 20M records Spark ek sathe memory ma load karshe?**
→ **Na.** 20M aa run ma process thashe e sachu, pan Spark ene ~64 MB na tukda ma todi ne ek sathe fakt 8 tukda (8 cores) process kare che.

**Q8. `write()` ke `count()` call thay tyare badha 20M memory ma aave?**
→ **Na.** `write()` tukde-tukde vanche ane lakhe. `count()` driver ne fakt numbers mokle. **`collect()` / `collectAsList()` ma ha**, badhu driver ma aave ane OOM thay.

**Q9. Core ne kevi rite khabar pade ke kya sudhi vanchyu ane aagal kyathi vanchvu?**
→ Core ne khabar rakhvani jarur j nathi. **Driver pehlethi j `_id` range vala badha tukda (task list) banave che.** Free core queue mathi aagal no task le che.

**Q10. Kon ane kevi rite decide kare ke kon ketlu vanchshe?**
→ **Driver + Mongo connector no Partitioner**: Mongo mathi count + sample `_id` le, partition size thi boundaries gane, ane **Task Scheduler** free core ne task aape.

**Q11. Ena mate kai configuration joie?**
→ `aggregation.pipeline` (incremental filter), `partitioner`, `partition.field=_id`, `partition.size`, `readPreference`, Mongo index, ane spark-submit ma `executor-memory`, `executor-cores`, `shuffle.partitions`. Juo [Section 10](#10-recommended-configuration).

**Q12. 320 tukda mate darek vakhte Mongo ne alag call thay ke master ek vaar data lavi ne memory ma rakhe?**
→ **Darek tukdo alag call kare che**, ane data **Mongo thi sidho executor** ma aave. Driver ma data **kyarey nathi** aavto, e fakt plan ane instructions aape che.

**Q13. Job chale tyare navo data aave to shu?**
→ Window ni upper bound (`< 10:00`) fix che. Etle e data **aagla run (2 PM)** ma aavse.

**Q14. Partition size 128 MB ke 64 MB?**
→ **128 MB e file reading (Parquet/CSV) no default che. Mongo connector no default 64 MB che.** `partition.size` thi badli shakay.

### General

**Q15. Kai methods mota data mate avoid karvi?**
→ `collect()`, `collectAsList()`, mota `take(n)` / `show(n)`, `toPandas()`, `collectAsMap()`, `countByKey()`, mota table nu `broadcast()`, `coalesce(1)`, `groupByKey()`, `partitionBy` vagar ni `Window`. Juo [Section 8](#8-danger-list--avoid-for-big-data).

**Q16. Mari cluster (2 × 8 GB) aa load mate chalshe?**
→ Chali shake, pan **dheemi**. **Disk (50 GB) sauthi moti problem** che, etle output alag storage par lakho. 5M/hour no number pan ek vaar confirm karo.

**Q17. Aavi baabat discuss kevi rite karvi?**
→ Aa 5 vaat aapo:
```
1. Data:     records, ek record ni size (sample JSON), collections
2. Kaam:     job shu kare (explode / join / groupBy / kayu table)
3. Cluster:  nodes, RAM, cores, disk, Spark version, spark-submit command
4. Schedule: ketla vakhte chale, full ke incremental
5. Problem:  error / job time / Spark UI screenshot
```

---

## Ek line ma yaad rakho 🧠

- **Driver = plan, Executor = kaam.** Data driver ma nathi aavto (`collect` sivay).
- **Transformation = notes, Action = kaam sharu.**
- **Partition = tukdo, Task = 1 tukda nu kaam, Core = ek sathe ketla tukda.**
- **Shuffle = sauthi bhaare** – ochha karo.
- **Incremental = start + end fix window + Mongo filter + index.**
- **Mota data no result = `write()`, `collect()` nahi.**
