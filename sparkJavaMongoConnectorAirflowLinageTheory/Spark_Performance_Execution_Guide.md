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
12. [Data Validation (DQ) & Rejected Records](#12-data-validation-dq--rejected-records)
13. [Monitoring – Spark UI](#13-monitoring--spark-ui)
14. [Final Checklist](#14-final-checklist)
15. [Tamara Sawal – Short Jawab](#15-tamara-sawal--short-jawab)
16. [Appendix – Parquet shu che?](#appendix--parquet-shu-che)

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

### 5.1 Summary table (Customer 360 jevi aggregated table) mate
- Rahul no navo order aave → **eni j row update** (grain = 1 row per customer)
- Navo customer → **navi row insert** (`dim_customer` LEFT JOIN thi)
- ⚠️ Juna number ma fakt "+1" na karo. Refund thay to `net_spent` ochho thavo joie, etle affected customer nu aakhu summary **fact table ni aakhi history** mathi farithi compute karo.
- Rit target par depend che:

| Target | Rit |
|---|---|
| **EPAS / Postgres** (aapdu) | Staging table + `INSERT ... ON CONFLICT DO UPDATE` (upsert). Juo 5.2. |
| Data lake – plain Parquet | Row update na thay, etle aakhu table **full history mathi** rebuild + overwrite |
| Data lake – Delta / Iceberg / Hudi | `MERGE INTO` (juo [Appendix](#appendix--parquet-shu-che)) |

### 5.2 EPAS (EDB Postgres) ma load – Overwrite nahi, Upsert

#### ❌ Khatro – incremental batch sathe overwrite

```java
newBatch.write().format("jdbc").mode("overwrite")...   // ❌ fakt 06:00–10:00 na 20M
```

```
 Pehla:  fact_sales = 6 mahina no data (karodo rows)
 Pachhi: fact_sales = fakt aa run na 20M rows   ← juno data GAYO ❌
```

- Spark JDBC `overwrite` default ma **table DROP kari ne navu banave che**, etle indexes, constraints ane grants pan jata rahe che.
- **Rule:** Incremental batch thi curated table **kyarey overwrite na karo.**

#### ✅ EPAS ma row UPDATE thai shake che

Parquet ni "row update na thay" vali limitation **EPAS / Postgres ma nathi**. Ema `UPDATE`, `INSERT ... ON CONFLICT` ane `MERGE` badhu chale che.
Pan **Spark JDBC pote upsert nathi kartu**, e fakt `append` ke `overwrite` kare che. Etle vacche **staging table** vaparo:

```
 Spark ──(overwrite + truncate)──► staging.stg_*  (fakt aa run no data)
                                        │
                                        ▼  SQL: INSERT ... ON CONFLICT DO UPDATE   (EPAS ni andar)
                                  curated.* table  (juno data SALAMAT + navo add/update)
```

#### Kaya table par kai rit?

| Table | Rit | Kem |
|---|---|---|
| `fact_sales`, `fact_payment` | **Append** + `ON CONFLICT DO NOTHING` | Orders navi j aave che. Re-run kare to duplicate na thay. |
| `dim_customer`, `dim_product` | **Upsert** (`ON CONFLICT DO UPDATE`) | Navo customer insert thay, ane city ke tier badle to update thay |
| `customer_360_summary` | Fakt **changed customers** nu upsert | 20M users nu aakhu rebuild nahi |
| `staging.stg_*` | `overwrite` + `truncate=true` | Ema fakt current run no data j rakhvano che |

#### (a) Spark → staging table
```java
batch.write().format("jdbc")
    .option("url", "jdbc:postgresql://epas-host:5444/dwh?reWriteBatchedInserts=true")
    .option("dbtable", "staging.stg_customer")
    .option("user", user).option("password", pwd)
    .option("truncate", "true")          // DROP nahi, fakt TRUNCATE (structure salamat)
    .option("batchsize", "10000")        // default 1000 → fast insert
    .option("numPartitions", "8")        // EPAS par ek sathe max 8 connections
    .mode("overwrite")                   // FAKT STAGING mate
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
ON CONFLICT (order_id, product_key) DO NOTHING;   -- same window farithi chale to duplicate nahi
```

#### (d) Customer 360: fakt changed customers, full history mathi
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

> Summary **`fact_sales` ni aakhi history** mathi bane che, fakt aaj na batch mathi nahi. Etle Rahul na juna 2 + navo 1 = sacha **3 orders**.
>
> Step (b), (c) ane (d) ni SQL **Airflow** ma `SQLExecuteQueryOperator` thi chalavo, athva Spark driver mathi ek JDBC statement thi. Aa fakt ek command che, ema data driver ma nathi aavto.

#### EPAS mate jaruri dhyan

| Point | Kem |
|---|---|
| **Surrogate key DB ma banavo** (`GENERATED ALWAYS AS IDENTITY`) | Spark nu `row_number()` darek run ma 1 thi sharu thay, etle keys takrai jay |
| **Unique index / constraint**: `customer_id`, `(order_id, product_key)` | `ON CONFLICT` mate jaruri che |
| **Fact table ne date thi partition** karo (Postgres declarative partitioning) | 120M rows/divas thay, etle query ane juno data delete karvanu saral bane |
| JDBC `numPartitions` ochha (e.g. 8) | 320 Spark tasks ek sathe connection kholshe to DB par bhaar aavshe |
| Upsert **transaction** ma | Aadhu-adhuru load na dekhay |
| Bahu moto load hoy to JDBC ne badle **`COPY`** | Postgres ma bulk load mate COPY sauthi fast che |
| Fact load pehla **dim upsert** | Fact ma `customer_key` lookup mate dim ready hovu joie |

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

1. **Disk sauthi moti problem** – output worker disk par lakhso to thoda j divas ma bharai jashe. Output **EPAS** ma (staging → upsert) athva **HDFS / S3 / MinIO** jeva alag storage par lakho.
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

### 10.4 Write – EPAS (aapdu target)
```java
stgFactSales.write().format("jdbc")
    .option("url", "jdbc:postgresql://epas-host:5444/dwh?reWriteBatchedInserts=true")
    .option("dbtable", "staging.stg_fact_sales")
    .option("truncate", "true")
    .option("batchsize", "10000")
    .option("numPartitions", "8")
    .mode("overwrite")                // FAKT staging; curated mate SQL upsert (Section 5.2)
    .save();
```

| Setting | Matlab |
|---|---|
| `truncate=true` | Overwrite vakhte table DROP nahi, fakt TRUNCATE |
| `batchsize` | Ek round-trip ma ketla rows insert (default 1000) |
| `numPartitions` | EPAS par ek sathe ketla connections |
| `reWriteBatchedInserts=true` | Batch insert ne multi-row INSERT ma badle – fast |

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
15. ✅ Output **EPAS** ma athva **HDFS / S3 / MinIO** par, worker local disk par nahi
16. ✅ Curated table ne incremental batch thi **kyarey overwrite nahi**. EPAS ma **staging + `ON CONFLICT` upsert**, data lake ma **Delta / Iceberg `MERGE`**

### Design
17. ✅ **Dims pehla, facts pachhi** (Airflow DAG dependency)
18. ✅ Aggregated tables (Customer 360) **incremental** – fakt badlayela keys
19. ✅ Juno data cleanup / retention policy
20. ✅ Driver par **kyarey** `collect()` mota data par nahi

---

## 12. Data Validation (DQ) & Rejected Records

> Sawal: 20M mathi 1M record corrupt che ke validation rule pass nathi karta.
> Emne **error message sathe alag (rejected) table** ma mukvana che.
> Shu aa e j plan ane e j execution flow ma thashe?

**Jawab:** Ha, validation rules pan **e j plan ma ane e j pass ma** chale che.
Pan **valid ane rejected ne alag-alag write karo to Mongo thi 2 vaar read** thay che, etle sachi rit vaparvi pade.

### 12.1 Rules plan ma kevi rite aave?

Validation rules pan **transformations** j che (`when`, `withColumn`, `filter`).
Etle Spark ene plan ma umeri de che, ane darek task **potana tukda par** read vakhte j rules lagave che.

```
 Task P8:  Mongo thi row aavi ─► explode ─► RULES CHECK ─► error_message set ─► dq_status set ─► write
           (badhu ek j pass ma, row-by-row – validation mate alag read nahi)
```

### 12.2 Pipeline ma validation kya aave?

```
 MongoDB (window 06:00–10:00, 20M)
        │
        ▼
 ① READ  (risky fields String tarike)
        │
        ▼
 ② FLATTEN / EXPLODE
        │
        ▼
 ③ VALIDATE  (row-level + lookup rules → errors[], dq_status)
        │
        ▼
 ④ EK J WRITE  partitionBy("dq_status")        ← Mongo thi fakt 1 vaar read
        │
        ├──► staging/orders_checked/dq_status=VALID/     (19M)
        │            │
        │            ▼
        │      ⑥ dim_* / fact_*  (curated)
        │
        └──► staging/orders_checked/dq_status=REJECTED/  (1M)
                     │
                     ▼
               ⑤ rejected_orders table (error_message sathe)
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
    ), null))                                                  // null kadhi nakho → fakt fail thayela rules rahe
    .withColumn("dq_status",     when(size(col("errors")).gt(0), "REJECTED").otherwise("VALID"))
    .withColumn("error_message", concat_ws("; ", col("errors")))
    .withColumn("run_id",        lit(runId))
    .withColumn("checked_at",    current_timestamp());
```

> Darek row ma **badha fail thayela rules** ek sathe aave che (fakt pehlo nahi). Etle debugging saral bane che.

### 12.4 ⚠️ Mukhya Trap – 2 writes = Mongo thi 2 vaar read

```java
// ❌ KHOTU
checked.filter("dq_status = 'VALID'").write()...      // Action 1 → Mongo thi 20M read
checked.filter("dq_status = 'REJECTED'").write()...   // Action 2 → Mongo thi FARITHI 20M read
```

Darek action **aakho plan farithi** chalave che: Mongo read, explode ane validate, badhu biji vaar.
Etle time double thay che, Mongo load double thay che, ane window vacche late data aave to banne run ma data alag pan hoi sake che.

| Rit | Kevi rite | Tamara cluster mate |
|---|---|---|
| **A. Ek write + `partitionBy("dq_status")`** | Ek j action thi VALID ane REJECTED alag folder ma lakhay | ✅ **Best** – Mongo thi 1 j read |
| B. `persist()` + 2 writes | Validated data cache, pachhi 2 writes | ⚠️ 20 GB vs 8 GB RAM, moto spill thay |
| C. Staging table | Aakho checked data ek vaar lakho, pachhi ema thi vancho | ✅ Safe, ane audit / re-run mate saru (A pan staging j che) |

```java
// ✅ SACHU – Option A
String stagingPath = "s3a://datalake/staging/orders_checked/run_id=" + runId;

checked.write()
    .mode("overwrite")
    .partitionBy("dq_status")
    .parquet(stagingPath);

// Aagal: VALID → curated
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

> **EPAS target hoy to:** JDBC ma `partitionBy` nathi. Etle aakho `checked` data **ek j write** thi `staging.stg_orders_checked` (`dq_status` column sathe) ma lakho.
> Pachhi EPAS ni andar SQL thi `dq_status='VALID'` → curated ane `dq_status='REJECTED'` → `rejected_orders` ma muko. Mongo thi read to 1 j vaar thay che.

### 12.5 Rejected table – sample output

**`rejected_orders`**

| run_id | source_collection | order_id | customer_id | product_id | quantity_raw | payment_mode | error_message | checked_at |
|---|---|---|---|---|---|---|---|---|
| 20261120_10 | orders | O2001 | null | P003 | 1 | UPI | customer_id missing | 2026-11-20 10:42 |
| 20261120_10 | orders | O2002 | C004 | P005 | abc | CARD | quantity not numeric | 2026-11-20 10:42 |
| 20261120_10 | orders | O2003 | C007 | P009 | -2 | PAYTM | quantity <= 0; invalid payment_mode | 2026-11-20 10:42 |
| 20261120_10 | orders | O2004 | C010 | P099 | 1 | COD | product_id not in dim_product | 2026-11-20 10:42 |

> Tip: Debug mate aakho **original document** pan JSON string tarike (`raw_payload`) rakhi shakay. `to_json(struct(col("*")))` vaparo.

### 12.6 Badha rules ek pass ma nathi chalta – 3 prakar

| Rule type | Example | Ek pass ma? | Kevi rite |
|---|---|---|---|
| **Row-level** | null, range (`qty > 0`), format (email), allowed values | ✅ Ha | `when(...)` – shuffle nahi |
| **Lookup / FK** | `product_id` `dim_product` ma che? | ✅ Ha, jo dim nanu hoy | **Broadcast left join** + `when(col("product_key").isNull(), "product_id not in dim_product")` |
| **Dataset-level** | Duplicate `order_id`, total count, "rejected > 5%" | ❌ Na | `groupBy` / `Window` (shuffle) ke alag `count()` |

**Lookup rule:**
```java
Dataset<Row> withLookup = items
    .join(broadcast(dimProduct.select("product_id", "product_key")), col("product_id"), "left");
// product_key null → "product_id not in dim_product"
```

**Duplicate rule (shuffle thay):**
```java
WindowSpec w = Window.partitionBy("order_id", "product_id").orderBy(col("updated_at").desc());
.withColumn("rn", row_number().over(w))
// rn > 1 → "duplicate order line"
```

### 12.7 Corrupt type vala records

Jem ke `quantity = "abc"` hoy, ke `order_date = "31-31-2026"` hoy.

| Rit | Shu thay |
|---|---|
| ❌ Strict schema (`IntegerType`) thi read | Type convert vakhte j **job fail** thai sake, validation sudhi pahochay j nahi |
| ✅ Risky fields **`StringType`** thi read, pachhi `cast` | Cast fail thay to value `null` bane, ane row ne error sathe **reject** karo. Job chalti rahe. |

```java
.withColumn("quantity", col("quantity_raw").cast("int"))
.withColumn("order_date", to_timestamp(col("order_date_raw")))
// cast pachhi null, pan raw null nathi → corrupt value
```

### 12.8 Threshold check – job kyare fail karvi?

Count **Mongo par farithi na chalavo**. Pehla lakhela staging output par j count karo, ema bahu ochho kharcho aave.

```java
long total    = spark.read().parquet(stagingPath).count();
long rejected = spark.read().parquet(stagingPath + "/dq_status=REJECTED").count();
double pct    = rejected * 100.0 / total;

if (pct > 5.0) {
    throw new RuntimeException("DQ FAIL: " + rejected + " / " + total + " (" + pct + "%) rejected");
    // Airflow task fail → alert, curated load nahi thay
}
```

Example: 20M mathi 1M reject = **5%**. Limit 5% hoy to border par che, ane 2% hoy to job fail thay.

### 12.9 DQ summary table (optional, monitoring mate)

**`dq_run_summary`**

| run_id | source_collection | total_records | valid_records | rejected_records | rejected_pct | status |
|---|---|---|---|---|---|---|
| 20261120_06 | orders | 20,000,000 | 19,950,000 | 50,000 | 0.25 | PASS |
| 20261120_10 | orders | 20,000,000 | 19,000,000 | 1,000,000 | 5.00 | WARN |

Aa table thi dashboard ma joi shakay ke kaya run ma data quality bagdi.

### 12.10 Airflow DAG ma validation

```
 extract_window
      │
      ▼
 validate_and_stage        (ek write, partitionBy dq_status)
      │
      ▼
 dq_threshold_check        (fail → alert, aagal na chale)
      │
      ├──────────────► load_rejected_orders
      ▼
 load_dims ──► load_facts ──► build_gold
```

### 12.11 Summary

- ✅ Row-level ane lookup validation **e j plan ane e j pass** ma chale che, ema extra Mongo read nathi.
- ⚠️ VALID ane REJECTED **2 alag writes = 2 Mongo reads**. Etle **ek write + `partitionBy("dq_status")`** vaparo.
- ⚠️ Duplicate ane threshold jeva rules mate shuffle ke biju action joie. Count **staging output** par karo.
- ✅ Risky fields `String` thi vancho, `cast` karo, ane fail thay to reject karo, crash nahi.
- ✅ Darek rejected row ma: `run_id`, `error_message` (badha rules), ane jarur hoy to `raw_payload`.
- ✅ Rejected % limit thi vadhare hoy to job fail + alert, ane curated load rokay.

---

## 13. Monitoring – Spark UI

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

## 14. Final Checklist

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

 DATA VALIDATION
 [ ] Risky fields String thi read, pachhi cast
 [ ] Badha rules errors[] ma, error_message sathe
 [ ] Ek j write + partitionBy("dq_status") (2 writes nahi)
 [ ] Rejected table ma run_id + error_message (+ raw_payload)
 [ ] Threshold check staging output par, fail → alert

 WRITE (EPAS)
 [ ] Curated table par KYAREY overwrite nahi
 [ ] Spark → staging.stg_* (overwrite + truncate=true)
 [ ] Staging → curated: INSERT ... ON CONFLICT (upsert / do nothing)
 [ ] Surrogate key DB identity, unique constraints set
 [ ] Fact table date thi partitioned, JDBC numPartitions ochha

 WRITE (data lake hoy to)
 [ ] Output alag storage (HDFS / S3 / MinIO)
 [ ] partitionBy(date_key)
 [ ] Upsert mate Delta/Iceberg MERGE

 OPS
 [ ] Dims → Facts → Gold (DAG order)
 [ ] Spark UI ma spill / skew check
 [ ] Disk retention / cleanup
```

---

## 15. Tamara Sawal – Short Jawab

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
→ **Ha**, navi row nahi, **e j row update** (grain = 1 row per customer). EPAS ma staging + `INSERT ... ON CONFLICT DO UPDATE` thi (Section 5.2).

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

### Data Validation

**Q18. Validation rules pan plan ma j manage thay ane execution vakhte j data validate thay?**
→ **Ha.** Rules transformations j che, etle e j plan ma aave ane read vakhte e j pass ma darek tukda par chale. Duplicate jeva dataset-level rules mate shuffle ke biju action joie.

**Q19. 20M mathi 1M corrupt / invalid records error message sathe alag table ma jashe, e j flow ma?**
→ **Ha**, pan **ek j write + `partitionBy("dq_status")`** thi. VALID ane REJECTED mate 2 alag writes karo to Mongo thi 2 vaar read thay. 19M curated ma jay ane 1M `rejected_orders` ma `error_message` sathe jay. Juo [Section 12](#12-data-validation-dq--rejected-records).

**Q20. Corrupt type (e.g. `quantity = "abc"`) thi job crash thay?**
→ Strict schema thi vancho to thai sake. Etle risky fields `String` thi vancho, `cast` karo, ane `null` aave to reject karo.

### Target DB (EPAS) & Parquet

**Q21. Hu EPAS vaparu chu ane incremental load karu chu. Table full rebuild + overwrite karu to juno data loss thashe?**
→ **Ha, thashe.** Overwrite pachhi table ma fakt aa run no data rahe, ane Spark JDBC table DROP pan kare. Etle curated table ne **kyarey overwrite na karo.** Spark thi **staging** ma lakho, pachhi SQL thi **upsert** karo (Section 5.2).

**Q22. EPAS ma row update thai shake?**
→ **Ha.** `UPDATE`, `INSERT ... ON CONFLICT`, `MERGE` badhu chale. Parquet ni limitation EPAS ma nathi. Fakt Spark JDBC upsert nathi kartu, etle staging + SQL vaparvu pade.

**Q23. Parquet shu che?**
→ **Column-wise, compressed file format.** Analytics mate fast che, pan file ek vaar lakhay pachhi badlay nahi (immutable). Update mate Delta / Iceberg vaparo. Juo [Appendix](#appendix--parquet-shu-che).

---

## Ek line ma yaad rakho 🧠

- **Driver = plan, Executor = kaam.** Data driver ma nathi aavto (`collect` sivay).
- **Transformation = notes, Action = kaam sharu.**
- **Partition = tukdo, Task = 1 tukda nu kaam, Core = ek sathe ketla tukda.**
- **Shuffle = sauthi bhaare** – ochha karo.
- **Incremental = start + end fix window + Mongo filter + index.**
- **Mota data no result = `write()`, `collect()` nahi.**
- **Validation = e j pass ma; VALID + REJECTED = ek j write (`partitionBy dq_status`).**
- **EPAS: curated par overwrite kyarey nahi → staging + `ON CONFLICT` upsert.**

---

## Appendix – Parquet shu che?

**Parquet ek file format che**, jem CSV, JSON ke Excel file format che. Big data duniya ma data store karva mate e sauthi vadhare vaparatu format che.
Data lake (HDFS / S3 / MinIO) par Spark tables mostly `.parquet` files tarike save thay che.

> Aapdu curated target **EPAS** che, etle tables DB ma che, Parquet ma nahi.
> Pan Parquet **raw archive, staging, ke data lake** mate bahu upyogi che, etle samajvu jaruri che.

### A.1 CSV vs Parquet – data kevi rite store thay?

Aapdo data:

| order_id | customer | city | amount |
|---|---|---|---|
| O1001 | Rahul | Ahmedabad | 29000 |
| O1002 | Priya | Surat | 2400 |
| O1003 | Amit | Mumbai | 55000 |

**CSV / JSON (row-wise):** ek line ma ek aakhi row.
```
O1001,Rahul,Ahmedabad,29000
O1002,Priya,Surat,2400
O1003,Amit,Mumbai,55000
```

**Parquet (column-wise):** ek column na badha values ek sathe.
```
order_id : O1001 | O1002 | O1003
customer : Rahul | Priya | Amit
city     : Ahmedabad | Surat | Mumbai
amount   : 29000 | 2400  | 55000     (+ min=2400, max=55000 jevi stats)
```

### A.2 Parquet file ni andar shu hoy?

```
 orders.parquet
 ├── Row Group 1  (~128 MB na rows)
 │     ├── Column chunk: order_id   (compressed + min/max)
 │     ├── Column chunk: customer   (compressed + min/max)
 │     ├── Column chunk: city       (compressed + min/max)
 │     └── Column chunk: amount     (compressed + min/max)
 ├── Row Group 2
 │     └── ...
 └── Footer (metadata): schema, column types, darek row group na min/max, row count
```

Spark pehla **footer** vanche che. Pachhi nakki kare ke kaya row groups ane kaya columns vanchva jaruri che, ane baki na skip kare che.

### A.3 Parquet kem vaparay? (Benefits)

| Faydo | Matlab |
|---|---|
| **Fakt jaruri column vanche** (column pruning) | `SUM(amount)` karvu hoy to fakt `amount` column vanchay. 20 columns mathi 1 j vanchvo pade. |
| **Compression** | Ek column ma same type na values (jem ke "Gujarat" vaaramvaar), etle bahu saru compress thay. 20 GB JSON ghani vaar **3–5 GB Parquet** bani jay. |
| **Skip (predicate pushdown)** | `amount > 100000` puchho ane row group ma max 55000 hoy, to aakho block vanchya vagar skip thay. |
| **Schema sathe** | Column na naam ane types (int, string, date) file ma j hoy che. |
| **Badha tools vanchi shake** | Spark, Hive, Trino, Athena, Databricks, DuckDB, Pandas |
| **Splittable** | Moti file ne pan Spark ghana tasks ma vahenchi ne parallel vanchi shake che |

### A.4 Limitation – row update na thay

Parquet file ek vaar lakhay pachhi **badlay nahi (immutable)**. Data column-wise ane compress thayelo hoy che, etle vacche thi ek value badlavani koi rit nathi.

```
 Rahul nu net_spent 29000 → 31000 karvu che

 CSV:      line shodho ane badlo        (shakya che, pan slow)
 EPAS:     UPDATE ... WHERE customer_id='C001'   ✅ direct
 Parquet:  ❌ ek value badlay nahi
           ✅ aakhi file (ke aakhu table) farithi lakhvi pade   → overwrite
```

Bija limitations:
- **Nani-nani files no problem** – darek run ma hajaro nani files bane to read slow thay. Ena mate `coalesce(n)` / AQE vaparo.
- **Delete / dedupe mushkel** – aakhi file farithi lakhvi pade.
- **Ek sathe be jobs lakhe to conflict** – koi transaction nathi.

### A.5 Delta Lake / Apache Iceberg / Apache Hudi

Aa nava file formats nathi. E **Parquet ni upar ek layer** che:

```
 Delta / Iceberg table
 ├── data files      → Parquet j (part-0001.parquet, part-0002.parquet …)
 └── transaction log → "kai file current che, kai juni thai gai" no hisab
```

- `MERGE` / `UPDATE` karo tyare e **fakt je file ma Rahul che e file** farithi lakhe che, ane log ma juni file ne "removed" mark kari de che.
- Tamne laage ke ek row update thai, pan andar file j badlay che.
- Bonus: **ACID** (aadhu-adhuru write nahi dekhay), **time travel** (gaye kale no data joi shakay), ane schema evolution.

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
| Compression | ❌ | ❌ | ✅ Bahu saru | ✅ | Thodu |
| Schema | ❌ | Flexible | ✅ | ✅ + evolution | ✅ Strict |
| Row update | Mushkel | Mushkel | ❌ | ✅ MERGE | ✅ UPDATE / UPSERT |
| Transactions (ACID) | ❌ | ❌ | ❌ | ✅ | ✅ |
| Analytics (karodo rows scan) | Slow | Slow | ✅ Fast | ✅ Fast | Medium (index / partition par depend) |
| Ek row shodhvi (lookup) | Slow | Slow | Medium | Medium | ✅ Fast (index) |
| Best use | Export / Excel | API / Mongo | Data lake, archive | Lakehouse | App + DWH, BI, upsert |

### A.7 Aapda project ma Parquet kya kaam aave?

| Jagya | Kem |
|---|---|
| **Raw archive** (`raw_orders` Mongo no as-is copy) | Sasti storage, compressed, re-process kari shakay. EPAS ne bharay nahi. |
| **Staging / DQ output** (`dq_status=VALID/REJECTED`) | Ek j write thi banne output, pachhi EPAS ma load |
| **Juno data (history)** | EPAS mathi 1 varsh juna partitions Parquet ma archive karo, ane DB nani rakho |

### A.8 Spark ma Parquet read / write

```java
// Write – date thi folder banshe: .../order_date=2026-11-20/part-0000.parquet
df.write()
  .mode("append")
  .partitionBy("order_date")
  .option("compression", "snappy")          // default; zstd vadhare compress kare
  .parquet("s3a://datalake/raw/orders");

// Read – fakt jaruri divas ane column vanchay (partition pruning + column pruning)
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

### A.9 Short ma

- **Parquet** = column-wise, compressed file. **Read ane analytics mate fast**, pan **update na thay**.
- **Delta / Iceberg** = Parquet + transaction log. Ema `UPDATE` / `MERGE` thai shake.
- **EPAS** = database. Ema row `UPDATE` / upsert direct thay che, etle aapda curated layer ma Parquet ni limitation lagu nathi padti.
