# Medallion Data Pipeline – Plan (ગુજરાતી)

> **Source (MongoDB) → Kafka Connect (MongoDB Source Connector) → Kafka → Java subscriber → Bronze (MongoDB) → Spark → Silver (MySQL) → Spark → Gold (MySQL)**
>
> બધું **Windows 11 પર native** (Docker વગર).
>
> ✅ **Status:** Infrastructure (Prompt 1) અને Application (Prompt 2) બંને implement અને test થઈ ગયા છે. Code ની સમજૂતી: **[MEDALLION_CODE_GUIDE_GU.md](MEDALLION_CODE_GUIDE_GU.md)**
> Setup: **[MONGO_CDC_COMPLETE_SETUP_GU.md](MONGO_CDC_COMPLETE_SETUP_GU.md)** (Source → Kafka → Bronze) અને **[MEDALLION_SETUP_GUIDE_GU.md](MEDALLION_SETUP_GUIDE_GU.md)** (databases)
> Tool ની પસંદગી કેમ કરી: [CDC_TOOL_COMPARISON_GU.md](CDC_TOOL_COMPARISON_GU.md)
>
> **ફેરફાર (2026-10-28):** Debezium અને MongoDB sink connector દૂર કર્યા. MongoDB CDC હવે *MongoDB Source Connector → એક generic topic `de_source_mongo` → પોતાનો Java subscriber (`mongo-cdc-subscriber`)* થી થાય છે. Bronze document = source document + `_op`, `_deleted`, `_source_ts`, `_ingested_at`. **MySQL CDC બંધ છે**: bronze ના જૂના `mysql_station` / `mysql_station_owner` collections જેમ છે તેમ વપરાય છે.

---

## અનુક્રમણિકા
1. [લક્ષ્ય – ટૂંકમાં](#1-લક્ષ્ય--ટૂંકમાં)
2. [Architecture (Visualization)](#2-architecture-visualization)
3. [5 Databases](#3-5-databases)
4. [Technologies](#4-technologies)
5. [Software List](#5-software-list)
6. [Stage 1 → 2: CDC અને જૂનો data](#6-stage-1--2-cdc-અને-જૂનો-data)
7. [Stage 2 → 3 → 4: Incremental Batch](#7-stage-2--3--4-incremental-batch)
8. [Data Model](#8-data-model)
9. [Project માં ફેરફારો](#9-project-માં-ફેરફારો)
10. [Implementation ક્રમ](#10-implementation-ક્રમ)
11. [Risks](#11-risks)
12. [Implementation Prompts (2)](#12-implementation-prompts-2)

---

## 1. લક્ષ્ય – ટૂંકમાં

| Stage | Layer | Database | Data કેવી રીતે આવે | Process |
|---|---|---|---|---|
| 1 | **Source / Original** | MongoDB + MySQL | Users / applications લખે | – |
| 2 | **Bronze / Raw** | MongoDB (1 database, ઘણા collections) | Stage 1 માંથી **as-is** + 4 metadata fields | **Real-time CDC**: MongoDB Source Connector + Kafka + Java subscriber |
| 3 | **Silver / Curated** | MySQL | Bronze માંથી સાફ, flatten, join કરેલો | **Batch**: Spark (ફક્ત નવો data) |
| 4 | **Gold / Aggregate** | MySQL | Silver માંથી summary / KPI | **Batch**: Spark (ફક્ત બદલાયેલા groups) |

**મુખ્ય નિયમ:** Stage 2→3 અને 3→4 માં **જૂનો data ફરી process નહીં થાય** (વિભાગ 7).

---

## 2. Architecture (Visualization)

```
┌───────────────────────────── STAGE 1 : SOURCE ─────────────────────────────────────────────────┐
│  MongoDB (Windows service, replica set rs0, :27017)      MySQL 8 (Windows service, :3306)       │
│  DB: de_source_mongo                                     DB: de_source_mysql                    │
│    └─ weather_observations (nested JSON)                   ├─ station                           │
│                                                            └─ station_owner                     │
│         │ change streams                                         (MySQL CDC બંધ)              │
└─────────┼───────────────────────────────────────────────────────────────────────────────────────┘
          │
┌─────────▼──────────────── KAFKA PLATFORM (native Windows) ─────────────────────────────────────┐
│  Kafka Connect (:8083)                                                                          │
│    └─ mongo-cdc-db-source : MongoDB Source Connector ──► topic de_source_mongo (બધા collections)│
│  Kafka broker (KRaft, :9092)            topic de_source_mongo.dlq (વાંચી ન શકાયેલા messages)      │
│  mongo-cdc-subscriber (Java app)        topic વાંચીને bronze માં insert / update / soft delete   │
└───────────────────────────────────────────────────────┬─────────────────────────────────────────┘
                                                        │
┌───────────────────────────── STAGE 2 : BRONZE (MongoDB) ───────────────────────────────────────┐
│  DB: de_bronze                                                                                  │
│    weather_observations : source document as-is + _op, _deleted, _source_ts, _ingested_at      │
│    mysql_station, mysql_station_owner : જૂના Debezium pipeline ના (હવે update થતા નથી)          │
└───────────────────────────────────────────────────────┬─────────────────────────────────────────┘
                                                        │ Spark Job #1  (every N min)
                                                        │ _ingested_at > watermark  (Mongo pushdown)
┌───────────────────────────── STAGE 3 : SILVER (MySQL) ▼────────────────────────────────────────┐
│  DB: de_silver                                                                                  │
│    weather_observation + 5 child tables, dim_station   (typed, deduplicated, upserted)          │
│    etl_watermark, etl_job_run                          (job state + audit)                       │
└───────────────────────────────────────────────────────┬─────────────────────────────────────────┘
                                                        │ Spark Job #2  (after job #1)
                                                        │ only changed (station, day) keys
┌───────────────────────────── STAGE 4 : GOLD (MySQL) ──▼────────────────────────────────────────┐
│  DB: de_gold                                                                                    │
│    daily_station_weather, monthly_region_weather, station_data_quality                          │
└─────────────────────────────────────────────────────────────────────────────────────────────────┘

 Orchestration : Spring Boot scheduler   Job #1 ──success──► Job #2
 Compute       : Spark 3.5.1 (default: local[*]; dev: Windows standalone cluster)
 Governance    : OpenMetadata (optional, Docker, પછીથી)
```

---

## 3. 5 Databases

| # | Database | Engine | Stage | Instance |
|---|---|---|---|---|
| 1 | `de_source_mongo` | MongoDB | 1 – Source | MongoDB Windows service |
| 2 | `de_source_mysql` | MySQL | 1 – Source | MySQL Windows service (પહેલેથી છે) |
| 3 | `de_bronze` | MongoDB | 2 – Bronze | **એ જ** MongoDB |
| 4 | `de_silver` | MySQL | 3 – Silver | **એ જ** MySQL |
| 5 | `de_gold` | MySQL | 4 – Gold | **એ જ** MySQL |

- **નામ dataset-independent છે.** Dataset બદલાય તો ફક્ત collections/tables બદલાશે, databases નહીં.
- **5 databases, ફક્ત 2 server installations.** RAM બચે છે.

---

## 4. Technologies

| Technology | Version | ભૂમિકા |
|---|---|---|
| **MongoDB Community Server** | 8.x | Source + Bronze. **Replica set** (CDC માટે ફરજિયાત) |
| **MySQL** | 8.0.30 (હાલનું) | Source + Silver + Gold |
| **Apache Kafka** (KRaft) | 4.3.1 | Event backbone. ZooKeeper નથી |
| **Kafka Connect** (Kafka સાથે આવે છે) | 4.3.1 | Source connector ચલાવે (code વગર, JSON config) |
| **MongoDB Kafka Connector (source)** | 3.1.2 | Change streams → Kafka (CDC), `copy_existing`, Canonical Extended JSON |
| **`RegexRouter` SMT** | – | બધા collections → એક topic (`de_source_mongo`) |
| **`mongo-cdc-subscriber`** (પોતાનો Java app) | `kafka-clients` 4.0, `mongodb-driver-sync` 5.1.4 | Kafka → Bronze: upsert, soft delete, metadata (`_op`, `_deleted`, `_source_ts`, `_ingested_at`) |
| **Apache Spark** | 3.5.1 (હાલનું) | Bronze→Silver, Silver→Gold |
| **MongoDB Spark Connector** | 10.x (`_2.12`) | Spark માંથી Bronze વાંચવા (filter pushdown) |
| **MySQL Connector/J** | (હાલનું) | Spark ↔ MySQL |
| **Flyway** | Spring Boot managed | Silver/Gold tables programmatically (SQL migrations) |
| **Spring Boot** | 2.7 (હાલનું) | Scheduler + config + profiles |

**CDC tool:** MongoDB Kafka Connector (source) + પોતાનો subscriber. પહેલાં Debezium હતું; કેમ બદલ્યું તે [CDC_TOOL_COMPARISON_GU.md](CDC_TOOL_COMPARISON_GU.md) વિભાગ 0 માં છે.

---

## 5. Software List

**બધું Windows native.** Docker ફક્ત optional OpenMetadata માટે.

| Software | Status |
|---|---|
| JDK 17, Maven, VS Code | ✅ પહેલેથી છે |
| Apache Spark 3.5.1 + winutils | ✅ પહેલેથી છે |
| MySQL 8.0.30 | ✅ પહેલેથી છે |
| **MongoDB Community Server 8.x** (service) | ✅ install |
| **mongosh**, **MongoDB Database Tools** (`mongoimport`), Compass (optional) | ✅ install |
| **Apache Kafka 4.3.1** (Kafka Connect સાથે) | ✅ install |
| **MongoDB Kafka Connector 3.1.2** | ✅ download |
| `mongo-cdc-subscriber` (project માં, Maven build) | ✅ |
| Docker Desktop + OpenMetadata | ✅ છે (optional; RAM માટે બંધ રાખો) |

### RAM budget: machine પર કુલ 9.9 GB ⚠️
| Component | મર્યાદા |
|---|---|
| Windows + VS Code | ~4 GB |
| MySQL | ~0.5 GB |
| MongoDB | cache 0.5 GB (~0.8 GB) |
| Kafka | heap 512 MB |
| Kafka Connect | heap 768 MB |
| Spark (`local[*]`, driver 1–1.5 GB) | ~1.5 GB |

➡️ **Docker Desktop, OpenMetadata અને Spark standalone cluster ને Kafka stack સાથે એક સાથે ન ચલાવો.** Cluster test કરવો હોય ત્યારે Kafka Connect થોડી વાર બંધ કરો. Bronze પહેલેથી ભરેલું હોય છે, એટલે Spark ને Kafka ની જરૂર નથી.

---

## 6. Stage 1 → 2: CDC અને જૂનો data

### 6.1 Flow
```
 Mongo de_source_mongo.*  ──(MongoDB Source Connector + RegexRouter)──► topic de_source_mongo ──(mongo-cdc-subscriber)──► de_bronze.<એ જ collection નામ>
 MySQL de_source_mysql.*  ── (CDC બંધ) ──  bronze ના જૂના mysql_station / mysql_station_owner જેમ છે તેમ
```
Setup: [MONGO_CDC_COMPLETE_SETUP_GU.md](MONGO_CDC_COMPLETE_SETUP_GU.md)

### 6.2 Bronze document = as-is + metadata (implementation પછી ચકાસેલું)

**`weather_observations` (અને source ના બીજા collections)**: subscriber લખે છે

| Field | અર્થ |
|---|---|
| original fields | Source document **જેમ છે તેમ**: એ જ `_id` (ObjectId), એ જ BSON types (Int32, Double, Date), એ જ ક્રમ |
| `_op` | છેલ્લું operation: `insert` / `update` / `replace` / `delete` |
| `_deleted` | `true` = source માં delete થયું (**soft delete**: document bronze માં રહે છે) |
| `_source_ts` | Source માં change થયાનો time (epoch ms, change event નો `wallTime`) |
| `_ingested_at` | Subscriber એ bronze માં લખ્યાનો time (epoch ms). **Job #1 નો watermark** |

**`mysql_*` collections**: જૂના Debezium pipeline એ લખેલા, **હવે update થતા નથી**

| Field | અર્થ |
|---|---|
| original columns | Row જેમ છે તેમ. DECIMAL string તરીકે, DATETIME epoch ms તરીકે |
| `__op`, `__source_ts_ms`, `__deleted`, `_ingested_at` | જૂના pipeline ના metadata |

### 6.3 Pipeline શરૂ થાય તે પહેલાનો (જૂનો) data
**Source connector નું `startup.mode=copy_existing`** આ સંભાળે છે:
1. Connector પહેલી વાર શરૂ થાય ત્યારે source ના **હાલના બધા documents** ને `insert` events તરીકે topic માં મોકલે.
2. પછી **live changes** (change stream) વાંચે.
3. ક્યાં સુધી વાંચ્યું તે (resume token) `connect-offsets` topic માં સાચવે, એટલે restart પર copy ફરી નથી થતી.

➡️ Subscriber upsert કરે છે, એટલે કોઈ event બે વાર આવે તો પણ bronze માં duplicate documents નથી બનતા.

**પછીથી નવું collection ઉમેરવું:** source database નું આખું change stream વંચાય છે, એટલે **કોઈ config બદલ્યા વગર** નવું collection એ જ topic માં આવે અને subscriber `de_bronze` માં એ જ નામે બનાવે. Spark માં એ collection વાંચવા માટે નવો transformer જોઈએ.

---

## 7. Stage 2 → 3 → 4: Incremental Batch

### 7.1 Control tables (`de_silver`)
```sql
etl_watermark (job_name PK, last_watermark DATETIME(3), updated_at)
etl_job_run   (run_id PK, job_name, started_at, finished_at, status,
               watermark_from, watermark_to, rows_read, rows_written, error_message)
```

### 7.2 Job #1: Bronze → Silver
```
1. wm_from = etl_watermark.last_watermark            (પહેલી વાર: 1970-01-01)
2. wm_to   = now() - safety lag (2 min)
3. Mongo read with pushdown:  _ingested_at > wm_from AND _ingested_at <= wm_to
4. Transform: flatten (હાલનું logic reuse), types, dedupe (latest per key), join dim_station,
              _deleted (weather_observations) / __deleted (mysql_*) → is_deleted
5. Load: Spark → staging table → INSERT ... SELECT ... ON DUPLICATE KEY UPDATE   (upsert, 1 transaction)
6. Success → watermark = wm_to, etl_job_run = SUCCESS
   Failure → watermark બદલાય નહીં → next run એ જ window ફરી લે
```

| Mechanism | કઈ સમસ્યા ઉકેલે |
|---|---|
| **Watermark** | ફક્ત નવો data. જૂનો ફરી process નહીં |
| **Safety lag** | Subscriber મોડો લખે તો પણ record miss ન થાય |
| **Upsert** | Retry, overlap કે update થી duplicate નહીં (idempotent) |
| **Success પછી જ watermark** | Fail થાય તો data loss નહીં |
| **Audit table** | દરેક run નો હિસાબ |

### 7.3 Job #2: Silver → Gold (changed-keys recompute)
```
1. silver_loaded_at > watermark → affected keys (station_code, obs_date), (region, month)
2. ફક્ત તે keys નું aggregate આખા silver માંથી ફરી ગણો
3. Gold upsert (ON DUPLICATE KEY UPDATE) → watermark update
```
Average જેવા aggregates ને ફક્ત નવી rows ઉમેરીને સાચા રાખી શકાતા નથી. તેથી **બદલાયેલા groups ને આખા ફરી ગણવામાં** આવે છે.

### 7.4 Scheduling
Spring scheduler: Job #1 → (success) → Job #2. એક જ thread, એટલે runs overlap નથી થતા.

---

## 8. Data Model

| Stage | Object |
|---|---|
| 1 | `de_source_mongo.weather_observations` (data1.json import) |
| 1 | `de_source_mysql.station` (station_code PK = callLetters), `de_source_mysql.station_owner` (region) |
| 2 | `de_bronze.weather_observations` (subscriber), `de_bronze.mysql_station`, `de_bronze.mysql_station_owner` (જૂના, હવે update નહીં) |
| 3 | `de_silver.weather_observation` + 5 child tables (હાલની design), `de_silver.dim_station` |
| 4 | `de_gold.daily_station_weather`, `de_gold.monthly_region_weather`, `de_gold.station_data_quality` |

Join: `weather_observation.call_letters = dim_station.station_code`.

---

## 9. Project માં ફેરફારો

**Legacy code રાખવાની જરૂર નથી.** હાલનો file → MySQL pipeline નવા design થી બદલાશે.

| દૂર થશે | કેમ |
|---|---|
| `WeatherEtlJob` (file → MySQL) | નવા jobs તેની જગ્યા લેશે. Flatten logic નવા job માં reuse થશે |
| JPA entities + Hibernate `ddl-auto` | Tables હવે **Flyway SQL** થી બનશે (2 databases, explicit DDL) |
| `sql/schema.sql`, `etl.source-file` settings | Source હવે Bronze MongoDB છે |
| Docker `prod` profile / files | Kafka stack native છે. Implementation વખતે મને પૂછીને નક્કી થશે |

| નવું / બદલાશે | વિગત |
|---|---|
| Packages | `config`, `common` (watermark, audit, upsert writer), `pipeline.bronze2silver`, `pipeline.silver2gold`, `scheduler` |
| `pom.xml` | + `mongo-spark-connector_2.12` (10.x), + `flyway-core` (+ `flyway-mysql` જો જરૂર હોય); − `spring-boot-starter-data-jpa` (જરૂર ન હોય તો) |
| `application*.properties` | Bronze Mongo URI, silver/gold JDBC URLs, `pipeline.interval-ms`, `pipeline.safety-lag-minutes`; dev profile માં executors માટે `spark.jars` |
| `src/main/resources/db/silver`, `db/gold` | Flyway migrations |
| `kafka-connect/*.json` | Source connector config `mongo-cdc-db-source.json` ([CDC setup](MONGO_CDC_COMPLETE_SETUP_GU.md)) |
| `mongo-cdc-subscriber/` | Kafka → Bronze Java subscriber (અલગ Maven project) |
| `docs/` | Code guide (ગુજરાતી) |

**Bronze નો data file કરતાં અલગ દેખાશે.** હવે numbers સાચા types માં હશે, `{"$numberDouble": ...}` wrappers નહીં હોય. તેથી `WeatherSchema`/`JsonCols` ને **bronze ના actual documents જોઈને** બદલવા પડશે.

**Cluster માટે jars (પહેલા શીખેલો નિયમ):** Executors Mongo વાંચે છે અને MySQL માં લખે છે. તેથી `mongo-spark-connector`, `mongodb-driver-sync`, `mongodb-driver-core`, `bson`, `mysql-connector-j` ની jars `spark.jars` થી executors સુધી પહોંચાડવી પડશે.

---

## 10. Implementation ક્રમ

| # | કામ | કેવી રીતે | પૂરું થયાની નિશાની |
|---|---|---|---|
| **1** | Installation + configuration: MongoDB, MySQL setup, Kafka, Connect, source connector, subscriber, CDC test | **[CDC setup](MONGO_CDC_COMPLETE_SETUP_GU.md)** + **[Setup guide](MEDALLION_SETUP_GUIDE_GU.md)** જાતે, અથવા **Prompt 1** થી | CDC setup વિભાગ 10 ✅ |
| **2** | Application: refactor, Flyway, Job #1, Job #2, scheduler, profiles, tests | **Prompt 2** | Source change → થોડી મિનિટમાં gold માં |

---

## 11. Risks

| Risk | ઉકેલ |
|---|---|
| **Kafka officially Windows support નથી કરતું** (file-locking crash) | Retention/cleaner settings (CDC setup 3.3); topics delete ન કરવા; crash → reset (CDC setup 12.6); વારંવાર થાય તો **ફક્ત Kafka ને WSL2** માં ખસેડો |
| **RAM 9.9 GB** | Heap limits, Docker/OpenMetadata બંધ, Spark `local[*]` (વિભાગ 5) |
| Station data stale (MySQL CDC બંધ) | Bronze `mysql_*` જૂના છે. Station માં ફેરફાર silver સુધી નહીં પહોંચે. જરૂર પડે તો MySQL CDC ફરી ચાલુ કરવો અથવા Spark માં MySQL JDBC read |
| Mongo replica set ન હોય | CDC નહીં ચાલે → CDC setup 2.3/2.4 |
| Bronze schema અને file schema અલગ | Code phase માં bronze ના actual documents પરથી schema |
| Version mismatch | Spark 3.5.1 / Scala 2.12 / Java 17 બધે; Mongo Spark connector `_2.12` |

---

## 12. Implementation Prompts (2)

**કેવી રીતે વાપરવા:** પહેલા Prompt 1 (અથવા CDC setup જાતે કરો). CDC setup નો વિભાગ 10 (test) ✅ થાય **પછી જ** Prompt 2.

> **નોંધ (2026-10-28):** Prompt 1 ને નવા setup (MongoDB Source Connector + subscriber, Debezium વગર) પ્રમાણે બદલ્યો છે. Prompt 2 થી બનેલો application code પહેલેથી છે, અને નવા bronze format (`weather_observations` + metadata) પ્રમાણે update થયેલો છે.

---

### Prompt 1 – Installation અને Configuration (બધું infrastructure)
```
Read docs/MEDALLION_PIPELINE_PLAN_GU.md, docs/MONGO_CDC_COMPLETE_SETUP_GU.md and
docs/MEDALLION_SETUP_GUIDE_GU.md.
Help me complete the whole infrastructure setup on my Windows 11 machine, natively (no Docker):
- MEDALLION_SETUP_GUIDE_GU.md parts A and B: MongoDB Database Tools, importing data1.json into
  de_source_mongo.weather_observations; MySQL databases de_source_mysql, de_silver, de_gold,
  source tables + seed data.
- MONGO_CDC_COMPLETE_SETUP_GU.md sections 1 to 10 in order: JDK 17, Maven, MongoDB 8.x (service,
  single-node replica set rs0, WiredTiger cache 0.5 GB), Kafka 4.3.1 in KRaft mode under C:\kafka with
  the Windows-safe settings, Kafka Connect with the MongoDB Kafka Connector 3.1.2 plugin, topics
  de_source_mongo (3 partitions) and de_source_mongo.dlq, the source connector mongo-cdc-db-source,
  and the mongo-cdc-subscriber project (build and run).
Rules:
- Before each part, check what is already installed/configured on my machine and skip what is done.
- Steps that need Administrator rights or GUI installers: tell me exactly what to do and wait for me.
- Never delete or overwrite existing data without asking (the old 'dataengineering' DB stays).
- Never delete Kafka topics on Windows (it crashes the broker).
- Keep RAM low (machine has ~10 GB): Kafka heap 512m, Connect 768m; ask me to stop Docker Desktop.
- After each part run the guide's verification and show me the result.
- Finish with MONGO_CDC_COMPLETE_SETUP_GU.md section 10 (insert/update/replace/delete and a new
  collection appear correctly in de_bronze with the metadata fields, counts match, DLQ empty).
- If anything in a guide is wrong or outdated, fix it in that guide (Gujarati) and tell me what
  changed. Report honestly anything that did not work.
```

### Prompt 2 – Application Code (Bronze → Silver → Gold)
```
Read docs/MEDALLION_PIPELINE_PLAN_GU.md (sections 6.2, 7, 8, 9) and docs/SPARK_PERFORMANCE_TUNING.md.
Infrastructure is ready (CDC setup section 10 passed). Refactor the Spring Boot + Spark project into the
medallion pipeline. Legacy code does not need to be kept.

1. Cleanup: remove the old file→MySQL pipeline (WeatherEtlJob, JPA entities, ddl-auto, sql/schema.sql,
   etl.source-file settings). Ask me before removing the Docker 'prod' profile files.
2. Dependencies: mongo-spark-connector_2.12 (latest 10.x compatible with Spark 3.5.1, verify),
   Flyway (+ flyway-mysql if needed for the managed version), keep current version-alignment rules.
3. Config: bronze Mongo URI (mongodb://localhost:27017/?replicaSet=rs0, db de_bronze), de_silver and
   de_gold JDBC URLs (root user from current properties), pipeline.interval-ms,
   pipeline.safety-lag-minutes, parallelism settings; dev profile spark.jars for all Mongo + MySQL jars
   so executors get them.
4. Flyway (two instances at startup): de_silver = curated tables (weather_observation + 5 child tables
   as today, dim_station), staging tables, etl_watermark, etl_job_run; de_gold = daily_station_weather,
   monthly_region_weather, station_data_quality.
5. Common services: WatermarkService, JobRunAuditService, JdbcUpsertWriter (Spark writes staging,
   then INSERT ... SELECT ... ON DUPLICATE KEY UPDATE in one transaction, MySQL 8 alias syntax).
6. Job #1 BronzeToSilverJob: FIRST inspect real documents in de_bronze and build the explicit schema
   from them (bronze has real types, no $numberX wrappers). Read with an aggregation-pipeline filter on
   _ingested_at (pushdown) between watermark and now - safety lag; flatten observations + child tables
   (bronze weather_observations = source document + _op, _deleted, _source_ts, _ingested_at);
   dim_station from mysql_station + mysql_station_owner; dedupe latest per key;
   _deleted / __deleted → is_deleted;
   add bronze_ingested_at, silver_loaded_at, batch_id; parent before children; watermark + audit only
   after success.
7. Job #2 SilverToGoldJob: changed-keys recompute ((station_code, obs_date), (region, month)) from
   silver rows with silver_loaded_at > watermark; ignore sentinel values (999.9, 9999, 99999) and
   is_deleted rows; upsert gold; watermark + audit.
8. PipelineScheduler: Job #1 then Job #2 every pipeline.interval-ms; skip Job #2 if Job #1 fails;
   no overlaps. Update .vscode/launch.json (default + dev).
9. Tests on my machine (default profile local[*]; my RAM is ~10 GB so do not start the Spark cluster
   while Kafka runs unless I agree): first run loads all; second run reads 0 rows; insert/update/delete
   one record in the SOURCE MongoDB → only that record flows through silver and only affected gold rows
   change; gold matches a direct SQL check on silver. Show etl_job_run rows.
10. Docs (Gujarati): docs/MEDALLION_CODE_GUIDE_GU.md explaining the code structure, how each job and
    the incremental mechanism work, how to backfill from a watermark and how to add a new source table
    end to end; update other docs that mention the removed pipeline.
Keep code simple and readable like the current code. Report honestly anything that did not work.
Optional (only if I ask later): OpenLineage → OpenMetadata lineage for the two jobs.
```
