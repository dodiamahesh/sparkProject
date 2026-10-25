# Medallion Data Pipeline – Plan (ગુજરાતી)

> **Source (MongoDB + MySQL) → Kafka Connect (Debezium CDC) → Bronze (MongoDB) → Spark → Silver (MySQL) → Spark → Gold (MySQL)**
>
> બધું **Windows 11 પર native** (Docker વગર).
>
> ✅ **Status:** Infrastructure (Prompt 1) અને Application (Prompt 2) બંને implement અને test થઈ ગયા છે. Code ની સમજૂતી: **[MEDALLION_CODE_GUIDE_GU.md](MEDALLION_CODE_GUIDE_GU.md)**
> Setup ના step-by-step instructions: **[MEDALLION_SETUP_GUIDE_GU.md](MEDALLION_SETUP_GUIDE_GU.md)**
> Tool ની પસંદગી કેમ કરી: [CDC_TOOL_COMPARISON_GU.md](CDC_TOOL_COMPARISON_GU.md)

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
| 2 | **Bronze / Raw** | MongoDB (1 database, ઘણા collections) | Stage 1 માંથી **as-is** | **Real-time CDC**: Kafka Connect + Debezium |
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
│         │ change streams                                         │ binlog                       │
└─────────┼────────────────────────────────────────────────────────┼─────────────────────────────┘
          │                                                        │
┌─────────▼──────────────── KAFKA PLATFORM (native Windows) ───────▼─────────────────────────────┐
│  Kafka Connect (:8083)                                                                          │
│    ├─ source-mongo  : Debezium MongoDB  ──► topic src.mongo.de_source_mongo.weather_observations│
│    ├─ source-mysql  : Debezium MySQL    ──► topics src.mysql.de_source_mysql.station / ..._owner │
│    └─ sink-bronze   : MongoDB Sink      ◄── all src.* topics  (upsert + _ingested_at + DLQ)     │
│  Kafka broker (KRaft, :9092)                                                                    │
└───────────────────────────────────────────────────────┬─────────────────────────────────────────┘
                                                        │
┌───────────────────────────── STAGE 2 : BRONZE (MongoDB) ───────────────────────────────────────┐
│  DB: de_bronze                                                                                  │
│    mongo_weather_observations, mysql_station, mysql_station_owner                               │
│    mysql_*: original row + __op, __source_ts_ms, __deleted, _ingested_at                        │
│    mongo_*: Debezium event as-is: op, after (original JSON), source.ts_ms, _ingested_at         │
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
| **MongoDB Community Server** | 8.0.x | Source + Bronze. **Replica set** (CDC માટે ફરજિયાત) |
| **MySQL** | 8.0.30 (હાલનું) | Source + Silver + Gold. Binlog `ROW`/`FULL` પહેલેથી ✅ |
| **Apache Kafka** (KRaft) | 4.3.1 | Event backbone. ZooKeeper નથી |
| **Kafka Connect** (Kafka સાથે આવે છે) | 4.3.1 | Connectors ચલાવે (code વગર, JSON config) |
| **Debezium MySQL connector** | 3.7.0.Final | MySQL binlog → Kafka (CDC) |
| **Debezium MongoDB connector** | 3.7.0.Final | Change streams → Kafka (CDC) |
| **MongoDB Kafka Connector (sink)** | 3.1.2 | Kafka → Bronze MongoDB (upsert) |
| **SMTs** | – | Envelope કાઢી as-is data રાખે + metadata + soft delete + topic→collection routing |
| **Apache Spark** | 3.5.1 (હાલનું) | Bronze→Silver, Silver→Gold |
| **MongoDB Spark Connector** | 10.x (`_2.12`) | Spark માંથી Bronze વાંચવા (filter pushdown) |
| **MySQL Connector/J** | (હાલનું) | Spark ↔ MySQL |
| **Flyway** | Spring Boot managed | Silver/Gold tables programmatically (SQL migrations) |
| **Spring Boot** | 2.7 (હાલનું) | Scheduler + config + profiles |

**CDC tool:** Debezium (બંને sources માટે). MySQL ને Debezium **officially support** કરે છે, એટલે EPAS વાળું compatibility જોખમ હવે રહ્યું નથી. ✅

---

## 5. Software List

**બધું Windows native.** Docker ફક્ત optional OpenMetadata માટે.

| Software | Status |
|---|---|
| JDK 17, Maven, VS Code | ✅ પહેલેથી છે |
| Apache Spark 3.5.1 + winutils | ✅ પહેલેથી છે |
| MySQL 8.0.30 (binlog ready) | ✅ પહેલેથી છે |
| **MongoDB Community Server 8.0** (service) | ⬜ install |
| **mongosh**, **MongoDB Database Tools** (`mongoimport`), Compass (optional) | ⬜ install |
| **Apache Kafka 4.3.1** (Kafka Connect સાથે) | ⬜ install |
| **Debezium MySQL + MongoDB plugins 3.7.0** | ⬜ download |
| **MongoDB Kafka Connector 3.1.2** | ⬜ download |
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
 Mongo de_source_mongo.*  ──(Debezium Mongo)──► src.mongo.de_source_mongo.<coll>   ─┐
 MySQL de_source_mysql.*  ──(Debezium MySQL)──► src.mysql.de_source_mysql.<table>  ─┼─(Mongo Sink)─► de_bronze.<mongo|mysql>_<name>
                                                                                     ┘
```

### 6.2 Bronze document = as-is + metadata (implementation પછી ચકાસેલું)
**`mysql_*` collections** (Debezium unwrap SMT):

| Field | અર્થ |
|---|---|
| original columns | Row જેમ છે તેમ. DECIMAL string તરીકે, DATETIME epoch ms તરીકે |
| `__op` | `r` = snapshot, `c` = insert, `u` = update, `d` = delete |
| `__source_ts_ms` | Source માં change થયાનો time |
| `__deleted` | `"true"` = source માં delete થયું (soft delete) |

**`mongo_*` collections** (SMT વગર, આખો Debezium event):

| Field | અર્થ |
|---|---|
| `after` | **મૂળ document નું ચોક્કસ JSON (string)**. Delete વખતે `null` |
| `op` | `r` / `c` / `u` / `d` |
| `source.ts_ms` | Source માં change થયાનો time |

> Mongo માં unwrap SMT કેમ નહીં: array માં int અને double ભેગા હોય (દા.ત. `[-172, 79.8]`) તો તે fail થાય છે. વિગત: setup guide E2.

**બંને માટે સરખું:**

| Field | ક્યાંથી | અર્થ |
|---|---|---|
| `_ingested_at` | Sink SMT | Kafka message time (epoch ms). **Job #1 નો watermark** |
| `_id` | Sink | Source નો primary key. **Upsert key** |

### 6.3 Kafka Connect શરૂ થાય તે પહેલાનો (જૂનો) data
**Debezium `snapshot.mode=initial`** આ સંભાળે છે:
1. Connector source નો **હાલનો log position નોંધે** (MySQL binlog position / Mongo resume token).
2. **હાલનો બધો data** વાંચે અને bronze માં મોકલે (`__op = "r"`).
3. પછી નોંધેલા position થી **live changes** વાંચે.

➡️ Snapshot દરમિયાન આવેલા changes પણ ગુમ નથી થતા. Restart પર snapshot ફરી નથી થતો (offsets સાચવાય છે). Duplicate events આવે તો પણ bronze upsert ને કારણે duplicate rows નથી બનતી.

**પછીથી નવું table ઉમેરવું:** live changes આપોઆપ આવે છે. જૂનો data લાવવા **incremental snapshot** (signal table) વાપરવો, અથવા learning માટે connector ને નવા નામથી ફરી register કરવો. (Setup guide ભાગ F)

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
              __deleted → is_deleted
5. Load: Spark → staging table → INSERT ... SELECT ... ON DUPLICATE KEY UPDATE   (upsert, 1 transaction)
6. Success → watermark = wm_to, etl_job_run = SUCCESS
   Failure → watermark બદલાય નહીં → next run એ જ window ફરી લે
```

| Mechanism | કઈ સમસ્યા ઉકેલે |
|---|---|
| **Watermark** | ફક્ત નવો data. જૂનો ફરી process નહીં |
| **Safety lag** | Sink મોડો લખે તો પણ record miss ન થાય |
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
| 2 | `de_bronze.mongo_weather_observations`, `de_bronze.mysql_station`, `de_bronze.mysql_station_owner` |
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
| `kafka-connect/*.json` | 3 connector configs (setup guide માંથી) |
| `docs/` | Code guide (ગુજરાતી) |

**Bronze નો data file કરતાં અલગ દેખાશે.** હવે numbers સાચા types માં હશે, `{"$numberDouble": ...}` wrappers નહીં હોય. તેથી `WeatherSchema`/`JsonCols` ને **bronze ના actual documents જોઈને** બદલવા પડશે.

**Cluster માટે jars (પહેલા શીખેલો નિયમ):** Executors Mongo વાંચે છે અને MySQL માં લખે છે. તેથી `mongo-spark-connector`, `mongodb-driver-sync`, `mongodb-driver-core`, `bson`, `mysql-connector-j` ની jars `spark.jars` થી executors સુધી પહોંચાડવી પડશે.

---

## 10. Implementation ક્રમ

| # | કામ | કેવી રીતે | પૂરું થયાની નિશાની |
|---|---|---|---|
| **1** | Installation + configuration: MongoDB, MySQL setup, Kafka, Connect, connectors, snapshot, CDC test | **[Setup guide](MEDALLION_SETUP_GUIDE_GU.md)** જાતે, અથવા **Prompt 1** થી | Setup guide ભાગ G ✅ |
| **2** | Application: refactor, Flyway, Job #1, Job #2, scheduler, profiles, tests | **Prompt 2** | Source change → થોડી મિનિટમાં gold માં |

---

## 11. Risks

| Risk | ઉકેલ |
|---|---|
| **Kafka officially Windows support નથી કરતું** (file-locking crash) | Retention/cleaner settings (guide C3); crash → reset (guide H3); વારંવાર થાય તો **ફક્ત Kafka ને WSL2** માં ખસેડો |
| **RAM 9.9 GB** | Heap limits, Docker/OpenMetadata બંધ, Spark `local[*]` (વિભાગ 5) |
| Binlog expiry (30 દિવસ) | Connector લાંબો બંધ રહે તો નવો snapshot |
| Mongo replica set ન હોય | CDC નહીં ચાલે → guide A3/A4 |
| Bronze schema અને file schema અલગ | Code phase માં bronze ના actual documents પરથી schema |
| Version mismatch | Spark 3.5.1 / Scala 2.12 / Java 17 બધે; Mongo Spark connector `_2.12` |

---

## 12. Implementation Prompts (2)

**કેવી રીતે વાપરવા:** પહેલા Prompt 1 (અથવા setup guide જાતે કરો). Setup guide નો ભાગ G ✅ થાય **પછી જ** Prompt 2.

---

### Prompt 1 – Installation અને Configuration (બધું infrastructure)
```
Read docs/MEDALLION_PIPELINE_PLAN_GU.md and docs/MEDALLION_SETUP_GUIDE_GU.md.
Help me complete the whole infrastructure setup on my Windows 11 machine, natively (no Docker),
following MEDALLION_SETUP_GUIDE_GU.md parts A to G in order:
MongoDB 8.0 (service, single-node replica set rs0, WiredTiger cache 0.5 GB), mongosh, Database Tools,
importing data1.json into de_source_mongo.weather_observations; MySQL databases de_source_mysql,
de_silver, de_gold, source tables + seed data, debezium user; Kafka 4.3.1 in KRaft mode under
C:\kafka with the Windows-safe settings; Kafka Connect with Debezium 3.7.0 (MySQL, MongoDB) and
MongoDB Kafka Connector 3.1.2 plugins; save the 3 connector configs into kafka-connect/ in the
project, add kafka-connect/register-connectors.ps1 (create/update, status, delete) and register them.
Rules:
- Before each part, check what is already installed/configured on my machine and skip what is done.
- Steps that need Administrator rights or GUI installers: tell me exactly what to do and wait for me.
- Never delete or overwrite existing data without asking (the old 'dataengineering' DB stays).
- Keep RAM low (machine has ~10 GB): Kafka heap 512m, Connect 768m; ask me to stop Docker Desktop.
- After each part run the guide's verification and show me the result.
- Finish with part F (initial snapshot counts in de_bronze match the sources) and part G
  (insert/update/delete in both sources appear correctly in bronze, DLQ empty).
- If anything in the guide is wrong or outdated, fix it in MEDALLION_SETUP_GUIDE_GU.md (Gujarati)
  and tell me what changed. Report honestly anything that did not work.
```

### Prompt 2 – Application Code (Bronze → Silver → Gold)
```
Read docs/MEDALLION_PIPELINE_PLAN_GU.md (sections 6.2, 7, 8, 9) and docs/SPARK_PERFORMANCE_TUNING.md.
Infrastructure is ready (setup guide part G passed). Refactor the Spring Boot + Spark project into the
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
   _ingested_at (pushdown) between watermark and now - safety lag; flatten observations + child tables;
   dim_station from mysql_station + mysql_station_owner; dedupe latest per key; __deleted → is_deleted;
   add bronze_ingested_at, silver_loaded_at, batch_id; parent before children; watermark + audit only
   after success.
7. Job #2 SilverToGoldJob: changed-keys recompute ((station_code, obs_date), (region, month)) from
   silver rows with silver_loaded_at > watermark; ignore sentinel values (999.9, 9999, 99999) and
   is_deleted rows; upsert gold; watermark + audit.
8. PipelineScheduler: Job #1 then Job #2 every pipeline.interval-ms; skip Job #2 if Job #1 fails;
   no overlaps. Update .vscode/launch.json (default + dev).
9. Tests on my machine (default profile local[*]; my RAM is ~10 GB so do not start the Spark cluster
   while Kafka runs unless I agree): first run loads all; second run reads 0 rows; insert/update/delete
   one record in each SOURCE DB → only that record flows through silver and only affected gold rows
   change; gold matches a direct SQL check on silver. Show etl_job_run rows.
10. Docs (Gujarati): docs/MEDALLION_CODE_GUIDE_GU.md explaining the code structure, how each job and
    the incremental mechanism work, how to backfill from a watermark and how to add a new source table
    end to end; update other docs that mention the removed pipeline.
Keep code simple and readable like the current code. Report honestly anything that did not work.
Optional (only if I ask later): OpenLineage → OpenMetadata lineage for the two jobs.
```
