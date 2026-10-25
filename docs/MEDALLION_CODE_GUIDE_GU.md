# Medallion Pipeline – Code Guide (ગુજરાતી)

> **Bronze (MongoDB) → Job #1 → Silver (MySQL) → Job #2 → Gold (MySQL)**: code કેવી રીતે કામ કરે છે, validation અને duplicates કેવી રીતે સંભાળાય છે, કેવી રીતે ચલાવવું અને ચકાસવું.
> Infrastructure (Source → Kafka → Bronze): [MEDALLION_SETUP_GUIDE_GU.md](MEDALLION_SETUP_GUIDE_GU.md) · Plan: [MEDALLION_PIPELINE_PLAN_GU.md](MEDALLION_PIPELINE_PLAN_GU.md)

---

## અનુક્રમણિકા
1. [એક નજરમાં](#1-એક-નજરમાં)
2. [Code Structure](#2-code-structure)
3. [Configuration](#3-configuration)
4. [Job #1: Bronze → Silver](#4-job-1-bronze--silver)
5. [Validation અને Duplicates (Rejects)](#5-validation-અને-duplicates-rejects)
6. [Job #2: Silver → Gold](#6-job-2-silver--gold)
7. [Incremental Mechanism: જૂનો data ફરી કેમ નથી આવતો](#7-incremental-mechanism-જૂનો-data-ફરી-કેમ-નથી-આવતો)
8. [Tables](#8-tables)
9. [ચલાવવું](#9-ચલાવવું)
10. [ચકાસણી માટેની SQL (Cheat Sheet)](#10-ચકાસણી-માટેની-sql-cheat-sheet)
11. [Backfill / Reprocess](#11-backfill--reprocess)
12. [નવું Source Table ઉમેરવું (end to end)](#12-નવું-source-table-ઉમેરવું-end-to-end)
13. [Test Results](#13-test-results)
14. [જાણીતી મર્યાદાઓ](#14-જાણીતી-મર્યાદાઓ)
15. [સામાન્ય સમસ્યાઓ](#15-સામાન્ય-સમસ્યાઓ)

---

## 1. એક નજરમાં

```
 de_bronze (MongoDB)                       de_silver (MySQL)                         de_gold (MySQL)
 ├ mongo_weather_observations ─┐           ├ weather_observation (+5 child tables)    ├ daily_station_weather
 ├ mysql_station ──────────────┼─ Job #1 ─►├ dim_station                    ─ Job #2 ─►├ station_data_quality
 └ mysql_station_owner ────────┘           ├ etl_rejected_record  ← validation/dups    └ monthly_region_weather
                                           ├ etl_watermark        ← ક્યાં સુધી process કર્યું
                                           └ etl_job_run          ← દરેક run નો હિસાબ

 PipelineScheduler:  Job #1 ──success──► Job #2   (pipeline.interval-ms પછી ફરી, runs overlap નથી થતા)
```

| Job | Input | Output | Incremental key |
|---|---|---|---|
| #1 `bronze_to_silver` | Bronze ના **નવા** documents | Silver upsert + soft delete + rejects | `_ingested_at` (bronze) |
| #2 `silver_to_gold` | Silver ના **બદલાયેલા groups** | Gold groups replace | `silver_loaded_at` (silver) |

---

## 2. Code Structure

```
src/main/java/com/de/spark/
├─ SparkEtlApplication.java          Spring Boot main (+ @EnableScheduling)
├─ config/
│   ├─ SparkConfig.java              SparkSession (spark.* properties; local / cluster)
│   ├─ PipelineProperties.java       બધા pipeline.* settings + Spark JDBC helpers
│   └─ DatabaseConfig.java           de_silver અને de_gold: DataSource, JdbcTemplate, Transaction, Flyway migrate
├─ common/
│   ├─ BronzeReader.java             Bronze collections વાંચે (MongoDB Spark Connector, filter pushdown)
│   ├─ WatermarkService.java         etl_watermark વાંચે/લખે
│   ├─ JobRunAuditService.java       etl_job_run: RUNNING → SUCCESS / FAILED / SKIPPED
│   └─ JdbcUpsertWriter.java         Spark → staging table, અને upsert/insert SQL બનાવે
├─ pipeline/
│   ├─ bronze2silver/
│   │   ├─ BronzeSchemas.java        Bronze documents ના explicit schemas
│   │   ├─ Rejects.java              Reject reasons + reject row બનાવવાની રીત
│   │   ├─ ObservationTransformer.java  parse → validate → duplicates → flatten (observations)
│   │   ├─ StationTransformer.java   dim_station (station + owner), validation
│   │   └─ BronzeToSilverJob.java    Job #1: extract → transform → stage → merge (1 transaction)
│   └─ silver2gold/
│       └─ SilverToGoldJob.java      Job #2: changed groups → recompute → replace (1 transaction)
└─ scheduler/
    └─ PipelineScheduler.java        Job #1 પછી Job #2; Job #1 fail → Job #2 SKIPPED

src/main/resources/
├─ application.properties            default profile (local[*])
├─ application-dev.properties        Windows standalone cluster (+ spark.jars for executors)
└─ db/
    ├─ silver/V1__silver_init.sql    Flyway: silver tables + control + staging
    └─ gold/V1__gold_init.sql        Flyway: gold tables + staging

kafka-connect/                       source-mysql.json, source-mongo.json, sink-bronze.json, register-connectors.ps1
```

**જૂનો pipeline દૂર થયો છે:** `WeatherEtlJob`, JPA entities, `ddl-auto`, `sql/schema.sql`, `etl.*` settings. Tables હવે **Flyway** બનાવે છે.

---

## 3. Configuration

`application.properties`:

| Property | Default | અર્થ |
|---|---|---|
| `pipeline.mongo-uri` | `mongodb://localhost:27017/?replicaSet=rs0` | Bronze MongoDB |
| `pipeline.bronze-database` | `de_bronze` | |
| `pipeline.silver-jdbc-url` / `gold-jdbc-url` | `jdbc:mysql://localhost:3306/de_silver` / `de_gold` | |
| `pipeline.db-username` / `db-password` | `root` / `Root@123` | |
| `pipeline.interval-ms` | `120000` | એક run પૂરો થયા પછી આગલા run સુધીનો વિરામ |
| `pipeline.safety-lag-minutes` | `2` | Job #1 bronze ને ફક્ત `now − 2 min` સુધી વાંચે |
| `pipeline.partitions` | `4` | Parallel tasks અને parallel MySQL connections |
| `pipeline.jdbc-batch-size` | `5000` | Rows per INSERT batch |
| `spark.master` | `local[*]` | `dev` profile માં `spark://localhost:7077` |

**Versions (`pom.xml`):** `mongo-spark-connector_2.12:10.7.0` (Spark 3.2–3.5 support). MongoDB driver ને `<mongodb.version>5.1.4</mongodb.version>` થી pin કર્યો છે. નહીંતર Spring Boot 2.7 driver ને 4.6.1 પર લઈ જાય, જે connector સાથે ચાલતો નથી. Jackson માટે પણ આવો જ નિયમ પહેલેથી છે.

---

## 4. Job #1: Bronze → Silver

```
1. window      from = etl_watermark(bronze_to_silver),  to = now − safety lag
2. EXTRACT     3 bronze collections, ફક્ત  from < _ingested_at <= to
               ($match aggregation pipeline → MongoDB પોતે filter કરે છે = pushdown)
3. TRANSFORM   ObservationTransformer:  parse "after" JSON → deletes → VALIDATE → DUPLICATES → flatten
               StationTransformer:      affected stations → dim rows (owner/region) → VALIDATE
4. STAGE       Spark → stg_* tables (parallel, TRUNCATE + INSERT)
5. MERGE       ONE MySQL transaction:
                 a. dim_station          upsert   (INSERT ... ON DUPLICATE KEY UPDATE)
                 b. weather_observation  upsert
                 c. 5 child tables       replace  (બદલાયેલા observations ના જૂના child rows delete → નવા insert)
                 d. deleted ids          soft delete (is_deleted = 1)
                 e. etl_rejected_record  insert
                 f. etl_watermark        = to
6. AUDIT       etl_job_run = SUCCESS (rows read / written / rejected)
```

### Mongo event માંથી observation
Bronze માં Mongo document `after` field માં JSON string તરીકે છે. `from_json(after, BronzeSchemas.OBSERVATION)` તેને columns માં ફેરવે છે:
- `ts: {"$date": 447343200000}` → `obs_ts` (timestamp)
- `_id: {"$oid": "5553..."}` → `id`
- `op = "d"` (delete) → `after` null હોય છે, તેથી id bronze `_id.id` માંથી લેવાય છે → `is_deleted = 1`

### Station dimension
`mysql_station` **કે** `mysql_station_owner` માં ફેરફાર થાય, તો તે stations ની dim row આખા bronze master data માંથી **ફરી બને છે** (owner = સૌથી નવો live owner). તેથી owner/region બદલાય કે delete થાય તો પણ dim સાચી રહે છે.

### Upsert SQL (ઉદાહરણ)
`JdbcUpsertWriter.upsertSql(...)` આવું SQL બનાવે છે (MySQL 8 alias syntax):
```sql
INSERT INTO weather_observation (id, st, obs_ts, ..., silver_loaded_at, batch_id)
SELECT * FROM (SELECT id, st, obs_ts, ..., ? AS silver_loaded_at, ? AS batch_id FROM stg_weather_observation) AS s
ON DUPLICATE KEY UPDATE st = s.st, obs_ts = s.obs_ts, ..., silver_loaded_at = s.silver_loaded_at, batch_id = s.batch_id
```
નવી `id` → insert; હાજર `id` → update. એ જ data બે વાર આવે તો પણ **duplicate row બનતી નથી**.

---

## 5. Validation અને Duplicates (Rejects)

**નિયમ: કોઈ record ચૂપચાપ ગુમ થતો નથી.** જે record silver માં ન જાય તે `de_silver.etl_rejected_record` માં જાય છે. ત્યાં **કારણ (reason)**, **વિગત (details)** અને **મૂળ data (raw_payload)** નોંધાય છે.

### 5.1 Validation rules
**Observations** (`ObservationTransformer.validationReason()`, ઉપરથી નીચે, પહેલો fail થતો rule લાગુ પડે):

| Reason | Condition | Details ઉદાહરણ |
|---|---|---|
| `MISSING_ID` | `after` ખાલી, invalid JSON, અથવા `_id.$oid` નથી | `'after' is empty, not valid JSON or has no _id.$oid` |
| `MISSING_TIMESTAMP` | `ts.$date` નથી અથવા epoch ms નથી | `ts.$date is missing or not epoch milliseconds` |
| `MISSING_STATION` | `callLetters` null અથવા ખાલી (spaces) | `callLetters is null or blank` |
| `INVALID_COORDINATES` | lon/lat નથી, અથવા \|lon\| > 180, \|lat\| > 90 | `coordinates [lon, lat] = [200.0,95.0] (lon must be -180..180, lat -90..90)` |

**Stations** (`StationTransformer`; deleted stations validate નથી થતા):

| Reason | Condition |
|---|---|
| `MISSING_STATION_NAME` | `station_name` null અથવા ખાલી |
| `INVALID_COORDINATES` | latitude/longitude number નથી, અથવા range બહાર |

> **Sentinel values (999.9, 9999.9, 99999) reject થતા નથી.** Weather data માં તેનો અર્થ "માપ નથી" છે. Silver તેમને જેમ છે તેમ રાખે છે, અને Gold તેમને aggregates માંથી બાદ કરે છે (વિભાગ 6).

### 5.2 Duplicates: 3 પ્રકાર, 3 ઉકેલ

| # | Duplicate નો પ્રકાર | ઉદાહરણ | ઉકેલ | Reject? |
|---|---|---|---|---|
| 1 | **એ જ record ફરી આવ્યો** (Kafka retry, connector restart, એ જ window ફરી process) | id `5553...ba` બે વાર | **Upsert** (`id` primary key) → એક જ row રહે | ના, ચૂપચાપ સાચું (idempotent) |
| 2 | **એક જ batch માં: એ જ station + એ જ time, અલગ id** | `...c0` અને `...c3`: બંને PLAT, 1984-03-05 15:00Z | સૌથી નવો change (`source_ts`) રાખો, બાકીના reject | ✅ `DUPLICATE_IN_BATCH` |
| 3 | **Silver માં પહેલેથી: એ જ station + time, અલગ id** | નવો doc PLAT 15:00Z, પણ silver માં `...c0` છે | નવો record reject, જૂનો રહે | ✅ `DUPLICATE_OF_EXISTING` |

Business key = **(call_letters, obs_ts)**: એક station એક જ સમયે બે અલગ observations ન આપી શકે.
- **પ્રકાર 2** માટે: window `partitionBy(call_letters, obs_ts).orderBy(source_ts desc)` → `row_number = 1` રાખો.
- **પ્રકાર 3** માટે: batch ના time range માં silver ની `(id, call_letters, obs_ts)` વાંચીને join. Silver આખું નહીં, ફક્ત range વંચાય છે.

### 5.3 Reject થયા પછી શું?
| સ્થિતિ | પરિણામ |
|---|---|
| **નવો** record reject થયો | Silver માં આવતો જ નથી. Source માં સુધારો તો નવો CDC event આવે અને ફરી validate થાય |
| **હાજર** record નું **update** reject થયું | Silver માં **જૂનું (છેલ્લું valid) version** રહે છે |
| Delete | ક્યારેય reject થતું નથી, હંમેશા soft delete |

### 5.4 ચકાસો: કયો record કેમ reject થયો
```sql
-- કારણ પ્રમાણે સંખ્યા
SELECT reason, COUNT(*) FROM de_silver.etl_rejected_record GROUP BY reason;

-- છેલ્લા rejects: વિગત + મૂળ data
SELECT rejected_at, source_collection, record_key, reason, details, raw_payload
FROM de_silver.etl_rejected_record ORDER BY id DESC LIMIT 20;

-- કોઈ ચોક્કસ run ના rejects (run_id = etl_job_run.run_id = silver batch_id)
SELECT * FROM de_silver.etl_rejected_record WHERE run_id = '<run_id>';

-- Duplicate: rejected record અને silver માં રાખેલો record સાથે જુઓ
SELECT r.record_key AS rejected_id, r.reason, r.details, w.id AS kept_id, w.call_letters, w.obs_ts
FROM de_silver.etl_rejected_record r
JOIN de_silver.weather_observation w
  ON w.id = SUBSTRING_INDEX(SUBSTRING_INDEX(r.details, ' as id ', -1), ' ', 1)   -- details માંથી "kept" id
WHERE r.reason LIKE 'DUPLICATE%';
```
`raw_payload` માં bronze નું **મૂળ JSON** છે, એટલે ખરેખર શું આવ્યું હતું તે જાતે જોઈ શકાય છે.

### 5.5 નવો rule ઉમેરવો
1. `Rejects.java` માં reason constant ઉમેરો.
2. `ObservationTransformer.validationReason()` માં `.when(<condition>, lit(NEW_REASON))` ઉમેરો.
3. `validationDetails()` માં message ઉમેરો.

Pipeline નો બાકીનો code બદલવાની જરૂર નથી.

---

## 6. Job #2: Silver → Gold

**Aggregates ને ફક્ત નવી rows ઉમેરીને update ન કરી શકાય** (દા.ત. average). તેથી **changed-keys recompute** pattern વપરાય છે:

```
1. touched groups (station, day):
     silver_loaded_at > watermark  વાળી observations   +   બદલાયેલા stations ની બધી observations
2. તે groups ની ALL live silver rows વાંચો           (MySQL key list થી filter કરે = pushdown)
3. Spark માં recompute: daily, data quality, monthly region
4. ONE gold transaction:
     DELETE  તે groups ની જૂની gold rows
     INSERT  નવી ગણેલી rows
   → જે group ની બધી rows delete થઈ, તે gold માંથી પણ જતો રહે છે
5. watermark = now,  audit = SUCCESS
```

**Monthly region:** touched **months** ના **બધા regions** ફરી ગણાય છે. Station નો region બદલાય તો **જૂના** region ની સંખ્યા પણ સુધરે છે. (Test: VC81 NORTH_AMERICA → EUROPE, અને NORTH_AMERICA row ગઈ ✅)

**Sentinels:** `air_temperature`, `dew_point`, `wind_speed_rate` ≥ 999.9 અને `pressure` ≥ 9999.9 એટલે "not measured". Aggregates માં NULL ગણાય છે, અને `station_data_quality` માં **missing** તરીકે ગણાય છે.

| Gold table | Key | Columns |
|---|---|---|
| `daily_station_weather` | station_code, obs_date | obs_count, avg/min/max air temp, avg dew point, avg pressure, max wind |
| `station_data_quality` | station_code, obs_date | total_obs, missing_* (4), complete_obs_pct |
| `monthly_region_weather` | region, obs_month | station_count, obs_count, avg/min/max temp, avg pressure |

---

## 7. Incremental Mechanism: જૂનો data ફરી કેમ નથી આવતો

| Mechanism | ક્યાં | શું સુરક્ષિત કરે |
|---|---|---|
| **Watermark** | `etl_watermark` | દરેક run ફક્ત watermark પછીનો data વાંચે |
| **Safety lag** (2 min) | Job #1 | Kafka sink મોડો લખે તો પણ record watermark ની પાછળ રહી કાયમ miss ન થાય |
| **Pushdown filter** | `BronzeReader` | MongoDB જ filter કરે છે; Spark આખું collection વાંચતું નથી |
| **Upsert / replace** | merge SQL | એ જ data ફરી આવે તો પણ duplicates નહીં (idempotent) |
| **એક જ transaction** | Job #1 merge | Data + rejects + watermark: બધું થાય અથવા કંઈ જ નહીં |
| **Audit** | `etl_job_run` | દરેક run: window, rows, status, error |

### Failure થાય તો?
| ક્યાં fail | પરિણામ |
|---|---|
| Job #1 extract/transform/stage દરમિયાન | Merge થયો જ નથી → silver unchanged, watermark unchanged → આગલો run એ જ window ફરી લે. `FAILED` + error audit માં |
| Job #1 merge transaction માં | Rollback → ઉપર જેવું જ |
| Job #1 fail | Job #2 `SKIPPED` (audit માં નોંધ) |
| Job #2 gold commit પછી, watermark પહેલા | આગલો run એ જ groups ફરી ગણે → પરિણામ સરખું (idempotent) |

---

## 8. Tables

**de_silver**

| Table | Key | નોંધ |
|---|---|---|
| `weather_observation` | `id` (ObjectId) | + `is_deleted`, `source_ts`, `bronze_ingested_at`, `silver_loaded_at`, `batch_id` |
| `observation_section`, `past_weather_observation`, `present_weather_observation` | auto id, FK `observation_id` | 1:N |
| `sky_condition_observation`, `marine_observation` | `observation_id` (PK = FK) | 1:1 |
| `dim_station` | `station_code` | owner_name, region, is_deleted |
| `etl_watermark` | `job_name` | `watermark_value` (epoch ms) |
| `etl_job_run` | `run_id` | status, window, rows, error |
| `etl_rejected_record` | auto id | run_id, reason, details, raw_payload |
| `stg_*` | – | Staging. દરેક run માં ખાલી થઈ ફરી ભરાય છે. તેને હાથ ન લગાડો |

**de_gold:** `daily_station_weather`, `station_data_quality`, `monthly_region_weather` + `stg_*`.

`batch_id` = `etl_job_run.run_id`. તેથી કોઈ પણ row **કયા run** માં લખાઈ તે શોધી શકાય છે.

> 📝 `watermark_value` નામ કેમ: પહેલા `last_value` હતું, પણ તે **MySQL 8 નો reserved word** છે (window function), અને Flyway migration fail થયું હતું.

---

## 9. ચલાવવું

### 9.1 Start ક્રમ
```
1. MySQL, MongoDB (Windows services)
2. Kafka → Kafka Connect   (setup guide ભાગ H)
3. Pipeline app             ↓
```

### 9.2 Pipeline app
| રીત | કેવી રીતે |
|---|---|
| VS Code (default, `local[*]`) | `Ctrl+Shift+D` → **"Pipeline - default (local[*])"** → F5 |
| VS Code (dev cluster) | **"Pipeline - dev (Windows standalone cluster)"**. પહેલા `mvn package` ચલાવો (executor jars), અને master + 2 workers ચાલુ હોવા જોઈએ |
| Maven | `mvn spring-boot:run` |

App શરૂ થાય ત્યારે **Flyway** silver/gold tables બનાવે છે (પહેલી વાર) અથવા નવા migrations લાગુ કરે છે. 5 સેકન્ડ પછી પહેલો pipeline run થાય છે.

### 9.3 Log માં શું દેખાય
```
[bronze_to_silver] started, window _ingested_at in (0, 1792870481287]
[bronze_to_silver] finished: read=30, written=24, rejected=1
[silver_to_gold] started, window silver_loaded_at in (1970-01-01 05:30:00.000, 2026-10-25 01:04:52.854]
[silver_to_gold] finished: groups=16, silver rows read=38, gold rows written=36
```

> ⚠️ **RAM (~10 GB machine):** Kafka stack ચાલુ હોય ત્યારે pipeline ને `default` profile માં ચલાવો. Dev cluster માટે આશરે 3 GB વધુ RAM જોઈએ.

---

## 10. ચકાસણી માટેની SQL (Cheat Sheet)

```sql
-- છેલ્લા runs
SELECT job_name, status, started_at, rows_read, rows_written, rows_rejected, error_message
FROM de_silver.etl_job_run ORDER BY started_at DESC LIMIT 10;

-- watermarks
SELECT job_name, watermark_value, FROM_UNIXTIME(watermark_value / 1000) AS as_time FROM de_silver.etl_watermark;

-- silver counts
SELECT COUNT(*) total, SUM(is_deleted) deleted FROM de_silver.weather_observation;

-- gold vs silver (0 mismatches હોવા જોઈએ)
SELECT COUNT(*) AS groups,
       SUM(NOT (g.obs_count <=> d.obs_count AND g.avg_air_temp <=> d.avg_air_temp)) AS mismatches
FROM (SELECT call_letters station_code, DATE(obs_ts) obs_date, COUNT(*) obs_count,
             ROUND(AVG(CASE WHEN air_temperature < 999.9 THEN air_temperature END), 2) avg_air_temp
      FROM de_silver.weather_observation WHERE is_deleted = 0 GROUP BY 1, 2) d
LEFT JOIN de_gold.daily_station_weather g ON g.station_code = d.station_code AND g.obs_date = d.obs_date;

-- rejects
SELECT reason, COUNT(*) FROM de_silver.etl_rejected_record GROUP BY reason;
```

---

## 11. Backfill / Reprocess

| જરૂર | શું કરવું (app બંધ કરીને) |
|---|---|
| **Gold આખું ફરી બનાવવું** | `UPDATE de_silver.etl_watermark SET watermark_value = 0 WHERE job_name = 'silver_to_gold';` |
| **Silver ને કોઈ સમયથી ફરી process કરવું** | `UPDATE de_silver.etl_watermark SET watermark_value = UNIX_TIMESTAMP('2026-10-25 00:00:00') * 1000 WHERE job_name = 'bronze_to_silver';` |
| **બધું શરૂથી** | `DELETE FROM de_silver.etl_watermark;` (silver/gold rows upsert થઈ સુધરી જશે) |

- Silver upsert થાય છે, એટલે reprocess થી **duplicates નહીં** બને.
- એ જ id silver માં હોય તો `DUPLICATE_OF_EXISTING` પણ નહીં લાગે (id સરખી છે).
- ⚠️ જે records પહેલા reject થયા હતા, તે reprocess વખતે **ફરી reject log** થશે (`etl_rejected_record` માં બીજી row, નવા `run_id` સાથે).

---

## 12. નવું Source Table ઉમેરવું (end to end)

ઉદાહરણ: MySQL `de_source_mysql.sensor`

| # | Layer | શું કરવું |
|---|---|---|
| 1 | Source | `CREATE TABLE sensor (...)` (primary key સાથે) |
| 2 | Kafka Connect | **કંઈ નહીં.** `database.include.list` આખા DB ને પકડે છે, અને sink નો `topics.regex` નવા topic ને પકડે છે. → bronze `mysql_sensor` આપોઆપ બને છે. જૂનો data જોઈએ તો setup guide F4 |
| 3 | `BronzeSchemas` | `MYSQL_SENSOR` schema ઉમેરો (bronze ના એક document ને જોઈને) |
| 4 | Flyway | `db/silver/V2__sensor.sql`: `dim_sensor` + `stg_dim_sensor`. **V1 ને ક્યારેય edit ન કરો**, નવી V2 file બનાવો |
| 5 | Transformer | `SensorTransformer`: validation + rejects (`StationTransformer` જેવો) |
| 6 | Job #1 | `readWindow("mysql_sensor", ...)` → transform → `stage` → merge માં upsert |
| 7 | Gold (જરૂર હોય તો) | `db/gold/V2__...sql` + `SilverToGoldJob` માં aggregate |
| 8 | Test | Source માં insert/update/delete → silver/gold/rejects ચકાસો (વિભાગ 10) |

---

## 13. Test Results

Machine પર `default` profile (`local[*]`) સાથે test કર્યું. Test માટે interval 20 સેકન્ડ અને lag 0 રાખ્યા હતા.

| Test | પરિણામ |
|---|---|
| પહેલો run | read **30** (20 obs + 5 stations + 5 owners), written **24** (19 obs + 5 dim), rejected **1** ✅ |
| Source data નો duplicate | `...c3` = `...c0` જેવું જ station + time → `DUPLICATE_IN_BATCH` ✅ |
| બીજો run | read **0**: જૂનો data ફરી process ન થયો ✅ |
| Mongo insert (valid) | silver + gold માં નવી row (PLAT 1984-03-06) ✅ |
| Mongo insert, `callLetters` ખાલી | `MISSING_STATION` ✅ |
| Mongo insert, coordinates `[200, 95]` | `INVALID_COORDINATES` ✅ |
| Mongo insert, silver ના record જેવું જ station + time | `DUPLICATE_OF_EXISTING` ✅ |
| Mongo update (temp 4.4 → 10.0) | silver update; gold PLAT 03-05 avg 5.23 → 6.63, max 10.0 ✅ |
| Mongo delete | silver `is_deleted = 1`; gold માંથી TFRB group ગયો ✅ |
| MySQL owner region (VC81 → EUROPE) | dim update; monthly NORTH_AMERICA ગયો, EUROPE = 3 stations ✅ |
| MySQL station, latitude 95 | `INVALID_COORDINATES` ✅ |
| ફક્ત affected gold rows બદલાઈ | દા.ત. EMIO પહેલા run ના `batch_id` સાથે જ રહ્યો ✅ |
| Gold vs direct SQL on silver | 16 groups, **0 mismatches** ✅ |
| Audit | 20 runs, બધા `SUCCESS` ✅ · Kafka DLQ: 0 messages ✅ |

**Test દરમિયાન મળેલી અને સુધારેલી સમસ્યાઓ:**
1. Connectors registered **ન હતા** (setup guide ભાગ E–G બાકી હતા) → register કર્યા.
2. `source-mongo`: unwrap SMT mixed-type array પર fail થયો → SMT કાઢ્યો (setup guide E2).
3. `sink-bronze`: renamed topic regex error → regex સુધાર્યો (setup guide E3).
4. Failed attempt ના 19 જૂના-format documents bronze માં રહી ગયા હતા → delete કર્યા.
5. Spring Boot એ MongoDB driver ને 4.6.1 પર લઈ ગયો હતો → 5.1.4 પર pin કર્યો.
6. `last_value` reserved word → `watermark_value`.

---

## 14. જાણીતી મર્યાદાઓ

| મર્યાદા | અસર | ભવિષ્યનો ઉકેલ |
|---|---|---|
| Observation નું **station કે time બદલાય** | Gold નો **જૂનો** (station, day) group ફરી ગણાતો નથી | Silver માં જૂની key રાખીને તે group પણ touched ગણવો |
| Reprocess વખતે rejects ફરી log થાય છે | `etl_rejected_record` માં એક જ record ની વધુ rows | Reject log માં (record_key, reason, payload hash) unique |
| Time zone | DATETIME columns JVM નો local time (IST) વાપરે છે | બધું UTC (JVM + JDBC `connectionTimeZone=UTC`) |
| Bronze માં ફક્ત છેલ્લું version | Silver ને વચ્ચેના changes દેખાતા નથી (history Kafka માં છે) | Bronze history collection (append-only) |
| Source schema change | નવું field `BronzeSchemas` ઉમેર્યા વગર silver માં નથી આવતું (bronze માં સચવાય છે) | Schema update + Flyway V2 |
| Dev cluster run | RAM ને કારણે **cluster પર test નથી કર્યું** (local[*] જ) | 16 GB+ machine અથવા Kafka થોડો સમય બંધ રાખીને |
| `monthly_region_weather` query | Touched months ની બધી rows વાંચે છે | મોટા data માટે month-range index/partition |

---

## 15. સામાન્ય સમસ્યાઓ

| સમસ્યા | ઉકેલ |
|---|---|
| Startup: `Migration V1__silver_init.sql failed` | SQL error જુઓ. MySQL DDL rollback નથી થતું, તેથી અધૂરી tables + Flyway history સાફ કરવી પડે (dev માં: `DROP DATABASE de_silver; CREATE DATABASE de_silver;`). **Applied migration ને edit ન કરો**, નવી V2 બનાવો |
| `Validate failed: Migrations have failed validation` | V1 file બદલાઈ ગઈ છે → નવી V2 file વાપરો |
| `NoSuchMethodError ... com.mongodb` | MongoDB driver version mismatch → `pom.xml` માં `mongodb.version` 5.1.x |
| Job #1 `read=0` જ આવે છે | Connectors RUNNING? Bronze માં નવા docs? `_ingested_at` < now − lag? (નવો change 2 મિનિટ પછી આવે છે) |
| ઘણા rejects | વિભાગ 5.4 ની SQL: `reason`, `details`, `raw_payload` જુઓ |
| Job #2 `SKIPPED` | Job #1 નો `error_message` (`etl_job_run`) જુઓ |
| `HADOOP_HOME and hadoop.home.dir are unset` | Warning છે, નુકસાન નથી. `HADOOP_HOME` સેટ કરીને નવી terminal ખોલો |
| Dev cluster: executor માં `ClassNotFoundException: com.mongodb...` | `mvn package` ચલાવ્યું? `target/executor-jars` માં 6 jars હોવા જોઈએ |
