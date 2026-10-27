# Medallion Pipeline – Installation અને Configuration Guide (Windows 11 Native, ગુજરાતી)

> આ guide માં Medallion pipeline ના **databases** નો setup છે: MongoDB (Source + Bronze) અને MySQL (Source + Silver + Gold).
>
> **Source → Kafka → Bronze (CDC) નો આખો setup હવે અલગ document માં છે:**
> **[MONGO_CDC_COMPLETE_SETUP_GU.md](MONGO_CDC_COMPLETE_SETUP_GU.md)**: Java, Maven, MongoDB replica set, Kafka, Kafka Connect, MongoDB Source Connector, Java subscriber, test, troubleshooting.
>
> Stage 3 (Silver) અને Stage 4 (Gold) નો Spark code: [MEDALLION_PIPELINE_PLAN_GU.md](MEDALLION_PIPELINE_PLAN_GU.md) વિભાગ 12.

> **ફેરફાર (2026-10-28):** Debezium (MySQL + MongoDB connectors) અને MongoDB **sink** connector હવે **વપરાતા નથી**. MongoDB → Bronze હવે *MongoDB Source Connector + એક generic topic + પોતાનો Java subscriber* થી થાય છે. MySQL નો CDC **બંધ** કર્યો છે. વિગત: [ભાગ C](#ભાગ-c--kafka-અને-cdc-source--bronze).

**Versions (Oct 2026 માં ચકાસેલા):**

| Software | Version |
|---|---|
| MongoDB Community Server | **8.x** (આ machine પર 8.3.13) |
| MySQL | 8.0.30 ✅ |
| JDK | 17 ✅ |
| Kafka, Kafka Connect, MongoDB Kafka Connector, subscriber | [MONGO_CDC_COMPLETE_SETUP_GU.md](MONGO_CDC_COMPLETE_SETUP_GU.md) વિભાગ 0.3 |

---

## અનુક્રમણિકા
- [0. શરૂ કરતા પહેલા](#0-શરૂ-કરતા-પહેલા)
- [ભાગ A – MongoDB (Source + Bronze)](#ભાગ-a--mongodb-source--bronze)
- [ભાગ B – MySQL (Source + Silver + Gold)](#ભાગ-b--mysql-source--silver--gold)
- [ભાગ C – Kafka અને CDC (Source → Bronze)](#ભાગ-c--kafka-અને-cdc-source--bronze)
- [ભાગ D – રોજનો Start / Stop ક્રમ](#ભાગ-d--રોજનો-start--stop-ક્રમ)
- [ભાગ E – સામાન્ય સમસ્યાઓ](#ભાગ-e--સામાન્ય-સમસ્યાઓ)

---

## 0. શરૂ કરતા પહેલા

### 0.1 આપણે શું બનાવીશું

```
 MongoDB (Windows service, replica set rs0, :27017)                MySQL 8 (Windows service, :3306)
   de_source_mongo  (બધા collections)                                 de_source_mysql.station, station_owner
          │ change streams                                            de_silver, de_gold  (Spark લખે છે)
          ▼
   Kafka Connect: mongo-cdc-db-source (MongoDB Source Connector)
          │
          ▼
   Kafka topic: de_source_mongo   (બધા collections ના changes, એક જ topic)
          │
          ▼
   mongo-cdc-subscriber (Java app): insert / update / soft delete
          │
          ▼
   MongoDB de_bronze.<એ જ collection નામ>   (source જેવા જ documents અને types + _op, _deleted, _source_ts, _ingested_at)
          │
          ▼
   Spark: Bronze → Silver → Gold (MySQL)
```
આ flow નો પૂરો setup: [MONGO_CDC_COMPLETE_SETUP_GU.md](MONGO_CDC_COMPLETE_SETUP_GU.md).

### 0.2 Databases ના નામ (dataset-independent)

| Stage | Engine | Database | Data કેવી રીતે આવે |
|---|---|---|---|
| 1 – Source | MongoDB | `de_source_mongo` | Application / `mongoimport` |
| 1 – Source | MySQL | `de_source_mysql` | SQL (ભાગ B) |
| 2 – Bronze | MongoDB | `de_bronze` | CDC subscriber (ભાગ C) |
| 3 – Silver | MySQL | `de_silver` | Spark |
| 4 – Gold | MySQL | `de_gold` | Spark |

Database ના નામ સામાન્ય (generic) રાખ્યા છે, એટલે dataset બદલાય તો પણ નામ બદલવા નહીં પડે. ફક્ત collections/tables dataset પ્રમાણે હશે.

> ⚠️ `de_source_mysql` નો data હવે **Kafka/Bronze માં stream થતો નથી** (MySQL CDC બંધ છે).

### 0.3 Folder layout
```
C:\kafka\                                  ← Kafka installation (CDC document)
C:\kafka\plugins\mongo-kafka-connect\      ← MongoDB Kafka Connector plugin
C:\kafka-data\                             ← Kafka નો data (topics)
D:\...\sparkProject\kafka-connect\         ← source connector JSON + register script
D:\...\sparkProject\mongo-cdc-subscriber\  ← Java subscriber (અલગ Maven project)
D:\...\sparkProject\docs\                  ← documents
```

### 0.4 RAM: તમારી machine પર 9.9 GB છે ⚠️
આ guide લખતી વખતે machine પર ફક્ત **0.7 GB free** હતી. તેથી:

| Component | RAM મર્યાદા |
|---|---|
| MongoDB | WiredTiger cache **0.5 GB** |
| Kafka broker | heap **512 MB** |
| Kafka Connect | heap **768 MB** |
| MySQL | (હાલનું) |

**સલાહ:**
1. **Docker Desktop બંધ રાખો.** તેનું `vmmem` process 2+ GB RAM લે છે, અને આ setup માં Docker ની જરૂર નથી. OpenMetadata ફક્ત જરૂર હોય ત્યારે ચાલુ કરો, અને ત્યારે બાકીનું બંધ રાખો.
2. **Spark cluster (master + 2 workers) અને Kafka stack એક સાથે ન ચલાવો.** Pipeline development વખતે Spark ને `default` profile (`local[*]`) માં ચલાવો.
3. **Setup વખતે Spark ETL app બંધ રાખો.**

### 0.5 Terminal
બધા commands **Windows PowerShell** માં ચલાવવાના છે. જ્યાં **(Admin)** લખ્યું હોય ત્યાં PowerShell ને *Run as Administrator* થી ખોલો.

---

# ભાગ A – MongoDB (Source + Bronze)

## A1. MongoDB install, replica set અને RAM મર્યાદા
MongoDB Server, mongosh, Compass નું install, `mongod.cfg` (`cacheSizeGB: 0.5`, `replSetName: rs0`) અને `rs.initiate`:
➡️ **[MONGO_CDC_COMPLETE_SETUP_GU.md](MONGO_CDC_COMPLETE_SETUP_GU.md) વિભાગ 2.1 થી 2.4**

હવેથી connection string હંમેશા આ વાપરો:
```
mongodb://localhost:27017/?replicaSet=rs0
```

## A2. MongoDB Database Tools install કરો (`mongoimport`)
| Tool | Download | કામ |
|---|---|---|
| **MongoDB Database Tools** | https://www.mongodb.com/try/download/database-tools (Windows MSI) | `mongoimport` (data import), `mongodump`/`mongorestore` (backup) |

PATH માં ઉમેરો (version folder તમારા install પ્રમાણે ચકાસો):
```powershell
$p = [Environment]::GetEnvironmentVariable("Path", "User")
[Environment]::SetEnvironmentVariable("Path", "$p;C:\Program Files\MongoDB\Tools\100\bin", "User")
```
**નવી PowerShell** ખોલીને ચકાસો:
```powershell
mongoimport --version
```

## A3. Source data import કરો (`de_source_mongo`)
```powershell
mongoimport --uri "mongodb://localhost:27017/?replicaSet=rs0" `
  --db de_source_mongo --collection weather_observations `
  --file "D:\ClaudeProjects\dataEngineeringProjects\sparkProject\inputData\originalFile\data1.json"
```
- File **JSON Lines** છે (એક line = એક document). `mongoimport` નું default પણ આ જ છે.
- `{"$oid":..}`, `{"$date":..}`, `{"$numberDouble":..}` જેવા Extended JSON values ને `mongoimport` **સાચા Mongo types** (ObjectId, Date, Double) માં ફેરવે છે.

ચકાસો:
```powershell
mongosh "mongodb://localhost:27017/?replicaSet=rs0" --quiet --eval "db.getSiblingDB('de_source_mongo').weather_observations.countDocuments()"
```
➡️ `10000` (તમારી file જેટલા records)

> `de_bronze` database બનાવવાની જરૂર નથી. CDC subscriber પહેલો document લખે ત્યારે તે આપોઆપ બની જશે.
> CDC ચાલુ હોય અને import કરો, તો બધા documents **live inserts** તરીકે bronze માં જશે. CDC પછીથી શરૂ કરો તો source connector નું `copy_existing` હાલના બધા documents લાવશે.

---

# ભાગ B – MySQL (Source + Silver + Gold)

> MySQL માં **CDC નથી** (Debezium કાઢી નાખ્યું છે). એટલે binlog settings કે replication user ની **જરૂર નથી**. MySQL 8 માં binlog default થી ચાલુ હોય છે, તેને બંધ કરવાની પણ જરૂર નથી.

## B1. 3 databases બનાવો
```sql
CREATE DATABASE IF NOT EXISTS de_source_mysql;
CREATE DATABASE IF NOT EXISTS de_silver;
CREATE DATABASE IF NOT EXISTS de_gold;
```
`de_silver` અને `de_gold` માં tables application (Flyway) બનાવશે. અહીં ફક્ત ખાલી databases બનાવવાના છે.

## B2. Source tables અને sample data
```sql
USE de_source_mysql;

CREATE TABLE station (
    station_code  VARCHAR(10)  PRIMARY KEY,      -- = callLetters (weather data)
    station_name  VARCHAR(100) NOT NULL,
    country       VARCHAR(50),
    latitude      DECIMAL(8,4),
    longitude     DECIMAL(8,4),
    station_type  VARCHAR(20),
    created_at    DATETIME     DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE station_owner (
    owner_id      INT          AUTO_INCREMENT PRIMARY KEY,
    station_code  VARCHAR(10)  NOT NULL,
    owner_name    VARCHAR(100) NOT NULL,
    region        VARCHAR(50)  NOT NULL,
    FOREIGN KEY (station_code) REFERENCES station(station_code)
);

INSERT INTO station (station_code, station_name, country, latitude, longitude, station_type) VALUES
 ('VCSZ', 'North Atlantic Ship VCSZ', 'International Waters', 47.6000, -47.9000, 'SHIP'),
 ('VC81', 'Bay of Fundy Buoy VC81',   'Canada',               45.2000, -66.5000, 'BUOY'),
 ('PLAT', 'North Sea Platform PLAT',  'Netherlands',          51.9000,   3.6000, 'PLATFORM'),
 ('TFRB', 'Faroe Ship TFRB',          'Faroe Islands',        60.9000,  -5.3000, 'SHIP'),
 ('TFBY', 'Iceland Ship TFBY',        'Iceland',              66.3000, -25.2000, 'SHIP');

INSERT INTO station_owner (station_code, owner_name, region) VALUES
 ('VCSZ', 'Atlantic Marine Services', 'NORTH_ATLANTIC'),
 ('VC81', 'Canadian Coast Guard',     'NORTH_AMERICA'),
 ('PLAT', 'North Sea Energy',         'EUROPE'),
 ('TFRB', 'Faroe Fisheries',          'EUROPE'),
 ('TFBY', 'Icelandic Met Office',     'EUROPE');
```
> આ ફક્ત 5 sample stations છે, learning માટે પૂરતા. Application phase માં જરૂર પડે તો આખા dataset ના stations પરથી seed બનાવી શકાય.

## B3. જૂનો Debezium user દૂર કરો (પહેલાં બનાવ્યો હોય તો)
```sql
DROP USER IF EXISTS 'debezium'@'localhost';
```

## B4. (Optional) જૂનો `dataengineering` database
જૂના file→MySQL pipeline નો database હવે વપરાશે નહીં. Application refactor પૂરું થાય અને બધું ચકાસાઈ જાય **પછી** તેને drop કરી શકો:
```sql
-- DROP DATABASE dataengineering;   -- ફક્ત ખાતરી થયા પછી
```

---

# ભાગ C – Kafka અને CDC (Source → Bronze)

➡️ **આખો setup: [MONGO_CDC_COMPLETE_SETUP_GU.md](MONGO_CDC_COMPLETE_SETUP_GU.md)** (Kafka install થી લઈને test સુધી, એક જ document માં).

### C1. પહેલાં અને હવે

| | પહેલાં (જૂનો setup) | હવે |
|---|---|---|
| Mongo → Kafka | Debezium MongoDB Connector (`source-mongo`) | **MongoDB Source Connector** (`mongo-cdc-db-source`) |
| MySQL → Kafka | Debezium MySQL Connector (`source-mysql`) | ❌ **બંધ** |
| Topics | દરેક table/collection માટે અલગ (`src.mongo.*`, `src.mysql.*`) | **એક જ topic** `de_source_mongo` |
| Kafka → Bronze | MongoDB **Sink** Connector (`sink-bronze`) | **પોતાનો Java subscriber** (`mongo-cdc-subscriber`) |
| Bronze collections | `mongo_weather_observations`, `mysql_station`, `mysql_station_owner` | `weather_observations`, `student`, ... (**source જેવા જ નામ**). જૂના `mysql_*` collections **રહે છે, પણ update થતા નથી** |
| Bronze document | Debezium envelope: `op`, `after` (JSON string), `source`, `_ingested_at` | **Source જેવો જ document** (એ જ `_id` ObjectId, એ જ types, એ જ fields) **+ 4 metadata fields**: `_op`, `_deleted`, `_source_ts`, `_ingested_at` |
| Delete | Soft delete (`op: "d"` / `__deleted`) | **Soft delete**: document રહે છે, `_deleted: true` |
| Plugins | Debezium MySQL, Debezium MongoDB, mongo-kafka-connect | ફક્ત **mongo-kafka-connect** |

### C2. Spark (Bronze → Silver) કેવી રીતે વાંચે છે

| Bronze collection | કોણ લખે છે | Spark શું કરે છે |
|---|---|---|
| `weather_observations` | `mongo-cdc-subscriber` (live) | `_ingested_at` પર incremental read. `_deleted: true` → silver માં `is_deleted = 1` |
| `mysql_station`, `mysql_station_owner` | જૂનો Debezium pipeline (**હવે કોઈ નહીં**) | જેમ છે તેમ વાંચે છે. નવા ફેરફાર આવતા નથી (MySQL CDC બંધ) |

Code (2026-10-28 એ નવા format પ્રમાણે બદલ્યો):
- `BronzeSchemas.OBSERVATION`: bronze document નો schema, એટલે કે original observation fields (`_id` STRING, `ts` TIMESTAMP, ...) + `_op`, `_deleted`, `_source_ts`, `_ingested_at`. `METADATA_FIELDS` એ 4 નામોની list છે.
- `ObservationTransformer`: `SOURCE = "weather_observations"`. Metadata સિવાયના fields ને `d` struct માં મૂકે છે (પહેલાં `after` JSON parse થતો હતો), `_deleted` પરથી deletes, અને `_source_ts` પરથી `source_ts`.
- `BronzeReader`, `StationTransformer`, `BronzeToSilverJob` નું logic એ જ છે (`_ingested_at` watermark).

> ⚠️ જૂના subscriber (metadata વગરના) એ લખેલા documents માં `_ingested_at` નથી, એટલે Spark એમને **ક્યારેય નહીં વાંચે**. નવો subscriber ચલાવ્યા પછી એક વાર replay કરો: [MONGO_CDC_COMPLETE_SETUP_GU.md](MONGO_CDC_COMPLETE_SETUP_GU.md) વિભાગ 12.1.

---

# ભાગ D – રોજનો Start / Stop ક્રમ

## D1. Start (આ ક્રમમાં)
```
1. MySQL, MongoDB     → Windows services, આપોઆપ ચાલુ થાય   (Get-Service MySQL80, MongoDB)
2. Kafka + Connect + subscriber → MONGO_CDC_COMPLETE_SETUP_GU.md વિભાગ 11.1
3. Spark pipeline app → default profile (local[*])
```

## D2. Stop (ઉલટા ક્રમમાં)
```
1. Spark app → Stop
2. Subscriber, Kafka Connect, Kafka → MONGO_CDC_COMPLETE_SETUP_GU.md વિભાગ 11.2
```

---

# ભાગ E – સામાન્ય સમસ્યાઓ

| સમસ્યા / Error | ઉકેલ |
|---|---|
| `mongod`/`mongosh`/`mongoimport` not recognized | PATH ચકાસો (version folder), નવી terminal ખોલો |
| MongoDB service restart પછી શરૂ ન થાય | `mongod.cfg` નું YAML indentation (spaces, tabs નહીં). Log: `C:\Program Files\MongoDB\Server\<version>\log\mongod.log` |
| `MongoServerError: not primary` / change streams error | `rs.initiate` કર્યું? `rs.status()` |
| `mongoimport` પછી count ઓછો | File JSON Lines છે કે નહીં, અને `--file` નો path |
| MySQL: `Access denied` | User/password અને `GRANT` ચકાસો |
| Kafka, Kafka Connect, connector, subscriber, bronze માં data ન આવે | **[MONGO_CDC_COMPLETE_SETUP_GU.md](MONGO_CDC_COMPLETE_SETUP_GU.md) વિભાગ 13 (ન કરવાની ભૂલો) અને 14 (Troubleshooting)** |
| Kafka/Connect: `OutOfMemoryError` અથવા machine ધીમું | Docker Desktop, Spark cluster, OpenMetadata બંધ કરો. 0.4 જુઓ |

---

## સારાંશ: તમે શું શીખ્યા
| Concept | ક્યાં |
|---|---|
| MongoDB replica set: CDC માટે કેમ જરૂરી | A1 (CDC document વિભાગ 2) |
| Source data import (`mongoimport`) | A3 |
| Source / Silver / Gold databases | B |
| Kafka KRaft, Kafka Connect, MongoDB Source Connector, generic topic, Java subscriber | C (CDC document) |
| Debezium/sink થી નવા setup માં શું બદલાયું, અને Spark પર અસર | C1, C2 |
