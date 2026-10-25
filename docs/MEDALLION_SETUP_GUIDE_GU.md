# Medallion Pipeline – Installation અને Configuration Guide (Windows 11 Native, ગુજરાતી)

> આ guide થી તમે **Stage 1 (Source) → Kafka → Stage 2 (Bronze)** સુધીનું આખું infrastructure તમારી Windows 11 machine પર **Docker વગર** step-by-step setup કરશો.
> Stage 3 (Silver) અને Stage 4 (Gold) નો Spark code પછીથી, application prompt થી બનશે ([MEDALLION_PIPELINE_PLAN_GU.md](MEDALLION_PIPELINE_PLAN_GU.md) વિભાગ 12).

**Versions (Oct 2026 માં ચકાસેલા):**

| Software | Version |
|---|---|
| Apache Kafka | **4.3.1** (Scala 2.13 build) |
| Debezium (MySQL + MongoDB connectors) | **3.7.0.Final** (Java 17+) |
| MongoDB Kafka Connector (sink) | **3.1.2** |
| MongoDB Community Server | **8.0.x** |
| MySQL | 8.0.30 (તમારી machine પર પહેલેથી છે) ✅ |
| JDK | 17 (પહેલેથી છે) ✅ |

---

## અનુક્રમણિકા
- [0. શરૂ કરતા પહેલા](#0-શરૂ-કરતા-પહેલા)
- [ભાગ A – MongoDB (Source + Bronze)](#ભાગ-a--mongodb-source--bronze)
- [ભાગ B – MySQL (Source + Silver + Gold)](#ભાગ-b--mysql-source--silver--gold)
- [ભાગ C – Apache Kafka (Native Windows)](#ભાગ-c--apache-kafka-native-windows)
- [ભાગ D – Kafka Connect + Plugins](#ભાગ-d--kafka-connect--plugins)
- [ભાગ E – Connectors બનાવવા અને register કરવા](#ભાગ-e--connectors-બનાવવા-અને-register-કરવા)
- [ભાગ F – જૂનો (existing) data Bronze માં: Initial Snapshot](#ભાગ-f--જૂનો-existing-data-bronze-માં-initial-snapshot)
- [ભાગ G – Live CDC Test (insert / update / delete)](#ભાગ-g--live-cdc-test-insert--update--delete)
- [ભાગ H – રોજનો Start / Stop ક્રમ](#ભાગ-h--રોજનો-start--stop-ક્રમ)
- [ભાગ I – સામાન્ય સમસ્યાઓ](#ભાગ-i--સામાન્ય-સમસ્યાઓ)

---

## 0. શરૂ કરતા પહેલા

### 0.1 આપણે શું બનાવીશું

```
 MongoDB (Windows service, replica set rs0, :27017)           MySQL 8 (Windows service, :3306)
   de_source_mongo.weather_observations                          de_source_mysql.station, station_owner
          │ change streams                                               │ binlog
          ▼                                                              ▼
   ┌──────────────────── Kafka Connect (native, :8083) ────────────────────────┐
   │  source-mongo  (Debezium MongoDB)        source-mysql (Debezium MySQL)     │
   │        │                                        │                          │
   │        ▼                                        ▼                          │
   │   Kafka (native, KRaft, :9092)  topics: src.mongo.*   src.mysql.*          │
   │        │                                                                   │
   │  sink-bronze  (MongoDB Kafka Sink) ──────────────────────────────┐         │
   └──────────────────────────────────────────────────────────────────┼─────────┘
                                                                      ▼
                                   MongoDB  de_bronze.mongo_weather_observations
                                                     .mysql_station
                                                     .mysql_station_owner
```

### 0.2 Databases ના નામ (dataset-independent)

| Stage | Engine | Database |
|---|---|---|
| 1 – Source | MongoDB | `de_source_mongo` |
| 1 – Source | MySQL | `de_source_mysql` |
| 2 – Bronze | MongoDB | `de_bronze` |
| 3 – Silver | MySQL | `de_silver` |
| 4 – Gold | MySQL | `de_gold` |

Database ના નામ સામાન્ય (generic) રાખ્યા છે, એટલે dataset બદલાય તો પણ નામ બદલવા નહીં પડે. ફક્ત collections/tables dataset પ્રમાણે હશે.

### 0.3 Folder layout
```
C:\kafka\                    ← Kafka installation
C:\kafka\plugins\            ← Kafka Connect plugins (Debezium, Mongo sink)
C:\kafka-data\               ← Kafka નો data (topics)
D:\...\sparkProject\kafka-connect\   ← connector JSON files (project માં, version control માટે)
```
> ⚠️ Kafka ને **ટૂંકા path** માં જ રાખો (`C:\kafka`). Windows `.bat` scripts લાંબા path પર `The input line is too long` error આપે છે.

### 0.4 RAM: તમારી machine પર 9.9 GB છે ⚠️
આ guide લખતી વખતે machine પર ફક્ત **0.7 GB free** હતી. તેથી:

| Component | RAM મર્યાદા (આ guide માં સેટ કરીશું) |
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

## A1. MongoDB Community Server install કરો
1. ખોલો: https://www.mongodb.com/try/download/community
2. Version: **8.0.x**, Platform: **Windows x64**, Package: **msi** → Download
3. Installer ચલાવો:
   - **Complete** setup
   - ✅ **Install MongoD as a Service** (Run service as Network Service user)
   - Data/Log directory: default રહેવા દો
   - Compass: ✅ (GUI, ઉપયોગી છે. RAM ઓછી હોય તો પછીથી install કરો)

## A2. mongosh અને Database Tools install કરો
| Tool | Download | કામ |
|---|---|---|
| **mongosh** | https://www.mongodb.com/try/download/shell (Windows MSI) | Mongo commands ચલાવવા |
| **MongoDB Database Tools** | https://www.mongodb.com/try/download/database-tools (Windows MSI) | `mongoimport` (data import) |

PATH માં ઉમેરો (Database Tools નું version folder તમારા install પ્રમાણે ચકાસો):
```powershell
$p = [Environment]::GetEnvironmentVariable("Path", "User")
[Environment]::SetEnvironmentVariable("Path", "$p;C:\Program Files\MongoDB\Server\8.0\bin;C:\Program Files\MongoDB\Tools\100\bin", "User")
```
**નવી PowerShell** ખોલીને ચકાસો:
```powershell
mongod --version
mongosh --version
mongoimport --version
```

## A3. Replica set અને RAM મર્યાદા સેટ કરો **(Admin)**

**કેમ?** CDC (change streams) **ફક્ત replica set** પર જ ચાલે છે. Local માટે 1 node નો replica set પૂરતો છે.

1. **Notepad (Admin)** થી ખોલો: `C:\Program Files\MongoDB\Server\8.0\bin\mongod.cfg`
2. `storage:` ની નીચે `wiredTiger` ઉમેરો, અને file ના અંતે `replication` ઉમેરો. File આવી દેખાવી જોઈએ:
```yaml
storage:
  dbPath: C:\Program Files\MongoDB\Server\8.0\data
  wiredTiger:
    engineConfig:
      cacheSizeGB: 0.5

systemLog:
  destination: file
  logAppend: true
  path:  C:\Program Files\MongoDB\Server\8.0\log\mongod.log

net:
  port: 27017
  bindIp: 127.0.0.1

replication:
  replSetName: rs0
```
> ⚠️ YAML માં **spaces** થી indentation થાય છે, Tab થી નહીં. `replication:` line ની શરૂઆતમાં કોઈ space ન હોવી જોઈએ.

3. Service restart કરો **(Admin PowerShell)**:
```powershell
Restart-Service MongoDB
Get-Service MongoDB
```

## A4. Replica set શરૂ કરો (initiate)
```powershell
mongosh "mongodb://localhost:27017/?directConnection=true"
```
mongosh ની અંદર:
```javascript
rs.initiate({ _id: "rs0", members: [ { _id: 0, host: "localhost:27017" } ] })
// થોડી સેકન્ડ પછી prompt "rs0 [direct: primary]" બને
rs.status().members[0].stateStr      // "PRIMARY"
exit
```
હવેથી connection string હંમેશા આ વાપરો:
```
mongodb://localhost:27017/?replicaSet=rs0
```

## A5. Source data import કરો (`de_source_mongo`)
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

> `de_bronze` database અત્યારે બનાવવાની જરૂર નથી. Sink connector પહેલો document લખે ત્યારે તે આપોઆપ બની જશે.

---

# ભાગ B – MySQL (Source + Silver + Gold)

## B1. Binlog settings ચકાસો (Debezium માટે)
Debezium MySQL ના **binlog** (change log) વાંચે છે. MySQL Workbench માં:
```sql
SELECT @@version, @@log_bin, @@binlog_format, @@binlog_row_image, @@server_id, @@binlog_expire_logs_seconds;
```
| Setting | જોઈએ | તમારી machine (ચકાસેલું) |
|---|---|---|
| `log_bin` | 1 | 1 ✅ |
| `binlog_format` | ROW | ROW ✅ |
| `binlog_row_image` | FULL | FULL ✅ |
| `server_id` | ≠ 0 | 1 ✅ |
| `binlog_expire_logs_seconds` | ઓછામાં ઓછા થોડા દિવસ | 2592000 (30 દિવસ) ✅ |

✅ **તમારી MySQL પહેલેથી તૈયાર છે.** `my.ini` બદલવાની જરૂર નથી.

> `binlog_expire_logs_seconds` નો અર્થ: Kafka Connect 30 દિવસથી વધુ બંધ રહે તો જૂનો binlog ભૂંસાઈ જાય, અને connector ને **ફરી snapshot** કરવો પડે.

## B2. 3 databases બનાવો
```sql
CREATE DATABASE IF NOT EXISTS de_source_mysql;
CREATE DATABASE IF NOT EXISTS de_silver;
CREATE DATABASE IF NOT EXISTS de_gold;
```
`de_silver` અને `de_gold` માં tables application (Flyway) બનાવશે. અહીં ફક્ત ખાલી databases બનાવવાના છે.

## B3. Source tables અને sample data
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

## B4. Debezium માટે MySQL user
Kafka Connect Windows પર જ ચાલે છે, એટલે `localhost` user પૂરતો છે:
```sql
CREATE USER IF NOT EXISTS 'debezium'@'localhost' IDENTIFIED BY 'Dbz@12345';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT, LOCK TABLES
  ON *.* TO 'debezium'@'localhost';
FLUSH PRIVILEGES;
```
| Privilege | કેમ |
|---|---|
| `SELECT` | Initial snapshot માં tables વાંચવા |
| `RELOAD`, `LOCK TABLES` | Snapshot વખતે consistent read |
| `SHOW DATABASES` | Databases શોધવા |
| `REPLICATION SLAVE`, `REPLICATION CLIENT` | **Binlog વાંચવા** (CDC) |

## B5. (Optional) જૂનો `dataengineering` database
જૂના file→MySQL pipeline નો database હવે વપરાશે નહીં. Application refactor પૂરું થાય અને બધું ચકાસાઈ જાય **પછી** તેને drop કરી શકો:
```sql
-- DROP DATABASE dataengineering;   -- ફક્ત ખાતરી થયા પછી
```

---

# ભાગ C – Apache Kafka (Native Windows)

> ⚠️ **જાણી લો:** Apache Kafka **officially Windows ને support કરતું નથી**. Development માટે ચાલે છે, પણ Windows ના file-locking ને કારણે **log segments delete કે compact કરતી વખતે** broker crash થઈ શકે છે (`AccessDeniedException`). આ guide માં તેનું જોખમ ઓછું કરવા settings આપી છે (C3), અને crash થાય તો શું કરવું તે ભાગ I માં છે.

## C1. Download અને extract
```powershell
cd $env:USERPROFILE\Downloads
Invoke-WebRequest -Uri "https://archive.apache.org/dist/kafka/4.3.1/kafka_2.13-4.3.1.tgz" -OutFile kafka_2.13-4.3.1.tgz
tar -xzf kafka_2.13-4.3.1.tgz
Move-Item kafka_2.13-4.3.1 C:\kafka
New-Item -ItemType Directory -Force C:\kafka-data, C:\kafka\plugins
```
ચકાસો: `C:\kafka\bin\windows\kafka-server-start.bat` હોવી જોઈએ.

## C2. Kafka ને Java 17 મળે છે?
```powershell
echo $env:JAVA_HOME        # C:\Program Files\Microsoft\jdk-17.0.19.10-hotspot
java -version              # 17
```

## C3. `server.properties` માં ફેરફાર
`C:\kafka\config\server.properties` ખોલો (Notepad) અને આ settings **શોધીને બદલો** (ન હોય તો અંતે ઉમેરો):
```properties
# Data folder (forward slashes!)
log.dirs=C:/kafka-data/kraft-combined-logs

# Windows: segments delete કરતી વખતે crash ટાળવા → data ક્યારેય auto-delete ન કરો (local learning)
log.retention.ms=-1
log.retention.bytes=-1

# Windows: compaction (cleaner) થી થતા file-rename crash ટાળવા
log.cleaner.enable=false

# Single broker
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
```
| Setting | અર્થ |
|---|---|
| `log.dirs` | Topics નો data ક્યાં રાખવો |
| `log.retention.ms=-1` | જૂના messages delete નહીં થાય. Windows પર delete operation જ crash નું કારણ છે |
| `log.cleaner.enable=false` | Compacted topics ની સફાઈ બંધ. Kafka version આ setting ન ઓળખે તો ફક્ત warning આપે છે |
| replication factor 1 | એક જ broker છે |

> Retention બંધ હોવાથી `C:\kafka-data` ધીમે ધીમે મોટું થશે. Learning માટે ઠીક છે. ક્યારેક reset કરવું હોય તો ભાગ H3 જુઓ.

## C4. Storage format કરો (ફક્ત પહેલી વાર)
KRaft mode માં ZooKeeper નથી. Kafka પોતે જ metadata સંભાળે છે, તે માટે storage format કરવો પડે:
```powershell
cd C:\kafka
$id = (.\bin\windows\kafka-storage.bat random-uuid | Select-Object -Last 1).Trim()
$id
.\bin\windows\kafka-storage.bat format --standalone -t $id -c .\config\server.properties
```
➡️ `Formatting ... C:/kafka-data/kraft-combined-logs` જેવો message આવવો જોઈએ.

## C5. Kafka start કરો (Terminal #1, ખુલ્લું રાખો)
```powershell
cd C:\kafka
$env:KAFKA_HEAP_OPTS = "-Xms512m -Xmx512m"
.\bin\windows\kafka-server-start.bat .\config\server.properties
```
➡️ Log માં `Kafka Server started` દેખાય એટલે તૈયાર.

## C6. ચકાસો (નવી PowerShell)
```powershell
cd C:\kafka
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --create --topic smoke-test
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --list
```
➡️ `smoke-test` દેખાવો જોઈએ. ✅

---

# ભાગ D – Kafka Connect + Plugins

## D1. Plugins download કરો
```powershell
cd C:\kafka\plugins
$m = "https://repo1.maven.org/maven2"

# Debezium MySQL source
Invoke-WebRequest "$m/io/debezium/debezium-connector-mysql/3.7.0.Final/debezium-connector-mysql-3.7.0.Final-plugin.zip" -OutFile dbz-mysql.zip
Expand-Archive dbz-mysql.zip -DestinationPath C:\kafka\plugins

# Debezium MongoDB source
Invoke-WebRequest "$m/io/debezium/debezium-connector-mongodb/3.7.0.Final/debezium-connector-mongodb-3.7.0.Final-plugin.zip" -OutFile dbz-mongo.zip
Expand-Archive dbz-mongo.zip -DestinationPath C:\kafka\plugins

# MongoDB Kafka Connector (sink) – "-all" jar માં MongoDB driver સાથે આવે છે
New-Item -ItemType Directory -Force C:\kafka\plugins\mongo-kafka-connect
Invoke-WebRequest "$m/org/mongodb/kafka/mongo-kafka-connect/3.1.2/mongo-kafka-connect-3.1.2-all.jar" -OutFile C:\kafka\plugins\mongo-kafka-connect\mongo-kafka-connect-3.1.2-all.jar

Remove-Item *.zip
Get-ChildItem C:\kafka\plugins
```
➡️ 3 folders: `debezium-connector-mysql`, `debezium-connector-mongodb`, `mongo-kafka-connect`

> **નિયમ:** દરેક plugin **અલગ folder** માં રાખો. Kafka Connect દરેક folder ને અલગ classloader માં load કરે છે, એટલે બે plugins ના MongoDB driver versions અથડાતા નથી.

## D2. Connect worker config બનાવો
```powershell
@"
bootstrap.servers=localhost:9092
group.id=de-connect-cluster

key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=false
value.converter.schemas.enable=false

config.storage.topic=connect-configs
offset.storage.topic=connect-offsets
status.storage.topic=connect-status
config.storage.replication.factor=1
offset.storage.replication.factor=1
status.storage.replication.factor=1
offset.flush.interval.ms=10000

listeners=http://localhost:8083
plugin.path=C:/kafka/plugins
"@ | Set-Content -Encoding ascii C:\kafka\config\connect-distributed-de.properties
```
| Setting | અર્થ |
|---|---|
| `group.id` | Connect cluster નું નામ |
| `JsonConverter`, `schemas.enable=false` | Messages સાદા JSON (schema વગર). Bronze માં વાંચવામાં સરળ |
| `*.storage.topic` | Connect પોતાનું state (connector configs, **ક્યાં સુધી વાંચ્યું તે offsets**, status) Kafka topics માં રાખે છે |
| `listeners` | REST API: connectors ને register/manage કરવા |
| `plugin.path` | D1 ના plugins |

## D3. Kafka Connect start કરો (Terminal #2, ખુલ્લું રાખો)
```powershell
cd C:\kafka
$env:KAFKA_HEAP_OPTS = "-Xms512m -Xmx768m"
.\bin\windows\connect-distributed.bat .\config\connect-distributed-de.properties
```

## D4. Plugins load થયા છે? (નવી PowerShell)
```powershell
(Invoke-RestMethod http://localhost:8083/connector-plugins) | Select-Object class, version
```
> ⚠️ **Parentheses `( )` જરૂરી છે.** Windows PowerShell 5.1 માં `Invoke-RestMethod` JSON array ને pipe માં **એક જ object** તરીકે મોકલે છે. Parentheses વગર `Select-Object` ખાલી line બતાવે છે.

➡️ આ ત્રણ દેખાવા જોઈએ (Kafka ના built-in plugins અને `MongoSourceConnector` જેવા વધારાના plugins પણ દેખાશે, તે સામાન્ય છે):
```
io.debezium.connector.mysql.MySqlConnector          3.7.0.Final
io.debezium.connector.mongodb.MongoDbConnector      3.7.0.Final
com.mongodb.kafka.connect.MongoSinkConnector        3.1.2
```

---

# ભાગ E – Connectors બનાવવા અને register કરવા

Connector = એક JSON config. તેને Connect ના REST API પર મોકલો એટલે તે ચાલુ થઈ જાય. Configs **project માં** રાખીશું:
```powershell
New-Item -ItemType Directory -Force D:\ClaudeProjects\dataEngineeringProjects\sparkProject\kafka-connect
```

## E1. `kafka-connect/source-mysql.json`
```json
{
  "connector.class": "io.debezium.connector.mysql.MySqlConnector",
  "tasks.max": "1",
  "database.hostname": "localhost",
  "database.port": "3306",
  "database.user": "debezium",
  "database.password": "Dbz@12345",
  "database.server.id": "184054",
  "topic.prefix": "src.mysql",
  "database.include.list": "de_source_mysql",
  "schema.history.internal.kafka.bootstrap.servers": "localhost:9092",
  "schema.history.internal.kafka.topic": "schemahistory.src.mysql",
  "include.schema.changes": "false",
  "snapshot.mode": "initial",
  "decimal.handling.mode": "string",
  "time.precision.mode": "connect",
  "tombstones.on.delete": "false",
  "transforms": "unwrap",
  "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
  "transforms.unwrap.add.fields": "op,source.ts_ms",
  "transforms.unwrap.delete.tombstone.handling.mode": "rewrite"
}
```

## E2. `kafka-connect/source-mongo.json`
```json
{
  "connector.class": "io.debezium.connector.mongodb.MongoDbConnector",
  "tasks.max": "1",
  "mongodb.connection.string": "mongodb://localhost:27017/?replicaSet=rs0",
  "topic.prefix": "src.mongo",
  "database.include.list": "de_source_mongo",
  "capture.mode": "change_streams_update_full",
  "snapshot.mode": "initial",
  "tombstones.on.delete": "false"
}
```
> ⚠️ **Mongo માટે unwrap SMT (`ExtractNewDocumentState`) વાપરતા નથી.** Testing માં તે આ error આપીને fail થયો:
> `Array elements with different Bson types are not supported: [BsonInt32{value=-172}, BsonDouble{value=79.8}]`
> એટલે કે array માં પૂર્ણાંક અને દશાંશ બંને પ્રકારની values હોય તો (દા.ત. `coordinates: [-172, 79.8]`) SMT સંભાળી શકતો નથી.
> તેથી Debezium નો **આખો change event** જ bronze માં જાય છે. તેમાં `after` field માં **મૂળ document નું ચોક્કસ JSON (string)** હોય છે. આ સાચું "as-is" છે, અને કોઈ પણ data type પર ક્યારેય fail થતું નથી. Spark (Job #1) આ JSON ને parse કરે છે.

## E3. `kafka-connect/sink-bronze.json`
```json
{
  "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
  "tasks.max": "1",
  "topics.regex": "src\\.(mongo|mysql)\\..*|(mongo|mysql)_.*",
  "connection.uri": "mongodb://localhost:27017/?replicaSet=rs0",
  "database": "de_bronze",
  "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.FullKeyStrategy",
  "document.id.strategy.overwrite.existing": "true",
  "writemodel.strategy": "com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneDefaultStrategy",
  "transforms": "route,ingest",
  "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
  "transforms.route.regex": "src\\.(mongo|mysql)\\.[^.]+\\.(.*)",
  "transforms.route.replacement": "$1_$2",
  "transforms.ingest.type": "org.apache.kafka.connect.transforms.InsertField$Value",
  "transforms.ingest.timestamp.field": "_ingested_at",
  "errors.tolerance": "all",
  "errors.log.enable": "true",
  "errors.deadletterqueue.topic.name": "dlq.sink-bronze",
  "errors.deadletterqueue.topic.replication.factor": "1",
  "errors.deadletterqueue.context.headers.enable": "true"
}
```

## E4. દરેક property નો અર્થ

**Source connectors (Debezium):**

| Property | અર્થ |
|---|---|
| `topic.prefix` | Topics ના નામની શરૂઆત. Topic = `<prefix>.<database>.<table/collection>` → દા.ત. `src.mysql.de_source_mysql.station` |
| `database.include.list` | કયા database ના changes પકડવા |
| `database.server.id` | Debezium MySQL ને "replica" તરીકે જોડાય છે. તેનો ID MySQL ના `server_id` (1) થી **અલગ** હોવો જોઈએ |
| `schema.history.internal.*` | MySQL tables નું structure (DDL history) Debezium અહીં સાચવે છે |
| `snapshot.mode=initial` | **પહેલી વાર: હાલનો બધો data વાંચે, પછી live changes** (ભાગ F) |
| `capture.mode=change_streams_update_full` | Mongo update વખતે **આખો document** મોકલે, ફક્ત બદલાયેલા fields નહીં |
| `decimal.handling.mode=string` | DECIMAL ને string તરીકે મોકલે (નહીંતર base64 bytes આવે) |
| `time.precision.mode=connect` | DATETIME ને epoch milliseconds તરીકે મોકલે |
| `tombstones.on.delete=false` | Delete પછીનો ખાલી (null) message ન મોકલે. Sink ને તેની જરૂર નથી |
| `ExtractNewRecordState` (**ફક્ત MySQL**) | Debezium envelope (`before`/`after`/`source`) કાઢીને **ફક્ત original row** રાખે = **as-is** |
| `add.fields=op,source.ts_ms` (MySQL) | Metadata ઉમેરે: `__op` (c=insert, u=update, d=delete, r=snapshot read), `__source_ts_ms` (source માં change નો time) |
| `delete.tombstone.handling.mode=rewrite` (MySQL) | Delete ને `__deleted: "true"` વાળા record માં ફેરવે = **soft delete** |
| Mongo: SMT નથી | આખો Debezium event: `op` (r/c/u/d), `after` (મૂળ document નું JSON; delete વખતે `null`), `source.ts_ms` |

**Sink connector:**

| Property | અર્થ |
|---|---|
| `topics.regex` | `src.mongo.*` અને `src.mysql.*` બધા topics વાંચે. **નવું table ઉમેરાય તો sink બદલવાની જરૂર નથી**. બીજો ભાગ `(mongo\|mysql)_.*` જરૂરી છે: sink દરેક record ને **RegexRouter પછીના નવા નામ** (દા.ત. `mysql_station`) થી ફરી ચકાસે છે. તે ન હોય તો `Unknown topic: mysql_station_owner, does not match ...` error આવે છે (testing માં આવ્યો હતો) |
| `database=de_bronze` | Bronze database |
| `RegexRouter` (`route`) | Topic `src.mysql.de_source_mysql.station` → collection **`mysql_station`**; `src.mongo.de_source_mongo.weather_observations` → **`mongo_weather_observations`** |
| `InsertField` (`ingest`) | `_ingested_at` field: Kafka message નો time. **Spark incremental watermark આ field પર** ચાલશે |
| `FullKeyStrategy` + `overwrite.existing` | Bronze `_id` = source નો primary key (દા.ત. `{station_code: "PLAT"}`) |
| `ReplaceOneDefaultStrategy` | એ જ `_id` ફરી આવે તો document **replace (upsert)** થાય → **duplicates નહીં** |
| `errors.*` + `deadletterqueue` | ખરાબ message આખા sink ને રોકે નહીં. તે `dlq.sink-bronze` topic માં જાય, જેને પછી તપાસી શકાય |

> **Bronze માં શું રહે:** દરેક source record નું **છેલ્લું version** (upsert), અને દરેક change નો **પૂરો history** Kafka topic માં (retention = unlimited).
>
> | Bronze collection | Document નું સ્વરૂપ |
> |---|---|
> | `mysql_*` | `{ _id: {station_code: "PLAT"}, <બધા columns>, __op, __deleted, __source_ts_ms, _ingested_at }` |
> | `mongo_*` | `{ _id: {id: "{\"$oid\": \"5553...\"}"}, op, after: "<મૂળ document JSON>", source: {ts_ms, ...}, _ingested_at }` |
>
> `_ingested_at` epoch **milliseconds (number)** તરીકે store થાય છે, Date તરીકે નહીં.

## E5. Connectors register કરો
Project માં તૈયાર script છે: [kafka-connect/register-connectors.ps1](../kafka-connect/register-connectors.ps1)
```powershell
cd D:\ClaudeProjects\dataEngineeringProjects\sparkProject\kafka-connect
.\register-connectors.ps1                       # ત્રણેય connectors create/update + status
.\register-connectors.ps1 status                # ફક્ત status
.\register-connectors.ps1 delete source-mongo   # એક connector delete
```
Script અંદરથી આ જ કરે છે:
```powershell
foreach ($n in "source-mysql", "source-mongo", "sink-bronze") {
  Invoke-RestMethod -Method Put -Uri "http://localhost:8083/connectors/$n/config" `
    -ContentType "application/json" -InFile ".\$n.json" | Select-Object name
}
```
> `PUT /connectors/<name>/config` થી connector **બને પણ છે અને update પણ થાય છે**. Config બદલો અને એ જ command ફરી ચલાવો.

## E6. Status ચકાસો
```powershell
(Invoke-RestMethod "http://localhost:8083/connectors?expand=status").PSObject.Properties | ForEach-Object {
  "{0,-14} connector={1,-8} task={2}" -f $_.Name, $_.Value.status.connector.state, $_.Value.status.tasks[0].state }
```
➡️ ત્રણેય `RUNNING`. કોઈ `FAILED` હોય તો error જુઓ:
```powershell
(Invoke-RestMethod http://localhost:8083/connectors/source-mysql/status).tasks[0].trace
```

---

# ભાગ F – જૂનો (existing) data Bronze માં: Initial Snapshot

### F1. પ્રશ્ન
Kafka Connect શરૂ થાય **તે પહેલા** source DBs માં જે data છે (Mongo માં 10,000 documents, MySQL માં 5 + 5 rows), તે bronze માં કેવી રીતે આવશે?

### F2. જવાબ: `snapshot.mode=initial`
Connector **પહેલી વાર** શરૂ થાય ત્યારે:
```
1. Source નો હાલનો position નોંધે   (MySQL: binlog file+position / Mongo: change stream resume token)
2. બધો હાલનો data વાંચે (snapshot)   → દરેક row/document એક event, __op = "r" (read)
3. Snapshot પૂરો → નોંધેલા position થી live changes (c/u/d) વાંચવાનું શરૂ
4. Position (offset) "connect-offsets" topic માં સાચવે
```
- **Snapshot દરમિયાન આવેલા નવા changes ગુમ નથી થતા.** Step 1 માં position પહેલેથી નોંધાયેલું છે, એટલે તે changes step 3 માં મળી જાય છે.
- **Connector restart થાય તો snapshot ફરી નથી થતો.** તે offset થી આગળ વાંચે છે.
- **કોઈ event બે વાર આવે તો પણ bronze માં duplicate નહીં.** Sink upsert (`_id` = primary key) કરે છે.

### F3. ચકાસો
```powershell
mongosh "mongodb://localhost:27017/?replicaSet=rs0" --quiet --eval "
  const b = db.getSiblingDB('de_bronze');
  printjson({ collections: b.getCollectionNames(),
              mongo_obs: b.mongo_weather_observations.countDocuments(),
              mysql_station: b.mysql_station.countDocuments(),
              mysql_owner: b.mysql_station_owner.countDocuments() });
  printjson(b.mysql_station.findOne());"
```
➡️ અપેક્ષા: `mongo_obs: 10000`, `mysql_station: 5`, `mysql_owner: 5`.
Document માં:
- `mysql_*`: original columns + `__op: "r"`, `__source_ts_ms`, `_ingested_at`, `__deleted: "false"`
- `mongo_weather_observations`: `op: "r"`, `after: "<મૂળ document JSON>"`, `source`, `_ingested_at`

> ⚠️ **Testing માં મળેલું:** connector પહેલી વાર ખોટા config થી fail થયો હોય, તો પહેલા attempt ના થોડા documents જૂના format/key સાથે bronze માં રહી જાય છે. તેથી bronze ની count, source કરતા **વધારે** દેખાય. ઉકેલ: connector stop → offsets reset → resume, અને જૂના format ના documents delete કરવા (ભાગ I જુઓ).

Kafka topics પણ જુઓ:
```powershell
cd C:\kafka
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --list
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic src.mysql.de_source_mysql.station --from-beginning --max-messages 1
```

### F4. પછીથી નવું table/collection ઉમેરવું હોય તો
- `database.include.list` આખા database ને પકડે છે, એટલે **નવું table આપોઆપ stream થાય છે**. પણ તેનો **જૂનો data** snapshot માં નથી આવતો.
- જૂનો data લાવવા Debezium નો **incremental snapshot** વપરાય: source DB માં એક *signal table* બનાવીને તેમાં `execute-snapshot` row insert કરવી પડે. આ advanced topic છે, જરૂર પડે ત્યારે અલગ શીખીશું.
- **સૌથી સરળ રીત (learning માટે):** connector delete કરો, નવા નામથી ફરી register કરો. નવા નામનો અર્થ નવા offsets, એટલે આખો snapshot ફરી થાય. Bronze upsert હોવાથી duplicates નહીં થાય.

### F5. Kafka topics: કયો topic શેના માટે, અને live messages કેવી રીતે જોવા

#### F5.1 દરેક topic કોના માટે છે
`kafka-topics.bat --list` આ topics બતાવે છે:

| Topic | કોણ બનાવે | શેના માટે | તમારે જોવાનો? |
|---|---|---|---|
| `src.mongo.de_source_mongo.weather_observations` | `source-mongo` (Debezium) | **Mongo** `de_source_mongo.weather_observations` ના દરેક insert/update/delete | ✅ **હા** |
| `src.mysql.de_source_mysql.station` | `source-mysql` (Debezium) | **MySQL** `de_source_mysql.station` ના દરેક change | ✅ **હા** |
| `src.mysql.de_source_mysql.station_owner` | `source-mysql` (Debezium) | **MySQL** `de_source_mysql.station_owner` ના દરેક change | ✅ **હા** |
| `dlq.sink-bronze` | `sink-bronze` | Sink જે message bronze માં **લખી ન શક્યો** તે (Dead Letter Queue). **ખાલી હોવો જોઈએ** | ⚠️ ફક્ત error ચકાસવા |
| `schemahistory.src.mysql` | `source-mysql` | MySQL tables નો structure (CREATE/ALTER) history. Debezium નો internal topic | ❌ ના |
| `connect-configs` | Kafka Connect | ત્રણેય connectors ના JSON configs | ❌ ના |
| `connect-offsets` | Kafka Connect | Source connectors ક્યાં સુધી વાંચી ગયા (binlog position / resume token) | ❌ ના |
| `connect-status` | Kafka Connect | Connectors નું status (RUNNING/FAILED) | ❌ ના |
| `__consumer_offsets` | Kafka | Sink જેવા consumers ક્યાં સુધી વાંચી ગયા | ❌ ના |
| `smoke-test` | તમે (C6) | ફક્ત test. વપરાતો નથી | ❌ ના |

**Registered connectors:** `source-mysql`, `source-mongo`, `sink-bronze`.
**Sink નો consumer group:** `connect-sink-bronze` (Kafka Connect નામ આપે છે: `connect-` + connector નું નામ).

**Topic ના નામનો નિયમ:**
```
src.mysql . de_source_mysql . station
└ prefix ┘   └─ database ─┘   └ table ┘
```
➡️ Source માં **નવું table/collection** બનાવો તો આ જ નિયમથી **નવો topic આપોઆપ** બને છે.

#### F5.2 Source માં entry થાય ત્યારે તે કયા topic માં જાય

| તમે ક્યાં ફેરફાર કરો | જાય છે આ topic માં | પછી bronze collection |
|---|---|---|
| MongoDB `de_source_mongo` → `weather_observations` | `src.mongo.de_source_mongo.weather_observations` | `de_bronze.mongo_weather_observations` |
| MySQL `de_source_mysql` → `station` | `src.mysql.de_source_mysql.station` | `de_bronze.mysql_station` |
| MySQL `de_source_mysql` → `station_owner` | `src.mysql.de_source_mysql.station_owner` | `de_bronze.mysql_station_owner` |

```
Source DB ──(Debezium source connector)──► Kafka topic ──(sink-bronze)──► de_bronze collection
```
Insert, update અને delete ત્રણેય **એ જ topic** માં જાય છે. ફરક ફક્ત message ના `op` / `__op` field માં છે: `c` = insert, `u` = update, `d` = delete, `r` = snapshot.

#### F5.3 Live messages જોવાનો command

**ક્યાં ચલાવવો:** **Windows PowerShell** માં, `C:\kafka` folder માંથી. Kafka (Terminal #1) અને Kafka Connect (Terminal #2) ચાલુ હોવા જોઈએ. આ માટે **નવી (ત્રીજી) PowerShell window** ખોલો.

```powershell
cd C:\kafka
```

**MySQL `station` ના live changes:**
```powershell
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic src.mysql.de_source_mysql.station --property print.timestamp=true --property print.key=true
```

**MySQL `station_owner` ના live changes:**
```powershell
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic src.mysql.de_source_mysql.station_owner --property print.timestamp=true --property print.key=true
```

**Mongo `weather_observations` ના live changes:**
```powershell
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic src.mongo.de_source_mongo.weather_observations --property print.timestamp=true --property print.key=true
```

**ત્રણેય source topics એક જ window માં:**
```powershell
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --include "src\..*" --property print.timestamp=true --property print.key=true --property print.topic=true
```

**કેવી રીતે વાપરવું:**
1. Command ચલાવો. Window **રાહ જોતી** દેખાશે, કારણ કે તે ફક્ત **નવા** messages બતાવે છે.
2. MySQL Workbench કે Compass માં source data માં insert/update/delete કરો.
3. 1–2 સેકન્ડમાં message આ window માં દેખાશે.
4. બંધ કરવા: `Ctrl+C`.

| Option | અર્થ |
|---|---|
| `--property print.key=true` | Message ની key (primary key / `_id`) બતાવે |
| `--property print.timestamp=true` | Message Kafka માં ક્યારે આવ્યો |
| `--property print.topic=true` | કયા topic નો message છે (એક સાથે ઘણા topics જોતી વખતે) |
| `--from-beginning` | જૂના messages પણ બતાવે (શરૂઆતથી) |
| `--max-messages 5` | 5 messages પછી જાતે બંધ થાય |

> Kafka 4.x આ warning બતાવે છે: `Option --property is deprecated ... Use --formatter-property instead`. Command હજુ ચાલે છે. Warning ટાળવી હોય તો `--property` ની જગ્યાએ `--formatter-property` લખો.

> ⚠️ **Message ન આવે તો:** પહેલા `kafka-connect\register-connectors.ps1 status` ચલાવો. એ topic નો source connector (`source-mongo` / `source-mysql`) list માં **RUNNING** હોવો જોઈએ. Connector delete થઈ ગયો હોય તો `.\register-connectors.ps1` ફરી ચલાવો.

#### F5.4 Sink એ બધું bronze માં લખ્યું? (LAG)
```powershell
.\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --describe --group connect-sink-bronze
```
| Column | અર્થ |
|---|---|
| `LOG-END-OFFSET` | Topic માં કુલ messages |
| `CURRENT-OFFSET` | Sink એ કેટલા process કર્યા |
| `LAG` | **હજુ bronze માં ન લખાયેલા** messages → **`0` = બધું bronze માં છે ✅** |

ઉદાહરણ:
```
TOPIC                                           CURRENT-OFFSET  LOG-END-OFFSET  LAG
src.mongo.de_source_mongo.weather_observations  47              47              0
src.mysql.de_source_mysql.station               6               6               0
src.mysql.de_source_mysql.station_owner         6               6               0
```

#### F5.5 Bronze માં સૌથી નવા documents
Compass નું `>_ MONGOSH` panel અથવા mongosh:
```javascript
use de_bronze
db.mysql_station.find().sort({_ingested_at: -1}).limit(3)
db.mongo_weather_observations.find({}, {op: 1, _ingested_at: 1}).sort({_ingested_at: -1}).limit(3)
```
Compass થી: collection ખોલો → **Sort** માં `{_ingested_at: -1}` → **Find**.

#### F5.6 DLQ ખાલી છે?
```powershell
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic dlq.sink-bronze --from-beginning --timeout-ms 5000
```
➡️ `Processed a total of 0 messages` = sink માં કોઈ error નથી ✅

#### F5.7 Output માંની ERROR line
```
main ERROR Reconfiguration failed: No configuration found for '5e2de80c' at 'null' in 'null'
```
**આ નુકસાનકારક નથી.** Kafka 4.x ના Windows CLI tools ને તેમની logging config file મળતી નથી, એટલે આ line આવે છે. Command સાચું કામ કરે છે. ✅

**આખો flow એક નજરમાં:**
```
Source change → (F5.3) topic માં message દેખાય → (F5.4) LAG 0 થાય → (F5.5) bronze માં નવો _ingested_at → Job #1 આગલા run માં લે
```

---

# ભાગ G – Live CDC Test (insert / update / delete)

## G1. MySQL
```sql
USE de_source_mysql;
INSERT INTO station VALUES ('TEST1','Test Station','India',23.0225,72.5714,'LAND',NOW());
UPDATE station SET station_name = 'Test Station Updated' WHERE station_code = 'TEST1';
DELETE FROM station WHERE station_code = 'TEST1';
```
દરેક statement પછી થોડી સેકન્ડમાં:
```powershell
mongosh "mongodb://localhost:27017/?replicaSet=rs0" --quiet --eval "printjson(db.getSiblingDB('de_bronze').mysql_station.findOne({'_id.station_code':'TEST1'}))"
```
| પછી | Bronze માં અપેક્ષા |
|---|---|
| INSERT | `__op: "c"`, `station_name: "Test Station"` |
| UPDATE | `__op: "u"`, `station_name: "Test Station Updated"` |
| DELETE | `__op: "d"`, `__deleted: "true"` (row ની છેલ્લી values સાથે) |

## G2. MongoDB
```powershell
mongosh "mongodb://localhost:27017/?replicaSet=rs0" --quiet --eval "
  const s = db.getSiblingDB('de_source_mongo').weather_observations;
  const r = s.insertOne({ st: 'x+00000+000000', callLetters: 'TEST', airTemperature: { value: 25.5, quality: '1' } });
  print('inserted ' + r.insertedId);
  s.updateOne({ _id: r.insertedId }, { \$set: { 'airTemperature.value': 26.1 } });
  s.deleteOne({ _id: r.insertedId });"
```
Bronze માં ચકાસો:
```powershell
mongosh "mongodb://localhost:27017/?replicaSet=rs0" --quiet --eval "db.getSiblingDB('de_bronze').mongo_weather_observations.find({op:{\$ne:'r'}}).forEach(printjson)"
```
➡️ છેલ્લી state `op: "d"`, `after: null`. ✅ (Mongo માં `__deleted` field નથી; `op = "d"` એ જ delete ની નિશાની છે.)

## G3. DLQ ખાલી છે?
```powershell
cd C:\kafka
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic dlq.sink-bronze --from-beginning --timeout-ms 5000
```
➡️ કોઈ message નહીં = sink માં કોઈ error નથી. (Topic હજુ બન્યો ન હોય તો warning આવે, તે પણ ઠીક છે.)

✅ **ભાગ G પૂરો એટલે Stage 1 → Stage 2 તૈયાર છે.** હવે application prompt (Silver/Gold) ચલાવી શકાય.

---

# ભાગ H – રોજનો Start / Stop ક્રમ

## H1. Start (આ ક્રમમાં)
```
1. MySQL, MongoDB                → Windows services: આપોઆપ ચાલુ થાય (Get-Service MySQL80, MongoDB)
2. Kafka            (Terminal #1) → C5 (heap 512m)
3. Kafka Connect    (Terminal #2) → D3 (heap 768m). Connectors જાતે resume થાય છે, ફરી register કરવાની જરૂર નથી
4. Status ચકાસો                  → E6
5. Spark pipeline app            → default profile (local[*])
```

## H2. Stop (ઉલટા ક્રમમાં)
```
1. Spark app → Stop
2. Kafka Connect terminal → Ctrl+C   (offsets સાચવીને બંધ થાય)
3. Kafka terminal → Ctrl+C
```
> **Kafka ને Connect પહેલા બંધ ન કરો.** નહીંતર Connect છેલ્લા offsets save નહીં કરી શકે, અને restart પર થોડા events ફરી આવશે. Upsert ને કારણે નુકસાન નથી, પણ સ્વચ્છ ક્રમ રાખો.

## H3. Kafka reset (crash થાય અથવા શરૂથી શરૂ કરવું હોય ત્યારે)
```powershell
# Kafka + Connect બંધ કર્યા પછી:
Remove-Item -Recurse -Force C:\kafka-data\kraft-combined-logs
cd C:\kafka
$id = (.\bin\windows\kafka-storage.bat random-uuid | Select-Object -Last 1).Trim()
.\bin\windows\kafka-storage.bat format --standalone -t $id -c .\config\server.properties
```
પછી Kafka (C5) → Connect (D3) → connectors register (E5).
Offsets ભૂંસાઈ ગયા હોવાથી connectors **ફરી snapshot** કરશે. Bronze upsert હોવાથી duplicates નહીં થાય. ✅

---

# ભાગ I – સામાન્ય સમસ્યાઓ

| સમસ્યા / Error | ઉકેલ |
|---|---|
| `mongod`/`mongosh`/`mongoimport` not recognized | A2 ના PATH ચકાસો (version folder), નવી terminal ખોલો |
| MongoDB service restart પછી શરૂ ન થાય | `mongod.cfg` નું YAML indentation (spaces, tabs નહીં). Log: `C:\Program Files\MongoDB\Server\8.0\log\mongod.log` |
| `MongoServerError: not primary` / change streams error | A4 `rs.initiate` કર્યું? `rs.status()` |
| `The input line is too long` (Kafka .bat) | Kafka ને `C:\kafka` જેવા ટૂંકા path પર રાખો |
| `bin\windows` folder જ નથી | આ Kafka version Windows scripts આપતું નથી → WSL2 માં Kafka ચલાવો (બાકીનું Windows પર રહે) |
| Kafka start: `No meta.properties found` | C4 storage format કર્યું નથી |
| Kafka crash: `AccessDeniedException` / `FileSystemException` | Windows file-locking (જાણીતી સમસ્યા) → H3 reset. વારંવાર થાય તો Kafka ને WSL2 માં ખસેડો |
| Kafka/Connect: `OutOfMemoryError` અથવા machine ધીમું | Docker Desktop, Spark cluster, OpenMetadata બંધ કરો. 0.4 જુઓ |
| `connector-plugins` માં plugin દેખાતો નથી | `plugin.path=C:/kafka/plugins` (forward slashes). દરેક plugin અલગ folder માં. Connect restart |
| source-mysql FAILED: `Access denied` | B4 user/privileges ચકાસો |
| source-mysql FAILED: `Public Key Retrieval is not allowed` | `ALTER USER 'debezium'@'localhost' IDENTIFIED WITH mysql_native_password BY 'Dbz@12345';` |
| source-mysql FAILED: `The connector is trying to read binlog ... no longer available` | Connector 30+ દિવસ બંધ રહ્યો → નવા નામથી ફરી register કરો (નવો snapshot) |
| source-mongo FAILED: `replica set` / `change stream` error | A3/A4 replica set setup |
| Bronze માં data નથી, connectors RUNNING | Topics માં data છે? (F3 consumer). DLQ (G3) જુઓ. Sink નો `topics.regex` ચકાસો |
| source-mongo FAILED: `Array elements with different Bson types are not supported` | Mongo connector માં `ExtractNewDocumentState` SMT ન વાપરો (E2 નો config) |
| sink-bronze FAILED: `Unknown topic: mysql_station_owner, does not match ...` | `topics.regex` માં `\|(mongo\|mysql)_.*` ઉમેરો (E3) |
| Connector config સુધાર્યા પછી snapshot ફરી કરવો છે | `Invoke-RestMethod -Method Put http://localhost:8083/connectors/<name>/stop` → `Invoke-RestMethod -Method Delete http://localhost:8083/connectors/<name>/offsets` → `.\register-connectors.ps1` → `Invoke-RestMethod -Method Put http://localhost:8083/connectors/<name>/resume` |
| Bronze માં source કરતા વધુ documents | Failed attempt ના જૂના format docs રહી ગયા છે (દા.ત. mongo માં `after` field વગરના) → તેમને delete કરો: `db.mongo_weather_observations.deleteMany({after: {$exists: false}})` |
| `Invoke-RestMethod ... \| Select-Object` ખાલી line બતાવે | Windows PowerShell 5.1: call ને parentheses માં મૂકો: `(Invoke-RestMethod ...) \| Select-Object ...` |
| Bronze collection નું નામ ખોટું | Sink નો `RegexRouter` regex/replacement ચકાસો |
| Connector config બદલ્યો પણ અસર નથી | E5 નો `PUT` ફરી ચલાવો (config update), પછી E6 |

---

## સારાંશ: તમે શું શીખ્યા
| Concept | ક્યાં |
|---|---|
| MongoDB replica set: CDC માટે કેમ જરૂરી | A3, A4 |
| MySQL binlog: CDC નો આધાર | B1, B4 |
| Kafka KRaft (ZooKeeper વગર) | C4, C5 |
| Kafka Connect: plugins, worker, REST API | D, E5, E6 |
| Debezium source + SMT (as-is + metadata + soft delete) | E1, E2, E4 |
| MongoDB sink: routing, upsert, DLQ | E3, E4 |
| **Initial snapshot: જૂનો data અને live changes વચ્ચે કોઈ gap નહીં** | F |
| CDC: insert/update/delete નો અંત-થી-અંત flow | G |
