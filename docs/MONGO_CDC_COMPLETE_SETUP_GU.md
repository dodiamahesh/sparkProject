# MongoDB CDC Pipeline: સંપૂર્ણ Setup Guide (એક જ document)

> **આ document શું છે?**
> MongoDB source database ના **દરેક change** (insert / update / delete) ને Kafka મારફતે **bronze database** માં લાવવાનું આખું setup.
> નવી Windows machine પર **શરૂઆતથી અંત સુધી** બધું (tools install, configuration, code, test) **ફક્ત આ એક file** થી કરી શકાય.
>
> **ટૂંકમાં:**
> `de_source_mongo` (બધા collections) → **MongoDB Source Connector** → Kafka topic **`de_source_mongo`** (એક જ topic) → **પોતાનો Java subscriber** → `de_bronze` (એ જ collection નામ, એ જ data types, + 4 metadata fields, soft delete) → Spark (Bronze → Silver)

---

## અનુક્રમણિકા

- [0. Overview: આપણે શું બનાવીએ છીએ](#0-overview-આપણે-શું-બનાવીએ-છીએ)
- [1. Java 17 અને Maven install કરો](#1-java-17-અને-maven-install-કરો)
- [2. MongoDB install અને replica set](#2-mongodb-install-અને-replica-set)
- [3. Apache Kafka install (KRaft mode)](#3-apache-kafka-install-kraft-mode)
- [4. Kafka Connect + MongoDB Connector plugin](#4-kafka-connect--mongodb-connector-plugin)
- [5. Kafka topics બનાવો](#5-kafka-topics-બનાવો)
- [6. Source connector configure અને register કરો](#6-source-connector-configure-અને-register-કરો)
- [7. Java subscriber project](#7-java-subscriber-project)
- [8. Build અને Run](#8-build-અને-run)
- [9. Example: એક document ની આખી સફર (3 JSON સરખામણી)](#9-example-એક-document-ની-આખી-સફર-3-json-સરખામણી)
- [10. End-to-end test](#10-end-to-end-test)
- [11. રોજનો Start / Stop ક્રમ](#11-રોજનો-start--stop-ક્રમ)
- [12. Operations: replay, નવો source DB, scaling, reset](#12-operations-replay-નવો-source-db-scaling-reset)
- [13. મહત્વના નિયમો (આ ભૂલો ન કરો)](#13-મહત્વના-નિયમો-આ-ભૂલો-ન-કરો)
- [14. Troubleshooting](#14-troubleshooting)
- [15. મર્યાદાઓ](#15-મર્યાદાઓ)
- [16. Setup Checklist](#16-setup-checklist)

---

## 0. Overview: આપણે શું બનાવીએ છીએ

### 0.1 Architecture

```
 ┌──────────────────────────── MongoDB (Windows service, replica set rs0, :27017) ───────────────────────────┐
 │                                                                                                             │
 │   de_source_mongo                                                       de_bronze                           │
 │   ├── weather_observations ─┐                                           ├── weather_observations  ◄──┐      │
 │   ├── student ──────────────┤                                           ├── student               ◄──┤      │
 │   └── (કોઈ પણ નવું collection)┤                                          └── (આપોઆપ બને)            ◄──┤      │
 └─────────────────────────────┼──────────────────────────────────────────────────────────────────────┼──────┘
                               │ change streams                                                       │
                               ▼                                                                      │
        ┌──── Kafka Connect (:8083) ─────────────┐                                                     │
        │  mongo-cdc-db-source                   │                                                     │
        │  (MongoSourceConnector + RegexRouter)  │                                                     │
        └──────────────────┬─────────────────────┘                                                     │
                           ▼                                                                          │
        ┌──── Kafka broker (KRaft, :9092) ───────────────────────┐                                     │
        │  topic: de_source_mongo        (3 partitions)          │                                     │
        │  topic: de_source_mongo.dlq    (ખરાબ messages)          │                                     │
        └──────────────────┬─────────────────────────────────────┘                                     │
                           ▼                                                                          │
        ┌──── mongo-cdc-subscriber (Java app, consumer group: mongo-cdc-subscriber) ──────────────────┐ │
        │  insert / replace / update → replaceOne(upsert) + metadata                                 ├─┘
        │  delete                    → _deleted: true (soft delete)                                  │
        │  metadata: _op, _deleted, _source_ts, _ingested_at                                         │
        │  write સફળ થાય પછી જ Kafka offset commit                                                    │
        └────────────────────────────────────────────────────────────────────────────────────────────┘
```

**કેમ આ design?**

| નિર્ણય | કારણ |
|---|---|
| **MongoDB Kafka Connector** (Debezium નહીં) | MongoDB નો official connector. Change event માં data types સચવાય છે (Extended JSON) |
| **એક જ topic** (source DB ના નામે) | દરેક collection માટે અલગ topic નહીં. નવું collection બને તો config બદલવો પડતો નથી |
| **Sink connector નહીં, પોતાનો subscriber** | Operation પ્રમાણે insert/update/delete નું પૂરું control આપણા code માં. Original types (`ObjectId`, `Int32`) અને field order સચવાય |
| **Bronze માં 4 metadata fields + soft delete** | Spark (Bronze → Silver) ને જાણવું પડે કે **કયા documents નવા છે** (`_ingested_at`) અને **કયા delete થયા** (`_deleted`). Original fields એ જ રહે છે, metadata એમના **પછી** ઉમેરાય છે ([7.7](#77-bronze-document-નું-format-metadata-fields)) |
| **Canonical Extended JSON** | `{"$oid": ...}`, `{"$numberInt": "98"}` જેવી type માહિતી message માં રહે, જેથી subscriber original BSON પાછું બનાવી શકે |

### 0.2 બધા નામો એક જગ્યાએ

| વસ્તુ | નામ / Value |
|---|---|
| MongoDB connection string | `mongodb://localhost:27017/?replicaSet=rs0` |
| MongoDB replica set | `rs0` |
| Source database | `de_source_mongo` |
| Destination (bronze) database | `de_bronze` |
| Kafka broker | `localhost:9092` (controller: `localhost:9093`) |
| Kafka Connect REST API | `http://localhost:8083` |
| Kafka Connect worker group | `de-connect-cluster` |
| Main CDC topic | `de_source_mongo` (3 partitions) |
| DLQ topic | `de_source_mongo.dlq` (1 partition) |
| Source connector | `mongo-cdc-db-source` |
| Subscriber consumer group | `mongo-cdc-subscriber` |
| Connect ના internal topics | `connect-configs`, `connect-offsets`, `connect-status` |

### 0.3 Versions (આ setup આ versions સાથે ચકાસેલો છે)

| Software | Version | ક્યાં |
|---|---|---|
| Windows | 11 | |
| JDK | **17** (Microsoft Build of OpenJDK 17.0.19) | `C:\Program Files\Microsoft\jdk-17.0.19.10-hotspot` |
| Apache Maven | **3.9.11** | `C:\tools\apache-maven-3.9.11` |
| MongoDB Community Server | **8.x** (આ machine પર 8.3.13) | Windows service `MongoDB` |
| mongosh | 2.x (આ machine પર 2.14.0) | |
| MongoDB Compass | કોઈ પણ નવું version (optional GUI) | |
| Apache Kafka | **4.3.1** (Scala 2.13 build) | `C:\kafka` |
| MongoDB Kafka Connector | **3.1.2** (`-all` jar) | `C:\kafka\plugins\mongo-kafka-connect` |
| Subscriber libraries | `kafka-clients` 4.0.0, `mongodb-driver-sync` 5.1.4 | Maven દ્વારા આપોઆપ |

### 0.4 Folder structure

```
C:\kafka\                                     ← Kafka installation
├── bin\windows\                              ← .bat scripts
├── config\
│   ├── server.properties                     ← broker config (Step 3.3)
│   └── connect-distributed-de.properties     ← Kafka Connect worker config (Step 4.2)
├── libs\
├── logs\                                     ← server.log, connect.log
└── plugins\
    └── mongo-kafka-connect\
        └── mongo-kafka-connect-3.1.2-all.jar ← MongoDB Kafka Connector (Step 4.1)

C:\kafka-data\
└── kraft-combined-logs\                      ← Kafka topics નો data

C:\Program Files\MongoDB\Server\<version>\bin\mongod.cfg   ← MongoDB config (Step 2.3)

D:\ClaudeProjects\dataEngineeringProjects\sparkProject\    ← project (તમારી machine પર path અલગ હોઈ શકે)
├── docs\
│   └── MONGO_CDC_COMPLETE_SETUP_GU.md        ← આ document
├── kafka-connect\
│   ├── mongo-cdc-db-source.json              ← source connector config (Step 6.1)
│   └── register-connectors.ps1               ← connectors register/status/delete script (Step 6.2)
└── mongo-cdc-subscriber\                     ← Java subscriber (અલગ Maven project)
    ├── pom.xml
    └── src\main\
        ├── java\com\de\cdc\
        │   ├── SubscriberApp.java            ← Kafka loop: read → apply → DLQ → commit
        │   └── ChangeEventApplier.java       ← change event → MongoDB write
        └── resources\
            └── subscriber.properties         ← subscriber settings
```

> ⚠️ Kafka ને **ટૂંકા path** (`C:\kafka`) માં જ રાખો. Windows `.bat` scripts લાંબા path પર `The input line is too long` error આપે છે.

### 0.5 Terminals

બધા commands **Windows PowerShell** માં છે. **(Admin)** લખ્યું હોય ત્યાં PowerShell ને *Run as Administrator* થી ખોલો.

| Terminal | શું ચાલે છે | ખુલ્લું રાખો? |
|---|---|---|
| #1 | Kafka broker | ✅ હા |
| #2 | Kafka Connect | ✅ હા |
| #3 | Subscriber (Java app) | ✅ હા |
| #4 | બાકીના commands (register, test, mongosh) | |

### 0.6 RAM

ઓછી RAM વાળી machine (આશરે 10 GB) પર આ મર્યાદાઓ રાખો:

| Component | Memory |
|---|---|
| MongoDB | WiredTiger cache **0.5 GB** (Step 2.3) |
| Kafka broker | heap **512 MB** (Step 3.5) |
| Kafka Connect | heap **768 MB** (Step 4.3) |
| Subscriber | JVM default (ઘણું ઓછું વાપરે છે) |

Docker Desktop અને Spark cluster આ setup વખતે બંધ રાખો.

---

## 1. Java 17 અને Maven install કરો

### 1.1 JDK 17

```powershell
winget install Microsoft.OpenJDK.17
```
અથવા https://learn.microsoft.com/java/openjdk/download પરથી **17** નું Windows x64 MSI install કરો.

`JAVA_HOME` set કરો. Folder નું નામ install થયેલા version પ્રમાણે ચકાસો:
```powershell
Get-ChildItem "C:\Program Files\Microsoft" -Directory -Filter "jdk-17*"
[Environment]::SetEnvironmentVariable("JAVA_HOME", "C:\Program Files\Microsoft\jdk-17.0.19.10-hotspot", "User")
```

### 1.2 Maven 3.9

```powershell
New-Item -ItemType Directory -Force C:\tools
cd $env:USERPROFILE\Downloads
Invoke-WebRequest "https://archive.apache.org/dist/maven/maven-3/3.9.11/binaries/apache-maven-3.9.11-bin.zip" -OutFile maven.zip
Expand-Archive maven.zip -DestinationPath C:\tools
Remove-Item maven.zip

$p = [Environment]::GetEnvironmentVariable("Path", "User")
[Environment]::SetEnvironmentVariable("Path", "$p;C:\tools\apache-maven-3.9.11\bin", "User")
```

### 1.3 ચકાસો (નવી PowerShell ખોલીને)
```powershell
java -version      # openjdk version "17.x"
echo $env:JAVA_HOME
mvn -v             # Apache Maven 3.9.11 ... Java version: 17.x
```

---

## 2. MongoDB install અને replica set

### 2.1 MongoDB Server, mongosh, Compass install કરો

| Tool | Download | Installer માં |
|---|---|---|
| **MongoDB Community Server** 8.x | https://www.mongodb.com/try/download/community (Windows x64, msi) | **Complete** setup, ✅ *Install MongoD as a Service*, data/log directory default |
| **mongosh** | https://www.mongodb.com/try/download/shell (Windows MSI) | default |
| **MongoDB Compass** (optional GUI) | Server installer સાથે આવે છે, અથવા https://www.mongodb.com/try/download/compass | default |

PATH માં ઉમેરો. `8.3` ની જગ્યાએ તમારું version folder લખો:
```powershell
Get-ChildItem "C:\Program Files\MongoDB\Server"
$p = [Environment]::GetEnvironmentVariable("Path", "User")
[Environment]::SetEnvironmentVariable("Path", "$p;C:\Program Files\MongoDB\Server\8.3\bin", "User")
```

નવી PowerShell માં ચકાસો:
```powershell
mongod --version
mongosh --version
```

### 2.2 Replica set કેમ જરૂરી છે?

Source connector MongoDB ના **change streams** વાપરીને changes વાંચે છે, અને change streams **ફક્ત replica set** પર જ ચાલે છે. Local machine માટે **1 node નો replica set** પૂરતો છે.

### 2.3 `mongod.cfg` બદલો **(Admin)**

**Notepad (Admin)** થી ખોલો: `C:\Program Files\MongoDB\Server\8.3\bin\mongod.cfg`

File આવી દેખાવી જોઈએ (`wiredTiger` અને `replication` ભાગ ઉમેરવાના છે):
```yaml
storage:
  dbPath: C:\Program Files\MongoDB\Server\8.3\data
  wiredTiger:
    engineConfig:
      cacheSizeGB: 0.5

systemLog:
  destination: file
  logAppend: true
  path: C:\Program Files\MongoDB\Server\8.3\log\mongod.log

net:
  port: 27017
  bindIp: 127.0.0.1

replication:
  replSetName: rs0
```

| Setting | અર્થ |
|---|---|
| `cacheSizeGB: 0.5` | MongoDB ની RAM મર્યાદા |
| `replSetName: rs0` | આ server ને `rs0` નામના replica set નો member બનાવે |

> ⚠️ YAML માં indentation **spaces** થી જ કરો, Tab થી નહીં. `replication:` line ની શરૂઆતમાં કોઈ space ન હોવી જોઈએ.

Service restart કરો **(Admin PowerShell)**:
```powershell
Restart-Service MongoDB
Get-Service MongoDB          # Status: Running
```

### 2.4 Replica set શરૂ કરો (ફક્ત પહેલી વાર)

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

હવેથી connection string **હંમેશા** આ જ વાપરો:
```
mongodb://localhost:27017/?replicaSet=rs0
```

### 2.5 Source database માં sample data

MongoDB માં database અને collection **હાથે બનાવવાની જરૂર નથી**. પહેલો document લખાય ત્યારે એ આપોઆપ બને છે.

```powershell
mongosh "mongodb://localhost:27017/?replicaSet=rs0"
```
```javascript
use de_source_mongo
db.weather_observations.insertOne({ empId: 101, name: "Ravi Kumar", department: "Data Engineering", salary: 75000 })
db.student.insertOne({ studentId: 1, name: "Dhanasvi Dodia", course: "Computer Science", marks: 98 })
show collections
```

> `de_bronze` database બનાવવાની **જરૂર નથી**. Subscriber પહેલો document લખશે ત્યારે એ આપોઆપ બનશે.

---

## 3. Apache Kafka install (KRaft mode)

> ⚠️ **જાણી લો:** Apache Kafka **officially Windows ને support કરતું નથી**. Development માટે ચાલે છે, પણ Windows ના file-locking ને કારણે **topic delete** કે log segment delete કરતી વખતે broker crash થાય છે. એટલે નીચેના settings માં auto-delete બંધ રાખ્યું છે, અને **topics ક્યારેય delete ન કરવા** ([Section 13](#13-મહત્વના-નિયમો-આ-ભૂલો-ન-કરો)).

### 3.1 Download અને extract
```powershell
cd $env:USERPROFILE\Downloads
Invoke-WebRequest -Uri "https://archive.apache.org/dist/kafka/4.3.1/kafka_2.13-4.3.1.tgz" -OutFile kafka_2.13-4.3.1.tgz
tar -xzf kafka_2.13-4.3.1.tgz
Move-Item kafka_2.13-4.3.1 C:\kafka
New-Item -ItemType Directory -Force C:\kafka-data, C:\kafka\plugins
```
ચકાસો: `C:\kafka\bin\windows\kafka-server-start.bat` હોવી જોઈએ.

### 3.2 Kafka ને Java 17 મળે છે?
```powershell
echo $env:JAVA_HOME
java -version
```

### 3.3 `C:\kafka\config\server.properties`

Kafka 4.x ની default `server.properties` પહેલેથી **KRaft combined mode** (broker + controller એક જ process) માટે તૈયાર છે. Notepad થી ખોલીને નીચેના settings **શોધીને બદલો**, અને ન હોય તો અંતે ઉમેરો:

```properties
# Topics no data (forward slashes!)
log.dirs=C:/kafka-data/kraft-combined-logs

# Windows: never auto-delete old data (segment delete is what crashes Kafka on Windows)
log.retention.ms=-1
log.retention.bytes=-1

# Windows: no compaction (file rename crash)
log.cleaner.enable=false

# Single broker
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
```

આ settings **default જ રહેવા દો** (ચકાસી લો):
```properties
process.roles=broker,controller
node.id=1
controller.quorum.bootstrap.servers=localhost:9093
listeners=PLAINTEXT://:9092,CONTROLLER://:9093
advertised.listeners=PLAINTEXT://localhost:9092,CONTROLLER://localhost:9093
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
```

| Setting | અર્થ |
|---|---|
| `log.dirs` | Topics નો data ક્યાં રાખવો |
| `log.retention.ms=-1`, `log.retention.bytes=-1` | Messages **ક્યારેય auto-delete નહીં** થાય. Windows પર delete operation crash નું કારણ છે. ફાયદો: topic માં શરૂઆતથી બધો history રહે, એટલે replay ([12.1](#121-replay-destination-ને-શરૂઆતથી-ફરી-બનાવવું)) હંમેશા શક્ય |
| `log.cleaner.enable=false` | Compaction બંધ |
| replication factor `1` | એક જ broker છે |

> Retention બંધ હોવાથી `C:\kafka-data` ધીમે ધીમે મોટું થશે. Local learning માટે ઠીક છે. જરૂર પડે તો reset ([12.4](#124-kafka-reset-બધું-નવેસરથી)).

### 3.4 Storage format (ફક્ત પહેલી વાર)

KRaft mode માં ZooKeeper નથી. Kafka પોતે metadata સંભાળે છે, એ માટે storage format કરવો પડે:
```powershell
cd C:\kafka
$id = (.\bin\windows\kafka-storage.bat random-uuid | Select-Object -Last 1).Trim()
$id
.\bin\windows\kafka-storage.bat format --standalone -t $id -c .\config\server.properties
```
➡️ છેલ્લે `Formatting dynamic metadata voter directory C:/kafka-data/kraft-combined-logs ...` આવે એટલે સફળ.

> દરેક Kafka command ની શરૂઆતમાં `ERROR Reconfiguration failed: No configuration found...` line આવે છે. એ **ફક્ત logging ની ચેતવણી** છે. અવગણો.

### 3.5 Kafka start કરો (Terminal #1, ખુલ્લું રાખો)
```powershell
cd C:\kafka
$env:KAFKA_HEAP_OPTS = "-Xms512m -Xmx512m"
.\bin\windows\kafka-server-start.bat .\config\server.properties
```
➡️ Log માં `Kafka Server started` દેખાય એટલે તૈયાર.

### 3.6 ચકાસો (Terminal #4)
```powershell
cd C:\kafka
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --list
```
➡️ Error વગર (ખાલી કે થોડા internal topics સાથે) list આવે.

---

## 4. Kafka Connect + MongoDB Connector plugin

### 4.1 Plugin download
```powershell
New-Item -ItemType Directory -Force C:\kafka\plugins\mongo-kafka-connect
Invoke-WebRequest "https://repo1.maven.org/maven2/org/mongodb/kafka/mongo-kafka-connect/3.1.2/mongo-kafka-connect-3.1.2-all.jar" -OutFile C:\kafka\plugins\mongo-kafka-connect\mongo-kafka-connect-3.1.2-all.jar
Get-ChildItem C:\kafka\plugins -Recurse
```
> **`-all` વાળી jar જ** લો. એમાં MongoDB Java driver પણ અંદર છે. **દરેક plugin અલગ folder** માં રાખો.

### 4.2 Worker config: `C:\kafka\config\connect-distributed-de.properties`
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
| `key/value.converter` | Worker નો default converter. આપણો source connector એને **override** કરે છે (`StringConverter`, Step 6.1) |
| `*.storage.topic` | Connect પોતાનું state Kafka topics માં રાખે છે: connector configs, **source offsets (ક્યાં સુધી વાંચ્યું)**, status |
| `listeners` | REST API: connectors register/manage કરવા |
| `plugin.path` | Plugins નું folder (**forward slashes**) |

### 4.3 Kafka Connect start કરો (Terminal #2, ખુલ્લું રાખો)
```powershell
cd C:\kafka
$env:KAFKA_HEAP_OPTS = "-Xms512m -Xmx768m"
.\bin\windows\connect-distributed.bat .\config\connect-distributed-de.properties
```
પહેલી વાર start થવામાં 30–60 સેકન્ડ લાગે (plugins scan). પછી `Kafka Connect started` દેખાય.

### 4.4 Plugin load થયો? (Terminal #4)
```powershell
(Invoke-RestMethod http://localhost:8083/connector-plugins) | Select-Object class, version
```
➡️ આ દેખાવું જોઈએ:
```
com.mongodb.kafka.connect.MongoSourceConnector   3.1.2
com.mongodb.kafka.connect.MongoSinkConnector     3.1.2
```
(Kafka ના built-in `MirrorMaker` plugins પણ દેખાશે, તે સામાન્ય છે.)

> ⚠️ **Parentheses `( )` જરૂરી છે.** Windows PowerShell 5.1 માં parentheses વગર `Select-Object` ખાલી line બતાવે છે.
> `Unable to connect to the remote server` આવે તો Connect હજુ start થઈ રહ્યું છે, અથવા Kafka broker બંધ છે.

---

## 5. Kafka topics બનાવો

Topics **પહેલેથી** બનાવો, જેથી partitions ની સંખ્યા આપણે નક્કી કરીએ. Kafka આપોઆપ બનાવે તો ફક્ત 1 partition આપે.

```powershell
cd C:\kafka

# Main CDC topic: source database નું નામ
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --create --topic de_source_mongo --partitions 3 --replication-factor 1

# DLQ: subscriber વાંચી ન શકે એવા messages
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --create --topic de_source_mongo.dlq --partitions 1 --replication-factor 1

.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --describe --topic de_source_mongo
```
➡️ `PartitionCount: 3`

| નિર્ણય | કારણ |
|---|---|
| **3 partitions** | પછીથી subscriber ના 3 instances સુધી parallel ચલાવી શકાય |
| Order ની ખાતરી | Message નો **key = document નો `_id`**. Kafka એક જ key ને **હંમેશા એક જ partition** માં મોકલે, અને partition ની અંદર ક્રમ સચવાય. એટલે **એક document ના બધા changes ક્રમમાં જ** આવે |

> Topic નામમાં `.` અને `_` બંને હોય તો Kafka ચેતવણી આપે છે (metrics માં બંને સરખા દેખાય). અવગણી શકાય.

---

## 6. Source connector configure અને register કરો

### 6.1 `kafka-connect\mongo-cdc-db-source.json`

```powershell
New-Item -ItemType Directory -Force D:\ClaudeProjects\dataEngineeringProjects\sparkProject\kafka-connect
```

```json
{
  "connector.class": "com.mongodb.kafka.connect.MongoSourceConnector",
  "tasks.max": "1",
  "connection.uri": "mongodb://localhost:27017/?replicaSet=rs0",
  "database": "de_source_mongo",
  "topic.prefix": "cdc",
  "startup.mode": "copy_existing",
  "change.stream.full.document": "updateLookup",
  "publish.full.document.only": "false",
  "output.format.key": "json",
  "output.format.value": "json",
  "output.json.formatter": "com.mongodb.kafka.connect.source.json.formatter.ExtendedJson",
  "key.converter": "org.apache.kafka.connect.storage.StringConverter",
  "value.converter": "org.apache.kafka.connect.storage.StringConverter",
  "transforms": "dbTopic",
  "transforms.dbTopic.type": "org.apache.kafka.connect.transforms.RegexRouter",
  "transforms.dbTopic.regex": "cdc\\.([^.]+)\\..*",
  "transforms.dbTopic.replacement": "$1"
}
```

| Setting | અર્થ |
|---|---|
| `database` (`collection` વગર) | આ database ના **બધા collections** watch થાય. નવું collection બને તો એ પણ આપોઆપ |
| `topic.prefix: cdc` | Connector પહેલાં `cdc.de_source_mongo.<collection>` નામ બનાવે... |
| `transforms.dbTopic` (RegexRouter) | ...અને આ transform એને બદલીને ફક્ત **`de_source_mongo`** કરે (regex નો `$1` = database નામ). એટલે **બધા collections એક જ topic** માં. આ ફેરફાર Kafka માં લખતા **પહેલાં** થાય છે, એટલે `cdc.*` નામનો કોઈ topic બનતો નથી |
| `startup.mode: copy_existing` | **પહેલી વાર** હાલના બધા documents ને `insert` events તરીકે મોકલે, પછી live changes |
| `change.stream.full.document: updateLookup` | Update event સાથે document ની **આખી નવી copy** (`fullDocument`) પણ આવે |
| `publish.full.document.only: false` | **આખો change event** મોકલે: `operationType`, `ns`, `documentKey`, `fullDocument`... **Delete events પણ** આવે |
| `output.format.key/value: json` | Key અને value JSON text તરીકે |
| `output.json.formatter: ExtendedJson` | **Canonical Extended JSON**: `{"$oid"}`, `{"$numberInt"}`, `{"$date"}`. Types ની માહિતી સચવાય |
| `key/value.converter: StringConverter` | Connector નું JSON text **જેમ છે તેમ** Kafka માં જાય. Worker નો `JsonConverter` એને ફરી wrap ન કરે |

### 6.2 `kafka-connect\register-connectors.ps1`

Folder ની બધી connector JSON files ને Connect માં register કરવા, status જોવા અને delete કરવા માટેની script. **File નું નામ = connector નું નામ.**

```powershell
# Manage Kafka Connect connectors from the JSON files in this folder.
# Usage:
#   .\register-connectors.ps1              -> create/update all connectors (mongo-cdc-db-source)
#   .\register-connectors.ps1 status       -> show connector + task state
#   .\register-connectors.ps1 delete NAME  -> delete one connector (its offsets stay in Kafka)
param(
    [string]$Action = "apply",
    [string]$Name
)

$connect = "http://localhost:8083"
$names = "mongo-cdc-db-source"

function Show-Status {
    foreach ($n in (Invoke-RestMethod "$connect/connectors")) {
        $s = Invoke-RestMethod "$connect/connectors/$n/status"
        $tasks = ($s.tasks | ForEach-Object { $_.state }) -join ","
        "{0,-14} connector={1,-8} tasks={2}" -f $n, $s.connector.state, $tasks
        foreach ($t in $s.tasks) { if ($t.state -eq "FAILED") { "   ERROR: " + ($t.trace -split "`n")[0] } }
    }
}

switch ($Action) {
    "apply" {
        foreach ($n in $names) {
            $file = Join-Path $PSScriptRoot "$n.json"
            # PUT .../config creates the connector or updates it if it already exists
            Invoke-RestMethod -Method Put -Uri "$connect/connectors/$n/config" -ContentType "application/json" -InFile $file | Out-Null
            "applied $n"
        }
        Start-Sleep -Seconds 5
        Show-Status
    }
    "status" { Show-Status }
    "delete" {
        if (-not $Name) { throw "Usage: .\register-connectors.ps1 delete <name>" }
        Invoke-RestMethod -Method Delete -Uri "$connect/connectors/$Name" | Out-Null
        "deleted $Name"
    }
    default { throw "Unknown action '$Action' (use apply | status | delete)" }
}
```

### 6.3 Register કરો (Terminal #4)
```powershell
cd D:\ClaudeProjects\dataEngineeringProjects\sparkProject\kafka-connect
.\register-connectors.ps1
```
➡️
```
applied mongo-cdc-db-source
mongo-cdc-db-source connector=RUNNING  tasks=RUNNING
```

> Script ચલાવવાની permission ન હોય (`running scripts is disabled`) તો એક વાર:
> `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`

### 6.4 Topic માં messages ચકાસો
```powershell
cd C:\kafka
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic de_source_mongo --from-beginning --formatter-property print.key=true --formatter-property print.partition=true
```
➡️ `copy_existing` ને કારણે બંને collections (`weather_observations`, `student`) ના documents **એક જ topic** માં `"operationType": "insert"` સાથે દેખાય. Message નું પૂરું structure [Section 9](#9-example-એક-document-ની-આખી-સફર-3-json-સરખામણી) માં છે.

ચકાસો:
- [ ] `ns.coll` માં અલગ અલગ collections, પણ topic એક જ
- [ ] `{"$numberInt": "..."}` અને `{"$oid": "..."}` દેખાય છે (types સચવાયા)
- [ ] `kafka-topics.bat --list` માં `cdc.*` નામનો કોઈ topic **નથી**

`Ctrl + C` થી બંધ કરો.

---

## 7. Java subscriber project

Subscriber એક **અલગ Maven project** છે, જેથી બીજા project ની dependencies સાથે ભળે નહીં.

### 7.1 Folders બનાવો
```powershell
cd D:\ClaudeProjects\dataEngineeringProjects\sparkProject
New-Item -ItemType Directory -Force mongo-cdc-subscriber\src\main\java\com\de\cdc
New-Item -ItemType Directory -Force mongo-cdc-subscriber\src\main\resources
```

### 7.2 `mongo-cdc-subscriber\pom.xml`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.de</groupId>
    <artifactId>mongo-cdc-subscriber</artifactId>
    <version>1.0.0</version>

    <properties>
        <maven.compiler.release>17</maven.compiler.release>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-clients</artifactId>
            <version>4.0.0</version>
        </dependency>
        <dependency>
            <groupId>org.mongodb</groupId>
            <artifactId>mongodb-driver-sync</artifactId>
            <version>5.1.4</version>
        </dependency>
        <!-- slf4j-api 2.x must win over the 1.7.x that kafka-clients pulls in -->
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
            <version>2.0.13</version>
        </dependency>
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-simple</artifactId>
            <version>2.0.13</version>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <!-- One runnable jar with all dependencies -->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-assembly-plugin</artifactId>
                <version>3.7.1</version>
                <configuration>
                    <descriptorRefs>
                        <descriptorRef>jar-with-dependencies</descriptorRef>
                    </descriptorRefs>
                    <archive>
                        <manifest>
                            <mainClass>com.de.cdc.SubscriberApp</mainClass>
                        </manifest>
                    </archive>
                </configuration>
                <executions>
                    <execution>
                        <phase>package</phase>
                        <goals>
                            <goal>single</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
```

| Dependency / Plugin | કામ |
|---|---|
| `kafka-clients` | Kafka consumer (topic વાંચવા) અને producer (DLQ માં લખવા) |
| `mongodb-driver-sync` | MongoDB માં લખવા. એમાં `BsonDocument.parse()` છે, જે Extended JSON ને **original BSON types** માં ફેરવે છે |
| `slf4j-api` + `slf4j-simple` | Console logs |
| `maven-assembly-plugin` | બધી dependencies સાથે **એક જ runnable jar** બનાવે |

### 7.3 `mongo-cdc-subscriber\src\main\resources\subscriber.properties`

```properties
# ---- Kafka ----
kafka.bootstrap.servers=localhost:9092
kafka.topic=de_source_mongo
kafka.group.id=mongo-cdc-subscriber
kafka.dlq.topic=de_source_mongo.dlq

# ---- MongoDB ----
mongo.uri=mongodb://localhost:27017/?replicaSet=rs0
# Every source collection is written to the collection with the same name in this database
target.database=de_bronze

# Apply "drop collection" events to the target? (false = only log them)
apply.drop=false

# ---- Logging (slf4j-simple) ----
org.slf4j.simpleLogger.defaultLogLevel=info
org.slf4j.simpleLogger.log.org.apache.kafka=warn
org.slf4j.simpleLogger.showDateTime=true
```

| Setting | અર્થ |
|---|---|
| `kafka.topic` | વાંચવાનો topic (source DB નું નામ) |
| `kafka.group.id` | Consumer group. Kafka **આ નામે** યાદ રાખે છે કે ક્યાં સુધી વાંચ્યું. એ જ નામે ફરી start કરો તો ત્યાંથી જ આગળ વાંચે |
| `kafka.dlq.topic` | વાંચી ન શકાય એવા messages અહીં જાય |
| `target.database` | Destination database. Collection નું નામ source જેવું જ રહે છે (`ns.coll`). Database ન હોય તો MongoDB **આપોઆપ બનાવે** |
| `apply.drop` | Source માં આખું collection drop થાય ત્યારે destination માં પણ drop કરવું? સલામતી માટે default `false` (ફક્ત log) |
| `org.slf4j.*` | Log level. Kafka library ના logs ફક્ત `warn` |

### 7.4 `src\main\java\com\de\cdc\ChangeEventApplier.java`

દરેક change event ને MongoDB write માં ફેરવે છે અને destination માં લાગુ કરે છે.

```java
package com.de.cdc;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Applies MongoDB change events to the target (bronze) database.
 *
 * Each Kafka message is one change event from the MongoDB source connector, for example:
 * <pre>
 * {
 *   "operationType": "update",
 *   "wallTime":     { "$date": { "$numberLong": "1793128500123" } },
 *   "ns":           { "db": "de_source_mongo", "coll": "student" },
 *   "documentKey":  { "_id": { "$oid": "6ae0e6da..." } },
 *   "fullDocument": { "_id": { "$oid": "6ae0e6da..." }, "name": "Dhanasvi", "marks": { "$numberInt": "98" } }
 * }
 * </pre>
 *
 * Every event is written to the collection with the same name ("ns.coll") in the target database:
 * <pre>
 *   insert / replace / update  ->  replace the whole document (insert it if it is missing)
 *   delete                     ->  keep the document, mark it _deleted: true (soft delete)
 *   drop                       ->  drop the collection (only when apply.drop=true)
 * </pre>
 *
 * After the original fields, every bronze document gets 4 metadata fields, used by Spark (Bronze -> Silver):
 * <pre>
 *   _op           last operation: "insert", "update", "replace" or "delete"
 *   _deleted      true after a delete, otherwise false
 *   _source_ts    when the change happened in the source (epoch millis, from wallTime)
 *   _ingested_at  when this subscriber wrote it to bronze (epoch millis); Spark reads new data by this field
 * </pre>
 *
 * Replacing or updating by _id gives the same result even if a message is applied twice,
 * so reading the same messages again after a crash is safe.
 */
public class ChangeEventApplier {

    private static final Logger log = LoggerFactory.getLogger(ChangeEventApplier.class);

    /** A message that could not be read (bad JSON or missing fields). SubscriberApp sends it to the DLQ topic. */
    public record FailedMessage(ConsumerRecord<String, String> message, Exception error) { }

    private final MongoDatabase targetDb;
    private final boolean applyDropEvents;

    public ChangeEventApplier(MongoDatabase targetDb, boolean applyDropEvents) {
        this.targetDb = targetDb;
        this.applyDropEvents = applyDropEvents;
    }

    /**
     * Applies all messages from one Kafka poll.
     *
     * The writes are collected per collection first and then saved with one bulk write per collection.
     * That is much faster than one database call per message. The bulk write keeps the list order,
     * so several changes to the same document are applied in the order they happened.
     *
     * @return the messages that could not be read
     */
    public List<FailedMessage> applyAll(ConsumerRecords<String, String> messages) {
        // collection name -> writes for that collection, in arrival order
        Map<String, List<WriteModel<BsonDocument>>> writesByCollection = new LinkedHashMap<>();
        List<FailedMessage> failedMessages = new ArrayList<>();
        // one ingestion time for the whole batch
        long ingestedAt = System.currentTimeMillis();

        for (ConsumerRecord<String, String> message : messages) {
            if (message.value() == null) {
                continue; // empty message (tombstone): nothing to apply
            }

            // Step 1: read the event. A message we cannot read is set aside; it must not stop the others.
            String operation;
            String collection;
            WriteModel<BsonDocument> write;
            try {
                // parse() turns Extended JSON back into the original types: {"$oid"} -> ObjectId, {"$numberInt"} -> Int32
                BsonDocument event = BsonDocument.parse(message.value());
                operation = event.getString("operationType").getValue();
                collection = getCollectionName(event);
                write = buildWrite(operation, event, ingestedAt);
            } catch (RuntimeException e) {
                failedMessages.add(new FailedMessage(message, e));
                continue;
            }

            // Step 2: queue the write, or handle events that are not about a single document
            if (write != null) {
                addWrite(writesByCollection, collection, write);
            } else if (operation.equals("drop")) {
                // save the changes that came before the drop first, then drop
                saveWrites(collection, writesByCollection.remove(collection));
                dropCollection(collection);
            } else {
                log.warn("Skipping '{}' event (partition {}, offset {})", operation, message.partition(), message.offset());
            }
        }

        // Step 3: save everything that was queued
        for (Map.Entry<String, List<WriteModel<BsonDocument>>> entry : writesByCollection.entrySet()) {
            saveWrites(entry.getKey(), entry.getValue());
        }
        return failedMessages;
    }

    /** Builds the MongoDB write for one event, or returns null when there is no document to write. */
    private static WriteModel<BsonDocument> buildWrite(String operation, BsonDocument event, long ingestedAt) {
        BsonDocument metadata = buildMetadata(operation, event, ingestedAt);
        switch (operation) {
            case "insert":
            case "replace":
                return replaceDocument(event, event.getDocument("fullDocument"), metadata);
            case "update":
                return updateDocument(event, metadata);
            case "delete":
                return markDeleted(event, metadata);
            default:
                // drop, rename, dropDatabase, invalidate: these are not changes to a single document
                return null;
        }
    }

    /** The 4 metadata fields that are added after the original fields of every bronze document. */
    private static BsonDocument buildMetadata(String operation, BsonDocument event, long ingestedAt) {
        BsonDocument metadata = new BsonDocument();
        metadata.put("_op", new BsonString(operation));
        metadata.put("_deleted", BsonBoolean.valueOf(operation.equals("delete")));
        metadata.put("_source_ts", new BsonInt64(getSourceTime(event, ingestedAt)));
        metadata.put("_ingested_at", new BsonInt64(ingestedAt));
        return metadata;
    }

    /**
     * Replaces the document with newDocument + metadata. upsert = insert it if it does not exist yet.
     * clone() keeps the event unchanged; the original fields stay first, the metadata comes after them.
     */
    private static WriteModel<BsonDocument> replaceDocument(BsonDocument event, BsonDocument newDocument,
                                                           BsonDocument metadata) {
        BsonDocument bronzeDocument = newDocument.clone();
        bronzeDocument.putAll(metadata);
        return new ReplaceOneModel<>(matchById(event), bronzeDocument, new ReplaceOptions().upsert(true));
    }

    /**
     * The source connector asks MongoDB for the full document after every update (updateLookup),
     * so normally we simply replace the whole document.
     *
     * If the document was deleted right after the update, MongoDB sends "fullDocument": null.
     * Then we apply only what the update changed: the changed fields ($set) and the removed fields ($unset).
     */
    private static WriteModel<BsonDocument> updateDocument(BsonDocument event, BsonDocument metadata) {
        BsonValue fullDocument = event.get("fullDocument");
        if (fullDocument != null && fullDocument.isDocument()) {
            return replaceDocument(event, fullDocument.asDocument(), metadata);
        }

        BsonDocument updateDescription = event.getDocument("updateDescription");

        // changed fields + metadata, for example { "marks": 99 }  ->  { "$set": { "marks": 99, "_op": "update", ... } }
        BsonDocument setFields = updateDescription.getDocument("updatedFields", new BsonDocument()).clone();
        setFields.putAll(metadata);
        BsonDocument changes = new BsonDocument("$set", setFields);

        // removed fields, for example [ "course" ]  ->  { "$unset": { "course": "" } }
        BsonDocument removedFields = new BsonDocument();
        for (BsonValue fieldName : updateDescription.getArray("removedFields", new BsonArray())) {
            removedFields.put(fieldName.asString().getValue(), new BsonString(""));
        }
        if (!removedFields.isEmpty()) {
            changes.put("$unset", removedFields);
        }
        return new UpdateOneModel<>(matchById(event), changes);
    }

    /**
     * Soft delete: the document stays in bronze with its last values, only the metadata changes
     * (_deleted: true). Spark sees it in the next run and marks the silver row as deleted.
     * upsert: if bronze never had the document, a small { _id, metadata } document is created.
     */
    private static WriteModel<BsonDocument> markDeleted(BsonDocument event, BsonDocument metadata) {
        return new UpdateOneModel<>(matchById(event), new BsonDocument("$set", metadata), new UpdateOptions().upsert(true));
    }

    /** Filter that finds the changed document: { "_id": documentKey._id } */
    private static Bson matchById(BsonDocument event) {
        BsonValue id = event.getDocument("documentKey").get("_id");
        return Filters.eq("_id", id);
    }

    /**
     * When the change happened in the source: wallTime, or clusterTime (seconds) on old MongoDB versions.
     * Events from startup.mode=copy_existing ("_id": { "copyingData": true }) have neither,
     * so for them we use the time the document was copied (ingestedAt).
     */
    private static long getSourceTime(BsonDocument event, long ingestedAt) {
        if (event.containsKey("wallTime")) {
            return event.getDateTime("wallTime").getValue();
        }
        if (event.containsKey("clusterTime")) {
            return event.getTimestamp("clusterTime").getTime() * 1000L;
        }
        return ingestedAt;
    }

    /** Collection name from "ns.coll". dropDatabase and invalidate events have no collection, so this returns null. */
    private static String getCollectionName(BsonDocument event) {
        if (!event.containsKey("ns")) {
            return null;
        }
        BsonDocument namespace = event.getDocument("ns");
        if (!namespace.containsKey("coll")) {
            return null;
        }
        return namespace.getString("coll").getValue();
    }

    private static void addWrite(Map<String, List<WriteModel<BsonDocument>>> writesByCollection,
                                 String collection, WriteModel<BsonDocument> write) {
        List<WriteModel<BsonDocument>> writes = writesByCollection.get(collection);
        if (writes == null) {
            writes = new ArrayList<>();
            writesByCollection.put(collection, writes);
        }
        writes.add(write);
    }

    /** Saves the writes in one call. ordered = apply them in list order and stop at the first error. */
    private void saveWrites(String collection, List<WriteModel<BsonDocument>> writes) {
        if (writes == null || writes.isEmpty()) {
            return;
        }
        BulkWriteResult result = targetDb.getCollection(collection, BsonDocument.class)
                .bulkWrite(writes, new BulkWriteOptions().ordered(true));

        log.info("{}: {} writes (matched={}, inserted={})",
                collection, writes.size(), result.getMatchedCount(), result.getUpserts().size());
    }

    private void dropCollection(String collection) {
        if (applyDropEvents) {
            targetDb.getCollection(collection).drop();
            log.warn("{}: collection dropped, because it was dropped in the source", collection);
        } else {
            log.warn("{}: collection was dropped in the source; target kept because apply.drop=false", collection);
        }
    }
}
```

### 7.5 `src\main\java\com\de\cdc\SubscriberApp.java`

Main program: Kafka માંથી વાંચે, `ChangeEventApplier` ને આપે, ખરાબ messages DLQ માં મોકલે, અને offset commit કરે.

```java
package com.de.cdc;

import com.de.cdc.ChangeEventApplier.FailedMessage;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Entry point of the subscriber. It repeats these 4 steps:
 * <pre>
 *   1. Read a batch of change events from the Kafka topic (for example "de_source_mongo").
 *   2. Apply them to the target MongoDB database (see ChangeEventApplier).
 *   3. Send the messages that could not be read to the DLQ topic.
 *   4. Tell Kafka that the batch is done (commit the offsets).
 * </pre>
 * Step 4 runs only after step 2 succeeded. If the app crashes, Kafka gives the same messages
 * again after the restart, so no change is lost. Applying a message twice is safe.
 *
 * Usage: java -jar mongo-cdc-subscriber-1.0.0-jar-with-dependencies.jar [path\to\subscriber.properties]
 */
public class SubscriberApp {

    public static void main(String[] args) throws IOException {
        Properties config = loadConfig(args);
        setUpLogging(config);
        Logger log = LoggerFactory.getLogger(SubscriberApp.class);

        String topic = config.getProperty("kafka.topic");
        String dlqTopic = config.getProperty("kafka.dlq.topic");
        String targetDbName = config.getProperty("target.database");
        boolean applyDrop = Boolean.parseBoolean(config.getProperty("apply.drop", "false"));

        // try-with-resources: MongoDB, the consumer and the producer are closed automatically at the end
        try (MongoClient mongo = MongoClients.create(config.getProperty("mongo.uri"));
             KafkaConsumer<String, String> consumer = createConsumer(config);
             KafkaProducer<String, String> dlqProducer = createDlqProducer(config)) {

            ChangeEventApplier applier = new ChangeEventApplier(mongo.getDatabase(targetDbName), applyDrop);
            stopOnCtrlC(consumer);

            consumer.subscribe(List.of(topic));
            log.info("Reading topic '{}' and writing to database '{}'", topic, targetDbName);

            while (true) {
                // 1. Read the next batch (waits up to 1 second for new messages)
                ConsumerRecords<String, String> messages = consumer.poll(Duration.ofSeconds(1));
                if (messages.isEmpty()) {
                    continue;
                }

                // 2. Apply the changes to MongoDB
                List<FailedMessage> failedMessages = applier.applyAll(messages);

                // 3. Send the messages that could not be read to the DLQ topic
                for (FailedMessage failed : failedMessages) {
                    ConsumerRecord<String, String> message = failed.message();
                    log.error("Could not read message (partition {}, offset {}), sending it to '{}'",
                            message.partition(), message.offset(), dlqTopic, failed.error());
                    sendToDlq(dlqProducer, dlqTopic, failed);
                }
                dlqProducer.flush();

                // 4. Mark the batch as done
                consumer.commitSync();
            }
        } catch (WakeupException e) {
            // poll() throws this after Ctrl+C (see stopOnCtrlC)
            log.info("Stopped");
        }
    }

    /** Reads subscriber.properties from the path given on the command line, or from inside the jar. */
    private static Properties loadConfig(String[] args) throws IOException {
        Properties config = new Properties();
        if (args.length > 0) {
            try (InputStream input = Files.newInputStream(Path.of(args[0]))) {
                config.load(input);
            }
        } else {
            try (InputStream input = SubscriberApp.class.getResourceAsStream("/subscriber.properties")) {
                config.load(input);
            }
        }
        return config;
    }

    /**
     * slf4j-simple reads its settings from system properties, so copy the "org.slf4j.*" lines
     * from the config there. This must happen before the first logger is created.
     */
    private static void setUpLogging(Properties config) {
        for (String key : config.stringPropertyNames()) {
            if (key.startsWith("org.slf4j.")) {
                System.setProperty(key, config.getProperty(key));
            }
        }
    }

    private static KafkaConsumer<String, String> createConsumer(Properties config) {
        Properties settings = new Properties();
        settings.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getProperty("kafka.bootstrap.servers"));
        // Kafka remembers how far this group has read, so a restart continues from there
        settings.put(ConsumerConfig.GROUP_ID_CONFIG, config.getProperty("kafka.group.id"));
        // key and value are plain JSON text
        settings.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        settings.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // we commit ourselves, only after the MongoDB writes succeeded
        settings.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // a new group starts at the beginning of the topic
        settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // at most 500 messages per batch
        settings.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
        return new KafkaConsumer<>(settings);
    }

    private static KafkaProducer<String, String> createDlqProducer(Properties config) {
        Properties settings = new Properties();
        settings.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getProperty("kafka.bootstrap.servers"));
        settings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        settings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // wait until Kafka has safely stored the DLQ message
        settings.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(settings);
    }

    /** Copies the bad message to the DLQ topic, with two headers: the error, and where the message came from. */
    private static void sendToDlq(KafkaProducer<String, String> producer, String dlqTopic, FailedMessage failed) {
        ConsumerRecord<String, String> original = failed.message();
        String source = original.topic() + "-" + original.partition() + "-" + original.offset();

        ProducerRecord<String, String> copy = new ProducerRecord<>(dlqTopic, original.key(), original.value());
        copy.headers().add("error", String.valueOf(failed.error()).getBytes(UTF_8));
        copy.headers().add("source", source.getBytes(UTF_8));
        producer.send(copy);
    }

    /**
     * Ctrl+C runs this hook. consumer.wakeup() makes the waiting poll() throw WakeupException,
     * so the main loop ends and everything is closed normally. The hook waits until that is done.
     */
    private static void stopOnCtrlC(KafkaConsumer<String, String> consumer) {
        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
    }
}
```

### 7.6 Code સમજો: એક batch નો આખો રસ્તો

```
SubscriberApp.main()
 │
 ├─ loadConfig()          subscriber.properties વાંચે
 ├─ setUpLogging()        org.slf4j.* settings લાગુ કરે
 ├─ createConsumer()      Kafka માંથી વાંચવા માટે
 ├─ createDlqProducer()   ખરાબ messages DLQ માં મોકલવા માટે
 ├─ stopOnCtrlC()         Ctrl+C વખતે સાફ રીતે બંધ થવા માટે
 │
 └─ while (true)  ── દરેક batch માટે 4 steps ──
       │
       ├─ 1. consumer.poll()                 → 500 સુધી messages
       │
       ├─ 2. applier.applyAll(messages)      ── ChangeEventApplier ──
       │        │
       │        ├─ દરેક message માટે:
       │        │    ├─ BsonDocument.parse()     JSON → original BSON types
       │        │    ├─ operationType વાંચો      insert / update / delete / ...
       │        │    ├─ getCollectionName()      ns.coll → "student"
       │        │    └─ buildWrite()
       │        │         ├─ buildMetadata()     _op, _deleted, _source_ts, _ingested_at
       │        │         ├─ insert, replace → replaceDocument()   (upsert, document + metadata)
       │        │         ├─ update          → updateDocument()    (replace, અથવા $set/$unset)
       │        │         ├─ delete          → markDeleted()       (soft delete: _deleted = true)
       │        │         └─ drop/rename/... → null → dropCollection() અથવા skip
       │        │
       │        ├─ addWrite()      write ને collection ની list માં મૂકે
       │        └─ saveWrites()    દરેક collection માટે એક bulkWrite (ordered)
       │
       ├─ 3. sendToDlq()                     → વાંચી ન શકાયેલા messages
       │
       └─ 4. consumer.commitSync()           → Kafka ને કહે: "અહીં સુધી પૂરું"
```

| `operationType` | Subscriber શું કરે છે | કેમ? |
|---|---|---|
| `insert` | `replaceOne({_id}, fullDocument + metadata, upsert)` | `insertOne` ફરી ચાલે તો *duplicate key* error આપે. Upsert થી એ જ message બે વાર આવે તો પણ પરિણામ એ જ (**idempotent**) |
| `replace` | `replaceOne({_id}, fullDocument + metadata, upsert)` | આખો document બદલાયો |
| `update` (`fullDocument` હોય) | `replaceOne({_id}, fullDocument + metadata, upsert)` | `updateLookup` આખી નવી copy આપે છે |
| `update` (`fullDocument` = null) | `updateOne({_id}, {$set: changes + metadata, $unset})` | Update પછી તરત document delete થયો હોય ત્યારે lookup ખાલી આવે. ત્યારે ફક્ત બદલાયેલા fields |
| `delete` | `updateOne({_id}, {$set: {_op: "delete", _deleted: true, ...}}, upsert)` | **Soft delete:** document છેલ્લી values સાથે bronze માં **રહે છે**, ફક્ત `_deleted: true` થાય. Spark એને જોઈને silver માં પણ deleted mark કરે છે |
| `drop` | `apply.drop=true` હોય તો collection drop, નહીં તો ફક્ત warning | ભૂલથી data ન જાય |
| `rename`, `dropDatabase`, `invalidate` | Warning log, skip | [Section 15](#15-મર્યાદાઓ) |

**Data ખોવાય નહીં તેની ખાતરી (at-least-once):**
```
poll() ──► messages ──► MongoDB bulkWrite ──► સફળ? ──► commitSync()
                               │
                               └── MongoDB error ──► app બંધ, commit નહીં
                                                     ફરી start ──► એ જ messages ફરી વંચાય
```
બધા writes idempotent છે (upsert / replace / soft delete by `_id`), એટલે એ જ message ફરી લાગુ થાય તો પણ data ખોટો થતો નથી. ફક્ત `_ingested_at` નવો સમય લે છે, એટલે Spark એ document ને ફરી process કરશે, જે silver માં upsert હોવાથી નુકસાન કરતું નથી.

### 7.7 Bronze document નું format (metadata fields)

Subscriber દરેક bronze document માં **original fields પછી** આ 4 fields ઉમેરે છે:

| Field | Type | Value | કોણ વાપરે |
|---|---|---|---|
| `_op` | String | છેલ્લું operation: `insert` / `update` / `replace` / `delete` | માહિતી, debugging |
| `_deleted` | Boolean | Delete પછી `true`, નહીં તો `false` | Spark: silver માં soft delete (`is_deleted = 1`) |
| `_source_ts` | Int64 (epoch ms) | Source માં change થયો તે સમય (event નો `wallTime`). `copy_existing` વાળા documents માં copy નો સમય | Spark: `source_ts` column, duplicates માં નવો record પસંદ કરવા |
| `_ingested_at` | Int64 (epoch ms) | Subscriber એ bronze માં લખ્યો તે સમય | Spark: **incremental read** (watermark). ફક્ત નવા `_ingested_at` વાળા documents વંચાય |

```
① Source document                        ③ Bronze document
{                                         {
  _id: ObjectId('6ae0...'),                 _id: ObjectId('6ae0...'),       ┐
  empId: 103,                               empId: 103,                     │ original fields
  name: 'Jayadip Dodia',          ──►       name: 'Jayadip Dodia',          │ (એ જ ક્રમ, એ જ types)
  department: 'Admin',                      department: 'Admin',            │
  salary: 85000                             salary: 85000,                  ┘
}                                           _op: 'insert',                  ┐
                                            _deleted: false,                │ metadata
                                            _source_ts: 1793128452146,      │ (subscriber ઉમેરે)
                                            _ingested_at: 1793132035415     ┘
                                          }
```

**Soft delete કેમ?** Spark દર run માં ફક્ત નવા `_ingested_at` વાળા documents વાંચે છે. Document bronze માંથી **કાઢી નાખીએ** તો Spark ને delete ની ખબર જ ન પડે. એટલે document રહે છે અને `_deleted: true` થાય છે. Source માં ફરી એ જ `_id` સાથે insert થાય તો `_deleted` પાછું `false` થાય છે.

> Source ના "live" documents ગણવા: `{ _deleted: false }` filter વાપરો ([10.6](#106-counts-સરખાવો)).

---

## 8. Build અને Run

### 8.1 Build
```powershell
cd D:\ClaudeProjects\dataEngineeringProjects\sparkProject\mongo-cdc-subscriber
mvn -q clean package
Get-ChildItem target\*.jar
```
➡️ `mongo-cdc-subscriber-1.0.0-jar-with-dependencies.jar`

> `Failed to delete ...jar` error આવે તો subscriber હજુ ચાલુ છે (Windows ચાલતી jar ને lock રાખે છે). પહેલાં એને `Ctrl + C` થી બંધ કરો.

### 8.2 Run (Terminal #3, ખુલ્લું રાખો)
```powershell
cd D:\ClaudeProjects\dataEngineeringProjects\sparkProject\mongo-cdc-subscriber
java -jar target\mongo-cdc-subscriber-1.0.0-jar-with-dependencies.jar
```
Settings બદલ્યા પછી ફરી build કરવો ન હોય તો properties file નો path આપો:
```powershell
java -jar target\mongo-cdc-subscriber-1.0.0-jar-with-dependencies.jar src\main\resources\subscriber.properties
```

➡️ Logs:
```
[main] INFO com.de.cdc.SubscriberApp - Reading topic 'de_source_mongo' and writing to database 'de_bronze'
[main] INFO com.de.cdc.ChangeEventApplier - weather_observations: 1 writes (matched=0, inserted=1)
[main] INFO com.de.cdc.ChangeEventApplier - student: 1 writes (matched=0, inserted=1)
```

| Log માં | અર્થ |
|---|---|
| `matched` | Destination માં **પહેલેથી હતા** અને replace/update/soft delete થયા એવા documents |
| `inserted` | Destination માં **નહોતા**, એટલે નવા બન્યા (upsert) |

બંધ કરવા: `Ctrl + C`. છેલ્લો batch પૂરો કરીને બંધ થાય. ફરી start કરો તો **જ્યાં અટક્યું હતું ત્યાંથી** આગળ વાંચે.

### 8.3 Consumer group ચકાસો (Terminal #4)
```powershell
cd C:\kafka
.\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --describe --group mongo-cdc-subscriber
```
➡️ ત્રણેય partitions માં `LAG = 0` હોય તો subscriber બધું વાંચી ચૂક્યો છે.

---

## 9. Example: એક document ની આખી સફર (3 JSON સરખામણી)

### 9.1 Source માં નવો document insert કર્યો

```javascript
use de_source_mongo
db.weather_observations.insertOne({ empId: 103, name: "Jayadip Dodia", department: "Admin", salary: 85000 })
```

### 9.2 ત્રણ તબક્કા સાથે સાથે

નીચેના ત્રણેય JSON આ જ setup ના **ખરેખરા** data છે.
① અને ③ MongoDB Compass માં જેવા દેખાય તેવા છે (Relaxed Extended JSON).
② Kafka નો message છે: ઉપરના 5 fields Kafka ના **record metadata** છે, અને `key`/`value` એ message નો ખરો content છે.

<table>
<tr>
<th>① Source<br><code>de_source_mongo.weather_observations</code></th>
<th>② Kafka message<br>topic <code>de_source_mongo</code></th>
<th>③ Destination<br><code>de_bronze.weather_observations</code></th>
</tr>
<tr>
<td valign="top">
<pre>
{
  "_id": {
    "$oid": "6ae0f804641f36c74b6506e8"
  },
  "empId": 103,
  "name": "Jayadip Dodia",
  "department": "Admin",
  "salary": 85000
}
</pre>
</td>
<td valign="top">
<pre>
{
  "topic": "de_source_mongo",
  "partition": 0,
  "offset": 5,
  "timestamp": 1793128452151,
  "headers": [],

  "key": {
    "_id": { "$oid": "6ae0f804641f36c74b6506e8" }
  },

  "value": {
    "_id": {
      "_data": "826AE0F804000000012B042C0100296E5A1004D25C4AE688B24C33ADDB9FFF115844C5463C6F7065726174696F6E54797065003C696E736572740046646F63756D656E744B65790046645F696400646AE0F804641F36C74B6506E8000004"
    },
    "operationType": "insert",
    "clusterTime": {
      "$timestamp": { "t": 1793128452, "i": 1 }
    },
    "wallTime": {
      "$date": { "$numberLong": "1793128452146" }
    },
    "fullDocument": {
      "_id": { "$oid": "6ae0f804641f36c74b6506e8" },
      "empId": { "$numberInt": "103" },
      "name": "Jayadip Dodia",
      "department": "Admin",
      "salary": { "$numberInt": "85000" }
    },
    "ns": {
      "db": "de_source_mongo",
      "coll": "weather_observations"
    },
    "documentKey": {
      "_id": { "$oid": "6ae0f804641f36c74b6506e8" }
    }
  }
}
</pre>
</td>
<td valign="top">
<pre>
{
  "_id": {
    "$oid": "6ae0f804641f36c74b6506e8"
  },
  "empId": 103,
  "name": "Jayadip Dodia",
  "department": "Admin",
  "salary": 85000,
  "_op": "insert",
  "_deleted": false,
  "_source_ts": 1793128452146,
  "_ingested_at": 1793132035415
}
</pre>
</td>
</tr>
</table>

➡️ ③ ના **original fields** ① જેવા જ છે: એ જ `_id` (ObjectId), એ જ types (Int32), એ જ fields નો order. એમના **પછી** subscriber 4 metadata fields ઉમેરે છે ([7.7](#77-bronze-document-નું-format-metadata-fields)). `_source_ts` એ ② ના `wallTime` માંથી આવે છે. Kafka message (②) માં **વધારાની બધી માહિતી** છે, જે subscriber ને "શું કરવું" તે કહે છે.

### 9.3 દરેક property નો અર્થ

| Property | ક્યાં | ઉદાહરણ | અર્થ | Bronze માં? |
|---|---|---|---|---|
| `topic` | ② Kafka metadata | `de_source_mongo` | Message કયા topic માં છે. RegexRouter એ `cdc.de_source_mongo.weather_observations` ને બદલીને આ કર્યું | ❌ |
| `partition` | ② Kafka metadata | `0` | Topic ના 3 partitions માંથી કયો. Key ના hash પરથી નક્કી થાય | ❌ |
| `offset` | ② Kafka metadata | `5` | Partition માં message નો ક્રમ નંબર. Subscriber આના વડે યાદ રાખે છે કે ક્યાં સુધી વાંચ્યું | ❌ |
| `timestamp` | ② Kafka metadata | `1793128452151` | Kafka માં message લખાયો તે સમય (epoch millis, `CreateTime`) | ❌ |
| `headers` | ② Kafka metadata | `[]` | વધારાની માહિતી. અહીં ખાલી. DLQ messages માં `error` અને `source` headers હોય છે | ❌ |
| `key._id` | ② key | `{"$oid": "6ae0..."}` | Document નો `_id`. **એક જ document ના બધા changes એક જ partition માં** જાય, એટલે ક્રમ સચવાય | ❌ |
| `value._id._data` | ② value | `"826AE0F8..."` | **Resume token**. Change stream માં આ event ની જગ્યા. Connector restart થાય ત્યારે અહીંથી આગળ વાંચે (`connect-offsets` topic માં સચવાય) | ❌ |
| `operationType` | ② value | `"insert"` | શું થયું: `insert` / `update` / `replace` / `delete` / `drop` / `rename` / `dropDatabase` / `invalidate`. **Subscriber આ જોઈને write નક્કી કરે** | ✅ `_op` તરીકે |
| `clusterTime` | ② value | `{"$timestamp": {"t": ..., "i": 1}}` | MongoDB oplog નો સમય: `t` = seconds, `i` = એ second માં ક્રમ. `copy_existing` વાળા events માં **નથી હોતું** | ❌ |
| `wallTime` | ② value | `{"$date": ...}` | Change થયો તે ઘડિયાળનો સમય. `copy_existing` વાળા events માં **નથી હોતું** (એમનો `_id` = `{"_id": ..., "copyingData": true}`) | ✅ `_source_ts` તરીકે |
| `fullDocument` | ② value | આખો document | Change **પછીનો** આખો document. Insert, replace અને update (`updateLookup`) માં હોય. Delete માં **ન હોય**. **Subscriber આ જ bronze માં લખે છે** | ✅ **આ જ document** |
| `ns.db` | ② value | `"de_source_mongo"` | Source database | ❌ |
| `ns.coll` | ② value | `"weather_observations"` | Source collection. **Subscriber આ જ નામના collection માં લખે છે** | ✅ collection નું નામ |
| `documentKey._id` | ② value | `{"$oid": "6ae0..."}` | બદલાયેલા document નો `_id`. **Subscriber આના વડે document શોધે છે** (`matchById`) | ✅ `_id` તરીકે |

**Types કેવી રીતે સચવાય છે:**

| Field | ① Source (BSON type) | ② Kafka (Canonical Extended JSON) | ③ Bronze (BSON type) |
|---|---|---|---|
| `_id` | ObjectId | `{"$oid": "6ae0f804..."}` | ObjectId ✅ |
| `empId` | Int32 | `{"$numberInt": "103"}` | Int32 ✅ |
| `salary` | Int32 | `{"$numberInt": "85000"}` | Int32 ✅ |
| `name` | String | `"Jayadip Dodia"` | String ✅ |
| Date field (હોય તો) | Date | `{"$date": {"$numberLong": "..."}}` | Date ✅ |
| Long (હોય તો) | Int64 | `{"$numberLong": "..."}` | Int64 ✅ |
| Double (હોય તો) | Double | `{"$numberDouble": "..."}` | Double ✅ |
| `_op`, `_deleted`, `_source_ts`, `_ingested_at` | (નથી) | (નથી) | String, Boolean, Int64, Int64 (subscriber ઉમેરે) |

### 9.4 Update અને Delete ના messages કેવા દેખાય?

**Update** (`db.weather_observations.updateOne({ empId: 103 }, { $set: { salary: 90000 } })`), Kafka `value`:
```json
{
  "_id": { "_data": "826AE0F9..." },
  "operationType": "update",
  "clusterTime": { "$timestamp": { "t": 1793128500, "i": 1 } },
  "wallTime": { "$date": { "$numberLong": "1793128500123" } },
  "fullDocument": {
    "_id": { "$oid": "6ae0f804641f36c74b6506e8" },
    "empId": { "$numberInt": "103" },
    "name": "Jayadip Dodia",
    "department": "Admin",
    "salary": { "$numberInt": "90000" }
  },
  "ns": { "db": "de_source_mongo", "coll": "weather_observations" },
  "documentKey": { "_id": { "$oid": "6ae0f804641f36c74b6506e8" } },
  "updateDescription": {
    "updatedFields": { "salary": { "$numberInt": "90000" } },
    "removedFields": [],
    "truncatedArrays": []
  }
}
```
| નવું property | અર્થ |
|---|---|
| `updateDescription.updatedFields` | ફક્ત બદલાયેલા fields અને એમની નવી values |
| `updateDescription.removedFields` | `$unset` થી કાઢેલા fields ના નામ |
| `updateDescription.truncatedArrays` | ટૂંકા કરાયેલા arrays (સામાન્ય રીતે ખાલી) |

➡️ Subscriber: `fullDocument` છે, એટલે bronze માં **આખો document replace** થાય. ત્યાં `salary: 90000`, `_op: "update"`, અને નવા `_source_ts`/`_ingested_at` થઈ જાય.

**Delete** (`db.weather_observations.deleteOne({ empId: 103 })`), Kafka `value`:
```json
{
  "_id": { "_data": "826AE0FA..." },
  "operationType": "delete",
  "clusterTime": { "$timestamp": { "t": 1793128600, "i": 1 } },
  "wallTime": { "$date": { "$numberLong": "1793128600456" } },
  "ns": { "db": "de_source_mongo", "coll": "weather_observations" },
  "documentKey": { "_id": { "$oid": "6ae0f804641f36c74b6506e8" } }
}
```
➡️ `fullDocument` **નથી** (document જ નથી રહ્યો). Subscriber `documentKey._id` વડે bronze નો document શોધીને **soft delete** કરે છે. Document છેલ્લી values સાથે રહે છે, ફક્ત metadata બદલાય છે:
```json
{ "_id": { "$oid": "6ae0f804641f36c74b6506e8" }, "empId": 103, "name": "Jayadip Dodia", "department": "Admin", "salary": 90000,
  "_op": "delete", "_deleted": true, "_source_ts": 1793128600456, "_ingested_at": 1793128600470 }
```

> Update અને delete ના `_data`, `clusterTime` અને `wallTime` ઉદાહરણ માટેની values છે. તમારા messages માં એ અલગ હશે.

---

## 10. End-to-end test

Kafka (#1), Connect (#2) અને subscriber (#3) ચાલુ રાખીને `mongosh` અથવા Compass માં:

### 10.1 Insert
```javascript
use de_source_mongo
db.student.insertOne({ studentId: 2, name: "Test Student", course: "Maths", marks: 70 })

use de_bronze
db.student.find({ studentId: 2 })
```
➡️ Document દેખાય. Compass માં types ચકાસો: `_id` = **ObjectId**, `marks` = **Int32**. છેલ્લે `_op: "insert"`, `_deleted: false`, `_source_ts`, `_ingested_at`.

### 10.2 Update
```javascript
use de_source_mongo
db.student.updateOne({ studentId: 2 }, { $set: { marks: 85 }, $unset: { course: "" } })
```
➡️ `de_bronze.student` માં `marks: 85`, `course` field નથી, `_op: "update"`, અને **document એક જ** છે.

### 10.3 Replace
Compass માં document edit કરીને save કરો. ➡️ Subscriber log: `matched=1`, અને destination માં ફેરફાર દેખાય.

### 10.4 Delete
```javascript
use de_source_mongo
db.student.deleteOne({ studentId: 2 })
```
➡️ `de_bronze.student` માં document **રહે છે**, પણ `_deleted: true` અને `_op: "delete"` થાય (soft delete). Log: `matched=1`.
```javascript
use de_bronze
db.student.find({ studentId: 2 }, { _op: 1, _deleted: 1 })
```

### 10.5 નવું collection
```javascript
use de_source_mongo
db.course.insertOne({ code: "CS101", title: "Intro to CS" })
```
➡️ **કોઈ config બદલ્યા વગર** `de_bronze.course` આપોઆપ બને અને document આવે.

### 10.6 Counts સરખાવો
```javascript
const s = db.getSiblingDB("de_source_mongo"), t = db.getSiblingDB("de_bronze");
s.getCollectionNames().forEach(c => print(c, "source=" + s[c].countDocuments(),
    "bronze_live=" + t[c].countDocuments({ _deleted: false }), "bronze_all=" + t[c].countDocuments()));
```
➡️ દરેક collection માટે `source` = `bronze_live`. `bronze_all` વધારે હોઈ શકે, કારણ કે એમાં soft-deleted documents પણ ગણાય છે.

### 10.7 DLQ ખાલી છે?
```powershell
cd C:\kafka
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic de_source_mongo.dlq --from-beginning --timeout-ms 5000 --formatter-property print.headers=true
```
➡️ `Processed a total of 0 messages`

---

## 11. રોજનો Start / Stop ક્રમ

### 11.1 Start (આ ક્રમમાં)
```
1. MongoDB             → Windows service, આપોઆપ ચાલુ થાય       (Get-Service MongoDB)
2. Kafka broker        → Terminal #1                            (Step 3.5)
3. Kafka Connect       → Terminal #2                            (Step 4.3)
                         Connector આપોઆપ resume થાય, ફરી register કરવાની જરૂર નથી
4. Connector status    → .\register-connectors.ps1 status       (RUNNING)
5. Subscriber          → Terminal #3                            (Step 8.2)
```

### 11.2 Stop (ઉલટા ક્રમમાં)
```
1. Subscriber      → Ctrl + C
2. Kafka Connect   → Ctrl + C   (source offsets સાચવીને બંધ થાય)
3. Kafka broker    → Ctrl + C
```
> **Kafka broker ને Connect પહેલાં બંધ ન કરો.** નહીં તો Connect છેલ્લા offsets save નહીં કરી શકે, અને restart પર થોડા events ફરી આવશે. Subscriber idempotent હોવાથી નુકસાન નથી, પણ સ્વચ્છ ક્રમ રાખો.

---

## 12. Operations: replay, નવો source DB, scaling, reset

### 12.1 Replay: destination ને શરૂઆતથી ફરી બનાવવું

Retention બંધ (`log.retention.ms=-1`) હોવાથી topic માં **બધો history** છે.

1. Subscriber બંધ કરો (`Ctrl + C`). Consumer group **inactive** હોવો જરૂરી છે.
2. Subscriber એ બનાવેલા bronze collections drop કરો (source ના collections જેવા જ નામ):
   ```javascript
   const s = db.getSiblingDB("de_source_mongo"), b = db.getSiblingDB("de_bronze");
   s.getCollectionNames().forEach(c => { b[c].drop(); print("dropped de_bronze." + c); });
   ```
   > ⚠️ **`db.dropDatabase()` ન કરો.** `de_bronze` માં જૂના pipeline ના `mysql_station` અને `mysql_station_owner` collections પણ છે, જે Spark હજુ વાંચે છે અને **ફરી બનાવી શકાતા નથી** (MySQL CDC બંધ છે).
3. Consumer group ને topic ની શરૂઆત પર લઈ જાઓ:
   ```powershell
   cd C:\kafka
   .\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --group mongo-cdc-subscriber --topic de_source_mongo --reset-offsets --to-earliest --execute
   ```
4. Subscriber ફરી start કરો.

Replay પછી બધા documents ને નવો `_ingested_at` મળે છે, એટલે Spark ના આગલા run માં **બધા documents ફરી silver માં** જશે (upsert હોવાથી duplicates નહીં બને).

### 12.2 Destination database બદલવો

`subscriber.properties` માં `target.database` બદલો, પછી 12.1 ના steps કરો (જૂનો data નવા database માં લાવવા). નવો database MongoDB **આપોઆપ બનાવે** છે.

### 12.3 બીજો source database ઉમેરવો (જેમ કે `de_source_crm`)

1. Topic બનાવો: `de_source_crm` (3 partitions) અને `de_source_crm.dlq`.
2. `kafka-connect\mongo-cdc-crm-source.json`: Step 6.1 ની copy, ફક્ત `"database": "de_source_crm"`.
3. `register-connectors.ps1` માં: `$names = "mongo-cdc-db-source", "mongo-cdc-crm-source"` અને register.
4. Subscriber: `subscriber.properties` ની copy (`crm-subscriber.properties`) બનાવો, જેમાં `kafka.topic=de_source_crm`, `kafka.group.id=mongo-cdc-crm-subscriber`, `kafka.dlq.topic=de_source_crm.dlq`. પછી:
   ```powershell
   java -jar target\mongo-cdc-subscriber-1.0.0-jar-with-dependencies.jar crm-subscriber.properties
   ```

### 12.4 Scaling (વધુ ઝડપ)

એ જ jar બીજા terminal માં **એ જ `kafka.group.id`** સાથે ફરી ચલાવો. Kafka 3 partitions ને instances વચ્ચે આપોઆપ વહેંચે છે. **Instances ≤ partitions** (3) રાખો. વધારાના instances ખાલી બેસી રહેશે.

### 12.5 Source connector ને ફરીથી શરૂઆતથી (copy_existing) ચલાવવો

Connector offsets **connector ના નામ** પ્રમાણે સચવાય છે. એ જ નામે ફરી register કરવાથી `copy_existing` **ફરી નહીં** ચાલે. ફરી ચલાવવા:
1. `.\register-connectors.ps1 delete mongo-cdc-db-source`
2. JSON file નું નામ બદલો (જેમ કે `mongo-cdc-db-source-v2.json`), અને script નો `$names` બદલો.
3. `.\register-connectors.ps1`

Subscriber idempotent હોવાથી ફરી આવેલા documents duplicate નહીં બને.

### 12.6 Kafka reset: બધું નવેસરથી

Broker crash થાય (`AccessDeniedException`, *"all log dirs ... have failed"*) અને restart થી પણ ન ચાલે ત્યારે:

```powershell
# 1. Subscriber, Connect, Kafka બધું બંધ. ચકાસો:
Get-Process java -ErrorAction SilentlyContinue

# 2. Kafka data delete કરીને ફરી format
Remove-Item -Recurse -Force C:\kafka-data\kraft-combined-logs
cd C:\kafka
$id = (.\bin\windows\kafka-storage.bat random-uuid | Select-Object -Last 1).Trim()
.\bin\windows\kafka-storage.bat format --standalone -t $id -c .\config\server.properties
```
પછી: Kafka (3.5) → Connect (4.3) → topics (5) → connector register (6.3) → subscriber (8.2).

> ⚠️ આનાથી **બધા topics, offsets અને connector configs** જશે. MongoDB ના data ને **કોઈ અસર નથી**. Connector નું offset પણ ગયું હોવાથી `copy_existing` ફરી ચાલશે, અને subscriber upsert કરતો હોવાથી duplicates નહીં બને.

**Restart પહેલાં ઓછો ઉપાય અજમાવો:** Broker crash ઘણી વાર ફક્ત `...-delete` folders ને કારણે થાય છે. Kafka બંધ કરીને આ folders delete કરો, પછી Kafka start કરો:
```powershell
Get-ChildItem C:\kafka-data\kraft-combined-logs -Directory | Where-Object Name -like "*-delete" | Remove-Item -Recurse -Force
```

---

## 13. મહત્વના નિયમો (આ ભૂલો ન કરો)

| ❌ ન કરો | કેમ | ✅ એની જગ્યાએ |
|---|---|---|
| Windows પર **topic delete** (`kafka-topics --delete`) | Broker crash થાય છે (`AccessDeniedException`, log dir failed) | જૂના topics પડ્યા રહેવા દો. નવા નામનો topic વાપરો |
| Connector ની **JSON file ખસેડીને/delete કરીને** માની લેવું કે connector બંધ થયો | Connector નો config Connect ના `connect-configs` topic માં છે. File ફક્ત input છે. Connector ચાલુ જ રહે છે, અને restart પછી પણ | `.\register-connectors.ps1 delete <name>`, પછી `status` થી ચકાસો |
| એક જ source DB માટે **બે source connectors** ચલાવવા | દરેક change બે વાર, અને બે અલગ જગ્યાએ જાય | `.\register-connectors.ps1 status` માં ફક્ત જરૂરી connectors |
| Connector ને **એ જ નામે** ફરી register કરીને `copy_existing` ની આશા રાખવી | જૂનો offset વંચાય છે, copy થતી નથી | નવું નામ (12.5) |
| `output.json.formatter` ને `SimplifiedJson`/`DefaultJson` કરવું | `ObjectId` → string, અને update/delete જૂના documents સાથે match ન થાય | `ExtendedJson` જ રાખો |
| Subscriber ચાલુ હોય ત્યારે `mvn clean package` | Jar locked, `Failed to delete` | પહેલાં `Ctrl + C` |
| Consumer group reset ચાલુ subscriber સાથે | Kafka ના પાડે છે (group active) | પહેલાં subscriber બંધ |
| Kafka broker ને Connect પહેલાં બંધ કરવું | Connect offsets save ન કરી શકે | Stop ક્રમ (11.2) |

---

## 14. Troubleshooting

| સમસ્યા / Error | કારણ | ઉકેલ |
|---|---|---|
| `ERROR Reconfiguration failed: No configuration found for '...'` (દરેક Kafka command માં) | Kafka tools ને log4j2 config file મળતી નથી | **નુકસાન નથી, અવગણો** |
| `Option --property is deprecated` | Kafka 4.x માં નવું નામ | `--formatter-property` વાપરો |
| `The consumer rebalance protocol (KIP-848) is production-ready!` | ફક્ત માહિતી | અવગણો |
| Console consumer કંઈ બતાવતો નથી | Default રીતે ફક્ત **નવા** messages બતાવે છે | `--from-beginning` ઉમેરો |
| `mongod` / `mongosh` not recognized | PATH ખોટો | Step 2.1 નું version folder ચકાસો, નવી terminal ખોલો |
| MongoDB service restart પછી શરૂ ન થાય | `mongod.cfg` નું YAML indentation | Spaces જ વાપરો. Log: `C:\Program Files\MongoDB\Server\<version>\log\mongod.log` |
| `not primary` / `$changeStream stage is only supported on replica sets` | Replica set શરૂ નથી | Step 2.4 (`rs.initiate`), `rs.status()` |
| `The input line is too long` (Kafka .bat) | Kafka લાંબા path માં છે | `C:\kafka` માં ખસેડો |
| Kafka start: `No meta.properties found` | Storage format કર્યું નથી | Step 3.4 |
| Kafka crash: `AccessDeniedException` / *"Shutdown broker because all log dirs ... have failed"* | Windows file locking. મોટા ભાગે topic delete પછી | Step 12.6 (પહેલાં `-delete` folders, પછી જરૂર હોય તો reset) |
| `Invoke-RestMethod : Unable to connect to the remote server` (port 8083) | Kafka Connect ચાલુ નથી, હજુ start થાય છે, અથવા broker બંધ છે | Terminal #2 ના logs જુઓ. `Test-NetConnection localhost -Port 8083` |
| Connect start: `Address already in use` | જૂનું Connect process હજુ port 8083 પર છે | `Get-NetTCPConnection -LocalPort 8083 \| Select-Object OwningProcess`, પછી `Stop-Process -Id <PID>` |
| `connector-plugins` માં `MongoSourceConnector` નથી | Jar ખોટા folder માં / `-all` વગરની jar / ખોટો `plugin.path` | Step 4.1, `plugin.path=C:/kafka/plugins` (forward slashes), Connect restart |
| `Invoke-RestMethod ... \| Select-Object` ખાલી line | PowerShell 5.1 નો array behavior | Call ને `( )` માં મૂકો |
| `running scripts is disabled on this system` | PowerShell execution policy | `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned` |
| Connector `RUNNING` પણ topic ખાલી | Source માં change નથી, અને `copy_existing` જૂના નામે પહેલેથી થઈ ગયું | Source માં insert કરો, અથવા 12.5 |
| `cdc.de_source_mongo.*` જેવા topics બન્યા | RegexRouter (`transforms.dbTopic.*`) ખૂટે છે કે ખોટો | Step 6.1. JSON માં `\\.` (બે backslash) |
| Destination માં data **બે databases માં** જાય છે, કે બે વાર | જૂનો connector (sink/source) હજુ ચાલે છે | `.\register-connectors.ps1 status`, વધારાના connectors `delete` (Section 13) |
| Destination માં `_id` string અને numbers `Long` | Formatter `ExtendedJson` નથી, અથવા વચ્ચે sink connector/`JsonConverter` વપરાય છે | Step 6.1 મુજબ `ExtendedJson` + `StringConverter`, અને sink connector નહીં |
| Source માં delete કર્યું, bronze માં document હજુ દેખાય છે | **આ અપેક્ષિત છે (soft delete).** Document રહે છે અને `_deleted: true` થાય છે ([7.7](#77-bronze-document-નું-format-metadata-fields)) | `_deleted` ચકાસો. એ `false` જ રહે તો: `publish.full.document.only=true` (delete events આવતા જ નથી), અથવા subscriber બંધ છે. Step 6.1 મુજબ `false` રાખો |
| Bronze document માં `_op`, `_deleted`, `_source_ts`, `_ingested_at` નથી | જૂના subscriber (metadata વગરના) એ લખેલો document | નવો subscriber build કરીને ચલાવો, પછી 12.1 replay |
| Spark (Bronze → Silver) નવો data નથી લેતું | Bronze document માં `_ingested_at` નથી, અથવા Spark નો watermark એનાથી આગળ છે | ઉપરની line, અને `de_silver.etl_watermark` table ચકાસો |
| Subscriber start થાય પણ કંઈ લખાતું નથી | Offset topic ના છેડે છે (બધું પહેલેથી વાંચ્યું) | 8.3 થી `LAG` જુઓ. જરૂર હોય તો 12.1 |
| `mvn package`: `release version 17 not supported` | Maven જૂની Java વાપરે છે | `mvn -v` માં Java 17, `JAVA_HOME` ચકાસો |
| `mvn package`: `Failed to delete ...jar-with-dependencies.jar` | Subscriber ચાલુ છે | `Ctrl + C`, પછી build |
| Subscriber log: `SLF4J: No SLF4J providers were found` | `slf4j-api` 1.7.x વપરાય છે | pom માં `slf4j-api` 2.0.13 છે કે નહીં |
| `MongoBulkWriteException`, subscriber બંધ | Destination MongoDB નો error (validation, disk full) | Error ઠીક કરો, subscriber ફરી start. Commit ન થયો હોવાથી data ખોવાયો નથી |
| `MongoTimeoutException` | MongoDB બંધ / `mongo.uri` ખોટો | `mongosh "mongodb://localhost:27017/?replicaSet=rs0"` |
| DLQ માં messages આવે છે | Message વાંચી ન શકાયો | 10.7 ના command થી `error` header વાંચો |
| `--reset-offsets`: group is not inactive | Subscriber ચાલુ છે | પહેલાં subscriber બંધ |

---

## 15. મર્યાદાઓ

| મર્યાદા | વિગત |
|---|---|
| **Update = latest state** | `updateLookup` lookup **વખતે** નો document આપે છે. ઝડપી અનેક updates માં વચ્ચેના states destination માં ન દેખાય, પણ **છેલ્લો state હંમેશા સાચો** |
| `rename` collection | Skip. Destination માં જૂનું નામ રહે, અને નવા નામનું collection નવા changes સાથે બને |
| `dropDatabase` / `invalidate` | Skip. Source database drop થાય ત્યારે connector નો change stream બંધ થઈ શકે, એટલે connector status ચકાસો |
| Transactions | Source ના એક transaction ના changes destination માં **અલગ અલગ** writes તરીકે (atomic નહીં) |
| Indexes | Source ના indexes destination માં **આપોઆપ નથી બનતા** |
| Bronze હંમેશા વધે છે | Soft delete ને કારણે delete થયેલા documents bronze માં રહે છે. જરૂર પડે તો જૂના `_deleted: true` documents હાથે સાફ કરો (Spark એ process કરી લીધા પછી) |
| Metadata field નામ | Source document માં પહેલેથી `_op`, `_deleted`, `_source_ts` કે `_ingested_at` નામના fields હોય તો subscriber એમને **overwrite** કરશે |
| Message size | Kafka નો default max message ~1 MB. મોટા documents માટે broker પર `message.max.bytes` અને connector પર `producer.override.max.request.size` વધારવા પડે |
| Windows પર Kafka | Officially unsupported. Topic delete ન કરો. Production માટે Linux |

---

## 16. Setup Checklist

**Tools**
- [ ] JDK 17, `JAVA_HOME` (1.1)
- [ ] Maven 3.9 (1.2)
- [ ] MongoDB 8.x service, mongosh, Compass (2.1)
- [ ] `mongod.cfg`: `cacheSizeGB: 0.5`, `replSetName: rs0` (2.3)
- [ ] `rs.initiate`, `PRIMARY` (2.4)
- [ ] Kafka 4.3.1 `C:\kafka`, `server.properties` (3.1–3.3)
- [ ] Storage format (3.4), broker ચાલુ (3.5)
- [ ] `mongo-kafka-connect-3.1.2-all.jar` (4.1), `connect-distributed-de.properties` (4.2)
- [ ] Connect ચાલુ, `MongoSourceConnector` દેખાય (4.3, 4.4)

**Pipeline**
- [ ] Topics: `de_source_mongo` (3 partitions), `de_source_mongo.dlq` (5)
- [ ] `mongo-cdc-db-source.json` + `register-connectors.ps1`, connector `RUNNING` (6)
- [ ] Topic માં events, અને `cdc.*` topic નથી (6.4)
- [ ] Subscriber project: pom, properties, 2 Java files (7)
- [ ] Build, run, `LAG = 0` (8)

**Test**
- [ ] Insert, update, replace, delete, નવું collection (10.1–10.5)
- [ ] Counts સરખા, types (`ObjectId`, `Int32`) સરખા (10.6)
- [ ] DLQ ખાલી (10.7)
