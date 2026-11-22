# mongoKafkaConnectorDemo: MongoDB → Kafka → Bronze (CDC)

> MongoDB Kafka Connector (source) અને પોતાના Java consumer (`mongo-cdc-subscriber`) નું **બધું એક જ folder માં**: config, code, documents અને start/stop steps.
>
> **Flow:** `sourceMongo_db` (MongoDB) → **MongoDB Source Connector** (Kafka Connect) → Kafka topic **`sourceMongo_db`** → **`mongo-cdc-subscriber`** (Java) → `bronze_db` (MongoDB) → ETL (`ecommersDataETLPipeline`, default `MONGO_DATABASE=bronze_db`)
>
> `sourceMongo_db` = **transaction (source) DB**. Source માં **hard delete** = bronze માં **soft delete** (`_deleted: true`). Test data અને steps: **[sample-data/README.md](sample-data/README.md)**
>
> આ README = **quick reference**. દરેક step ની પૂરી વિગત (કેમ, શું output આવે, error આવે તો શું કરવું): **[docs/MONGO_CDC_COMPLETE_SETUP_GU.md](docs/MONGO_CDC_COMPLETE_SETUP_GU.md)**. નીચે `(§3.5)` જેવું લખ્યું હોય એ એ document નો વિભાગ છે.

---

## અનુક્રમણિકા

- [1. Folder structure](#1-folder-structure)
- [2. Documents: ક્યારે કયું વાંચવું](#2-documents-ક્યારે-કયું-વાંચવું)
- [3. બધા નામો અને ports](#3-બધા-નામો-અને-ports)
- [4. પહેલી વાર setup (નવી machine, ફક્ત એક વાર)](#4-પહેલી-વાર-setup-નવી-machine-ફક્ત-એક-વાર)
- [5. રોજનો Start (step by step)](#5-રોજનો-start-step-by-step)
- [6. ચકાસો કે બધું ચાલે છે](#6-ચકાસો-કે-બધું-ચાલે-છે)
- [7. રોજનો Stop](#7-રોજનો-stop)
- [8. ફેરફાર કરવા હોય ત્યારે](#8-ફેરફાર-કરવા-હોય-ત્યારે)
- [9. ઝડપી Troubleshooting](#9-ઝડપી-troubleshooting)
- [10. Change Log (track)](#10-change-log-track)

---

## 1. Folder structure

```
mongoKafkaConnectorDemo\
├── README.md                                   ← આ file (quick start + change log)
├── docs\
│   ├── DAILY_OPERATIONS_COMMANDS_GU.md         ← રોજનો start/stop + Kafka topics/messages/DLQ/groups ના commands
│   ├── MONGO_CDC_COMPLETE_SETUP_GU.md          ← આખો setup: install → config → code → test → operations
│   ├── MONGO_SOURCE_CONNECTOR_CONFIG_REFERENCE_GU.md ← connector ની દરેક config key નો અર્થ
│   ├── CDC_EXAMPLE_ORDERS_COMPARISON_GU.md     ← insert/update/replace/delete: source vs Kafka vs bronze
│   └── CDC_TOOL_COMPARISON_GU.md               ← Debezium vs MongoDB connector: કેમ આ tool પસંદ કર્યું
├── kafka-connect\
│   ├── mongo-cdc-db-source.json                ← ✅ source connector: sourceMongo_db → topic sourceMongo_db
│   ├── register-connectors.ps1                 ← connector register / status / delete script
│   └── deletedConnectorOld\                    ← ❌ જૂના connectors (ફક્ત reference, register ન કરવા)
│       ├── mongo-cdc-source.json
│       └── mongo-cdc-sink-bronze.json
└── mongo-cdc-subscriber\                       ← Java consumer (અલગ Maven project)
    ├── pom.xml
    └── src\main\
        ├── java\com\de\cdc\
        │   ├── SubscriberApp.java              ← Kafka loop: read → apply → DLQ → commit
        │   └── ChangeEventApplier.java         ← change event → MongoDB write (upsert / soft delete)
        └── resources\
            └── subscriber.properties           ← Kafka, MongoDB, target DB settings (topic sourceMongo_db → bronze_db)
└── sample-data\                                ← sourceMongo_db માટે test data (6 pipelines)
    ├── README.md                               ← hard delete → soft delete test: steps + expected counts
    ├── 99_verify_bronze.js                     ← sourceMongo_db vs bronze_db સરખામણી
    └── <pipeline>\01_mongo_full_load.js, 02_mongo_incremental.js
```

Project ની બહાર (machine પર) રહેતી વસ્તુઓ:

```
C:\kafka\                                       ← Kafka 4.3.1
C:\kafka\config\server.properties               ← broker config (§3.3)
C:\kafka\config\connect-distributed-de.properties ← Kafka Connect worker config (§4.2)
C:\kafka\plugins\mongo-kafka-connect\           ← mongo-kafka-connect-3.1.2-all.jar (§4.1)
C:\kafka-data\kraft-combined-logs\              ← topics નો data
C:\Program Files\MongoDB\Server\<version>\bin\mongod.cfg ← replica set rs0 (§2.3)
```

---

## 2. Documents: ક્યારે કયું વાંચવું

| જરૂર | Document |
|---|---|
| રોજનો start / stop, topics, messages, DLQ, consumer groups, connectors જોવાના commands (expected output સાથે) | [DAILY_OPERATIONS_COMMANDS_GU.md](docs/DAILY_OPERATIONS_COMMANDS_GU.md) |
| નવી machine પર શરૂઆતથી setup / કોઈ step ની પૂરી વિગત | [MONGO_CDC_COMPLETE_SETUP_GU.md](docs/MONGO_CDC_COMPLETE_SETUP_GU.md) |
| Connector JSON ની કોઈ key બદલવી છે, એની અસર શું? | [MONGO_SOURCE_CONNECTOR_CONFIG_REFERENCE_GU.md](docs/MONGO_SOURCE_CONNECTOR_CONFIG_REFERENCE_GU.md) |
| એક document Kafka અને bronze માં કેવો દેખાય? | [CDC_EXAMPLE_ORDERS_COMPARISON_GU.md](docs/CDC_EXAMPLE_ORDERS_COMPARISON_GU.md) |
| Debezium કેમ નહીં? Sink connector કેમ નહીં? | [CDC_TOOL_COMPARISON_GU.md](docs/CDC_TOOL_COMPARISON_GU.md) |
| Bronze → Silver → Gold (Spark) | [../docs/MEDALLION_CODE_GUIDE_GU.md](../docs/MEDALLION_CODE_GUIDE_GU.md) |

---

## 3. બધા નામો અને ports

| વસ્તુ | Value |
|---|---|
| MongoDB URI | `mongodb://localhost:27017/?replicaSet=rs0` |
| Source DB → Bronze DB | `sourceMongo_db` → `bronze_db` |
| Kafka broker | `localhost:9092` (controller `9093`) |
| Kafka Connect REST | `http://localhost:8083` |
| CDC topic / DLQ topic | `sourceMongo_db` (3 partitions) / `cdc-dlq` |
| Source connector નામ | `mongo-cdc-db-source` |
| Subscriber consumer group | `mongo-cdc-subscriber` |
| Versions | JDK 17, Maven 3.9.11, MongoDB 8.x, Kafka 4.3.1, MongoDB Kafka Connector 3.1.2 |

---

## 4. પહેલી વાર setup (નવી machine, ફક્ત એક વાર)

> દરેક step ના commands અને અપેક્ષિત output setup guide માં છે. અહીં ✔ કરતા જાઓ.

| # | Step | Guide વિભાગ | થઈ ગયું? |
|---|---|---|---|
| 1 | JDK 17 install, `JAVA_HOME` set | §1.1 | [ ] |
| 2 | Maven 3.9 install, PATH | §1.2–1.3 | [ ] |
| 3 | MongoDB 8.x (service), mongosh, Compass | §2.1 | [ ] |
| 4 | `mongod.cfg`: `cacheSizeGB: 0.5`, `replication.replSetName: rs0` → `Restart-Service MongoDB` **(Admin)** | §2.3 | [ ] |
| 5 | `rs.initiate(...)` → `PRIMARY` | §2.4 | [ ] |
| 6 | Kafka 4.3.1 → `C:\kafka` (ટૂંકો path જ) | §3.1 | [ ] |
| 7 | `server.properties`: `log.dirs`, retention `-1`, cleaner off | §3.3 | [ ] |
| 8 | Kafka storage format (`kafka-storage.bat format --standalone`) | §3.4 | [ ] |
| 9 | `mongo-kafka-connect-3.1.2-all.jar` → `C:\kafka\plugins\mongo-kafka-connect\` | §4.1 | [ ] |
| 10 | `C:\kafka\config\connect-distributed-de.properties` બનાવો | §4.2 | [ ] |
| 11 | Kafka + Connect start કરો (નીચે [Step 5.2, 5.3](#5-રોજનો-start-step-by-step)) | §3.5, §4.3 | [ ] |
| 12 | Topics બનાવો (નીચેનો command) | §5 | [ ] |
| 13 | Connector register (`.\register-connectors.ps1`) | §6.3 | [ ] |
| 14 | Subscriber build (`mvn -q clean package`) અને run | §8 | [ ] |
| 15 | End-to-end test: insert / update / replace / delete / નવું collection | §10 | [ ] |

**Step 12: Topics (ફક્ત પહેલી વાર, Kafka ચાલુ હોય ત્યારે):**
```powershell
cd C:\kafka
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --create --topic sourceMongo_db --partitions 3 --replication-factor 1
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --create --topic cdc-dlq --partitions 1 --replication-factor 1
```

**Step 13: Connector register (ફક્ત પહેલી વાર, અથવા JSON બદલ્યા પછી):**
```powershell
cd D:\ClaudeProjects\dataEngineeringProjects\sparkProject\mongoKafkaConnectorDemo\kafka-connect
.\register-connectors.ps1
# ➡️ applied mongo-cdc-db-source
# ➡️ mongo-cdc-db-source connector=RUNNING  tasks=RUNNING
```
> `running scripts is disabled` આવે તો એક વાર: `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`

**Step 14: Subscriber build:**
```powershell
cd D:\ClaudeProjects\dataEngineeringProjects\sparkProject\mongoKafkaConnectorDemo\mongo-cdc-subscriber
mvn -q clean package
Get-ChildItem target\*.jar     # ➡️ mongo-cdc-subscriber-1.0.0-jar-with-dependencies.jar
```

---

## 5. રોજનો Start (step by step)

> **ક્રમ મહત્વનો છે:** MongoDB → Kafka → Connect → (status) → Subscriber.
> દરેક process માટે **અલગ PowerShell window** ખોલો અને **ખુલ્લી રાખો**.

| Terminal | શું ચાલે |
|---|---|
| #1 | Kafka broker |
| #2 | Kafka Connect |
| #3 | Subscriber |
| #4 | ચકાસણી / બીજા commands |

### 5.1 MongoDB ચાલુ છે? (Terminal #4)
```powershell
Get-Service MongoDB            # Status: Running
# બંધ હોય તો (Admin PowerShell): Start-Service MongoDB
```

### 5.2 Kafka broker (Terminal #1, ખુલ્લું રાખો)
```powershell
cd C:\kafka
$env:KAFKA_HEAP_OPTS = "-Xms512m -Xmx512m"
.\bin\windows\kafka-server-start.bat .\config\server.properties
```
➡️ `Kafka Server started` દેખાય ત્યાં સુધી રાહ જુઓ.

### 5.3 Kafka Connect (Terminal #2, ખુલ્લું રાખો)
```powershell
cd C:\kafka
$env:KAFKA_HEAP_OPTS = "-Xms512m -Xmx768m"
.\bin\windows\connect-distributed.bat .\config\connect-distributed-de.properties
```
➡️ 30–60 સેકન્ડ પછી `Kafka Connect started`. **Connector આપોઆપ resume થાય છે, ફરી register કરવાની જરૂર નથી.**

### 5.4 Connector status (Terminal #4)
```powershell
cd D:\ClaudeProjects\dataEngineeringProjects\sparkProject\mongoKafkaConnectorDemo\kafka-connect
.\register-connectors.ps1 status
```
➡️ `mongo-cdc-db-source connector=RUNNING  tasks=RUNNING`
> ફક્ત **આ એક જ** connector દેખાવો જોઈએ. બીજો (જૂનો) દેખાય તો [Section 9](#9-ઝડપી-troubleshooting).

### 5.5 Subscriber (Terminal #3, ખુલ્લું રાખો)
```powershell
cd D:\ClaudeProjects\dataEngineeringProjects\sparkProject\mongoKafkaConnectorDemo\mongo-cdc-subscriber
java -jar target\mongo-cdc-subscriber-1.0.0-jar-with-dependencies.jar
```
➡️ `Reading topic 'sourceMongo_db' and writing to database 'bronze_db'`

`subscriber.properties` બદલ્યા પછી ફરી build ન કરવો હોય તો:
```powershell
java -jar target\mongo-cdc-subscriber-1.0.0-jar-with-dependencies.jar src\main\resources\subscriber.properties
```

---

## 6. ચકાસો કે બધું ચાલે છે

**(a) Subscriber બધું વાંચી ચૂક્યો? (`LAG = 0`)**
```powershell
cd C:\kafka
.\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --describe --group mongo-cdc-subscriber
```

**(b) એક નાનો live test (mongosh):**
```javascript
use sourceMongo_db
db.student.insertOne({ studentId: 999, name: "Readme Test", marks: 50 })

use bronze_db
db.student.find({ studentId: 999 })     // _op: "insert", _deleted: false, _source_ts, _ingested_at
```

**(c) Source અને bronze counts સરખા?**
```javascript
const s = db.getSiblingDB("sourceMongo_db"), t = db.getSiblingDB("bronze_db");
s.getCollectionNames().forEach(c => print(c, "source=" + s[c].countDocuments(),
    "bronze_live=" + t[c].countDocuments({ _deleted: false })));
```

**(d) DLQ ખાલી?**
```powershell
cd C:\kafka
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic cdc-dlq --from-beginning --timeout-ms 5000 --formatter-property print.headers=true
```
➡️ `Processed a total of 0 messages`

---

## 7. રોજનો Stop

**ઉલટા ક્રમમાં** (દરેક terminal માં `Ctrl + C`):
```
1. Subscriber      (Terminal #3)  → છેલ્લો batch પૂરો કરીને બંધ થાય
2. Kafka Connect   (Terminal #2)  → source offsets સાચવીને બંધ થાય
3. Kafka broker    (Terminal #1)
```
> ⚠️ Kafka broker ને Connect **પહેલાં** બંધ ન કરો.
> MongoDB Windows service છે, એને બંધ કરવાની જરૂર નથી.

---

## 8. ફેરફાર કરવા હોય ત્યારે

| શું બદલવું છે | શું કરવું | વિગત |
|---|---|---|
| Connector JSON (`mongo-cdc-db-source.json`) | File બદલો → `.\register-connectors.ps1` (PUT = update) → `status` | [Config reference](docs/MONGO_SOURCE_CONNECTOR_CONFIG_REFERENCE_GU.md) |
| Subscriber code / `pom.xml` | Subscriber બંધ (`Ctrl + C`) → `mvn -q clean package` → ફરી run | §8 |
| `subscriber.properties` | Subscriber બંધ → properties path આપીને run (5.5) અથવા rebuild | §7.3 |
| Bronze ફરી શરૂઆતથી બનાવવું (replay) | Subscriber બંધ → bronze collections drop → consumer group `--reset-offsets --to-earliest` → subscriber start | §12.1 |
| Connector નો `copy_existing` ફરી ચલાવવો | જૂનો connector `delete` → **નવા નામે** JSON + script માં `$names` → register | §12.5 |
| બીજો source DB ઉમેરવો | નવો topic + નવી connector JSON + નવી properties file / consumer group. **DLQ નવો નહીં**: `kafka.dlq.topic=cdc-dlq` | §12.3 |
| **Sync થતા collections બદલવા** (હાલ `customers`, `products`) | `mongo-cdc-db-source.json` માં `pipeline` + `startup.mode.copy.existing.namespace.regex` બંનેમાં નામ ઉમેરો/કાઢો. નવા collection નો જૂનો data જોઈએ તો `offset.partition.name` બદલો (`-v3`, ...). પછી `.\register-connectors.ps1`. `sample-data\99_verify_bronze.js` નું `SYNCED` પણ બદલો | §6.1 |
| એક DLQ (`cdc-dlq`) બનાવવો / જૂના DLQ topics દૂર કરવા | [DAILY_OPERATIONS §5.3, §5.4](docs/DAILY_OPERATIONS_COMMANDS_GU.md#53-cdc-dlq-બનાવવો-ફક્ત-એક-વાર) | |
| Kafka crash (`AccessDeniedException`) | પહેલાં `*-delete` folders દૂર કરો, ન ચાલે તો reset | §12.6 |
| **જૂના setup (`de_source_mongo` → `de_bronze`) થી નવા DB નામ પર જવું** (ફક્ત એક વાર) | નીચે [8.1](#81-જૂના-db-નામ-થી-sourcemongo_db--bronze_db-પર-જવું-એક-વાર) | |

### 8.1 જૂના DB નામ થી `sourceMongo_db` → `bronze_db` પર જવું (એક વાર)

જેણે પહેલાં `de_source_mongo` → `de_bronze` વાળો connector register કર્યો હોય તેના માટે. Kafka + Connect ચાલુ, subscriber **બંધ**:
```powershell
# 1) નવા topics
cd C:\kafka
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --create --topic sourceMongo_db --partitions 3 --replication-factor 1
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --create --topic cdc-dlq --partitions 1 --replication-factor 1

# 2) Connector update: એ જ નામ, નવો database. "offset.partition.name" નવું હોવાથી connector નવેસરથી
#    શરૂ કરે છે: sourceMongo_db ના હાલના documents copy_existing થી આવે, પછી live changes.
cd D:\ClaudeProjects\dataEngineeringProjects\sparkProject\mongoKafkaConnectorDemo\kafka-connect
.\register-connectors.ps1
# ➡️ applied mongo-cdc-db-source / mongo-cdc-db-source connector=RUNNING  tasks=RUNNING

# 3) Subscriber rebuild (properties jar ની અંદર છે) અને start
cd ..\mongo-cdc-subscriber
mvn -q clean package
java -jar target\mongo-cdc-subscriber-1.0.0-jar-with-dependencies.jar
# ➡️ Reading topic 'sourceMongo_db' and writing to database 'bronze_db'
```
- Consumer group `mongo-cdc-subscriber` એ જ રહે છે. નવા topic `sourceMongo_db` માટે એનો કોઈ offset નથી, એટલે subscriber એ topic **શરૂઆતથી** વાંચે છે.
- જૂનો topic `de_source_mongo` અને DB `de_source_mongo` / `de_bronze` જેમ છે તેમ રહે છે. Topic **delete ન કરો** (Windows). Root Spark project (`src\main\resources\application.properties`: `pipeline.bronze-database=de_bronze`) હજુ `de_bronze` વાંચે છે.
- `bronze_db` માં જૂના ETL sample scripts એ સીધા load કરેલા documents (`_op` વગરના) હોય તો પહેલાં drop કરો ([sample-data/README.md](sample-data/README.md)).

**⚠️ ક્યારેય ન કરો** (પૂરી list §13):
- Windows પર **Kafka topic delete** ન કરો (broker crash થાય).
- Connector JSON file delete કરવાથી connector બંધ **નથી** થતો. `.\register-connectors.ps1 delete <name>` વાપરો.
- `deletedConnectorOld\` ની files **register ન કરો** (data બે વાર / બે જગ્યાએ જશે).
- Subscriber ચાલુ હોય ત્યારે `mvn clean package` ન કરો (jar lock).

---

## 9. ઝડપી Troubleshooting

| લક્ષણ | પહેલા આ ચકાસો |
|---|---|
| `Unable to connect to the remote server` (8083) | Connect હજુ start થાય છે, અથવા Kafka broker બંધ છે |
| Connector `FAILED` | `.\register-connectors.ps1 status` ની `ERROR` line. MongoDB replica set `PRIMARY` છે? |
| `$changeStream stage is only supported on replica sets` | `rs.status()`. URI માં `?replicaSet=rs0` છે? |
| બે connectors RUNNING | જૂનો connector `.\register-connectors.ps1 delete <name>` |
| Bronze માં data નથી આવતો | Subscriber ચાલુ? `LAG` (6a)? DLQ (6d)? |
| `Failed to delete ...jar` | Subscriber ચાલુ છે, પહેલાં `Ctrl + C` |
| Source માં delete કર્યું પણ bronze માં document છે | **અપેક્ષિત (soft delete)**: `_deleted: true` ચકાસો |

બાકીના errors: setup guide **§14**.

---

## 10. Change Log (track)

> દરેક ફેરફાર પછી અહીં **એક line** ઉમેરો (નવું ઉપર). Git commit hash પણ લખો, જેથી પછી `git show <hash>` થી જોઈ શકાય.

| તારીખ | ફેરફાર | Commit |
|---|---|---|
| 2026-11-22 | Connector હવે **ફક્ત `customers`, `products`** sync કરે છે (`pipeline` + `startup.mode.copy.existing.namespace.regex`), `offset.partition.name` → `sourceMongo_db-v2` (આ 2 નો existing data ફરી copy થાય). `99_verify_bronze.js` માં `SYNCED` list | _(commit પછી લખો)_ |
| 2026-11-22 | **એક જ DLQ `cdc-dlq`** બધા CDC topics માટે (`subscriber.properties`). Subscriber DLQ message માં નવા headers `source.topic`, `source.partition`, `source.offset`. Docs: DLQ filter commands, જૂના DLQ topics દૂર કરવાના steps (DAILY_OPERATIONS §5.3–5.4) | _(commit પછી લખો)_ |
| 2026-11-22 | `docs\DAILY_OPERATIONS_COMMANDS_GU.md`: રોજનો start/stop, topics, messages, DLQ, consumer groups, Connect REST, logs, health check (commands + expected output) | _(commit પછી લખો)_ |
| 2026-11-22 | **Source DB `de_source_mongo` → `sourceMongo_db`, bronze DB `de_bronze` → `bronze_db`** (topic `sourceMongo_db`, DLQ `sourceMongo_db.dlq`). `mongo-cdc-db-source.json` માં `database` + `offset.partition.name`, `subscriber.properties`, Java comments, બધા docs. Migration: [8.1](#81-જૂના-db-નામ-થી-sourcemongo_db--bronze_db-પર-જવું-એક-વાર) | _(commit પછી લખો)_ |
| 2026-11-22 | `register-connectors.ps1 apply NAME`. `sample-data\`: DB `sourceMongo_db`, banking collections `bank_*`, 02 scripts માં soft delete ની જગ્યાએ **hard delete**, 01 માં `drop` ની જગ્યાએ `deleteMany`, `99_verify_bronze.js`; `.sql` files કાઢી | _(commit પછી લખો)_ |
| 2026-11-21 | `kafka-connect\`, `mongo-cdc-subscriber\` અને 4 CDC documents ને root/`docs` માંથી આ folder માં **move** કર્યા. Docs ના paths/links અને બીજા docs ની links update કરી. આ README બનાવ્યું | _(commit પછી લખો)_ |
| 2026-10-29 | Setup guide અને docs માં સુધારા | `d5899bc` |
| 2026-10-28 | Setup guide (`MONGO_CDC_COMPLETE_SETUP_GU.md`) લખ્યો, subscriber માં metadata fields + soft delete | `cb16d8f`, `bd199b6` |
| 2026-10-28 | Debezium/sink connector ની જગ્યાએ MongoDB source connector + પોતાનો Java subscriber. જૂના connectors `deletedConnectorOld\` માં | `e09b547` |
| 2026-10-25 | Initial: `kafka-connect` (source-mongo, source-mysql, sink-bronze) | `710bfa1` |

**જૂનો history જોવા** (move પહેલાંનો history પણ દેખાય):
```powershell
cd D:\ClaudeProjects\dataEngineeringProjects\sparkProject
git log --follow --oneline -- mongoKafkaConnectorDemo/docs/MONGO_CDC_COMPLETE_SETUP_GU.md
git log --oneline -- mongoKafkaConnectorDemo kafka-connect mongo-cdc-subscriber
```
