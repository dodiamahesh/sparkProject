# Debezium દૂર કરવું, MongoDB Kafka Connector વાપરવું

> **ધ્યેય:**
> 1. Debezium (MySQL + MongoDB plugins) ને system માંથી **કાયમ માટે** દૂર કરવું.
> 2. MongoDB source → Kafka → MongoDB bronze માટે **ફક્ત MongoDB Kafka Connector** (`mongo-kafka-connect`, પહેલેથી install છે) વાપરવો.
> 3. MySQL માટે CDC **બંધ** કરવું અને તેનું setup સાફ કરવું.

---

## અનુક્રમણિકા

- [0. પહેલાં અને પછી](#0-પહેલાં-અને-પછી)
- [1. Backup લો](#1-backup-લો)
- [2. જૂના connectors delete કરો](#2-જૂના-connectors-delete-કરો)
- [3. Kafka Connect બંધ કરો](#3-kafka-connect-બંધ-કરો)
- [4. Debezium plugins delete કરો](#4-debezium-plugins-delete-કરો)
- [5. Kafka Connect ફરી શરૂ કરો અને ચકાસો](#5-kafka-connect-ફરી-શરૂ-કરો-અને-ચકાસો)
- [6. જૂના Debezium topics અને consumer group સાફ કરો](#6-જૂના-debezium-topics-અને-consumer-group-સાફ-કરો)
- [7. MySQL માંથી CDC setup સાફ કરો](#7-mysql-માંથી-cdc-setup-સાફ-કરો)
- [8. Project ની connector files બદલો](#8-project-ની-connector-files-બદલો)
- [9. જૂનો bronze data સાફ કરો](#9-જૂનો-bronze-data-સાફ-કરો)
- [10. નવા connectors register કરો](#10-નવા-connectors-register-કરો)
- [11. End-to-end test](#11-end-to-end-test)
- [12. Spark code અને docs update કરો](#12-spark-code-અને-docs-update-કરો)
- [13. Checklist](#13-checklist)
- [14. Troubleshooting](#14-troubleshooting)

---

## 0. પહેલાં અને પછી

```
પહેલાં (Debezium):
  MongoDB ──(source-mongo: Debezium)──► src.mongo.de_source_mongo.*  ──┐
  MySQL   ──(source-mysql: Debezium)──► src.mysql.de_source_mysql.*  ──┼──(sink-bronze)──► de_bronze
                                       schemahistory.src.mysql         ┘

પછી (ફક્ત MongoDB Kafka Connector):
  MongoDB ──(mongo-source: MongoSourceConnector)──► mongo.de_source_mongo.* ──(mongo-sink-bronze: MongoSinkConnector)──► de_bronze
  MySQL   ──  (કોઈ CDC નહીં)
```

### Bronze document નો ફેરફાર

| | Debezium (પહેલાં) | MongoDB Connector (પછી) |
|---|---|---|
| Format | Envelope: `op`, `before`, `after` (JSON **string**), `source`, `ts_ms` | **સીધું original document** |
| `_id` | `{ "id": "{\"$oid\": \"...\"}" }` | `"6ae0ccfb641f36c74b65069d"` |
| Fields | `after` ની અંદર string તરીકે | Top level પર: `empId`, `name`, `salary`... |
| Update | નવો event | Bronze માં એ જ `_id` વાળો document **replace** થાય |
| Delete | `op: "d"` event | ⚠️ **Propagate થતું નથી** (જુઓ [Section 14](#14-troubleshooting)) |

**નવો bronze document આવો દેખાશે:**
```json
{
  "_id": "6ae0ccfb641f36c74b65069d",
  "empId": 101,
  "name": "Ravi Kumar",
  "department": "Data Engineering",
  "salary": 75000,
  "_ingested_at": 1793117435396
}
```

### નામો કેમ બદલીએ છીએ?

| જૂનું | નવું | કારણ |
|---|---|---|
| Connector `source-mongo` | `mongo-source` | જૂના Debezium offsets (`connect-offsets` topic માં) નવા connector સાથે ભળે નહીં |
| Connector `sink-bronze` | `mongo-sink-bronze` | નવો consumer group (`connect-mongo-sink-bronze`) બને, જે શરૂઆતથી વાંચે |
| Topic prefix `src.mongo` | `mongo` | જૂના Debezium envelope messages નવા topic માં ભળે નહીં |

---

## 1. Backup લો

Kafka Connect ચાલુ હોય ત્યારે PowerShell માં:

```powershell
cd D:\ClaudeProjects\dataEngineeringProjects\sparkProject
New-Item -ItemType Directory -Force .\kafka-connect\_backup_debezium

# Project ની JSON files ની copy
Copy-Item .\kafka-connect\*.json .\kafka-connect\_backup_debezium\

# Connect માં હાલ જે config ચાલે છે તે પણ save કરો
foreach ($n in "source-mysql", "source-mongo", "sink-bronze") {
    Invoke-RestMethod "http://localhost:8083/connectors/$n/config" |
        ConvertTo-Json -Depth 10 |
        Set-Content -Encoding utf8 ".\kafka-connect\_backup_debezium\$n.live.json"
}
Get-ChildItem .\kafka-connect\_backup_debezium
```

➡️ 6 files દેખાવી જોઈએ (3 `.json` + 3 `.live.json`).

> Plugins નો backup જોઈએ તો: `Compress-Archive C:\kafka\plugins\debezium-* C:\kafka\debezium-plugins-backup.zip`

---

## 2. જૂના connectors delete કરો

ત્રણેય જૂના connectors delete કરો. Sink પણ delete કરવાનું કારણ એ છે કે તેને નવા નામ અને નવા config સાથે ફરી બનાવવાનો છે.

```powershell
cd D:\ClaudeProjects\dataEngineeringProjects\sparkProject\kafka-connect

.\register-connectors.ps1 delete source-mysql
.\register-connectors.ps1 delete source-mongo
.\register-connectors.ps1 delete sink-bronze
```

ચકાસો:
```powershell
Invoke-RestMethod http://localhost:8083/connectors
```
➡️ ખાલી result (`{}` અથવા કંઈ નહીં) આવવું જોઈએ.

---

## 3. Kafka Connect બંધ કરો

Kafka Connect ચાલુ હોય ત્યારે plugin folders delete ન થાય, કારણ કે Windows JAR files ને lock રાખે છે.

- Kafka Connect વાળા terminal (Terminal #2) માં **`Ctrl + C`** દબાવો.
- `Terminate batch job (Y/N)?` પૂછે તો **`Y`** લખો.

ચકાસો કે port 8083 બંધ છે:
```powershell
Test-NetConnection localhost -Port 8083 | Select-Object TcpTestSucceeded
```
➡️ `False` આવવું જોઈએ.

> ⚠️ **Kafka broker (Terminal #1) બંધ ન કરો.** Step 6 માટે તે ચાલુ જોઈએ.

---

## 4. Debezium plugins delete કરો

### 4.1 કઈ files છે તે જુઓ
```powershell
Get-ChildItem C:\kafka\plugins
```
➡️ ત્રણ folders: `debezium-connector-mysql`, `debezium-connector-mongodb`, `mongo-kafka-connect`

### 4.2 ફક્ત Debezium folders delete કરો
```powershell
Remove-Item -Recurse -Force C:\kafka\plugins\debezium-connector-mysql
Remove-Item -Recurse -Force C:\kafka\plugins\debezium-connector-mongodb
```

> ⚠️ **`mongo-kafka-connect` folder delete ન કરો.** નવા source અને sink બંને connectors એમાં જ છે.

### 4.3 બીજે ક્યાંય Debezium JAR બાકી નથી ને?
```powershell
Get-ChildItem C:\kafka -Recurse -Filter "*debezium*" -ErrorAction SilentlyContinue | Select-Object FullName
```
➡️ કંઈ ન દેખાવું જોઈએ. જૂની `.zip` (જેમ કે `dbz-mysql.zip`) દેખાય તો એ પણ delete કરો. Step 1 નો backup zip (`debezium-plugins-backup.zip`) દેખાય તો તેને રાખવો કે નહીં તે તમારી મરજી.

### 4.4 Worker config માં ફેરફાર જરૂરી નથી
`C:\kafka\config\connect-distributed-de.properties` માં `plugin.path=C:/kafka/plugins` જ રહેવા દો. એ આખું folder scan કરે છે, એટલે Debezium folders ન હોય તો આપોઆપ load નહીં થાય.

---

## 5. Kafka Connect ફરી શરૂ કરો અને ચકાસો

Terminal #2 માં:
```powershell
cd C:\kafka
$env:KAFKA_HEAP_OPTS = "-Xms512m -Xmx768m"
.\bin\windows\connect-distributed.bat .\config\connect-distributed-de.properties
```

નવી PowerShell માં plugins ની યાદી જુઓ:
```powershell
(Invoke-RestMethod http://localhost:8083/connector-plugins) | Select-Object class, version
```

➡️ આ **બંને** દેખાવા જોઈએ:
```
com.mongodb.kafka.connect.MongoSinkConnector     3.1.2
com.mongodb.kafka.connect.MongoSourceConnector   3.1.2
```
➡️ `io.debezium...` વાળી **કોઈ line ન દેખાવી જોઈએ**. (Kafka ના built-in `MirrorMaker` plugins દેખાય તો તે સામાન્ય છે.)

---

## 6. જૂના Debezium topics અને consumer group સાફ કરો

### 6.1 Debezium વાળા topics ની યાદી
```powershell
cd C:\kafka
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --list
```

Debezium એ બનાવેલા topics:

| Topic | કોનો |
|---|---|
| `src.mongo.de_source_mongo.weather_observations` (અને બીજા `src.mongo.*`) | Debezium MongoDB |
| `src.mysql.de_source_mysql.station`, `src.mysql.de_source_mysql.station_owner` (અને બીજા `src.mysql.*`) | Debezium MySQL |
| `src.mysql` | Debezium MySQL (schema change events, હોય તો) |
| `schemahistory.src.mysql` | Debezium MySQL (DDL history) |

> ⛔ આ topics **delete ન કરો:** `connect-configs`, `connect-offsets`, `connect-status`, `dlq.sink-bronze`, `__consumer_offsets`. એ Kafka/Connect ના પોતાના topics છે.

### 6.2 Topics delete કરો (દરેક topic માટે એક વાર)
```powershell
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --delete --topic src.mongo.de_source_mongo.weather_observations
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --delete --topic src.mysql.de_source_mysql.station
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --delete --topic src.mysql.de_source_mysql.station_owner
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --delete --topic schemahistory.src.mysql
```
6.1 ની યાદીમાં બીજા `src.mongo.*` / `src.mysql.*` topics હોય તો એ પણ આ જ રીતે delete કરો.

> ⚠️ **Windows પર ધ્યાન રાખો:** Windows પર Kafka માં topic delete કરતી વખતે ક્યારેક broker `AccessDeniedException` આપીને crash થાય છે, કારણ કે Windows ખુલ્લી log files ને rename થવા દેતું નથી.
> - આવું થાય તો broker ફરી start કરો. મોટા ભાગે delete પૂરું થઈ જાય છે.
> - Broker વારંવાર crash થાય તો **આ step છોડી દો.** નવો setup નવા topic prefix (`mongo`) વાપરે છે, એટલે જૂના topics પડ્યા રહે તો પણ નવા pipeline ને કોઈ અસર નથી. એ ફક્ત disk પર જગ્યા રોકશે.

### 6.3 જૂના sink નો consumer group delete કરો
```powershell
.\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --list
.\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --delete --group connect-sink-bronze
```
➡️ `Deletion of requested consumer groups ('connect-sink-bronze') was successful.`

> `connect-offsets` topic માં જૂના source connectors (`source-mysql`, `source-mongo`) ના offsets રહી જશે. નવો connector અલગ નામ (`mongo-source`) વાપરે છે, એટલે એ offsets ક્યારેય વંચાશે નહીં અને નુકસાન નહીં કરે.

---

## 7. MySQL માંથી CDC setup સાફ કરો

MySQL Workbench અથવા `mysql` CLI માં **root** user થી:

### 7.1 Debezium user delete કરો
```sql
-- પહેલાં જુઓ કે user છે કે નહીં
SELECT user, host FROM mysql.user WHERE user = 'debezium';

DROP USER IF EXISTS 'debezium'@'localhost';

-- ફરી ચકાસો, હવે 0 rows આવવી જોઈએ
SELECT user, host FROM mysql.user WHERE user = 'debezium';
```

### 7.2 Binlog settings (optional)

MySQL 8.x માં binlog **default થી ON** હોય છે અને ROW format પણ default જ છે. એટલે Debezium માટે કરેલા settings પાછા બદલવાની **જરૂર નથી**. MySQL ના બીજા કામ (replication, point-in-time recovery) માં binlog ઉપયોગી છે.

જૂની binlog files થી disk ભરાતી હોય તો:
```sql
SHOW BINARY LOGS;
PURGE BINARY LOGS BEFORE NOW() - INTERVAL 1 DAY;
```

> ⚠️ `my.ini` માં `log_bin` બંધ (`skip-log-bin`) **ન કરો**, સિવાય કે તમને ખાતરી હોય કે બીજું કોઈ એનો ઉપયોગ નથી કરતું.

### 7.3 MySQL data નું શું થશે?
- `de_bronze.mysql_station` અને `de_bronze.mysql_station_owner` માં **નવો data આવવાનો બંધ** થશે. જૂનો data એમ ને એમ રહેશે.
- એ collections ની જરૂર ન હોય તો Step 9 માં drop કરી શકો છો.
- ભવિષ્યમાં MySQL data જોઈએ તો CDC વગર **Spark JDBC batch read** (`spark.read.format("jdbc")`) થી સીધો વાંચી શકાય છે.

---

## 8. Project ની connector files બદલો

Folder: `D:\ClaudeProjects\dataEngineeringProjects\sparkProject\kafka-connect\`

### 8.1 જૂની files delete કરો
Step 1 માં backup લઈ લીધો છે, એટલે:
```powershell
cd D:\ClaudeProjects\dataEngineeringProjects\sparkProject\kafka-connect
Remove-Item .\source-mysql.json, .\source-mongo.json, .\sink-bronze.json
```

### 8.2 નવી file: `mongo-source.json`

```json
{
  "connector.class": "com.mongodb.kafka.connect.MongoSourceConnector",
  "tasks.max": "1",
  "connection.uri": "mongodb://localhost:27017/?replicaSet=rs0",
  "database": "de_source_mongo",
  "topic.prefix": "mongo",
  "startup.mode": "copy_existing",
  "change.stream.full.document": "updateLookup",
  "publish.full.document.only": "true",
  "output.format.key": "json",
  "output.format.value": "json",
  "output.json.formatter": "com.mongodb.kafka.connect.source.json.formatter.SimplifiedJson",
  "key.converter": "org.apache.kafka.connect.storage.StringConverter",
  "value.converter": "org.apache.kafka.connect.storage.StringConverter"
}
```

| Setting | અર્થ |
|---|---|
| `database` (ફક્ત, `collection` વગર) | `de_source_mongo` ના **બધા collections** watch થાય |
| `topic.prefix: mongo` | Topic નું નામ `mongo.<database>.<collection>` બને, જેમ કે `mongo.de_source_mongo.weather_observations` |
| `startup.mode: copy_existing` | પહેલી વાર start થાય ત્યારે **હાલના બધા documents** copy કરે, પછી નવા changes વાંચે |
| `change.stream.full.document: updateLookup` | Update થાય ત્યારે ફક્ત બદલાયેલા fields નહીં, **આખો document** મોકલે |
| `publish.full.document.only: true` | Envelope વગર **ફક્ત document** મોકલે. આનાથી જ `before`/`after`/`source` જેવો wrapper નીકળી જાય છે |
| `SimplifiedJson` | Plain JSON બનાવે: `ObjectId` → hex string, date → ISO string, numbers → સાદા numbers (`$oid`, `$numberLong` વગર) |
| `StringConverter` (key/value) | Mongo connector જે JSON text બનાવે તે **જેમ છે તેમ** Kafka માં લખાય, ફરી wrap ન થાય |

> **નોંધ:** આ connector પણ અંદરથી MongoDB **change streams** વાપરે છે. આ MongoDB નું પોતાનું feature છે, Debezium નહીં. એટલે MongoDB replica set (`rs0`) ચાલુ હોવો જરૂરી છે, જે તમારે પહેલેથી છે.

### 8.3 નવી file: `mongo-sink-bronze.json`

```json
{
  "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
  "tasks.max": "1",
  "topics.regex": "mongo\\..*|mongo_.*",
  "connection.uri": "mongodb://localhost:27017/?replicaSet=rs0",
  "database": "de_bronze",
  "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.ProvidedInValueStrategy",
  "writemodel.strategy": "com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneDefaultStrategy",
  "transforms": "route,ingest",
  "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
  "transforms.route.regex": "mongo\\.[^.]+\\.(.*)",
  "transforms.route.replacement": "mongo_$1",
  "transforms.ingest.type": "org.apache.kafka.connect.transforms.InsertField$Value",
  "transforms.ingest.timestamp.field": "_ingested_at",
  "errors.tolerance": "all",
  "errors.log.enable": "true",
  "errors.deadletterqueue.topic.name": "dlq.sink-bronze",
  "errors.deadletterqueue.topic.replication.factor": "1",
  "errors.deadletterqueue.context.headers.enable": "true"
}
```

જૂના `sink-bronze.json` સાથે સરખામણી:

| Setting | જૂનું | નવું | કેમ? |
|---|---|---|---|
| `topics.regex` | `src\.(mongo\|mysql)\..*\|(mongo\|mysql)_.*` | `mongo\..*\|mongo_.*` | હવે MySQL topics નથી, અને નવો prefix `mongo` છે. બીજો ભાગ `mongo_.*` **જરૂરી છે**: `route` transform topic નું નામ `mongo_<collection>` કરે છે, અને MongoDB sink લખતા પહેલાં **બદલાયેલા નામ** ને ફરી `topics.regex` સાથે સરખાવે છે. એ match ન થાય તો task `Unknown topic` error સાથે FAILED થાય છે |
| `document.id.strategy` | `FullKeyStrategy` | `ProvidedInValueStrategy` | Mongo source નો key **resume token** છે, જે દરેક event માટે અલગ હોય. એ વાપરીએ તો દરેક update નવો document બનાવે. એટલે document ની અંદરનો `_id` વાપરીએ, જેથી update એ જ document ને **replace** કરે |
| `document.id.strategy.overwrite.existing` | `true` | (કાઢી નાખ્યું) | Value નો `_id` જ રાખવો છે |
| `transforms.route.regex` | `src\.(mongo\|mysql)\.[^.]+\.(.*)` | `mongo\.[^.]+\.(.*)` | નવો prefix |
| `transforms.route.replacement` | `$1_$2` | `mongo_$1` | Collection નું નામ પહેલાં જેવું જ રહે: `mongo_weather_observations` |

> Sink ને worker ના default `JsonConverter` (`schemas.enable=false`) મળે છે. એ topic ના JSON text ને map માં ફેરવે છે, એટલે `InsertField` (`_ingested_at`) કામ કરે છે. Sink માં converter override **ન કરો**.

### 8.4 `register-connectors.ps1` માં નામો બદલો

આ line:
```powershell
$names = "source-mysql", "source-mongo", "sink-bronze"
```
ને આ રીતે બદલો:
```powershell
$names = "mongo-source", "mongo-sink-bronze"
```

ઉપરની comment પણ update કરો:
```powershell
#   .\register-connectors.ps1              -> create/update all connectors (mongo-source, mongo-sink-bronze)
```

> Script file ના નામ પરથી જ connector નું નામ બનાવે છે (`$n.json`), એટલે file નામ અને connector નામ સરખા હોવા જોઈએ.

### 8.5 આખરે folder આવું દેખાવું જોઈએ
```
kafka-connect/
├── _backup_debezium/          ← Step 1 નો backup
├── mongo-sink-bronze.json
├── mongo-source.json
└── register-connectors.ps1
```

> `_backup_debezium` ને git માં commit ન કરવું હોય તો `.gitignore` માં `kafka-connect/_backup_debezium/` ઉમેરો. એમાં MySQL password પણ છે.

---

## 9. જૂનો bronze data સાફ કરો

જૂના collections માં Debezium envelope format વાળા documents છે. નવા format સાથે ભેળસેળ ન થાય એ માટે એમને drop કરો.

`mongosh` માં:
```javascript
use de_bronze
show collections

// Debezium envelope વાળો જૂનો Mongo data
db.mongo_weather_observations.drop()

// MySQL ના જૂના collections (જરૂર ન હોય તો)
db.mysql_station.drop()
db.mysql_station_owner.drop()

show collections
```

> જૂનો data રાખવો હોય તો drop ને બદલે rename કરો:
> `db.mongo_weather_observations.renameCollection("mongo_weather_observations_debezium_old")`

---

## 10. નવા connectors register કરો

```powershell
cd D:\ClaudeProjects\dataEngineeringProjects\sparkProject\kafka-connect
.\register-connectors.ps1
```

➡️ Output આવો હોવો જોઈએ:
```
applied mongo-source
applied mongo-sink-bronze
mongo-source      connector=RUNNING  tasks=RUNNING
mongo-sink-bronze connector=RUNNING  tasks=RUNNING
```

કોઈ task `FAILED` હોય તો script `ERROR:` line બતાવશે. વિગત માટે [Section 14](#14-troubleshooting) જુઓ.

---

## 11. End-to-end test

### 11.1 નવો topic બન્યો?
```powershell
cd C:\kafka
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --list
```
➡️ `mongo.de_source_mongo.weather_observations` દેખાવો જોઈએ.

### 11.2 Topic નો data જુઓ (Terminal #3, ચાલુ રાખો)
```powershell
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic mongo.de_source_mongo.weather_observations --from-beginning --formatter-property print.key=true
```

### 11.3 Source માં insert કરો (`mongosh`)
```javascript
use de_source_mongo
db.weather_observations.insertOne({ empId: 102, name: "Neha Shah", department: "Analytics", salary: 82000 })
```

➡️ Consumer માં **envelope વગરનો** document દેખાવો જોઈએ:
```
{"_id": {"_data": "8267..."}}	{"_id": "6ae0...", "empId": 102, "name": "Neha Shah", "department": "Analytics", "salary": 82000}
```
(પહેલો ભાગ key છે, એટલે કે resume token. બીજો ભાગ value છે.)

### 11.4 Bronze માં જુઓ
```javascript
use de_bronze
db.mongo_weather_observations.find().sort({ _ingested_at: -1 }).limit(2)
```
➡️ Section 0 માં બતાવ્યા જેવો સાદો document, ઉપરાંત `_ingested_at` field.

### 11.5 Update test
```javascript
use de_source_mongo
db.weather_observations.updateOne({ empId: 102 }, { $set: { salary: 90000 } })

use de_bronze
db.mongo_weather_observations.find({ empId: 102 })
```
➡️ **એક જ** document હોવો જોઈએ, જેમાં `salary: 90000` હોય. બે documents દેખાય તો Section 8.3 નો `document.id.strategy` ચકાસો.

---

## 12. Spark code અને docs update કરો

Bronze નું format બદલાયું છે, એટલે Spark jobs માં ફેરફાર કરવો પડશે.

### 12.1 Spark code
| File | શું બદલવું |
|---|---|
| `src/main/java/com/de/spark/pipeline/bronze2silver/BronzeSchemas.java` | `mongo_*` નો schema હાલ Debezium envelope ધારે છે (`_id:{id}`, `op`, `after STRING`, `source.ts_ms`). હવે `after` JSON parse કરવાની જરૂર નથી. Document ના fields **સીધા top level** પર છે, `_id` સાદી string છે, અને `op`/`source.ts_ms` નથી |
| Bronze → Silver job (જે `after` parse કરે છે) | `from_json(after, ...)` વાળું logic કાઢો અને fields સીધા વાંચો |
| MySQL (`mysql_*`) વાળા schemas અને jobs | MySQL CDC બંધ છે, એટલે એ ભાગ કાઢો અથવા disable કરો |
| Deletes/ordering માટે `op`, `__deleted`, `__source_ts_ms` વાપરતું logic | હવે આ fields નથી. Ordering માટે `_ingested_at` વાપરો |

### 12.2 Docs (Debezium નો ઉલ્લેખ દૂર કરવો)
| File | Section |
|---|---|
| `docs/MEDALLION_SETUP_GUIDE_GU.md` | Version table (Debezium line), ભાગ B (binlog, `debezium` user), D1 (plugin download), D4 (expected plugins), ભાગ E (connector configs), topics table, troubleshooting |
| `docs/TOOLS_INVENTORY_GU.md` | #19 Debezium MySQL, #20 Debezium MongoDB, CDC table, `debezium` DB user |
| `docs/MEDALLION_PIPELINE_PLAN_GU.md` | Architecture માં Debezium નો ઉલ્લેખ |
| `docs/CDC_TOOL_COMPARISON_GU.md` | નિર્ણય બદલાયો છે તે નોંધ ઉમેરો |

---

## 13. Checklist

- [ ] Step 1: Backup (`kafka-connect\_backup_debezium\`)
- [ ] Step 2: `source-mysql`, `source-mongo`, `sink-bronze` delete કર્યા
- [ ] Step 3: Kafka Connect બંધ કર્યું
- [ ] Step 4: `C:\kafka\plugins\debezium-connector-*` delete કર્યા, `mongo-kafka-connect` રાખ્યું
- [ ] Step 5: `connector-plugins` માં `io.debezium` નથી, `MongoSourceConnector` + `MongoSinkConnector` છે
- [ ] Step 6: `src.*` અને `schemahistory.*` topics તથા `connect-sink-bronze` group delete કર્યા
- [ ] Step 7: MySQL માંથી `debezium` user drop કર્યો
- [ ] Step 8: `mongo-source.json`, `mongo-sink-bronze.json` બનાવી, script માં નામો બદલ્યા
- [ ] Step 9: જૂના bronze collections drop/rename કર્યા
- [ ] Step 10: બંને connectors `RUNNING`
- [ ] Step 11: Insert અને update બંને test pass
- [ ] Step 12: Spark code અને docs update કર્યા

---

## 14. Troubleshooting

| સમસ્યા | કારણ | ઉકેલ |
|---|---|---|
| Step 4 માં `Remove-Item` પર *"being used by another process"* | Kafka Connect હજુ ચાલુ છે | Step 3 ફરી કરો. જરૂર પડે તો Task Manager માં `java.exe` (Connect) બંધ કરો |
| `connector-plugins` માં `MongoSourceConnector` નથી | `mongo-kafka-connect` folder ખૂટે છે કે ખોટી jar છે | `C:\kafka\plugins\mongo-kafka-connect\mongo-kafka-connect-3.1.2-all.jar` છે કે નહીં તે ચકાસો. `-all` વાળી jar જ જોઈએ |
| Register વખતે *"Failed to find any class that implements Connector ... io.debezium"* | જૂની JSON file હજુ register થાય છે | `register-connectors.ps1` માં `$names` ચકાસો (Step 8.4) |
| `mongo-source` FAILED: *"The $changeStream stage is only supported on replica sets"* | MongoDB replica set ચાલુ નથી | `mongosh` માં `rs.status()` ચલાવો. `mongod` ને `--replSet rs0` સાથે start કરો |
| `mongo-sink-bronze` task FAILED: *"Unknown topic: mongo_weather_observations, does not match: mongo\\..\*"* | `route` transform પછીનું topic નામ (`mongo_...`) `topics.regex` સાથે match થતું નથી. Sink લખતા પહેલાં બદલાયેલું નામ ફરીથી ચકાસે છે. આ config error છે, એટલે `errors.tolerance=all` હોવા છતાં record DLQ માં જતો નથી અને આખો task બંધ થાય છે | `topics.regex` ને `mongo\\..*\|mongo_.*` કરો (Step 8.3). પછી `.\register-connectors.ps1` ચલાવો. Task હજુ FAILED હોય તો: `Invoke-RestMethod -Method Post "http://localhost:8083/connectors/mongo-sink-bronze/restart?includeTasks=true&onlyFailed=true"`. Offset commit થયો ન હોવાથી કોઈ message ખોવાતો નથી |
| Topic બને છે પણ bronze ખાલી છે | Sink નો regex/route ખોટો છે અથવા DLQ માં જાય છે | `.\register-connectors.ps1 status` ચલાવો. પછી `dlq.sink-bronze` topic ને `--from-beginning` થી વાંચો અને headers માં error જુઓ |
| Sink error: *"Only Map objects supported in absence of schema for [field insertion]"* | Sink પર `StringConverter` લાગ્યો છે | Sink JSON માં `value.converter` **ન** રાખો. Worker નો default `JsonConverter` વાપરો |
| દરેક update પર bronze માં નવો document | `document.id.strategy` હજુ `FullKeyStrategy` છે | `ProvidedInValueStrategy` વાપરો (Step 8.3) |
| Bronze માં value `"{\"_id\": ...}"` જેવી **string** તરીકે દેખાય | Source પર `StringConverter` નથી, એટલે JSON બે વાર encode થયો | `mongo-source.json` માં `key.converter` અને `value.converter` = `StringConverter` ચકાસો |
| Source માં delete કર્યું પણ bronze માં document રહ્યો | `publish.full.document.only=true` હોય ત્યારે delete event માં document હોતો નથી, એટલે sink સુધી delete પહોંચતું નથી | Bronze ને history (append) layer તરીકે રાખવો હોય તો આ ચાલે. Delete પણ propagate કરવા હોય તો source પર `publish.full.document.only=false` રાખો અને sink પર `"change.data.capture.handler": "com.mongodb.kafka.connect.sink.cdc.mongodb.ChangeStreamHandler"` વાપરો. એ handler insert/update/delete ત્રણેય લાગુ કરે છે, પણ `RegexRouter` અને `InsertField` સાથે ફરી test કરવું પડશે |
| Step 6 માં topic delete પર broker crash | Windows file locking | Broker restart કરો. વારંવાર થાય તો topics delete કરવાનું છોડી દો (Step 6.2 ની નોંધ) |

### Rollback (Debezium પાછું લાવવું હોય તો)
1. `mongo-source` અને `mongo-sink-bronze` delete કરો.
2. Kafka Connect બંધ કરો. `MEDALLION_SETUP_GUIDE_GU.md` ના D1 પ્રમાણે Debezium plugins ફરી download કરીને `C:\kafka\plugins` માં મૂકો.
3. `_backup_debezium\*.json` પાછી `kafka-connect\` માં copy કરો અને script ના `$names` જૂના કરો.
4. MySQL માં `debezium` user ફરી બનાવો (setup guide B4).
5. Kafka Connect start કરો અને `.\register-connectors.ps1` ચલાવો.
