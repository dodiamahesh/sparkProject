# રોજનું કામ: Start / Stop અને Kafka ચકાસણીના બધા Commands (ગુજરાતી)

> **આ document શેના માટે:** રોજ pipeline ચાલુ / બંધ કરવી, અને Kafka ના topics, messages, DLQ, consumer groups, connectors જોવા.
> દરેક command માટે: **ક્યાં (કયું terminal, કયું folder)**, **શું લખવું**, **શું output આવવું જોઈએ**, અને **ન આવે તો શું કરવું**.
>
> પહેલી વાર setup (install, config files) આ document માં નથી: એ માટે [MONGO_CDC_COMPLETE_SETUP_GU.md](MONGO_CDC_COMPLETE_SETUP_GU.md). નીચે `(setup §3.5)` જેવું લખ્યું હોય એ એ guide નો વિભાગ છે.

---

## અનુક્રમણિકા

- [0. શરૂ કરતા પહેલાં](#0-શરૂ-કરતા-પહેલાં)
- [1. રોજનો Start (step by step)](#1-રોજનો-start-step-by-step)
- [2. રોજનો Stop](#2-રોજનો-stop)
- [3. Topics જોવા](#3-topics-જોવા)
- [4. કોઈ topic ના messages જોવા](#4-કોઈ-topic-ના-messages-જોવા)
- [5. DLQ ચકાસવો](#5-dlq-ચકાસવો)
- [6. Consumer groups (subscriber ક્યાં સુધી વાંચ્યું)](#6-consumer-groups-subscriber-ક્યાં-સુધી-વાંચ્યું)
- [7. Kafka Connect અને Connectors (REST API)](#7-kafka-connect-અને-connectors-rest-api)
- [8. MongoDB બાજુ ચકાસણી](#8-mongodb-બાજુ-ચકાસણી)
- [9. Logs, ports અને processes](#9-logs-ports-અને-processes)
- [10. Kafka Reset (બધું નવેસરથી): topics delete, storage format, fresh topics](#10-kafka-reset-બધું-નવેસરથી)
- [11. રોજનું 2 મિનિટનું Health Check](#11-રોજનું-2-મિનિટનું-health-check)
- [12. ઝડપી Troubleshooting](#12-ઝડપી-troubleshooting)
- [13. ક્યારેય ન કરો](#13-ક્યારેય-ન-કરો)

---

## 0. શરૂ કરતા પહેલાં

### 0.1 બધા નામો

| વસ્તુ | નામ |
|---|---|
| Source DB (transaction DB) | `sourceMongo_db` |
| Connector | `mongo-cdc-db-source` |
| Topic | `sourceMongo_db` (3 partitions) |
| DLQ topic | `cdc-dlq` (1 partition) |
| Consumer group | `mongo-cdc-subscriber` |
| Properties file | `subscriber.properties` (jar ની અંદર, default; અથવા `src\main\resources\subscriber.properties` path આપીને) |
| Bronze DB | `bronze_db` |
| MongoDB | `mongodb://localhost:27017/?replicaSet=rs0` |
| Kafka broker / Connect REST | `localhost:9092` / `http://localhost:8083` |

### 0.2 Terminals

બધું **Windows PowerShell** માં. દરેક લાંબી ચાલતી process માટે **અલગ window**, અને એ **ખુલ્લી રાખો**.

| Terminal | શું ચાલે | Folder |
|---|---|---|
| #1 | Kafka broker | `C:\kafka` |
| #2 | Kafka Connect | `C:\kafka` |
| #3 | Subscriber | `...\mongoKafkaConnectorDemo\mongo-cdc-subscriber` |
| #4 | ચકાસણી: Kafka tools, `register-connectors.ps1`, mongosh | જે command માં લખ્યું હોય તે |

> **Terminal નો નંબર = window નું નામ, steps નો ક્રમ નહીં.** Start ના steps આ ક્રમમાં ચાલે છે:
> `#4` (1.1 MongoDB check) → `#1` (1.2 broker) → `#2` (1.3 Connect) → `#4` (1.4 connector status) → **`#3` (1.5 subscriber)** → `#4` (1.6 LAG check).
> Terminal #3 ને 1.4 **પછી** વાપરીએ છીએ, કારણ કે subscriber શરૂ કરતા પહેલાં connector `RUNNING` છે એ ચકાસવું જરૂરી છે.

Project folder (ટૂંકમાં `$P`), Terminal #4 માં એક વાર:
```powershell
$P = "D:\ClaudeProjects\dataEngineeringProjects\sparkProject\mongoKafkaConnectorDemo"
```
> ⚠️ `$P` ફક્ત **એ જ PowerShell window** માં યાદ રહે છે. નવી window ખોલો અથવા terminal restart કરો તો આ line **ફરી ચલાવો**.
> ન ચલાવો તો `$P` ખાલી રહે, અને `cd $P\kafka-connect` ખોટા folder (`\kafka-connect`) માં જવાનો પ્રયાસ કરે: *"Cannot find path ... because it does not exist"*.
> ચકાસવા: `$P` લખીને Enter ➡️ project નો path દેખાવો જોઈએ.

**PowerShell નો નિયમ: એક line = એક command.** આ document માં block ની દરેક line **અલગ command** છે. Copy કરતી વખતે બે lines **એક line માં ભળી ન જાય** એ ધ્યાન રાખો. એક જ line માં બે commands લખવા હોય તો વચ્ચે `;` મૂકો:
```powershell
cd $P\kafka-connect                                # ✅ line 1
.\register-connectors.ps1 status                   # ✅ line 2

cd $P\kafka-connect; .\register-connectors.ps1 status   # ✅ એક line, વચ્ચે ;

cd $P\kafka-connect .\register-connectors.ps1 status    # ❌ ; વગર
```
❌ વાળી line નો error:
```
Set-Location : A positional parameter cannot be found that accepts argument '.\register-connectors.ps1'.
```
અર્થ: PowerShell એ આખી line ને **એક જ `cd` command** માની. `cd` ફક્ત એક path લે છે, એટલે વધારાનો `.\register-connectors.ps1` એને સમજાયો નહીં. ઉકેલ: બે lines માં લખો, અથવા વચ્ચે `;` મૂકો.

### 0.3 દરેક Kafka command સાથે આવતા "errors" જે અવગણવાના છે

| દેખાય | અર્થ |
|---|---|
| `ERROR Reconfiguration failed: No configuration found for '...'` | Kafka tools ને log4j2 file મળતી નથી. **નુકસાન નથી** |
| `The consumer rebalance protocol (KIP-848) is production-ready!` | ફક્ત માહિતી |
| `WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_') could collide` | Topic બનાવતી વખતે. **અવગણો** |

---

## 1. રોજનો Start (step by step)

> **ક્રમ મહત્વનો છે:** MongoDB → Kafka broker → Kafka Connect → connector status → Subscriber(s).
> આગળનું step ત્યારે જ કરો જ્યારે પાછલા step નું ➡️ output આવી જાય.

### 1.1 MongoDB ચાલુ છે? (Terminal #4)

```powershell
Get-Service MongoDB
```
➡️ Expected:
```
Status   Name               DisplayName
------   ----               -----------
Running  MongoDB            MongoDB Server (MongoDB)
```
`Stopped` હોય તો **Admin PowerShell** માં: `Start-Service MongoDB`

Replica set PRIMARY છે? (CDC માટે જરૂરી)
```powershell
mongosh "mongodb://localhost:27017/?replicaSet=rs0" --quiet --eval "rs.status().members.map(m => m.name + ' ' + m.stateStr)"
```
➡️ `[ 'localhost:27017 PRIMARY' ]`

| ન આવે તો | ઉકેલ |
|---|---|
| `MongoServerSelectionError` / timeout | Service બંધ છે, અથવા હજુ શરૂ થાય છે. 10 સેકન્ડ રાહ જુઓ |
| `no replset config has been received` | Replica set શરૂ નથી (setup §2.4: `rs.initiate`) |

### 1.2 Kafka broker (Terminal #1, ખુલ્લું રાખો)

```powershell
cd C:\kafka
$env:KAFKA_HEAP_OPTS = "-Xms512m -Xmx512m"
.\bin\windows\kafka-server-start.bat .\config\server.properties
```
➡️ છેલ્લે આવી line દેખાય ત્યાં સુધી રાહ જુઓ (5–15 સેકન્ડ):
```
[... ] INFO [KafkaRaftServer nodeId=1] Kafka Server started (kafka.server.KafkaRaftServer)
```

| ન આવે તો | ઉકેલ |
|---|---|
| `No meta.properties found` | Storage format નથી (setup §3.4) |
| `AccessDeniedException: ...\__cluster_metadata-0\...checkpoint.deleted` (start થતાં જ `Encountered fatal fault`) | જૂની KRaft snapshot files **ReadOnly** છે, Windows પર Kafka એમને delete કરી શકતું નથી. Broker બંધ હોય ત્યારે: `Remove-Item C:\kafka-data\kraft-combined-logs\__cluster_metadata-0\*.checkpoint.deleted -Force`, પછી ફરી start ([12](#12-ઝડપી-troubleshooting)) |
| `AccessDeniedException` / *all log dirs have failed* | `-delete` folders દૂર કરો ([12](#12-ઝડપી-troubleshooting)), પછી ફરી start |
| `Address already in use` (9092) | જૂનો broker હજુ ચાલે છે ([9.3](#93-ports-અને-processes)) |

### 1.3 Kafka Connect (Terminal #2, ખુલ્લું રાખો)

```powershell
cd C:\kafka
$env:KAFKA_HEAP_OPTS = "-Xms512m -Xmx768m"
.\bin\windows\connect-distributed.bat .\config\connect-distributed-de.properties
```
➡️ 30–60 સેકન્ડ પછી:
```
[... ] INFO Kafka Connect started (org.apache.kafka.connect.runtime.Connect)
```
> **Connectors આપોઆપ resume થાય છે.** Config Kafka ના `connect-configs` topic માં સચવાયેલો છે, એટલે રોજ register કરવાની જરૂર **નથી**.

### 1.4 Connector status (Terminal #4)

> Terminal #3 (subscriber) **હજુ નહીં**: પહેલાં અહીં connector `RUNNING` ચકાસો, પછી 1.5 માં Terminal #3.

```powershell
$P = "D:\ClaudeProjects\dataEngineeringProjects\sparkProject\mongoKafkaConnectorDemo"   # નવી window હોય તો
cd $P\kafka-connect
.\register-connectors.ps1 status
```
> આ **ત્રણ અલગ lines** છે (અથવા એક line: `cd $P\kafka-connect; .\register-connectors.ps1 status`). કેમ: [0.2](#02-terminals).

➡️ Expected:
```
mongo-cdc-db-source            connector=RUNNING  tasks=RUNNING
```

| દેખાય | અર્થ / ઉકેલ |
|---|---|
| `Set-Location : A positional parameter cannot be found that accepts argument '.\register-connectors.ps1'` | બે commands `;` વગર એક line માં ભળી ગયા. બે lines માં લખો અથવા વચ્ચે `;` ([0.2](#02-terminals)) |
| `Cannot find path '...\kafka-connect' because it does not exist` | આ window માં `$P` set નથી. ઉપરની પહેલી line ચલાવો |
| `.\register-connectors.ps1 : The term ... is not recognized` | તમે `kafka-connect` folder માં નથી (`cd` ચાલ્યો નહીં). `Get-Location` થી ચકાસો |
| `Unable to connect to the remote server` | Connect હજુ start થાય છે. 30 સેકન્ડ પછી ફરી |
| કોઈ line નથી | Connector register નથી: `.\register-connectors.ps1` (અથવા `apply <name>`) |
| `tasks=FAILED` + `ERROR: ...` | Error વાંચો. મોટા ભાગે MongoDB બંધ / PRIMARY નથી. ઠીક કરીને [7.4](#74-connector-restart--pause--resume) થી restart |
| `tasks=` ખાલી | Task હજુ બને છે. 10 સેકન્ડ પછી ફરી `status` |
| `mongo-cdc-db-source` સિવાય બીજો connector | જૂનો connector: `.\register-connectors.ps1 delete <name>` |

> `running scripts is disabled` આવે તો એક વાર: `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`

### 1.5 Subscriber (Terminal #3, ખુલ્લું રાખો)

```powershell
cd D:\ClaudeProjects\dataEngineeringProjects\sparkProject\mongoKafkaConnectorDemo\mongo-cdc-subscriber
java -jar target\mongo-cdc-subscriber-1.0.0-jar-with-dependencies.jar
```
`subscriber.properties` બદલ્યા પછી ફરી build ન કરવો હોય તો path આપો:
```powershell
java -jar target\mongo-cdc-subscriber-1.0.0-jar-with-dependencies.jar src\main\resources\subscriber.properties
```
➡️ Expected:
```
[main] INFO com.de.cdc.SubscriberApp - Reading topic 'sourceMongo_db' and writing to database 'bronze_db'
```
પછી કોઈ change આવે ત્યારે જ log આવે:
```
[main] INFO com.de.cdc.ChangeEventApplier - bank_payments: 3 writes (matched=3, inserted=0)
```
> `matched` = bronze માં પહેલેથી હતા (update / soft delete), `inserted` = નવા બન્યા. કંઈ નવું ન હોય તો **કોઈ log નહીં**, એ સામાન્ય છે.
> ⚠️ `Reading topic 'de_source_mongo' ... 'de_bronze'` દેખાય તો jar જૂનો છે (properties jar ની અંદર છે): subscriber બંધ → `mvn -q clean package` → ફરી start.

| ન આવે તો | ઉકેલ |
|---|---|
| `Unable to access jarfile` | Build નથી: `mvn -q clean package` (setup §8.1) |
| `MongoTimeoutException` | MongoDB બંધ (1.1) |
| લાંબો સમય `Node -1 disconnected` / `Connection to node ... could not be established` | Kafka broker બંધ (1.2) |
| `NoSuchFileException: ...subscriber.properties` | ખોટા folder માં છો. `mongo-cdc-subscriber` folder માં જાઓ |

### 1.6 બધું બરાબર ચાલુ થયું? (Terminal #4)

```powershell
cd C:\kafka
.\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --describe --group mongo-cdc-subscriber
```
➡️ દરેક partition માં `LAG 0` અને `CONSUMER-ID` ભરેલો (subscriber જોડાયેલો). પૂરું output [6.2](#62-describe-lag-અને-offsets) માં.

---

## 2. રોજનો Stop

**ઉલટા ક્રમમાં**, દરેક terminal માં `Ctrl + C`:

| # | Terminal | Expected (છેલ્લી lines) |
|---|---|---|
| 1 | #3 Subscriber | `INFO com.de.cdc.SubscriberApp - Stopped` (છેલ્લો batch પૂરો કરીને) |
| 2 | #2 Kafka Connect | `INFO Kafka Connect stopped` (source offsets save કરીને) |
| 3 | #1 Kafka broker | `INFO [KafkaRaftServer nodeId=1] shut down completed` |

`Terminate batch job (Y/N)?` પૂછે તો `Y`.

> ⚠️ **Kafka broker ને Connect પહેલાં બંધ ન કરો.** Connect છેલ્લા offsets save નહીં કરી શકે, અને restart પર થોડા events ફરી આવશે (subscriber idempotent છે એટલે નુકસાન નથી, પણ ક્રમ સાચવો).
> MongoDB Windows service છે, એને બંધ કરવાની જરૂર નથી.

બધું બંધ થયું? (Terminal #4)
```powershell
Get-Process java -ErrorAction SilentlyContinue
```
➡️ કંઈ output નહીં = બધા Java processes બંધ. Process દેખાય અને terminal બંધ કરી દીધું હોય તો [9.3](#93-ports-અને-processes).

---

## 3. Topics જોવા

**બધા commands Terminal #4 માં, `cd C:\kafka` પછી.**

### 3.1 બધા topics ની list

```powershell
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --list
```
➡️ Expected (ક્રમ અલગ હોઈ શકે):
```
__consumer_offsets
connect-configs
connect-offsets
connect-status
cdc-dlq
sourceMongo_db
```
(જૂના `de_source_mongo`, `de_source_mongo.dlq`, `sourceMongo_db.dlq` પણ દેખાઈ શકે: [5.4](#54-જૂના-dlq-topics-દૂર-કરવા).)

| Topic | શું છે |
|---|---|
| `sourceMongo_db` | CDC events (source DB નો એક topic, બધા collections એમાં) |
| `cdc-dlq` | **એક જ DLQ, બધા CDC topics માટે**: subscriber વાંચી ન શક્યો એવા messages. Header `source.topic` કહે કે કયા topic નો છે |
| `__consumer_offsets` | Consumer groups ક્યાં સુધી વાંચ્યું (Kafka internal) |
| `connect-configs` / `connect-offsets` / `connect-status` | Kafka Connect નું state: connector config, source offsets (resume token), status |

> ⚠️ `cdc.sourceMongo_db.bank_payments` જેવો topic દેખાય તો RegexRouter ખોટો છે (setup §6.1).

ફક્ત આપણા topics (internal વગર):
```powershell
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --list --exclude-internal | Select-String -NotMatch "^connect-"
```

### 3.2 એક topic ની વિગત (partitions, leader)

```powershell
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --describe --topic sourceMongo_db
```
➡️ Expected:
```
Topic: sourceMongo_db  TopicId: xxxxxxxxxxxxxxxx  PartitionCount: 3  ReplicationFactor: 1  Configs:
        Topic: sourceMongo_db  Partition: 0  Leader: 1  Replicas: 1  Isr: 1  ...
        Topic: sourceMongo_db  Partition: 1  Leader: 1  Replicas: 1  Isr: 1  ...
        Topic: sourceMongo_db  Partition: 2  Leader: 1  Replicas: 1  Isr: 1  ...
```
ચકાસો: `PartitionCount: 3` (DLQ માં 1), `Leader: 1` (`none` / `-1` હોય તો broker માં problem).

બધા topics એકસાથે: `--describe` (`--topic` વગર).

### 3.3 Topic માં કેટલા messages છે?

```powershell
.\bin\windows\kafka-get-offsets.bat --bootstrap-server localhost:9092 --topic sourceMongo_db
```
➡️ Expected (`topic:partition:છેલ્લો offset`):
```
sourceMongo_db:0:42
sourceMongo_db:1:37
sourceMongo_db:2:51
```
Retention બંધ છે (`log.retention.ms=-1`), એટલે topic માં **શરૂઆતથી બધા** messages છે: કુલ = 42 + 37 + 51 = **130**.

કુલ એક જ number માં:
```powershell
(.\bin\windows\kafka-get-offsets.bat --bootstrap-server localhost:9092 --topic sourceMongo_db | Select-String "^sourceMongo_db:" | ForEach-Object { [long]($_.Line -split ":")[-1] } | Measure-Object -Sum).Sum
```
➡️ `130`

> Messages વધતા નથી? Source માં change કર્યો? Connector `RUNNING` છે (1.4)?

### 3.4 નવો topic બનાવવો (નવો flow ઉમેરતી વખતે જ)

```powershell
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --create --topic sourceMongo_db --partitions 3 --replication-factor 1
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --create --topic cdc-dlq --partitions 1 --replication-factor 1
```
➡️ `Created topic sourceMongo_db.`
પહેલેથી હોય તો: `Topic 'sourceMongo_db' already exists.` (નુકસાન નથી).

> ⚠️ Windows પર topic **delete ન કરો** ([13](#13-ક્યારેય-ન-કરો)).

---

## 4. કોઈ topic ના messages જોવા

**Terminal #4, `cd C:\kafka`.** બંધ કરવા: `Ctrl + C` (`Terminate batch job (Y/N)?` → `Y`).

> ✅ **Console consumer થી subscriber ને કોઈ અસર નથી.** `--group` ન આપો તો Kafka એક નવો, કામચલાઉ group (`console-consumer-xxxxx`) બનાવે છે. આપણા `mongo-cdc-subscriber` group નો offset **બદલાતો નથી**.
> ⚠️ ક્યારેય `--group mongo-cdc-subscriber` આપીને console consumer ન ચલાવો: એ subscriber ના messages "ખાઈ" જશે.

### 4.1 શરૂઆતથી બધા messages (key, partition, offset સાથે)

```powershell
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic sourceMongo_db --from-beginning --formatter-property print.key=true --formatter-property print.partition=true --formatter-property print.offset=true
```
➡️ દરેક message એક line, tab થી અલગ: `Partition:<n>  Offset:<n>  <key>  <value>`
```
Partition:0	Offset:0	{"_id": {"$oid": "6ae0e6da..."}}	{"_id": {"_id": {"$oid": "6ae0e6da..."}, "copyingData": true}, "operationType": "insert", "documentKey": {"_id": {"$oid": "6ae0e6da..."}}, "fullDocument": {...}, "ns": {"db": "sourceMongo_db", "coll": "bank_payments"}}
```
| ભાગ | અર્થ |
|---|---|
| key | `documentKey` = document નો `_id`. એક document ના બધા events એક જ partition માં, ક્રમમાં |
| `operationType` | `insert` / `update` / `replace` / `delete` / `drop` |
| `copyingData: true` | `copy_existing` વખતે આવેલો (પહેલી વાર register વખતે હાલનો data) |
| `ns.coll` | કયું collection |
| `fullDocument` | આખો document (delete માં નથી હોતો) |

> Message કેવો દેખાય તેનો પૂરો example: [CDC_EXAMPLE_ORDERS_COMPARISON_GU.md](CDC_EXAMPLE_ORDERS_COMPARISON_GU.md)
> કંઈ ન દેખાય: `--from-beginning` ભૂલી ગયા? (એના વગર ફક્ત **નવા** messages દેખાય.)

### 4.2 ફક્ત પહેલા N messages, અને પછી આપોઆપ બંધ

```powershell
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic sourceMongo_db --from-beginning --max-messages 5
```
➡️ 5 messages, પછી `Processed a total of 5 messages` અને command પૂરો.

### 4.3 બધા messages વાંચીને આપોઆપ બંધ (wait ન કરે)

```powershell
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic sourceMongo_db --from-beginning --timeout-ms 10000
```
➡️ 10 સેકન્ડ સુધી નવો message ન આવે તો બંધ: `Processed a total of 130 messages` (3.3 નો total).

### 4.4 ફક્ત હવે પછીના (live) messages

```powershell
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic sourceMongo_db --formatter-property print.partition=true
```
➡️ પહેલાં કંઈ નહીં. બીજા terminal માં source DB માં change કરો (mongosh), તો અહીં તરત message દેખાય. **Live CDC જોવાનો સૌથી સરળ રસ્તો.**

### 4.5 એક partition, ચોક્કસ offset થી

```powershell
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic sourceMongo_db --partition 2 --offset 40 --max-messages 3 --formatter-property print.offset=true
```
➡️ Partition 2 ના offset 40, 41, 42. (`--offset` સાથે `--partition` જરૂરી. `--offset earliest` / `latest` પણ ચાલે.)

**છેલ્લા 5 messages** (એક partition ના): 3.3 થી છેલ્લો offset જુઓ (જેમ કે 51), પછી `--offset 46 --partition 2 --max-messages 5`.

### 4.6 Time સાથે

```powershell
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic sourceMongo_db --from-beginning --timeout-ms 10000 --formatter-property print.timestamp=true
```
➡️ `CreateTime:1795349200123  {...}` (epoch millis). Readable time:
```powershell
[DateTimeOffset]::FromUnixTimeMilliseconds(1795349200123).LocalDateTime
```

### 4.7 Filter: એક collection, અથવા ફક્ત deletes

PowerShell ના `Select-String` થી (bat command નું output text છે):
```powershell
# ફક્ત bank_payments collection ના events
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic sourceMongo_db --from-beginning --timeout-ms 10000 | Select-String '"coll":\s*"bank_payments"'

# ફક્ત DELETE events (hard delete → bronze soft delete test)
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic sourceMongo_db --from-beginning --timeout-ms 10000 | Select-String '"operationType":\s*"delete"'

# એક document (business key) ના બધા events, ક્રમમાં
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic sourceMongo_db --from-beginning --timeout-ms 10000 | Select-String 'PAY-0005'

# ગણતરી: કેટલા delete events?
(.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic sourceMongo_db --from-beginning --timeout-ms 10000 | Select-String '"operationType":\s*"delete"').Count
```
➡️ Sample-data ના બધા 02 scripts પછી `sourceMongo_db` માં delete events: **23** (+ 01 ફરી ચલાવ્યું હોય તો જૂના documents ના વધારાના deletes).

> Delete event માં `fullDocument` નથી હોતો, ફક્ત `documentKey._id`. એટલે `'PAY-0005'` થી delete event **નહીં મળે**; એ માટે bronze માંથી `_id` લો અને એનાથી શોધો.

### 4.8 Messages ને file માં સાચવવા

```powershell
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic sourceMongo_db --from-beginning --timeout-ms 10000 --formatter-property print.key=true | Out-File -Encoding utf8 "$env:TEMP\sourceMongo_db_messages.txt"
notepad "$env:TEMP\sourceMongo_db_messages.txt"
```

---

## 5. DLQ ચકાસવો

DLQ = subscriber જે message **વાંચી ન શક્યો** (ખરાબ JSON, જરૂરી field નથી) તે અહીં જાય છે, અને બાકીના messages અટકતા નથી.

**એક જ DLQ topic: `cdc-dlq`** (1 partition), **બધા CDC topics અને subscribers માટે**. નવો source DB / topic ઉમેરો તો નવો DLQ **ન** બનાવો; એની properties file માં પણ `kafka.dlq.topic=cdc-dlq` રાખો. કયો message ક્યાંથી આવ્યો તે headers કહે છે (5.2).

### 5.1 DLQ ખાલી છે? (રોજ ચકાસો)

```powershell
cd C:\kafka
.\bin\windows\kafka-get-offsets.bat --bootstrap-server localhost:9092 --topic cdc-dlq
```
➡️ Expected: `cdc-dlq:0:0` (છેલ્લો number `0` = ખાલી).

### 5.2 DLQ ના messages, error સાથે

```powershell
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic cdc-dlq --from-beginning --timeout-ms 5000 --formatter-property print.headers=true --formatter-property print.offset=true
```
➡️ ખાલી હોય તો: `Processed a total of 0 messages` ✅

Message હોય તો:
```
Offset:0	error:org.bson.json.JsonParseException: ...,source:sourceMongo_db-1-57,source.topic:sourceMongo_db,source.partition:1,source.offset:57	<original message>
```
| Header | અર્થ |
|---|---|
| `error` | કેમ વાંચી ન શકાયો (exception) |
| `source` | મૂળ message ક્યાંથી, એક જ text માં: `<topic>-<partition>-<offset>` |
| `source.topic` | મૂળ **topic** (એક DLQ માં ઘણા topics ના messages હોય, એટલે આનાથી અલગ પાડો) |
| `source.partition` / `source.offset` | મૂળ message ની જગ્યા. જોવા: [4.5](#45-એક-partition-ચોક્કસ-offset-થી) `--topic sourceMongo_db --partition 1 --offset 57 --max-messages 1` |

> `source.topic` / `source.partition` / `source.offset` headers નવા subscriber build થી આવે છે. એ પહેલાંના DLQ messages માં ફક્ત `error` અને `source` હોય.

**ફક્ત એક topic ના DLQ messages:**
```powershell
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic cdc-dlq --from-beginning --timeout-ms 5000 --formatter-property print.headers=true | Select-String 'source\.topic:sourceMongo_db(,|\s)'
```
**કયા topic ના કેટલા DLQ messages:**
```powershell
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic cdc-dlq --from-beginning --timeout-ms 5000 --formatter-property print.headers=true |
    Select-String 'source\.topic:([^,\s]+)' | ForEach-Object { $_.Matches[0].Groups[1].Value } | Group-Object | Select-Object Name, Count
```
➡️ `Name = sourceMongo_db  Count = 2` જેવું (DLQ ખાલી હોય તો કંઈ નહીં).

> DLQ માં message = data **ખોવાયો નથી**, ફક્ત bronze માં **નથી ગયો**. Error ઠીક કર્યા પછી એ document ને source માં ફરી touch (update) કરો, તો નવો event આવશે.

### 5.3 `cdc-dlq` બનાવવો (ફક્ત એક વાર)

**Subscriber start કરતા પહેલાં** બનાવો (નહીં તો પહેલો DLQ message આવે ત્યારે Kafka એને broker ના default પ્રમાણે આપોઆપ બનાવે):
```powershell
cd C:\kafka
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --create --topic cdc-dlq --partitions 1 --replication-factor 1
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --describe --topic cdc-dlq
```
➡️ `Created topic cdc-dlq.` અને `PartitionCount: 1`. 1 partition = બધા DLQ messages એક જ ક્રમમાં.

પછી subscriber નો jar નવો હોવો જોઈએ (`kafka.dlq.topic=cdc-dlq` jar ની અંદર છે):
```powershell
cd D:\ClaudeProjects\dataEngineeringProjects\sparkProject\mongoKafkaConnectorDemo\mongo-cdc-subscriber
mvn -q clean package        # subscriber બંધ હોય ત્યારે જ
```

### 5.4 જૂના DLQ topics દૂર કરવા

જૂના: `de_source_mongo.dlq`, `sourceMongo_db.dlq`. પહેલાં જુઓ કે કયા છે અને એમાં કંઈ છે કે નહીં:
```powershell
cd C:\kafka
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --list | Select-String "dlq"
.\bin\windows\kafka-get-offsets.bat --bootstrap-server localhost:9092 --topic de_source_mongo.dlq
.\bin\windows\kafka-get-offsets.bat --bootstrap-server localhost:9092 --topic sourceMongo_db.dlq
```
➡️ છેલ્લો number `0` = ખાલી. Messages હોય અને રાખવા હોય તો પહેલાં file માં save કરો ([4.8](#48-messages-ને-file-માં-સાચવવા), `--topic <જૂનો dlq>`).

> ⚠️ **Windows પર topic delete કરવાથી Kafka broker crash થઈ શકે છે** (`AccessDeniedException`, *"all log dirs have failed"*), કારણ કે Windows ખુલ્લી files rename/delete થવા દેતું નથી.
> **વિકલ્પ A (ભલામણ, કોઈ જોખમ નહીં):** જૂના DLQ topics **રહેવા દો**. કોઈ એમાં લખતું નથી, disk પર નહિવત્ જગ્યા લે છે.
> **વિકલ્પ B:** નીચેના steps થી, **controlled restart** સાથે delete કરો.

**વિકલ્પ B: delete (controlled restart સાથે)**

1. Subscriber બંધ (Terminal #3, `Ctrl + C`).
2. Delete (Terminal #4):
   ```powershell
   cd C:\kafka
   .\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --delete --topic de_source_mongo.dlq
   .\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --delete --topic sourceMongo_db.dlq
   ```
   ➡️ Output કંઈ નહીં (અથવા `Topic ... does not exist` જો એ topic હતો જ નહીં).
3. Broker crash થાય કે ન થાય, **તરત controlled restart** કરો: Connect (Terminal #2) `Ctrl + C` → broker (Terminal #1) `Ctrl + C`.
4. Broker બંધ થયા પછી બાકી રહેલા `-delete` folders હાથે દૂર કરો:
   ```powershell
   Get-ChildItem C:\kafka-data\kraft-combined-logs -Directory | Where-Object Name -like "*-delete"
   Get-ChildItem C:\kafka-data\kraft-combined-logs -Directory | Where-Object Name -like "*-delete" | Remove-Item -Recurse -Force
   ```
   ➡️ પહેલો command delete થનારા folders બતાવે (જેમ કે `sourceMongo_db.dlq-0.<id>-delete`), બીજો એમને દૂર કરે.
5. ફરી start: broker (1.2) → Connect (1.3) → connector status (1.4) → subscriber (1.5).
6. ચકાસો:
   ```powershell
   .\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --list | Select-String "dlq"
   ```
   ➡️ ફક્ત `cdc-dlq`.

Broker start ન થાય (`AccessDeniedException`) તો step 4 ફરી કરો. પછી પણ ન ચાલે તો [10. Kafka Reset](#10-kafka-reset-બધું-નવેસરથી).

---

## 6. Consumer groups (subscriber ક્યાં સુધી વાંચ્યું)

**Terminal #4, `cd C:\kafka`.**

### 6.1 બધા groups

```powershell
.\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --list
```
➡️ Expected:
```
mongo-cdc-subscriber
mongo-cdc-subscriber
connect-...                 (Connect ના internal, હોઈ શકે)
console-consumer-12345      (4. ના console consumer ના કામચલાઉ groups, નુકસાન નથી)
```

### 6.2 Describe: LAG અને offsets

```powershell
.\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --describe --group mongo-cdc-subscriber
```
➡️ Expected (subscriber ચાલુ, બધું વાંચી ચૂક્યો):
```
GROUP                 TOPIC            PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID                     HOST        CLIENT-ID
mongo-cdc-subscriber  sourceMongo_db  0          42              42              0    consumer-mongo-cdc-...-1-xxxx   /127.0.0.1  consumer-mongo-cdc-...
mongo-cdc-subscriber  sourceMongo_db  1          37              37              0    consumer-mongo-cdc-...-1-xxxx   /127.0.0.1  consumer-mongo-cdc-...
mongo-cdc-subscriber  sourceMongo_db  2          51              51              0    consumer-mongo-cdc-...-1-xxxx   /127.0.0.1  consumer-mongo-cdc-...
```
| Column | અર્થ |
|---|---|
| `CURRENT-OFFSET` | Subscriber એ commit કરેલો offset (bronze માં લખ્યા **પછી** જ commit થાય) |
| `LOG-END-OFFSET` | Topic માં છેલ્લો offset (3.3) |
| **`LAG`** | હજુ વાંચવાના બાકી messages. **`0` = bronze up to date** |
| `CONSUMER-ID` | ભરેલો = subscriber જોડાયેલો. `-` = subscriber બંધ |

| દેખાય | અર્થ / ઉકેલ |
|---|---|
| `LAG > 0` અને ઘટે છે | Subscriber કામ કરે છે, થોડી રાહ જુઓ |
| `LAG > 0` અને ઘટતો નથી | Subscriber બંધ (`CONSUMER-ID` = `-`), અથવા error માં (Terminal #3 જુઓ) |
| `Consumer group 'mongo-cdc-subscriber' has no active members.` | Subscriber બંધ છે. Offsets હજુ દેખાય છે; start કરો તો ત્યાંથી આગળ વાંચશે |
| `Consumer group ... does not exist` | Subscriber ક્યારેય ચાલ્યો નથી (પહેલી વાર start કરો તો શરૂઆતથી વાંચશે) |

### 6.3 Group નો state અને members

```powershell
.\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --describe --group mongo-cdc-subscriber --state
.\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --describe --group mongo-cdc-subscriber --members
```
➡️ `--state`: `STATE` = `Stable` (subscriber ચાલુ) અથવા `Empty` (બંધ). `--members`: દરેક subscriber instance અને એના `#PARTITIONS` (1 instance = 3 partitions).

### 6.4 Offsets reset (replay: bronze ફરી બનાવવો)

> ⚠️ **પહેલાં subscriber બંધ કરો** (`Ctrl + C`), નહીં તો `Error: Assignments can only be reset if the group ... is inactive`.
> પૂરા replay steps (bronze collections drop સાથે): setup §12.1.

પહેલાં **dry run** (ફક્ત બતાવે, કંઈ બદલે નહીં):
```powershell
.\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --group mongo-cdc-subscriber --topic sourceMongo_db --reset-offsets --to-earliest --dry-run
```
➡️ `NEW-OFFSET` column માં `0` દેખાય.

સાચે reset:
```powershell
.\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --group mongo-cdc-subscriber --topic sourceMongo_db --reset-offsets --to-earliest --execute
```
બીજા વિકલ્પો (`--to-earliest` ની જગ્યાએ):

| Option | અર્થ |
|---|---|
| `--to-latest` | બધું "વાંચ્યું" ગણો (જૂના messages skip) |
| `--to-datetime 2026-11-22T10:00:00.000` | આ સમય પછીના messages ફરી |
| `--shift-by -10` | દરેક partition માં 10 messages પાછળ |
| `--topic sourceMongo_db:1 --to-offset 30` | ફક્ત partition 1, offset 30 થી |

પછી subscriber start કરો. Subscriber idempotent (replace / soft delete by `_id`) હોવાથી ફરી વાંચવાથી duplicates **નહીં** બને.

---

## 7. Kafka Connect અને Connectors (REST API)

**Terminal #4.** Kafka Connect (Terminal #2) ચાલુ હોવો જોઈએ. PowerShell 5.1 માં `Invoke-RestMethod` ને `( )` માં રાખો, નહીં તો `Select-Object` / `ConvertTo-Json` ખાલી બતાવે.

### 7.1 Connect ચાલુ છે? અને કયા plugins છે?

```powershell
Invoke-RestMethod http://localhost:8083/
(Invoke-RestMethod http://localhost:8083/connector-plugins) | Select-Object class, version
```
➡️ `version : 4.3.1 ...` અને list માં:
```
com.mongodb.kafka.connect.MongoSourceConnector   3.1.2
com.mongodb.kafka.connect.MongoSinkConnector     3.1.2
```

### 7.2 Connectors ની list અને status

```powershell
Invoke-RestMethod http://localhost:8083/connectors
cd $P\kafka-connect; .\register-connectors.ps1 status
```
➡️ `mongo-cdc-db-source`, અને `connector=RUNNING tasks=RUNNING`.

એક connector નો પૂરો status (failed task નો error trace સાથે):
```powershell
(Invoke-RestMethod http://localhost:8083/connectors/mongo-cdc-db-source/status) | ConvertTo-Json -Depth 5
```

### 7.3 Connector નો હાલનો config (Connect માં સચવાયેલો)

```powershell
(Invoke-RestMethod http://localhost:8083/connectors/mongo-cdc-db-source/config) | ConvertTo-Json
```
➡️ `kafka-connect\mongo-cdc-db-source.json` જેવો જ (+ `name`). અલગ હોય તો file બદલ્યા પછી register (`apply`) કરવાનું રહી ગયું.

### 7.4 Connector restart / pause / resume

```powershell
# Failed task restart (MongoDB ફરી ચાલુ થયા પછી)
Invoke-RestMethod -Method Post "http://localhost:8083/connectors/mongo-cdc-db-source/restart?includeTasks=true&onlyFailed=true"

# થોડા સમય માટે CDC રોકવો (source changes Kafka માં નહીં જાય, resume પછી આવશે)
Invoke-RestMethod -Method Put http://localhost:8083/connectors/mongo-cdc-db-source/pause
Invoke-RestMethod -Method Put http://localhost:8083/connectors/mongo-cdc-db-source/resume
```
➡️ `status` માં `PAUSED`, પછી `RUNNING`. Pause દરમ્યાનના changes **ખોવાતા નથી** (resume token થી આગળ વાંચે), જ્યાં સુધી MongoDB oplog માં એ changes હજુ છે.

### 7.5 Connector register / update / delete

```powershell
cd $P\kafka-connect
.\register-connectors.ps1                                     # kafka-connect ની JSON file(s) register / update
.\register-connectors.ps1 apply mongo-cdc-db-source  # ફક્ત એક
.\register-connectors.ps1 delete mongo-cdc-db-source # connector બંધ અને દૂર (offsets Kafka માં રહે)
```
➡️ `applied <name>` અને પછી status. Delete: `deleted <name>`.

> JSON file delete કરવાથી connector બંધ **નથી** થતો. `delete` જ વાપરો.

### 7.6 Connector ક્યાં સુધી વાંચ્યું? (source offset / resume token)

```powershell
(Invoke-RestMethod http://localhost:8083/connectors/mongo-cdc-db-source/offsets) | ConvertTo-Json -Depth 6
```
➡️ `offsets` માં એક entry: `partition` (કયો source DB) અને `offset` (`_id` = MongoDB change stream નો resume token). Connect restart પર અહીંથી આગળ વાંચે છે.

એ જ માહિતી Kafka topic માંથી:
```powershell
cd C:\kafka
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic connect-offsets --from-beginning --timeout-ms 5000 --formatter-property print.key=true
```
➡️ Key માં connector નું નામ (`["mongo-cdc-db-source",{...}]`), value માં resume token. દરેક flush પર નવી line આવે છે; **છેલ્લી line** જ હાલનો offset.

---

## 8. MongoDB બાજુ ચકાસણી

**Terminal #4.** mongosh ખોલો: `mongosh "mongodb://localhost:27017/?replicaSet=rs0"`

### 8.1 Source અને bronze ના counts (ઝડપી)

```javascript
const s = db.getSiblingDB("sourceMongo_db"), t = db.getSiblingDB("bronze_db");
s.getCollectionNames().forEach(c => print(c, "source=" + s[c].countDocuments(),
    "bronze_live=" + t[c].countDocuments({ _deleted: false }), "bronze_deleted=" + t[c].countDocuments({ _deleted: true })));
```
➡️ દરેક collection માં `source` = `bronze_live`. `bronze_deleted` = source માં delete થયેલા.

### 8.2 આખી ચકાસણી એક script થી (sample-data)

```powershell
cd $P
mongosh "mongodb://localhost:27017/?replicaSet=rs0" --quiet sample-data\99_verify_bronze.js
```
➡️ દરેક collection સામે `✔`, soft-deleted documents ની list, અને છેલ્લે:
```
✔ bronze_db matches sourceMongo_db: every live document exists in the source, every hard delete is a soft delete
```
Steps અને expected counts: [../sample-data/README.md](../sample-data/README.md)

### 8.3 Soft-deleted documents જોવા

```javascript
use bronze_db
db.bank_payments.find({ _deleted: true }, { paymentId: 1, status: 1, _op: 1, _source_ts: 1 })
```
➡️ `_op: "delete"`, `_deleted: true`, બાકીના fields **delete પહેલાંના છેલ્લા values** સાથે.

### 8.4 છેલ્લી 10 મિનિટમાં bronze માં શું આવ્યું?

```javascript
use bronze_db
db.bank_payments.find({ _ingested_at: { $gt: Date.now() - 10 * 60 * 1000 } }, { _op: 1, _deleted: 1, _ingested_at: 1 })
```

---

## 9. Logs, ports અને processes

### 9.1 Log files

| Log | ક્યાં | ક્યારે જોવું |
|---|---|---|
| Kafka broker | Terminal #1, અને `C:\kafka\logs\server.log` | Broker crash, topic errors |
| Kafka Connect | Terminal #2, અને `C:\kafka\logs\connect.log` | Connector `FAILED` |
| Kafka controller (KRaft) | `C:\kafka\logs\controller.log` | Broker start નથી થતો |

> જૂના logs દર કલાકે અલગ file માં જાય છે (`connect-2026-10-25-00.log`, `server.log.2026-...`). હાલનો log હંમેશા તારીખ વગરની file માં.
| Subscriber | Terminal #3 (ફક્ત console) | Bronze માં data નથી આવતો |
| MongoDB | `C:\Program Files\MongoDB\Server\<version>\log\mongod.log` | MongoDB શરૂ નથી થતું |

છેલ્લી 50 lines, અને live follow:
```powershell
Get-Content C:\kafka\logs\server.log -Tail 50
Get-Content C:\kafka\logs\server.log -Tail 20 -Wait          # Ctrl + C થી બંધ
Select-String -Path C:\kafka\logs\server.log -Pattern "ERROR" | Select-Object -Last 10
```

### 9.2 Kafka data folder (disk usage)

```powershell
Get-ChildItem C:\kafka-data\kraft-combined-logs -Directory | Where-Object Name -match "^(sourceMongo_db|sourceMongo_db)"
"{0:N1} MB" -f ((Get-ChildItem C:\kafka-data\kraft-combined-logs -Recurse -File | Measure-Object Length -Sum).Sum / 1MB)
```
➡️ દરેક partition નો એક folder: `sourceMongo_db-0`, `sourceMongo_db-1`, `sourceMongo_db-2`, ...

### 9.3 Ports અને processes

```powershell
Test-NetConnection localhost -Port 9092 -InformationLevel Quiet   # Kafka broker
Test-NetConnection localhost -Port 8083 -InformationLevel Quiet   # Kafka Connect
Test-NetConnection localhost -Port 27017 -InformationLevel Quiet  # MongoDB
```
➡️ ચાલુ હોય તો `True`, બંધ હોય તો `False` (અને warning).

Port કોણ વાપરે છે? (`Address already in use` વખતે)
```powershell
Get-NetTCPConnection -LocalPort 8083 -State Listen | Select-Object LocalPort, OwningProcess
Get-Process -Id <OwningProcess>
Stop-Process -Id <OwningProcess>     # ફક્ત જો terminal બંધ થઈ ગયું હોય અને process રહી ગઈ હોય
```

બધી Java processes (Kafka, Connect, subscriber):
```powershell
Get-CimInstance Win32_Process -Filter "Name='java.exe'" | Select-Object ProcessId, @{n='What';e={
    if ($_.CommandLine -match 'ConnectDistributed') {'Kafka Connect'} elseif ($_.CommandLine -match 'kafka\.Kafka') {'Kafka broker'}
    elseif ($_.CommandLine -match 'mongo-cdc-subscriber') {'Subscriber'} else {'other'} }}
```
➡️ ચાલુ setup માં: `Kafka broker`, `Kafka Connect`, `Subscriber` (દરેક એક વાર).

---

## 10. Kafka Reset (બધું નવેસરથી)

**ક્યારે:** બધા topics (જૂના DLQ સહિત) કાઢીને Kafka ને શરૂઆતથી બનાવવું હોય ત્યારે. જેમ કે broker વારંવાર crash થાય (`AccessDeniedException`), જૂના topics સાફ કરવા હોય, અથવા fresh test કરવો હોય.

આ setup માં હાથે ફક્ત **2 topics** બનાવવાના છે: `sourceMongo_db` (3 partitions) અને `cdc-dlq` (1 partition). Connect ના 3 internal topics અને `__consumer_offsets` Kafka આપોઆપ બનાવે છે.

| જશે (Kafka નો બધો data) | રહેશે (કોઈ અસર નહીં) |
|---|---|
| બધા topics અને messages, જૂના DLQ topics સહિત | MongoDB: `sourceMongo_db`, `bronze_db` અને બાકીની DBs |
| Connector registration અને એનો resume token | `server.properties`, `connect-distributed-de.properties` |
| Subscriber ના consumer group offsets | Project files, plugin jar |

> આ reset ની બે અસર:
> - Connector ફરી register કરવો પડે. Register થતાં જ `copy_existing` ફરી ચાલે, એટલે `sourceMongo_db` ના હાલના બધા documents ફરી topic માં આવે.
> - Subscriber topic ને શરૂઆતથી વાંચે. એ idempotent છે, એટલે bronze માં duplicates **નહીં** બને.

### 10.1 બધું બંધ કરો (delete પહેલાં)

| Terminal | શું કરવું | સ્થિતિ |
|---|---|---|
| #4 | કોઈ console consumer ચાલતો હોય તો `Ctrl + C` | ❌ બંધ |
| #3 Subscriber | `Ctrl + C` | ❌ બંધ |
| #2 Kafka Connect | `Ctrl + C` (`Terminate batch job?` પૂછે તો `Y`) | ❌ બંધ |
| #1 Kafka broker | `Ctrl + C` (crash થયેલો હોય તો બંધ જ હશે) | ❌ બંધ |
| MongoDB service | કંઈ ન કરો | ✅ ચાલુ રાખો |

Kafka ની કોઈ process ચાલુ નથી ને, એ ચકાસો (Terminal #4):
```powershell
Get-CimInstance Win32_Process -Filter "Name='java.exe'" | Where-Object CommandLine -match "kafka\.Kafka|ConnectDistributed|mongo-cdc-subscriber" | Select-Object ProcessId
```
➡️ **કંઈ output ન આવવું જોઈએ.** PID દેખાય તો એ terminal શોધીને `Ctrl + C` કરો. Terminal મળે જ નહીં, તો છેલ્લા ઉપાય તરીકે `Stop-Process -Id <PID>`.

### 10.2 Kafka નો data delete કરો (Terminal #4)

```powershell
Remove-Item -Recurse -Force C:\kafka-data\kraft-combined-logs
Test-Path C:\kafka-data\kraft-combined-logs
```
➡️ `False` આવવું જોઈએ.

`Access denied` અથવા `being used by another process` આવે તો કોઈ Kafka process હજુ ચાલુ છે. 10.1 ફરી કરો.

(Optional) જૂની log files પણ સાફ કરવી હોય તો:
```powershell
Remove-Item C:\kafka\logs\* -Force
```

### 10.3 Storage format (Terminal #4)

```powershell
cd C:\kafka
$id = (.\bin\windows\kafka-storage.bat random-uuid | Select-Object -Last 1).Trim()
$id
.\bin\windows\kafka-storage.bat format --standalone -t $id -c .\config\server.properties
```
➡️ પહેલાં `$id` નો UUID દેખાશે (જેમ કે `q1Sh-9_ISia_zwGINzRvyQ`). પછી આવી line આવશે:
```
Formatting metadata directory C:/kafka-data/kraft-combined-logs with metadata.version 4.x-IV...
```
`ERROR Reconfiguration failed...` વાળી line આવે તો એને અવગણો ([0.3](#03-દરેક-kafka-command-સાથે-આવતા-errors-જે-અવગણવાના-છે)).

`--standalone` એટલા માટે વાપર્યું છે કે `server.properties` માં `controller.quorum.bootstrap.servers` છે, એટલે આ single-node KRaft setup છે.

### 10.4 Broker start કરો (Terminal #1, ખુલ્લું રાખો)

```powershell
cd C:\kafka
$env:KAFKA_HEAP_OPTS = "-Xms512m -Xmx512m"
.\bin\windows\kafka-server-start.bat .\config\server.properties
```
➡️ `Kafka Server started` દેખાય ત્યાં સુધી રાહ જુઓ ([1.2](#12-kafka-broker-terminal-1-ખુલ્લું-રાખો)).

### 10.5 Topics બનાવો (Terminal #4)

Connect અને subscriber શરૂ કરતા **પહેલાં** આ topics બનાવો. Broker માં `num.partitions=1` છે, એટલે topic આપોઆપ બને તો એમાં ફક્ત 1 partition આવે.
```powershell
cd C:\kafka
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --create --topic sourceMongo_db --partitions 3 --replication-factor 1
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --create --topic cdc-dlq --partitions 1 --replication-factor 1
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --list
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --describe --topic sourceMongo_db
```
➡️ આ output આવવું જોઈએ:
- `Created topic sourceMongo_db.` અને `Created topic cdc-dlq.` (topic નામમાં `_` વિશેની WARNING આવે તો અવગણો)
- `--list` માં ફક્ત `cdc-dlq` અને `sourceMongo_db`
- `--describe` માં `PartitionCount: 3`

### 10.6 Kafka Connect start કરો (Terminal #2, ખુલ્લું રાખો)

```powershell
cd C:\kafka
$env:KAFKA_HEAP_OPTS = "-Xms512m -Xmx768m"
.\bin\windows\connect-distributed.bat .\config\connect-distributed-de.properties
```
➡️ 30–60 સેકન્ડ પછી `Kafka Connect started` દેખાશે. Connect પોતાના 3 internal topics (`connect-configs`, `connect-offsets`, `connect-status`) આપોઆપ બનાવી લે છે.

### 10.7 Connector register કરો (Terminal #4)

Reset પછી connector **ફરી register કરવો જ પડે** (રોજના start માં જરૂર નથી, પણ અહીં એનો config ગયો છે):
```powershell
$P = "D:\ClaudeProjects\dataEngineeringProjects\sparkProject\mongoKafkaConnectorDemo"
cd $P\kafka-connect
.\register-connectors.ps1
```
➡️ આ output આવવું જોઈએ:
```
applied mongo-cdc-db-source
mongo-cdc-db-source            connector=RUNNING  tasks=RUNNING
```

Messages આવ્યા કે નહીં તે ચકાસો:
```powershell
cd C:\kafka
.\bin\windows\kafka-get-offsets.bat --bootstrap-server localhost:9092 --topic sourceMongo_db
```
➡️ 3 lines આવશે (`sourceMongo_db:0:N` જેવી):
- `sourceMongo_db` માં data હોય તો numbers નો સરવાળો 0 થી વધારે આવશે. એ `copy_existing` એ મોકલેલા documents છે.
- DB ખાલી હોય તો બધે `0` આવશે.

### 10.8 (Optional) bronze_db પણ નવેસરથી

આ step ફક્ત ત્યારે કરો જ્યારે જૂના test ના soft-deleted documents પણ કાઢવા હોય. Subscriber **બંધ** હોવો જોઈએ. પહેલાં collections જુઓ:
```powershell
mongosh "mongodb://localhost:27017/?replicaSet=rs0" --quiet --eval "db.getSiblingDB('bronze_db').getCollectionNames()"
```
કોઈ બીજું project `bronze_db` ના એ collections ન વાપરતું હોય, તો drop કરો:
```powershell
mongosh "mongodb://localhost:27017/?replicaSet=rs0" --quiet --eval "const b = db.getSiblingDB('bronze_db'); b.getCollectionNames().forEach(c => { b.getCollection(c).drop(); print('dropped ' + c) })"
```

### 10.9 Subscriber rebuild અને start (Terminal #3, ખુલ્લું રાખો)

```powershell
cd D:\ClaudeProjects\dataEngineeringProjects\sparkProject\mongoKafkaConnectorDemo\mongo-cdc-subscriber
mvn -q clean package
java -jar target\mongo-cdc-subscriber-1.0.0-jar-with-dependencies.jar
```
➡️ પહેલાં `Reading topic 'sourceMongo_db' and writing to database 'bronze_db'` દેખાશે. પછી `copy_existing` ના documents માટે `<collection>: N writes (...)` જેવી lines આવશે.

> `mvn clean package` ફક્ત ત્યારે જ જરૂરી છે જ્યારે code અથવા `subscriber.properties` બદલાયા હોય. ન બદલાયા હોય તો સીધું `java -jar ...` ચાલે.

### 10.10 આખી ચકાસણી (Terminal #4)

```powershell
cd C:\kafka
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --list
.\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --describe --group mongo-cdc-subscriber
.\bin\windows\kafka-get-offsets.bat --bootstrap-server localhost:9092 --topic cdc-dlq
cd $P
mongosh "mongodb://localhost:27017/?replicaSet=rs0" --quiet sample-data\99_verify_bronze.js
```

| Check | Expected |
|---|---|
| Topics | `__consumer_offsets`, `cdc-dlq`, `connect-configs`, `connect-offsets`, `connect-status`, `sourceMongo_db`. જૂના `*.dlq` અને `de_source_mongo` **નથી** |
| LAG | ત્રણેય partitions માં `0` |
| DLQ | `cdc-dlq:0:0` |
| Verify script | દરેક collection સામે `✔` (`sourceMongo_db` ખાલી હોય તો કોઈ collection નહીં દેખાય) |

હવે test data નાખવો હોય તો [../sample-data/README.md](../sample-data/README.md) ના "Test run" steps પ્રમાણે 01 scripts અને પછી 02 scripts ચલાવો.

> ⚠️ **ધ્યાન રાખવાની વાતો:**
> - 10.2 ત્યારે જ કરો જ્યારે 10.1 ની ચકાસણીમાં કોઈ process ન દેખાય. Kafka ચાલુ હોય ત્યારે files delete કરશો તો અડધો data રહી જશે.
> - 10.5 ને 10.7 અને 10.9 પહેલાં જ કરો. નહીં તો `sourceMongo_db` અને `cdc-dlq` ફરી 1 partition સાથે આપોઆપ બની જશે.

---

## 11. રોજનું 2 મિનિટનું Health Check

Start પછી, Terminal #4 માં આ block એકસાથે paste કરો:

```powershell
$P = "D:\ClaudeProjects\dataEngineeringProjects\sparkProject\mongoKafkaConnectorDemo"
"--- MongoDB";   (Get-Service MongoDB).Status
"--- Ports";     9092, 8083, 27017 | ForEach-Object { "{0,-6} {1}" -f $_, (Test-NetConnection localhost -Port $_ -InformationLevel Quiet -WarningAction SilentlyContinue) }
"--- Connectors"; & "$P\kafka-connect\register-connectors.ps1" status
cd C:\kafka
"--- DLQ (છેલ્લો number 0 હોવો જોઈએ)"
.\bin\windows\kafka-get-offsets.bat --bootstrap-server localhost:9092 --topic cdc-dlq 2>$null
"--- LAG (0 હોવો જોઈએ)"
"mongo-cdc-subscriber" | ForEach-Object {
    .\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --describe --group $_ 2>$null }
```

✅ બધું બરાબર:

| Check | Expected |
|---|---|
| MongoDB | `Running` |
| Ports | `9092 True`, `8083 True`, `27017 True` |
| Connectors | `mongo-cdc-db-source connector=RUNNING tasks=RUNNING` |
| DLQ | `...dlq:0:0` |
| LAG | દરેક line માં `LAG` = `0`, અને `CONSUMER-ID` ભરેલો |

---

## 12. ઝડપી Troubleshooting

| લક્ષણ | પહેલા આ ચકાસો |
|---|---|
| Source માં change કર્યો, topic માં message નથી | Connector `RUNNING`? (1.4) • 4.4 live consumer ચાલુ રાખીને ફરી change કરો • Connect ના logs (Terminal #2) |
| Topic માં message છે, bronze માં નથી | Subscriber ચાલુ? (`CONSUMER-ID`, 6.2) • `LAG` • DLQ (5) • Terminal #3 ના errors |
| Source માં delete કર્યું, bronze માં document હજુ છે | **અપેક્ષિત (soft delete).** `_deleted: true` અને `_op: "delete"` ચકાસો (8.3) |
| Connector `FAILED` | `.\register-connectors.ps1 status` ની `ERROR` line • MongoDB `PRIMARY`? (1.1) • ઠીક કરીને restart (7.4) |
| `Unable to connect to the remote server` (8083) | Connect હજુ start થાય છે, અથવા Kafka broker બંધ |
| Console consumer કંઈ બતાવતો નથી | `--from-beginning` છે? Topic નામ સાચું? (3.1, case-sensitive: `sourceMongo_db`) |
| Broker start: `AccessDeniedException: ...__cluster_metadata-0\...checkpoint.deleted` | Kafka એ જૂની snapshot files delete માટે mark કરી (`.deleted`), પણ એ **ReadOnly** હોવાથી Windows પર delete ન થઈ. Kafka **બંધ** હોય ત્યારે (`Get-CimInstance Win32_Process -Filter "Name='java.exe'"` માં broker નથી): `Remove-Item C:\kafka-data\kraft-combined-logs\__cluster_metadata-0\*.checkpoint.deleted -Force`. ફક્ત `.deleted` વાળી files; બીજી files ને હાથ ન લગાડો. પછી start |
| Kafka crash: `AccessDeniedException` | Kafka બંધ કરીને: `Get-ChildItem C:\kafka-data\kraft-combined-logs -Directory \| Where-Object Name -like "*-delete" \| Remove-Item -Recurse -Force`, પછી start. ન ચાલે તો [10. Kafka Reset](#10-kafka-reset-બધું-નવેસરથી) |
| `--reset-offsets`: group is not inactive | Subscriber ચાલુ છે, પહેલાં `Ctrl + C` |
| `mvn package`: `Failed to delete ...jar` | Subscriber ચાલુ છે, પહેલાં `Ctrl + C` |

બાકીના errors: setup guide **§14**.

---

## 13. ક્યારેય ન કરો

| ❌ ન કરો | કેમ | ✅ એની જગ્યાએ |
|---|---|---|
| Windows પર topic delete (`kafka-topics --delete`) | Broker crash (`AccessDeniedException`) | Topic પડ્યો રહેવા દો, નવું નામ વાપરો |
| Console consumer માં `--group mongo-cdc-subscriber` | Subscriber ના messages console "વાંચી" લે, bronze માં ન જાય | `--group` વગર ચલાવો (4) |
| Kafka broker ને Connect પહેલાં બંધ કરવું | Connect offsets save ન કરી શકે | Stop ક્રમ (2) |
| Subscriber ચાલુ રાખીને offsets reset | Kafka ના પાડે | પહેલાં subscriber બંધ (6.4) |
| JSON file delete કરીને માની લેવું કે connector બંધ થયો | Config Connect ના topic માં છે | `.\register-connectors.ps1 delete <name>` |
| `deletedConnectorOld\` ની files register કરવી | Data બે વાર / બે જગ્યાએ | ફક્ત `kafka-connect\mongo-cdc-db-source.json` |
| એક જ consumer group ના 3 થી વધુ subscribers | Partitions 3 જ છે, વધારાના ખાલી બેસે | Instances ≤ partitions |
