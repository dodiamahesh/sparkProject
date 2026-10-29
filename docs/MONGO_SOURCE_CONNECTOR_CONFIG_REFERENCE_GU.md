# MongoDB Source Connector: બધી Configuration Keys અને Values (ગુજરાતી Reference)

> File: [kafka-connect/mongo-cdc-db-source.json](../kafka-connect/mongo-cdc-db-source.json)
> Connector: **MongoDB Kafka Connector 3.1.2** (`com.mongodb.kafka.connect.MongoSourceConnector`) + Kafka Connect 4.3.1
> Setup: [MONGO_CDC_COMPLETE_SETUP_GU.md](MONGO_CDC_COMPLETE_SETUP_GU.md)
>
> દરેક key માટે: **શક્ય values**, **દરેક value નો હેતુ**, **default**, **આપણો setup શું વાપરે છે** અને **બદલશો તો શું અસર થશે**.
> Official list (version પ્રમાણે નવી keys આવે છે): https://www.mongodb.com/docs/kafka-connector/current/source-connector/configuration-properties/

---

## અનુક્રમણિકા

- [0. આ document કેવી રીતે વાંચવું](#0-આ-document-કેવી-રીતે-વાંચવું)
- [1. આપણી file: દરેક line એક નજરમાં](#1-આપણી-file-દરેક-line-એક-નજરમાં)
- [2. Kafka Connect ની સામાન્ય keys (દરેક connector માં)](#2-kafka-connect-ની-સામાન્ય-keys-દરેક-connector-માં)
- [3. MongoDB connection](#3-mongodb-connection)
- [4. શું watch કરવું: database / collection](#4-શું-watch-કરવું-database--collection)
- [5. Topic નું નામ](#5-topic-નું-નામ)
- [6. Change stream settings](#6-change-stream-settings)
- [7. Startup: connector પહેલી વાર ક્યાંથી વાંચે](#7-startup-connector-પહેલી-વાર-ક્યાંથી-વાંચે)
- [8. Output format: message કેવો દેખાય](#8-output-format-message-કેવો-દેખાય)
- [9. Converters](#9-converters)
- [10. Transforms (SMT)](#10-transforms-smt)
- [11. Errors, heartbeat અને offsets](#11-errors-heartbeat-અને-offsets)
- [12. Topic creation અને producer overrides](#12-topic-creation-અને-producer-overrides)
- [13. Values ના સાચા combinations](#13-values-ના-સાચા-combinations)
- [14. ⚠️ આ values બદલશો તો આપણો subscriber તૂટશે](#14-️-આ-values-બદલશો-તો-આપણો-subscriber-તૂટશે)
- [15. Config બદલવાની સલામત રીત](#15-config-બદલવાની-સલામત-રીત)

---

## 0. આ document કેવી રીતે વાંચવું

| નિશાની | અર્થ |
|---|---|
| ✅ **આપણું** | `mongo-cdc-db-source.json` માં હાલ આ value છે |
| (default) | Key ન લખો તો connector આ value વાપરે છે |
| ⚠️ | બદલવાથી આપણો subscriber કે setup તૂટી શકે છે |
| 🔸 | MongoDB ના કોઈ ચોક્કસ version થી જ ચાલે છે |

**સામાન્ય નિયમો:**
- JSON file માં **બધી values string** તરીકે લખાય છે: `"tasks.max": "1"`, `"publish.full.document.only": "false"`.
- જે key ન લખો તેની **default value** વપરાય છે.
- Config ની ભૂલ હોય (ખોટું નામ, ખોટી value) તો register વખતે અથવા task start વખતે error આવે છે. `.\register-connectors.ps1 status` થી જુઓ.

---

## 1. આપણી file: દરેક line એક નજરમાં

| Key | ✅ આપણી value | બીજી શક્ય values | આપણે આ value કેમ રાખી |
|---|---|---|---|
| `connector.class` | `com.mongodb.kafka.connect.MongoSourceConnector` | `...MongoSinkConnector` (ઉલટી દિશા) | MongoDB → Kafka |
| `tasks.max` | `1` | `1`, `2`, ... (`0` ન ચાલે) | Mongo source connector **હંમેશા 1 task** વાપરે છે ([2.3](#23-tasksmax)) |
| `connection.uri` | `mongodb://localhost:27017/?replicaSet=rs0` | કોઈ પણ MongoDB connection string | Local replica set `rs0` |
| `database` | `de_source_mongo` | `""` (આખું cluster), કોઈ પણ DB નામ | ફક્ત source database |
| `topic.prefix` | `cdc` | કોઈ પણ text, `""` | RegexRouter ઓળખી શકે એવું temporary નામ |
| `startup.mode` | `copy_existing` | `latest`, `timestamp`, `copy_existing` | પહેલી વાર હાલનો data પણ લાવવો |
| `change.stream.full.document` | `updateLookup` | `default`, `updateLookup`, `whenAvailable`, `required` | Update વખતે આખો document જોઈએ |
| `publish.full.document.only` | `false` | `true`, `false` | આખો change event (delete સહિત) જોઈએ |
| `output.format.key` | `json` | `json`, `bson`, `schema` | Key JSON text |
| `output.format.value` | `json` | `json`, `bson`, `schema` | Value JSON text |
| `output.json.formatter` | `...ExtendedJson` | `DefaultJson`, `ExtendedJson`, `SimplifiedJson`, પોતાનો class | **Types સાચવવા** (subscriber માટે જરૂરી) |
| `key.converter` | `StringConverter` | `StringConverter`, `JsonConverter`, `ByteArrayConverter`, Avro... | JSON text જેમ છે તેમ |
| `value.converter` | `StringConverter` | એ જ | એ જ |
| `transforms` | `dbTopic` | કોઈ પણ નામોની list | એક SMT: RegexRouter |
| `transforms.dbTopic.type` | `...RegexRouter` | બીજા SMTs ([10](#10-transforms-smt)) | Topic નું નામ બદલવું |
| `transforms.dbTopic.regex` | `cdc\\.([^.]+)\\..*` | કોઈ પણ Java regex | `cdc.<db>.<coll>` ને પકડવું |
| `transforms.dbTopic.replacement` | `$1` | Regex groups સાથે કોઈ પણ text | બધા collections → `<db>` નામનો એક topic |

---

## 2. Kafka Connect ની સામાન્ય keys (દરેક connector માં)

### 2.1 `name`
| | |
|---|---|
| Values | કોઈ પણ unique નામ |
| હેતુ | Connector નું નામ. **Source offsets (ક્યાં સુધી વાંચ્યું) આ નામ પ્રમાણે સચવાય છે** |
| આપણું | File માં નથી. `register-connectors.ps1` URL માં file નું નામ (`mongo-cdc-db-source`) આપે છે |
| નોંધ | નામ બદલો = **નવો connector**, એટલે offsets શરૂઆતથી અને `copy_existing` ફરી ચાલે |

### 2.2 `connector.class`
| Value | હેતુ |
|---|---|
| `com.mongodb.kafka.connect.MongoSourceConnector` ✅ | MongoDB → Kafka (change streams) |
| `com.mongodb.kafka.connect.MongoSinkConnector` | Kafka → MongoDB (આપણે હવે વાપરતા નથી, subscriber છે) |

Plugin `C:\kafka\plugins` માં હોવો જોઈએ: `(Invoke-RestMethod http://localhost:8083/connector-plugins) | Select-Object class`

### 2.3 `tasks.max`
| Value | અર્થ |
|---|---|
| `0` | ❌ **Invalid.** ઓછામાં ઓછું 1 જોઈએ (register વખતે error) |
| `1` ✅ (default) | એક task: change stream વાંચવાનું કામ એક thread કરે |
| `2`, `3`, ... | ઘણા connectors માં કામ tasks વચ્ચે વહેંચાય છે, **પણ MongoDB Source Connector હંમેશા ફક્ત 1 task બનાવે છે**. વધારે value આપો તો પણ 1 જ ચાલશે, error નહીં આવે |

**કેમ 1 જ?** એક change stream નો ક્રમ (order) સાચવવો જરૂરી છે, અને એક stream ને ઘણા readers માં વહેંચી શકાતો નથી.
**વધુ ઝડપ જોઈએ તો:** અલગ connectors બનાવો (દા.ત. collection દીઠ અથવા database દીઠ એક), અથવા consumer બાજુ (subscriber) scale કરો.

### 2.4 `key.converter`, `value.converter`, `header.converter`
વિગત: [વિભાગ 9](#9-converters).

### 2.5 `transforms`, `predicates`
વિગત: [વિભાગ 10](#10-transforms-smt).

### 2.6 `config.action.reload`
| Value | હેતુ |
|---|---|
| `restart` (default) | Config માં વપરાયેલી externalized value (જેમ કે file માંથી password) બદલાય તો connector restart થાય |
| `none` | Restart ન થાય |

---

## 3. MongoDB connection

### 3.1 `connection.uri`
| | |
|---|---|
| Default | `mongodb://localhost:27017,localhost:27018,localhost:27019` |
| ✅ આપણું | `mongodb://localhost:27017/?replicaSet=rs0` |
| હેતુ | MongoDB સાથે જોડાવાનું address. **Change streams માટે replica set (અથવા sharded cluster) જરૂરી** |

URI માં ઉમેરી શકાય એવા મહત્વના options (`?` પછી `&` થી જોડો):

| Option | ઉદાહરણ | હેતુ |
|---|---|---|
| `replicaSet` | `replicaSet=rs0` | Replica set નું નામ |
| `authSource` | `authSource=admin` | User કયા database માં બનાવ્યો છે |
| user/password | `mongodb://cdc_user:pass@host:27017/...` | Authentication (production માં જરૂરી) |
| `tls` | `tls=true` | Encrypted connection |
| `readPreference` | `readPreference=secondaryPreferred` | Secondary માંથી વાંચવું, જેથી primary પર ભાર ઓછો પડે |
| `directConnection` | `directConnection=true` | Replica set discovery વગર એક જ node સાથે (CDC માટે સામાન્ય રીતે નહીં) |
| `mongodb+srv://` | `mongodb+srv://cluster0.xxx.mongodb.net` | MongoDB Atlas (DNS seed list) |

**Production user ને જોઈતા privileges:** `find` અને `changeStream` (સામાન્ય રીતે built-in `read` role), અને `copy_existing` માટે `find`.

### 3.2 `server.api.version`, `server.api.strict`, `server.api.deprecationErrors`
| | |
|---|---|
| Values | `server.api.version`: `""` (default) અથવા `1`. બાકીના બે: `true`/`false` (default `false`) |
| હેતુ | MongoDB **Stable API**: server upgrade થાય તો પણ behavior બદલાય નહીં (Atlas માં ઉપયોગી) |
| આપણું | વાપરતા નથી |

---

## 4. શું watch કરવું: database / collection

### 4.1 `database`
| Value | અર્થ |
|---|---|
| `""` (default) | **આખું cluster**: બધા databases ના બધા collections (system databases સિવાય) |
| `de_source_mongo` ✅ | ફક્ત આ database ના બધા collections. નવું collection બને તો એ પણ આપોઆપ |

### 4.2 `collection`
| Value | અર્થ |
|---|---|
| `""` (default) ✅ | `database` ના **બધા** collections |
| `weather_observations` | ફક્ત આ એક collection. **`database` પણ આપવો જરૂરી** |

> ઘણા (પણ બધા નહીં) collections જોઈએ તો: `collection` ખાલી રાખો અને `pipeline` થી filter કરો ([6.1](#61-pipeline)).

| `database` | `collection` | શું watch થાય |
|---|---|---|
| `""` | `""` | આખું cluster |
| `de_source_mongo` | `""` | એક database ✅ |
| `de_source_mongo` | `student` | એક collection |
| `""` | `student` | ❌ ખોટું (database જરૂરી) |

---

## 5. Topic નું નામ

**Default નિયમ:** `<topic.prefix><separator><database><separator><collection><separator><topic.suffix>`
ઉદાહરણ: prefix `cdc`, separator `.` → `cdc.de_source_mongo.student`. ખાલી ભાગ છોડી દેવાય છે.

### 5.1 `topic.prefix`
| Value | Topic નું નામ (`de_source_mongo.student` માટે) |
|---|---|
| `""` (default) | `de_source_mongo.student` |
| `cdc` ✅ | `cdc.de_source_mongo.student` (પછી RegexRouter એને `de_source_mongo` કરે છે) |
| `prod` | `prod.de_source_mongo.student` |

### 5.2 `topic.suffix`
| Value | ઉદાહરણ |
|---|---|
| `""` (default) ✅ | – |
| `v1` | `cdc.de_source_mongo.student.v1` |

### 5.3 `topic.separator`
| Value | ઉદાહરણ |
|---|---|
| `.` (default) ✅ | `cdc.de_source_mongo.student` |
| `_` | `cdc_de_source_mongo_student` |
| `-` | `cdc-de_source_mongo-student` |

> ⚠️ Separator બદલો તો `transforms.dbTopic.regex` પણ બદલવો પડે, કારણ કે regex `.` પર આધાર રાખે છે.

### 5.4 `topic.namespace.map`
| | |
|---|---|
| Default | `""` (કોઈ mapping નહીં) |
| Value | JSON (string તરીકે): namespace → topic નામ |
| હેતુ | **Transform વગર** connector પોતે જ topic નું નામ નક્કી કરે |

ઉદાહરણ:
```json
"topic.namespace.map": "{\"de_source_mongo.student\": \"students\", \"de_source_mongo\": \"school\", \"*\": \"others\"}"
```
| Key પ્રકાર | અર્થ |
|---|---|
| `db.collection` | એ collection ના events આ topic માં |
| `db` | એ database ના બાકીના collections. Connector મોટા ભાગે collection નું નામ પાછળ જોડે છે (`school.<collection>`) |
| `*` | બાકીના બધા namespaces આ topic માં |

> `topic.prefix`/`suffix` સાથે આ mapping કેવી રીતે જોડાય છે, અને `db` key પાછળ collection જોડાય કે નહીં, તે version પ્રમાણે ફરી શકે છે. **વાપરતા પહેલાં test કરો** (`kafka-topics.bat --list`). આપણે RegexRouter વાપરીએ છીએ, કારણ કે એનું behavior ચોક્કસ અને જાણીતું છે.

### 5.5 `topic.mapper`
| Value | હેતુ |
|---|---|
| `com.mongodb.kafka.connect.source.topic.mapping.DefaultTopicMapper` (default) | ઉપરના prefix/suffix/separator/namespace.map નિયમો |
| પોતાનો Java class (`TopicMapper` interface) | પોતાના logic થી topic નક્કી કરવો (code + jar plugin folder માં) |

---

## 6. Change stream settings

### 6.1 `pipeline`
| | |
|---|---|
| Default | `[]` (બધા events) |
| Value | MongoDB aggregation pipeline (JSON array, string તરીકે) |
| હેતુ | **MongoDB માં જ** events filter કે બદલવા, જેથી Kafka માં ઓછો data જાય |

ઉદાહરણો:
```json
// ફક્ત insert/update/replace/delete (drop, rename વગેરે નહીં)
"pipeline": "[{\"$match\": {\"operationType\": {\"$in\": [\"insert\", \"update\", \"replace\", \"delete\"]}}}]"

// ફક્ત બે collections
"pipeline": "[{\"$match\": {\"ns.coll\": {\"$in\": [\"student\", \"ordes\"]}}}]"

// sensitive field કાઢવું (fullDocument માંથી)
"pipeline": "[{\"$project\": {\"fullDocument.password\": 0}}]"
```
> ⚠️ `_id` (resume token), `operationType`, `ns`, `documentKey` ને **કાઢો નહીં**. Connector ને resume token જોઈએ છે, અને subscriber ને બાકીના fields.

### 6.2 `change.stream.full.document`
**Update event માં `fullDocument` (આખો document) આવે કે નહીં, અને કયો આવે.** Insert અને replace માં `fullDocument` **હંમેશા** હોય છે. આ setting ફક્ત **update** ને અસર કરે છે.

| Value | Update event માં `fullDocument` | MongoDB | ક્યારે વાપરવું |
|---|---|---|---|
| `default` (અથવા ખાલી, default) | ❌ **નથી**. ફક્ત `updateDescription` (બદલાયેલા fields) | બધા | ફક્ત ફેરફારો જોઈએ, ઓછો data |
| `updateLookup` ✅ | ✅ MongoDB **lookup વખતે** document વાંચીને મૂકે છે | બધા | આખો document જોઈએ (આપણો subscriber replace કરે છે) |
| `whenAvailable` 🔸 | ✅ **Update પછીનો ચોક્કસ** document (post-image), હોય તો. નહીં તો `null` | 6.0+ | ચોક્કસ state જોઈએ, અને post-images ચાલુ છે |
| `required` 🔸 | ✅ Post-image **ફરજિયાત**. ન હોય તો **error** | 6.0+ | Post-image વગર ચાલે જ નહીં |

**`updateLookup` vs `whenAvailable`:**

| | `updateLookup` | `whenAvailable` / `required` |
|---|---|---|
| મળતો document | Lookup **વખતનો** (પછીના updates પણ હોઈ શકે) | Update **પછી તરતનો** ચોક્કસ |
| Document પછીથી delete થયો | `fullDocument: null` (આપણો subscriber ત્યારે `$set/$unset` વાપરે છે) | Post-image પહેલેથી સચવાયેલું છે |
| Collection પર setup | કંઈ નહીં | `db.runCommand({collMod: "student", changeStreamPreAndPostImages: {enabled: true}})` |
| Server પર ભાર | દરેક update પર એક વધારાનું read | Post-images config.system.preimages માં સચવાય (disk) |

### 6.3 `change.stream.full.document.before.change` 🔸
**Update/replace/delete પહેલાંનો document** (`fullDocumentBeforeChange`) event માં જોઈએ કે નહીં. MongoDB 6.0+.

| Value | અર્થ |
|---|---|
| `default` / `off` (default) ✅ | ❌ પહેલાંનો document નહીં |
| `whenAvailable` | ✅ હોય તો મળે (pre-images ચાલુ હોય ત્યારે), નહીં તો `null` |
| `required` | ✅ ફરજિયાત. ન હોય તો error |

**ઉપયોગ:** Delete થયેલા document ની **છેલ્લી values** event માં જ જોઈએ (audit), અથવા "પહેલાં vs પછી" સરખામણી.
**જરૂરી setup:** collection પર `changeStreamPreAndPostImages: {enabled: true}` (6.2 ની જેમ).

### 6.4 `publish.full.document.only`
| Value | Kafka message ની value | Delete events |
|---|---|---|
| `false` (default) ✅ | **આખો change event**: `operationType`, `ns`, `documentKey`, `fullDocument`, `updateDescription`... | ✅ આવે છે |
| `true` | **ફક્ત `fullDocument`** (document પોતે), wrapper વગર | ❌ **નથી આવતા** (document જ નથી, એટલે event skip થાય છે) |

> ⚠️ આપણો subscriber `operationType` અને `ns.coll` વાંચે છે, એટલે **`false` જરૂરી** છે.

### 6.5 `publish.full.document.only.tombstone.on.delete`
| Value | અર્થ |
|---|---|
| `false` (default) ✅ | – |
| `true` | `publish.full.document.only=true` હોય ત્યારે delete માટે **tombstone** (key સાથે, value `null`) મોકલે, જેથી consumer ને delete ની ખબર પડે |

ફક્ત `publish.full.document.only=true` સાથે અર્થ ધરાવે છે.

### 6.6 `change.stream.show.expanded.events` 🔸
| Value | અર્થ |
|---|---|
| `false` (default) ✅ | ફક્ત સામાન્ય events (insert, update, replace, delete, drop, rename, dropDatabase, invalidate) |
| `true` | વધારાના **DDL events** પણ: `create`, `createIndexes`, `dropIndexes`, `modify`, `shardCollection`... (MongoDB 6.0+) |

> આપણો subscriber અજાણ્યા events ને "Skipping" log કરીને આગળ વધે છે. `true` કરો તો logs માં ઘણી warnings આવશે.

### 6.7 `collation`
| | |
|---|---|
| Default | `""` |
| Value | JSON collation, જેમ કે `{"locale": "en", "strength": 2}` |
| હેતુ | `pipeline` ના string comparisons (દા.ત. case-insensitive `$match`) કઈ ભાષાના નિયમથી થાય |

### 6.8 `batch.size`
| Value | અર્થ |
|---|---|
| `0` (default) ✅ | MongoDB server નું default batch size |
| `100`, `1000`, ... | Change stream cursor એક વારમાં કેટલા events લાવે. મોટું = ઓછા round trips, વધુ memory |

### 6.9 `poll.await.time.ms`
| Value | અર્થ |
|---|---|
| `5000` (default) | નવો event ન હોય તો MongoDB કેટલી વાર (ms) રાહ જોઈને ખાલી જવાબ આપે |
| ઓછું (`1000`) | ઝડપી પ્રતિભાવ, વધુ requests |
| વધારે | ઓછા requests, પણ connector shutdown થોડું ધીમું |

### 6.10 `poll.max.batch.size`
| Value | અર્થ |
|---|---|
| `1000` (default) | Connector એક poll માં Kafka ને વધુમાં વધુ કેટલા records આપે |
| વધારે | વધુ throughput, વધુ memory |

---

## 7. Startup: connector પહેલી વાર ક્યાંથી વાંચે

> આ settings **ફક્ત ત્યારે** લાગુ પડે છે જ્યારે connector ના **offsets ન હોય** (નવો connector, અથવા નવું નામ). Offset (resume token) હોય તો connector હંમેશા ત્યાંથી જ આગળ વાંચે છે.

### 7.1 `startup.mode`
| Value | અર્થ | ક્યારે |
|---|---|---|
| `latest` (default) | **અત્યારથી** ના નવા changes જ. હાલનો data ❌ | ફક્ત નવા ફેરફારો જોઈએ |
| `timestamp` | **આપેલા સમયથી** changes (oplog માં એટલો જૂનો history હોવો જોઈએ) | થોડા કલાક પહેલાંથી ફરી લેવું |
| `copy_existing` ✅ | પહેલાં **હાલના બધા documents** `insert` events તરીકે, પછી live changes | Destination ને શરૂઆતથી ભરવું |

**`copy_existing` events ની ઓળખ:** `_id` = `{"_id": ..., "copyingData": true}`, અને `clusterTime`/`wallTime` **નથી** હોતા. આપણો subscriber ત્યારે `_source_ts` = copy નો સમય મૂકે છે.

### 7.2 `startup.mode.timestamp.start.at.operation.time`
| | |
|---|---|
| ક્યારે | ફક્ત `startup.mode=timestamp` સાથે |
| Values | Epoch **seconds** (`1793163860`), ISO-8601 (`2026-10-28T05:04:20Z`), અથવા BSON Timestamp JSON (`{"$timestamp": {"t": 1793163860, "i": 0}}`) |
| ⚠️ | સમય **oplog window** ની અંદર હોવો જોઈએ, નહીં તો error. Oplog કેટલો જૂનો છે તે જોવા: `rs.printReplicationInfo()` |

### 7.3 `copy_existing` ની sub-settings
| Key | Default | હેતુ |
|---|---|---|
| `startup.mode.copy.existing.namespace.regex` | `""` (બધા) | કયા collections copy કરવા, regex થી. દા.ત. `de_source_mongo\\.(student\|ordes)` |
| `startup.mode.copy.existing.pipeline` | `[]` | Copy વખતે documents filter કરવા. દા.ત. `[{"$match": {"status": "ACTIVE"}}]` |
| `startup.mode.copy.existing.max.threads` | CPU cores ની સંખ્યા | એક સાથે કેટલા collections copy થાય |
| `startup.mode.copy.existing.queue.size` | `16000` | Copy કરેલા documents નો memory queue |
| `startup.mode.copy.existing.allow.disk.use` | `true` | મોટા collections માટે MongoDB ને disk વાપરવા દેવી |

> જૂના નામ `copy.existing=true`, `copy.existing.pipeline` વગેરે **deprecated** છે. નવા `startup.mode.*` વાપરો.

> ⚠️ Copy ચાલુ હોય ત્યારે થયેલા ફેરફારો પણ પછીથી change stream માં આવે છે, એટલે એક document **બે વાર** આવી શકે. આપણો subscriber upsert કરે છે, એટલે duplicate નથી બનતો.

---

## 8. Output format: message કેવો દેખાય

### 8.1 `output.format.key` અને `output.format.value`
| Value | Kafka માં શું જાય | કયા converter સાથે |
|---|---|---|
| `json` (default) ✅ | JSON **text** (string) | `StringConverter` ✅ |
| `bson` | Raw **BSON bytes** | `ByteArrayConverter` |
| `schema` | Kafka Connect **Struct** (schema સાથે) | `JsonConverter` (`schemas.enable=true`) અથવા Avro/Protobuf (Schema Registry) |

**Key માં શું હોય?** આપણા setup માં `documentKey` (`{"_id": ...}`). એટલે એક document ના બધા events એક જ partition માં જાય છે.

### 8.2 `output.json.formatter` (ફક્ત `json` format સાથે)
| Value (class) | ટૂંકમાં |
|---|---|
| `com.mongodb.kafka.connect.source.json.formatter.DefaultJson` (default) | જૂનો "legacy" format: મોટા ભાગના numbers સાદા, ખાસ types (`$oid`, `$date`, `$numberLong`...) wrappers સાથે |
| `com.mongodb.kafka.connect.source.json.formatter.ExtendedJson` ✅ | **Canonical Extended JSON**: **દરેક** type wrapper સાથે. Type ની માહિતી સંપૂર્ણ સચવાય |
| `com.mongodb.kafka.connect.source.json.formatter.SimplifiedJson` | **સાદું JSON**: ObjectId → string, Date → ISO string. વાંચવામાં સરળ, પણ **types ખોવાય** |
| પોતાનો class (`JsonWriterSettingsProvider`) | પોતાનો format |

**એક જ document ત્રણે formats માં** (source: `_id` ObjectId, `qty` Int32, `big` Int64, `amount` Double, `at` Date):

| Field | `ExtendedJson` ✅ | `SimplifiedJson` | `DefaultJson` (આશરે) |
|---|---|---|---|
| `_id` | `{"$oid": "6ae1..."}` | `"6ae1..."` | `{"$oid": "6ae1..."}` |
| `qty` (Int32) | `{"$numberInt": "5"}` | `5` | `5` |
| `big` (Int64) | `{"$numberLong": "5"}` | `5` | `{"$numberLong": "5"}` |
| `amount` (Double) | `{"$numberDouble": "2499.5"}` | `2499.5` | `2499.5` |
| `at` (Date) | `{"$date": {"$numberLong": "1793163860102"}}` | `"2026-10-28T05:04:20.102Z"` | `{"$date": 1793163860102}` |
| પાછું BSON બને ત્યારે | ✅ **બધા types એ જ** | ❌ ObjectId/Date → String, Int64 → Int32 | ⚠️ Int32/Double સાદા numbers બને |

> `DefaultJson` નું ચોક્કસ output version પ્રમાણે થોડું અલગ હોઈ શકે છે. Console consumer થી જોઈને ચકાસો.
> ⚠️ **આપણા subscriber માટે `ExtendedJson` જ રાખો** ([14](#14-️-આ-values-બદલશો-તો-આપણો-subscriber-તૂટશે)).

### 8.3 `output.schema.key`, `output.schema.value`
| | |
|---|---|
| ક્યારે | ફક્ત `output.format.*=schema` સાથે |
| Value | **Avro schema** (JSON). Default: change stream event નો generic schema |
| હેતુ | Struct નો schema જાતે નક્કી કરવો (ક્યા fields, કયા types) |

### 8.4 `output.schema.infer.value`
| Value | અર્થ |
|---|---|
| `false` (default) | `output.schema.value` નો schema |
| `true` | દરેક document પરથી schema **આપોઆપ અનુમાન** (infer). મોટે ભાગે `publish.full.document.only=true` સાથે વપરાય |

---

## 9. Converters

Connector JSON file માં લખેલો converter, worker ના default (`connect-distributed-de.properties` માંના `JsonConverter`) ને **override** કરે છે.

| Converter class | Kafka માં શું લખાય | ક્યારે વાપરવું |
|---|---|---|
| `org.apache.kafka.connect.storage.StringConverter` ✅ | String જેમ છે તેમ | Connector પોતે JSON text બનાવે (`output.format=json`) |
| `org.apache.kafka.connect.json.JsonConverter` | Connect data → JSON. `schemas.enable=true` હોય તો `{"schema":..., "payload":...}` | `output.format=schema` સાથે |
| `org.apache.kafka.connect.converters.ByteArrayConverter` | Bytes જેમ છે તેમ | `output.format=bson` સાથે |
| `io.confluent.connect.avro.AvroConverter` (અલગ download) | Avro + Schema Registry | Schema governance જોઈએ ત્યારે |

Converter ના settings: `key.converter.<setting>` / `value.converter.<setting>`, જેમ કે:
```json
"value.converter": "org.apache.kafka.connect.json.JsonConverter",
"value.converter.schemas.enable": "false"
```

> ⚠️ `output.format=json` સાથે `JsonConverter` વાપરો તો JSON text **ફરી string તરીકે wrap** થાય છે (`"{\"_id\": ...}"`). આપણો subscriber એને સીધું parse નહીં કરી શકે.

`header.converter`: default `SimpleHeaderConverter`. આપણે headers વાપરતા નથી.

---

## 10. Transforms (SMT)

**SMT (Single Message Transform)** = દરેક record Kafka માં લખાય **તે પહેલાં** એને બદલવાની નાની પ્રક્રિયા.

```json
"transforms": "dbTopic",                       // ક્રમમાં લાગુ થતા SMT નામોની list (comma થી)
"transforms.dbTopic.type": "<SMT class>",
"transforms.dbTopic.<setting>": "<value>"
```

### 10.1 `RegexRouter` ✅ (આપણો)
| Key | ✅ આપણી value | અર્થ |
|---|---|---|
| `type` | `org.apache.kafka.connect.transforms.RegexRouter` | Topic નું નામ regex થી બદલવું |
| `regex` | `cdc\\.([^.]+)\\..*` | `cdc.` + **(database)** + `.` + બાકીનું. `( )` = group 1 |
| `replacement` | `$1` | નવું નામ = group 1 = database નામ |

| Original topic | Regex match? | નવો topic |
|---|---|---|
| `cdc.de_source_mongo.student` | ✅ | `de_source_mongo` |
| `cdc.de_source_mongo.ordes` | ✅ | `de_source_mongo` |
| `other.topic` | ❌ | `other.topic` (બદલાય નહીં) |

**બીજા replacement ઉદાહરણો:**

| `replacement` | પરિણામ (`cdc.de_source_mongo.student`) |
|---|---|
| `$1` ✅ | `de_source_mongo` |
| `$1.cdc` | `de_source_mongo.cdc` |
| `all_changes` | `all_changes` (બધા databases પણ એક જ topic માં) |

> JSON માં backslash બે વાર લખવો પડે: `\\.` = regex નો `\.` (literal dot).

### 10.2 બીજા ઉપયોગી built-in SMTs (Kafka સાથે આવે છે)

| SMT (`org.apache.kafka.connect.transforms.`) | કામ | આપણા setup માં ચાલે? |
|---|---|---|
| `TimestampRouter` | Topic નામમાં date ઉમેરવી (`topic-20261028`) | ✅ (ફક્ત topic નામ બદલે છે) |
| `InsertField$Value` / `$Key` | Field ઉમેરવું (static value, timestamp, topic નામ) | ❌ **String value પર ન ચાલે** (Struct/Map જોઈએ) |
| `ReplaceField$Value` | Fields કાઢવા કે rename કરવા | ❌ String value પર ન ચાલે |
| `MaskField$Value` | Field ની value છુપાવવી | ❌ String value પર ન ચાલે |
| `Filter` + predicate | Record ને drop કરવો | ✅ (predicate topic/header પર આધારિત હોય તો) |
| `InsertHeader`, `DropHeaders` | Kafka headers ઉમેરવા/કાઢવા | ✅ |

> Fields ઉમેરવા/કાઢવા હોય તો આપણા setup માં **`pipeline`** ([6.1](#61-pipeline)) અથવા **subscriber** માં કરો, SMT માં નહીં.

### 10.3 Predicates (SMT ને શરત સાથે લાગુ કરવું)
```json
"transforms": "dbTopic",
"transforms.dbTopic.predicate": "isCdc",
"transforms.dbTopic.negate": "false",
"predicates": "isCdc",
"predicates.isCdc.type": "org.apache.kafka.connect.transforms.predicates.TopicNameMatches",
"predicates.isCdc.pattern": "cdc\\..*"
```
| Predicate | ક્યારે સાચું |
|---|---|
| `TopicNameMatches` (`pattern`) | Topic નું નામ regex સાથે match થાય |
| `HasHeaderKey` (`name`) | Record માં આ header હોય |
| `RecordIsTombstone` | Value `null` હોય |

---

## 11. Errors, heartbeat અને offsets

### 11.1 Kafka Connect error handling
| Key | Values | અર્થ |
|---|---|---|
| `errors.tolerance` | `none` (default) / `all` | `none` = ખરાબ record પર task **FAILED**. `all` = ખરાબ record skip કરીને આગળ |
| `errors.log.enable` | `false` (default) / `true` | Skip થયેલા records ના errors log કરવા |
| `errors.log.include.messages` | `false` (default) / `true` | Log માં record નો content પણ |
| `errors.retry.timeout` | `0` (default), ms, `-1` (હંમેશા) | Retry થઈ શકે એવા errors માટે કેટલો સમય retry કરવું |
| `errors.retry.delay.max.ms` | `60000` (default) | Retries વચ્ચે વધુમાં વધુ રાહ |

> Source connector માં આ settings **converter અને transform** ના errors ને લાગુ પડે છે.

### 11.2 MongoDB connector ના પોતાના error settings
| Key | Values | અર્થ |
|---|---|---|
| `mongo.errors.tolerance` | `none` (default) / `all` | Connector ના પોતાના errors (દા.ત. document ને JSON/schema માં ફેરવવામાં ભૂલ) |
| `mongo.errors.log.enable` | `false` / `true` | એ errors log કરવા |
| `errors.deadletterqueue.topic.name` | `""` (default) / topic નામ | ફેરવી ન શકાયેલા documents આ DLQ topic માં (`mongo.errors.tolerance=all` સાથે) |

> આપણો subscriber નો DLQ (`de_source_mongo.dlq`) **અલગ** છે. એ consumer બાજુનો છે, connector નો નહીં.

### 11.3 Heartbeat
| Key | Default | અર્થ |
|---|---|---|
| `heartbeat.interval.ms` | `0` (બંધ) ✅ | દર N ms એ heartbeat message મોકલે, જેથી **resume token આગળ વધતો રહે** |
| `heartbeat.topic.name` | `__mongodb_heartbeats` | Heartbeat messages નો topic |

**ક્યારે ચાલુ કરવું?** જ્યારે watch કરેલા collections માં **લાંબો સમય ફેરફાર ન થાય**. ત્યારે resume token જૂનો થઈ જાય, અને oplog માંથી નીકળી જાય તો connector restart પર *"resume token not found"* error આપે. ઉદાહરણ: `"heartbeat.interval.ms": "60000"`.

### 11.4 `offset.partition.name`
| Value | અર્થ |
|---|---|
| `""` (default) ✅ | Offsets નું default નામ |
| કોઈ પણ નવું નામ, જેમ કે `v2` | **એ જ connector નામ રાખીને** offsets "નવેસરથી" શરૂ કરવા. `startup.mode` (જેમ કે `copy_existing`) ફરી લાગુ થાય |

> આ connector નું નામ બદલવાનો વિકલ્પ છે ([MONGO_CDC_COMPLETE_SETUP_GU.md 12.5](MONGO_CDC_COMPLETE_SETUP_GU.md)).

---

## 12. Topic creation અને producer overrides

### 12.1 `topic.creation.*` (Kafka Connect, source connectors માટે)
Topic **ન હોય તો** connector એને કયા settings સાથે બનાવે. Worker માં `topic.creation.enable=true` (default) હોવું જોઈએ.

```json
"topic.creation.default.partitions": "3",
"topic.creation.default.replication.factor": "1"
```
| Key | અર્થ |
|---|---|
| `topic.creation.default.partitions` | નવા topic ના partitions |
| `topic.creation.default.replication.factor` | Replication factor (single broker = 1) |
| `topic.creation.default.<topic config>` | જેમ કે `cleanup.policy`, `retention.ms` |
| `topic.creation.groups` | અલગ topics માટે અલગ settings (`include`/`exclude` regex સાથે) |

> આપણે topics **હાથે** બનાવીએ છીએ ([CDC setup વિભાગ 5](MONGO_CDC_COMPLETE_SETUP_GU.md)), એટલે આ settings નથી રાખી. ન બનાવો તો broker ના `num.partitions` (આપણું 1) પ્રમાણે topic બને છે.

### 12.2 `producer.override.*`
Connector નો Kafka producer કેવી રીતે લખે, એ બદલવું. Worker માં `connector.client.config.override.policy=All` હોવું જોઈએ (Kafka 3.0+ માં default `All`).

| ઉદાહરણ | હેતુ |
|---|---|
| `producer.override.max.request.size=5242880` | ~1 MB થી મોટા documents (broker પર `message.max.bytes` પણ વધારવું પડે) |
| `producer.override.compression.type=lz4` | Network/disk બચાવવા |
| `producer.override.linger.ms=50` | Batches મોટા કરવા (throughput) |
| `producer.override.acks=all` | વધુ સલામત લખાણ |

---

## 13. Values ના સાચા combinations

| `output.format.value` | `value.converter` | `output.json.formatter` | પરિણામ |
|---|---|---|---|
| `json` | `StringConverter` | `ExtendedJson` | ✅ **આપણો setup**: types સાથે JSON text |
| `json` | `StringConverter` | `SimplifiedJson` | ✅ ચાલે, પણ types ખોવાય |
| `json` | `JsonConverter` | ગમે તે | ⚠️ JSON ફરી string માં wrap થાય (`"\"{...}\""` જેવું) |
| `bson` | `ByteArrayConverter` | – | ✅ Raw BSON bytes (consumer બાજુ BSON decode કરવું પડે) |
| `bson` | `StringConverter` | – | ❌ ખોટું |
| `schema` | `JsonConverter` (`schemas.enable=true`) | – | ✅ Schema + payload JSON |
| `schema` | Avro converter + Schema Registry | – | ✅ Avro |

| `publish.full.document.only` | `change.stream.full.document` | પરિણામ |
|---|---|---|
| `false` | `updateLookup` | ✅ **આપણો setup**: આખો event, update માં પણ આખો document |
| `false` | `default` | ✅ Update માં ફક્ત `updateDescription` (આપણો subscriber `$set/$unset` થી સંભાળી લે છે) |
| `true` | `updateLookup` | ✅ ફક્ત documents, **deletes નહીં** |
| `true` | `default` | ⚠️ Update events માં `fullDocument` નથી, એટલે updates પણ **skip** થાય |

| `startup.mode` | જરૂરી બીજી key |
|---|---|
| `latest` | – |
| `copy_existing` | (optional) `startup.mode.copy.existing.*` |
| `timestamp` | **`startup.mode.timestamp.start.at.operation.time`** |

---

## 14. ⚠️ આ values બદલશો તો આપણો subscriber તૂટશે

| Key | ✅ જરૂરી value | બદલશો તો શું થાય |
|---|---|---|
| `publish.full.document.only` | `false` | `operationType`, `ns`, `documentKey` નહીં આવે → subscriber parse fail → **બધા messages DLQ માં**. Deletes પણ નહીં આવે |
| `output.json.formatter` | `ExtendedJson` | `SimplifiedJson`: `_id` string બને, એટલે bronze માં જૂના ObjectId documents સાથે match નહીં થાય (duplicates). `DefaultJson`: Int32/Double ની ચોક્કસ type માહિતી ખોવાય |
| `output.format.value` | `json` | `bson`/`schema`: value JSON text નહીં રહે → subscriber નું `BsonDocument.parse()` fail |
| `value.converter` | `StringConverter` | `JsonConverter`: JSON ફરી string માં wrap થાય → parse fail |
| `transforms.dbTopic.*` | હાલનું | Topic નું નામ બદલાય → subscriber ના `kafka.topic` (`de_source_mongo`) માં કંઈ નહીં આવે |
| `topic.prefix` / `topic.separator` | `cdc` / `.` | Regex match નહીં થાય → `cdc.de_source_mongo.<coll>` જેવા **ઘણા topics** બનશે |
| `database` | `de_source_mongo` | `""`: બીજા databases ના events પણ આવશે, અને RegexRouter દરેક DB નો **અલગ topic** બનાવશે |
| `change.stream.full.document` | `updateLookup` (ભલામણ) | `default`: ચાલશે (subscriber `$set/$unset` કરે છે), પણ bronze માં update પછી આખો document replace નહીં થાય |

---

## 15. Config બદલવાની સલામત રીત

1. `kafka-connect\mongo-cdc-db-source.json` બદલો.
2. Apply કરો (`PUT` = update, connector restart થાય):
   ```powershell
   cd D:\ClaudeProjects\dataEngineeringProjects\sparkProject\kafka-connect
   .\register-connectors.ps1
   ```
3. Status ચકાસો: `.\register-connectors.ps1 status` (`RUNNING`, અને `FAILED` હોય તો `ERROR:` line વાંચો).
4. Console consumer થી નવો message જુઓ (`--from-beginning` **વગર**, નવો change કરીને).

**કયો ફેરફાર ક્યારે લાગુ પડે?**

| ફેરફાર | Restart પછી તરત? | નોંધ |
|---|---|---|
| Format, formatter, converter, transforms, pipeline, full.document | ✅ નવા events પર | જૂના messages topic માં **જૂના format** માં રહે છે |
| `startup.mode` અને `startup.mode.*` | ❌ **ના** | ફક્ત offsets ન હોય ત્યારે. નવું connector નામ અથવા `offset.partition.name` બદલો |
| `database` / `collection` | ✅ | Resume token બીજા stream નો હોવાથી error આવી શકે. આવે તો `offset.partition.name` બદલો |
| `topic.*` / RegexRouter | ✅ | નવો topic બને. Subscriber નો `kafka.topic` પણ બદલવો પડે |

> Format જેવો મોટો ફેરફાર કરો ત્યારે **નવો topic** (નવો prefix/replacement) વાપરવો સૌથી સલામત છે, જેથી એક જ topic માં બે format ભળે નહીં. Windows પર જૂનો topic **delete ન કરો**.
