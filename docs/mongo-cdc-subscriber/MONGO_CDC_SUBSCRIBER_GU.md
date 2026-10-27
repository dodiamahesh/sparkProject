# MongoDB CDC: એક Generic Topic + પોતાનો Subscriber (Sink Connector વગર)

> **ધ્યેય:**
> 1. Source database (`de_source_mongo`) ના **બધા collections** ના changes **એક જ Kafka topic** માં જાય. Topic નું નામ = **source database નું નામ** (`de_source_mongo`).
> 2. **MongoDB Sink Connector વાપરવો નથી.** એની જગ્યાએ **આપણો પોતાનો Java subscriber** (Kafka consumer) topic વાંચશે.
> 3. Subscriber દરેક message નો `operationType` જોઈને destination database માં **એ જ collection** માં **insert / update / delete** કરશે.
> 4. Data **original format** માં જ રહે: `ObjectId`, `Int32`, `Date` અને fields નો order બધું એ જ.

---

## અનુક્રમણિકા

- [0. Architecture](#0-architecture)
- [1. જૂના connectors દૂર કરો](#1-જૂના-connectors-દૂર-કરો)
- [2. Kafka topics બનાવો](#2-kafka-topics-બનાવો)
- [3. Source connector: બધા collections → એક topic](#3-source-connector-બધા-collections--એક-topic)
- [4. Topic માં message ચકાસો](#4-topic-માં-message-ચકાસો)
- [5. Subscriber project બનાવો](#5-subscriber-project-બનાવો)
- [6. Build અને Run](#6-build-અને-run)
- [7. End-to-end test](#7-end-to-end-test)
- [8. Subscriber અંદરથી કેવી રીતે કામ કરે છે](#8-subscriber-અંદરથી-કેવી-રીતે-કામ-કરે-છે)
- [9. શરૂઆતથી ફરી sync કરવું (Replay)](#9-શરૂઆતથી-ફરી-sync-કરવું-replay)
- [10. મર્યાદાઓ](#10-મર્યાદાઓ)
- [11. Troubleshooting](#11-troubleshooting)
- [12. Checklist](#12-checklist)

---

## 0. Architecture

```
            MongoDB (rs0)
   de_source_mongo
   ├── weather_observations ─┐
   ├── student ──────────────┼──► Kafka Connect: mongo-cdc-db-source
   └── (કોઈ પણ નવું collection)┘    (MongoSourceConnector + RegexRouter)
                                              │
                                              ▼
                           Kafka topic: de_source_mongo   (3 partitions)
                           ┌──────────────────────────────────────────┐
                           │ key   = {"_id": {"$oid": "..."}}          │
                           │ value = આખો change event (Extended JSON)   │
                           │         operationType, ns.coll,           │
                           │         documentKey, fullDocument, ...    │
                           └──────────────────────────────────────────┘
                                              │
                                              ▼
                           Java app: mongo-cdc-subscriber  (Kafka consumer)
                           ├─ insert / replace / update → replaceOne(upsert)
                           ├─ delete                    → deleteOne
                           ├─ parse ન થાય એવો message    → de_source_mongo.dlq
                           └─ write સફળ થાય પછી જ offset commit
                                              │
                                              ▼
            MongoDB: de_target_mongo
            ├── weather_observations
            ├── student
            └── ...  (source જેવા જ નામ, collection આપોઆપ બને)
```

| ભાગ | નામ |
|---|---|
| Source database | `de_source_mongo` |
| Kafka topic (એક જ) | `de_source_mongo` |
| DLQ topic | `de_source_mongo.dlq` |
| Source connector | `mongo-cdc-db-source` |
| Subscriber (consumer group) | `mongo-cdc-subscriber` |
| Destination database | `de_target_mongo` (config માં બદલી શકાય) |

> **નોંધ:** આ setup માં **sink connector નથી.** Kafka Connect ફક્ત source બાજુ (MongoDB → Kafka) માટે છે. Kafka → MongoDB નું કામ તમારો subscriber કરશે.

---

## 1. જૂના connectors દૂર કરો

```powershell
cd D:\ClaudeProjects\dataEngineeringProjects\sparkProject\kafka-connect

.\register-connectors.ps1 delete mongo-cdc-sink-bronze
.\register-connectors.ps1 delete mongo-cdc-source

Invoke-RestMethod http://localhost:8083/connectors
```
➡️ છેલ્લો command ખાલી result આપવો જોઈએ.

જૂની JSON files ખસેડો:
```powershell
New-Item -ItemType Directory -Force .\_old
Move-Item .\mongo-cdc-source.json, .\mongo-cdc-sink-bronze.json .\_old\
```

> ⚠️ **જૂના topics (`mongo.*`, `mongocdc.*`) delete ન કરો.** Windows પર topic delete કરવાથી broker crash થાય છે (જે પહેલાં થયું હતું). એ પડ્યા રહેશે તો નવા setup ને કોઈ અસર નથી.

> **નવું connector નામ કેમ?** Kafka Connect source offsets (resume token) **connector ના નામ** પ્રમાણે સાચવે છે. નવું નામ (`mongo-cdc-db-source`) હશે તો `copy_existing` ફરીથી ચાલશે અને હાલના બધા documents પણ topic માં આવશે.

---

## 2. Kafka topics બનાવો

Topic **પહેલેથી** બનાવી લો, જેથી partitions ની સંખ્યા આપણે નક્કી કરીએ. Kafka આપોઆપ બનાવે તો ફક્ત 1 partition આપે છે.

```powershell
cd C:\kafka

# Main CDC topic: source database નું નામ
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --create --topic de_source_mongo --partitions 3 --replication-factor 1

# DLQ: subscriber parse ન કરી શકે એવા messages
.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --create --topic de_source_mongo.dlq --partitions 1 --replication-factor 1

.\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --describe --topic de_source_mongo
```
➡️ `PartitionCount: 3`

| નિર્ણય | કારણ |
|---|---|
| **3 partitions** | પછી subscriber ના 3 instances સુધી parallel ચલાવી શકાય |
| Order ની ચિંતા? | Message નો **key = document નો `_id`**. Kafka એક જ key ને **હંમેશા એક જ partition** માં મોકલે છે, અને partition ની અંદર order સચવાય છે. એટલે **એક document ના બધા changes ક્રમમાં જ** આવશે |

> Kafka topic નામમાં `.` અને `_` બંને ચાલે છે, પણ બંને સાથે વાપરવા પર Kafka ચેતવણી આપે છે (metrics માં બંને સરખા દેખાય છે). એ ચેતવણી અવગણી શકાય.

---

## 3. Source connector: બધા collections → એક topic

### 3.1 નવી file: `kafka-connect\mongo-cdc-db-source.json`

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
| `database` (`collection` વગર) | Database ના **બધા collections** watch થાય. નવું collection બને તો એ પણ આપોઆપ આવે |
| `topic.prefix: cdc` | Connector પહેલાં `cdc.de_source_mongo.<collection>` નામ બનાવે છે... |
| `RegexRouter` (`dbTopic`) | ...અને આ transform એને બદલીને ફક્ત **`de_source_mongo`** કરે છે (regex નો `$1` = database નામ). એટલે **બધા collections એક જ topic** માં જાય. આ બદલાવ Kafka માં લખતા **પહેલાં** થાય છે, એટલે `cdc.*` નામનો કોઈ topic બનતો નથી |
| `startup.mode: copy_existing` | પહેલી વાર હાલના બધા documents ને `insert` events તરીકે મોકલે, પછી live changes |
| `publish.full.document.only: false` | **આખો change event** મોકલે: `operationType`, `ns.coll`, `documentKey`, `fullDocument`, `updateDescription`. Delete events પણ આવે |
| `change.stream.full.document: updateLookup` | Update event સાથે document ની **આખી નવી copy** (`fullDocument`) પણ આવે |
| `ExtendedJson` | **Canonical Extended JSON**: `{"$oid": ...}`, `{"$numberInt": "101"}`, `{"$date": ...}`. Types ની માહિતી સચવાય, જેથી subscriber original BSON પાછું બનાવી શકે |
| `StringConverter` | Connector નું JSON text **જેમ છે તેમ** Kafka માં જાય, ફરી wrap ન થાય |

> **Source DB ને બદલે આખું MongoDB cluster watch કરવું હોય તો** `database` line કાઢી નાખો. RegexRouter દરેક database માટે અલગ topic બનાવશે (`<database_name>`), એટલે ત્યારે દરેક database નો topic Step 2 ની જેમ પહેલેથી બનાવી લો.

### 3.2 `register-connectors.ps1` માં નામ બદલો

```powershell
$names = "mongo-cdc-db-source"
```
Line 3 ની comment પણ સુધારો:
```powershell
#   .\register-connectors.ps1              -> create/update all connectors (mongo-cdc-db-source)
```

### 3.3 Register કરો

```powershell
cd D:\ClaudeProjects\dataEngineeringProjects\sparkProject\kafka-connect
.\register-connectors.ps1
```
➡️ `mongo-cdc-db-source  connector=RUNNING  tasks=RUNNING`

---

## 4. Topic માં message ચકાસો

```powershell
cd C:\kafka
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic de_source_mongo --from-beginning --formatter-property print.key=true --formatter-property print.partition=true
```

➡️ `copy_existing` ના કારણે **બંને collections** (`weather_observations`, `student`) ના documents **એક જ topic** માં દેખાશે:
```
Partition:1  {"_id": {"$oid": "6ae0e20a..."}}  {"_id": {"_data": "..."}, "operationType": "insert", ..., "fullDocument": {"_id": {"$oid": "6ae0e20a..."}, "empId": {"$numberInt": "101"}, ...}, "ns": {"db": "de_source_mongo", "coll": "weather_observations"}, "documentKey": {"_id": {"$oid": "6ae0e20a..."}}}
Partition:0  {"_id": {"$oid": "6ae0e6da..."}}  {"_id": {"_data": "..."}, "operationType": "insert", ..., "ns": {"db": "de_source_mongo", "coll": "student"}, ...}
```

ચકાસો:
- [ ] `ns.coll` માં **અલગ અલગ collections** ના નામ છે, પણ topic એક જ છે
- [ ] `empId` = `{"$numberInt": "101"}` અને `_id` = `{"$oid": ...}`, એટલે types સચવાયા છે
- [ ] Topic list માં `cdc.*` નામનો કોઈ topic **નથી** બન્યો

`Ctrl + C` થી બંધ કરો.

---

## 5. Subscriber project બનાવો

Subscriber એક **અલગ Maven project** છે. Spark project (`pom.xml`, Spring Boot parent) સાથે dependencies ભળે નહીં, એ માટે એને અલગ રાખીએ છીએ.

### 5.1 Folder structure

```
D:\ClaudeProjects\dataEngineeringProjects\sparkProject\mongo-cdc-subscriber\
├── pom.xml
└── src\main\
    ├── java\com\de\cdc\
    │   ├── SubscriberApp.java         ← Kafka consumer loop, offsets, DLQ
    │   └── ChangeEventApplier.java    ← change event → MongoDB write
    └── resources\
        └── subscriber.properties      ← બધી settings
```

```powershell
cd D:\ClaudeProjects\dataEngineeringProjects\sparkProject
New-Item -ItemType Directory -Force mongo-cdc-subscriber\src\main\java\com\de\cdc
New-Item -ItemType Directory -Force mongo-cdc-subscriber\src\main\resources
```

### 5.2 `pom.xml`

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

| Dependency | કામ |
|---|---|
| `kafka-clients` | Kafka consumer (topic વાંચવા) અને producer (DLQ માં લખવા) |
| `mongodb-driver-sync` | MongoDB માં લખવા. એમાં `BsonDocument.parse()` છે, જે Extended JSON ને **original BSON types** માં ફેરવે છે. Version Spark project જેવું જ (`5.1.4`) |
| `slf4j-simple` | Console માં logs |

### 5.3 `src\main\resources\subscriber.properties`

```properties
# ---- Kafka ----
kafka.bootstrap.servers=localhost:9092
kafka.topic=de_source_mongo
kafka.group.id=mongo-cdc-subscriber
kafka.dlq.topic=de_source_mongo.dlq

# ---- MongoDB ----
mongo.uri=mongodb://localhost:27017/?replicaSet=rs0
# Every source collection is written to the collection with the same name in this database
target.database=de_target_mongo

# Apply "drop collection" events to the target? (false = only log them)
apply.drop=false

# ---- Logging (slf4j-simple) ----
org.slf4j.simpleLogger.defaultLogLevel=info
org.slf4j.simpleLogger.log.org.apache.kafka=warn
org.slf4j.simpleLogger.showDateTime=true
```

| Setting | અર્થ |
|---|---|
| `kafka.group.id` | Consumer group. Kafka **આ નામે** યાદ રાખે છે કે ક્યાં સુધી વાંચ્યું. એ જ નામે ફરી start કરો તો ત્યાંથી જ આગળ વાંચે |
| `target.database` | Destination database. Collection નું નામ source જેવું જ રહે છે (`ns.coll`) |
| `apply.drop` | Source માં આખું collection drop થાય ત્યારે destination માં પણ drop કરવું કે નહીં. સલામતી માટે default `false` |

> Logging settings properties file માંથી વંચાય તે માટે `SubscriberApp` start થતાં જ એમને System properties માં મૂકે છે (નીચે code જુઓ).

### 5.4 `src\main\java\com\de\cdc\ChangeEventApplier.java`

```java
package com.de.cdc;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Turns MongoDB change events (canonical Extended JSON from the MongoDB source connector)
 * into writes on the target database. Every write is idempotent, so replaying a message is safe.
 */
public class ChangeEventApplier {

    private static final Logger log = LoggerFactory.getLogger(ChangeEventApplier.class);
    private static final ReplaceOptions UPSERT = new ReplaceOptions().upsert(true);
    private static final BulkWriteOptions ORDERED = new BulkWriteOptions().ordered(true);

    private final MongoDatabase target;
    private final boolean applyDrop;

    /** op = operationType, coll = ns.coll, write = null when the event needs no document write */
    private record Change(String op, String coll, WriteModel<BsonDocument> write) { }

    public ChangeEventApplier(MongoClient mongo, String targetDatabase, boolean applyDrop) {
        this.target = mongo.getDatabase(targetDatabase);
        this.applyDrop = applyDrop;
    }

    /**
     * Applies one poll's records. Writes are grouped per collection and sent as ordered bulk writes.
     * All changes to one document share a key, so they sit in one partition and keep their order.
     * Records that cannot be parsed go to onBadRecord; MongoDB errors are thrown to the caller.
     */
    public void apply(ConsumerRecords<String, String> records,
                      BiConsumer<ConsumerRecord<String, String>, Exception> onBadRecord) {
        Map<String, List<WriteModel<BsonDocument>>> pending = new LinkedHashMap<>();

        for (ConsumerRecord<String, String> rec : records) {
            if (rec.value() == null) {
                continue; // tombstone
            }
            Change change;
            try {
                change = parse(rec.value());
            } catch (RuntimeException e) {
                onBadRecord.accept(rec, e);
                continue;
            }

            if (change.write() != null) {
                pending.computeIfAbsent(change.coll(), c -> new ArrayList<>()).add(change.write());
            } else if ("drop".equals(change.op())) {
                // writes queued before the drop must land first
                flush(change.coll(), pending.remove(change.coll()));
                drop(change.coll());
            } else {
                log.warn("Skipping '{}' event (partition {}, offset {})", change.op(), rec.partition(), rec.offset());
            }
        }
        pending.forEach(this::flush);
    }

    private static Change parse(String json) {
        BsonDocument event = BsonDocument.parse(json); // canonical Extended JSON -> original BSON types
        String op = event.getString("operationType").getValue();
        String coll = event.containsKey("ns") && event.getDocument("ns").containsKey("coll")
                ? event.getDocument("ns").getString("coll").getValue()
                : null;

        WriteModel<BsonDocument> write = switch (op) {
            case "insert", "replace" -> new ReplaceOneModel<>(byId(event), event.getDocument("fullDocument"), UPSERT);
            case "update" -> {
                BsonValue full = event.get("fullDocument");
                // updateLookup gives the whole document; it is null when the document was deleted meanwhile
                yield full != null && full.isDocument()
                        ? new ReplaceOneModel<>(byId(event), full.asDocument(), UPSERT)
                        : fromUpdateDescription(byId(event), event.getDocument("updateDescription"));
            }
            case "delete" -> new DeleteOneModel<>(byId(event));
            default -> null; // drop, rename, dropDatabase, invalidate
        };
        return new Change(op, coll, write);
    }

    private static Bson byId(BsonDocument event) {
        return Filters.eq("_id", event.getDocument("documentKey").get("_id"));
    }

    private static WriteModel<BsonDocument> fromUpdateDescription(Bson byId, BsonDocument desc) {
        BsonDocument update = new BsonDocument();
        BsonDocument set = desc.getDocument("updatedFields", new BsonDocument());
        if (!set.isEmpty()) {
            update.put("$set", set);
        }
        BsonDocument unset = new BsonDocument();
        for (BsonValue field : desc.getArray("removedFields", new BsonArray())) {
            unset.put(field.asString().getValue(), new BsonString(""));
        }
        if (!unset.isEmpty()) {
            update.put("$unset", unset);
        }
        return update.isEmpty() ? null : new UpdateOneModel<>(byId, update);
    }

    private void flush(String coll, List<WriteModel<BsonDocument>> writes) {
        if (writes == null || writes.isEmpty()) {
            return;
        }
        BulkWriteResult r = target.getCollection(coll, BsonDocument.class).bulkWrite(writes, ORDERED);
        log.info("{}: {} writes (matched={}, upserted={}, deleted={})",
                coll, writes.size(), r.getMatchedCount(), r.getUpserts().size(), r.getDeletedCount());
    }

    private void drop(String coll) {
        if (applyDrop) {
            target.getCollection(coll).drop();
            log.warn("{}: collection dropped (source collection was dropped)", coll);
        } else {
            log.warn("{}: source collection was dropped; target kept because apply.drop=false", coll);
        }
    }
}
```

### 5.5 `src\main\java\com\de\cdc\SubscriberApp.java`

```java
package com.de.cdc;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.apache.kafka.clients.consumer.ConsumerConfig;
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
 * Reads MongoDB change events from one Kafka topic and applies them to the target database.
 * Offsets are committed only after the writes succeed (at-least-once; writes are idempotent).
 *
 * Usage: java -jar mongo-cdc-subscriber.jar [path\to\subscriber.properties]
 */
public class SubscriberApp {

    public static void main(String[] args) throws IOException {
        Properties cfg = loadConfig(args);
        // slf4j-simple reads its settings from system properties; set them before the first logger is created
        cfg.stringPropertyNames().stream()
                .filter(k -> k.startsWith("org.slf4j."))
                .forEach(k -> System.setProperty(k, cfg.getProperty(k)));
        Logger log = LoggerFactory.getLogger(SubscriberApp.class);

        String topic = cfg.getProperty("kafka.topic");
        String dlqTopic = cfg.getProperty("kafka.dlq.topic");

        try (MongoClient mongo = MongoClients.create(cfg.getProperty("mongo.uri"));
             KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps(cfg));
             KafkaProducer<String, String> dlq = new KafkaProducer<>(producerProps(cfg))) {

            ChangeEventApplier applier = new ChangeEventApplier(mongo,
                    cfg.getProperty("target.database"),
                    Boolean.parseBoolean(cfg.getProperty("apply.drop", "false")));

            // Ctrl+C: break out of poll() and close everything cleanly
            Thread mainThread = Thread.currentThread();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                consumer.wakeup();
                try {
                    mainThread.join();
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }));

            consumer.subscribe(List.of(topic));
            log.info("Subscribed to '{}', writing to database '{}'", topic, cfg.getProperty("target.database"));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                if (records.isEmpty()) {
                    continue;
                }
                applier.apply(records, (rec, err) -> {
                    log.error("Bad message at partition {} offset {} -> {}", rec.partition(), rec.offset(), dlqTopic, err);
                    ProducerRecord<String, String> out = new ProducerRecord<>(dlqTopic, rec.key(), rec.value());
                    out.headers().add("error", String.valueOf(err).getBytes(UTF_8));
                    out.headers().add("source", (rec.topic() + "-" + rec.partition() + "-" + rec.offset()).getBytes(UTF_8));
                    dlq.send(out);
                });
                dlq.flush();
                consumer.commitSync();
            }
        } catch (WakeupException e) {
            log.info("Stopped");
        }
    }

    private static Properties loadConfig(String[] args) throws IOException {
        Properties p = new Properties();
        try (InputStream in = args.length > 0
                ? Files.newInputStream(Path.of(args[0]))
                : SubscriberApp.class.getResourceAsStream("/subscriber.properties")) {
            p.load(in);
        }
        return p;
    }

    private static Properties consumerProps(Properties cfg) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.getProperty("kafka.bootstrap.servers"));
        p.put(ConsumerConfig.GROUP_ID_CONFIG, cfg.getProperty("kafka.group.id"));
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");    // commit only after MongoDB writes succeed
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");  // first run: read the topic from the start
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
        return p;
    }

    private static Properties producerProps(Properties cfg) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.getProperty("kafka.bootstrap.servers"));
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        return p;
    }
}
```

---

## 6. Build અને Run

### 6.1 Build
```powershell
cd D:\ClaudeProjects\dataEngineeringProjects\sparkProject\mongo-cdc-subscriber
mvn -q clean package
Get-ChildItem target\*.jar
```
➡️ `mongo-cdc-subscriber-1.0.0-jar-with-dependencies.jar` બનવી જોઈએ.

### 6.2 Run (નવું terminal, ખુલ્લું રાખો: Terminal #4)
```powershell
cd D:\ClaudeProjects\dataEngineeringProjects\sparkProject\mongo-cdc-subscriber
java -jar target\mongo-cdc-subscriber-1.0.0-jar-with-dependencies.jar
```

Settings બદલવા માટે ફરી build ન કરવું હોય તો properties file નો path આપો:
```powershell
java -jar target\mongo-cdc-subscriber-1.0.0-jar-with-dependencies.jar src\main\resources\subscriber.properties
```

➡️ Logs:
```
[main] INFO com.de.cdc.SubscriberApp - Subscribed to 'de_source_mongo', writing to database 'de_target_mongo'
[main] INFO com.de.cdc.ChangeEventApplier - weather_observations: 1 writes (matched=0, upserted=1, deleted=0)
[main] INFO com.de.cdc.ChangeEventApplier - student: 1 writes (matched=0, upserted=1, deleted=0)
```

બંધ કરવા: `Ctrl + C`. એ છેલ્લો batch પૂરો કરીને બંધ થશે. ફરી start કરશો તો **જ્યાંથી અટક્યું હતું ત્યાંથી** આગળ વાંચશે.

### 6.3 Consumer group ચકાસો
```powershell
cd C:\kafka
.\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --describe --group mongo-cdc-subscriber
```
➡️ ત્રણેય partitions માં `LAG = 0` હોય તો subscriber બધા messages વાંચી ચૂક્યો છે.

---

## 7. End-to-end test

Subscriber (Terminal #4) ચાલુ રાખીને `mongosh` અથવા Compass માં:

### 7.1 Insert
```javascript
use de_source_mongo
db.student.insertOne({ studentId: 2, name: "Test Student", course: "Maths", marks: 70 })
```
```javascript
use de_target_mongo
db.student.find({ studentId: 2 })
```
➡️ Document દેખાય. Compass માં types ચકાસો: `_id` = **ObjectId**, `marks` = **Int32**.

### 7.2 Update
```javascript
use de_source_mongo
db.student.updateOne({ studentId: 2 }, { $set: { marks: 85 } })
```
➡️ `de_target_mongo.student` માં `marks: 85`, અને **document એક જ** (duplicate નહીં).

### 7.3 Replace (Compass માં document edit કરીને save કરો)
➡️ Log માં `student: 1 writes (matched=1, ...)` આવે, અને destination માં ફેરફાર દેખાય.

### 7.4 Delete
```javascript
use de_source_mongo
db.student.deleteOne({ studentId: 2 })
```
➡️ `de_target_mongo.student` માંથી પણ delete થાય. Log: `deleted=1`.

### 7.5 નવું collection (એક જ topic નો ફાયદો)
```javascript
use de_source_mongo
db.course.insertOne({ code: "CS101", title: "Intro to CS" })
```
➡️ **કોઈ config બદલ્યા વગર** `de_target_mongo.course` આપોઆપ બને અને document આવે. નવો topic નથી બનતો, એટલે 5 મિનિટ ની રાહ પણ નથી.

### 7.6 Count મેળવો
```javascript
// mongosh
const s = db.getSiblingDB("de_source_mongo"), t = db.getSiblingDB("de_target_mongo");
s.getCollectionNames().forEach(c => print(c, s[c].countDocuments(), t[c].countDocuments()));
```
➡️ દરેક collection માટે બંને numbers સરખા હોવા જોઈએ.

---

## 8. Subscriber અંદરથી કેવી રીતે કામ કરે છે

### 8.1 Operation → MongoDB write

| `operationType` | Subscriber શું કરે છે | કેમ આ રીતે? |
|---|---|---|
| `insert` | `replaceOne({_id}, fullDocument, upsert)` | `insertOne` replay વખતે *duplicate key* error આપે. Upsert વાપરવાથી એ જ message બે વાર આવે તો પણ પરિણામ એ જ રહે (**idempotent**) |
| `replace` | `replaceOne({_id}, fullDocument, upsert)` | આખો document બદલાયો છે |
| `update` (`fullDocument` હોય) | `replaceOne({_id}, fullDocument, upsert)` | `updateLookup` આખી નવી copy આપે છે, એટલે સીધું replace |
| `update` (`fullDocument` = null) | `updateOne({_id}, {$set: updatedFields, $unset: removedFields})` | Document પાછળથી delete થઈ ગયો હોય ત્યારે lookup ખાલી આવે. ત્યારે ફક્ત બદલાયેલા fields લાગુ કરીએ |
| `delete` | `deleteOne({_id})` | `documentKey._id` પરથી |
| `drop` | `apply.drop=true` હોય તો collection drop, નહીં તો ફક્ત warning log | ભૂલથી આખો data ન જાય એ માટે default બંધ |
| `rename`, `dropDatabase`, `invalidate` | ફક્ત warning log, skip | Section 10 જુઓ |

### 8.2 Original format કેવી રીતે સચવાય છે?

```
MongoDB BSON  ──(source: ExtendedJson)──►  {"empId": {"$numberInt": "101"}, "_id": {"$oid": "..."}}
                                                          │
                                       Subscriber: BsonDocument.parse(...)
                                                          ▼
                                     BsonDocument { empId: BsonInt32(101), _id: BsonObjectId(...) }
                                                          │
                                            replaceOne(...) → MongoDB
                                                          ▼
                                     { _id: ObjectId(...), empId: 101 (Int32) }  ✅ source જેવું જ
```
`BsonDocument` field order સાચવે છે, અને વચ્ચે Java `Map` કે Jackson conversion નથી. એટલે `Int32` → `Long` કે `ObjectId` → `String` જેવા ફેરફાર **થતા નથી**.

### 8.3 Offsets, failures અને "at-least-once"

```
poll() ──► 500 messages ──► MongoDB bulkWrite ──► સફળ? ──► commitSync()  (Kafka: "અહીં સુધી વાંચ્યું")
                                    │
                                    └── MongoDB error ──► exception, app બંધ, commit નહીં
                                                          ફરી start કરો ──► એ જ messages ફરી વંચાય
```
- Offset **write સફળ થયા પછી જ** commit થાય છે. એટલે crash થાય તો કોઈ change ખોવાતો નથી. વધુમાં વધુ થોડા messages **ફરી** લાગુ થાય.
- બધા writes idempotent છે (upsert, replace, delete), એટલે ફરી લાગુ થવાથી data ખોટો થતો નથી.
- **Parse ન થાય એવો message** (ખરાબ JSON) subscriber ને અટકાવતો નથી. એ `de_source_mongo.dlq` માં જાય છે, `error` header સાથે.

### 8.4 Order

- એક જ document ના બધા events નો key સરખો (`_id`) હોય છે → એક જ partition → **ક્રમમાં** આવે.
- Batch ની અંદર writes collection દીઠ ભેગા થાય, પણ **ordered bulk write** થાય, એટલે ક્રમ બદલાતો નથી.
- અલગ documents વચ્ચે global ક્રમ નથી, પણ એની જરૂર પણ નથી, કારણ કે દરેક document નો state સ્વતંત્ર છે.

### 8.5 Scaling (વધુ ઝડપ જોઈએ ત્યારે)

એ જ jar બીજા terminal માં ફરી ચલાવો (એ જ `kafka.group.id` સાથે). Kafka 3 partitions ને instances વચ્ચે આપોઆપ વહેંચી દેશે. **Instances ≤ partitions** (3) રાખો, કારણ કે વધારાના instances બેકાર બેસી રહેશે.

---

## 9. શરૂઆતથી ફરી sync કરવું (Replay)

Destination ને ફરીથી શરૂઆતથી બનાવવું હોય ત્યારે:

1. Subscriber બંધ કરો (`Ctrl + C`). Consumer group **inactive** હોવો જરૂરી છે.
2. Destination database drop કરો:
   ```javascript
   use de_target_mongo
   db.dropDatabase()
   ```
3. Consumer group ને topic ની શરૂઆત પર લઈ જાઓ:
   ```powershell
   cd C:\kafka
   .\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --group mongo-cdc-subscriber --topic de_source_mongo --reset-offsets --to-earliest --execute
   ```
4. Subscriber ફરી start કરો.

> ⚠️ આ ત્યારે જ પૂરો data આપે જ્યાં સુધી topic માં **બધા messages હજુ હોય**. Kafka default રીતે messages **7 દિવસ** (`log.retention.hours=168`) રાખે છે. એના કરતાં જૂનો data જોઈએ તો source connector ને **નવા નામ** સાથે ફરી register કરો, જેથી `copy_existing` ફરી ચાલે.

---

## 10. મર્યાદાઓ

| મર્યાદા | વિગત |
|---|---|
| **Update = latest state** | `updateLookup` lookup **વખતે** નો document આપે છે, update વખતે નો નહીં. એક document પર ઝડપથી ઘણા updates થાય તો વચ્ચેના states destination માં ન દેખાય, પણ **છેલ્લો state હંમેશા સાચો** આવે |
| `rename` collection | Skip થાય છે. Destination માં જૂના નામે collection રહે, અને નવા નામનું collection નવા changes સાથે બને |
| `dropDatabase` / `invalidate` | Skip. Source database drop થાય ત્યારે source connector નો change stream પણ બંધ થઈ શકે, એટલે connector નું status ચકાસો |
| Transactions | Source માં એક transaction ના ઘણા changes, destination માં **અલગ અલગ** writes તરીકે લાગુ થાય (atomic નહીં) |
| Indexes | Source ના indexes destination માં **આપોઆપ બનતા નથી**. જરૂર હોય તો destination માં હાથે બનાવો |
| Message size | Kafka નો default max message size ~1 MB છે. એના કરતાં મોટા documents માટે broker પર `message.max.bytes` અને connector પર `producer.override.max.request.size` વધારવા પડે |

---

## 11. Troubleshooting

| સમસ્યા | કારણ | ઉકેલ |
|---|---|---|
| Topic list માં `cdc.de_source_mongo.student` જેવા topics બન્યા | `transforms.dbTopic.*` lines ખૂટે છે કે regex ખોટો છે | Step 3.1 નો config ફરી ચકાસો. JSON માં `\\.` (બે backslash) જરૂરી છે |
| Connector `RUNNING` પણ topic ખાલી | Source માં હજુ કોઈ change થયો નથી, અને `copy_existing` જૂના connector નામે પહેલેથી થઈ ગયું છે | Connector નું નામ નવું રાખો (Step 1 ની નોંધ) |
| Subscriber start થાય પણ કંઈ લખાતું નથી | Consumer group નો offset પહેલેથી topic ના છેડે છે | Section 6.3 થી `LAG` જુઓ. જરૂર હોય તો Section 9 થી reset કરો |
| `mvn package` વખતે `release version 17 not supported` | Maven જૂની Java વાપરે છે | `mvn -v` માં `Java version: 17` હોવું જોઈએ. `JAVA_HOME` ને JDK 17 પર set કરો |
| Log માં `SLF4J: No SLF4J providers were found` | `slf4j-api` 1.7.x જીતી ગયું છે | pom માં `slf4j-api` 2.0.13 ની dependency છે કે નહીં તે ચકાસો |
| `MongoBulkWriteException` અને app બંધ | Destination MongoDB માં error (જેમ કે document validation, disk full) | Error વાંચીને ઠીક કરો, પછી subscriber ફરી start કરો. Commit ન થયો હોવાથી data ખોવાયો નથી |
| `MongoTimeoutException` | MongoDB બંધ છે અથવા `mongo.uri` ખોટો છે | `mongosh "mongodb://localhost:27017/?replicaSet=rs0"` થી connection ચકાસો |
| DLQ માં messages આવે છે | Message parse ન થયો | DLQ વાંચો: `kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic de_source_mongo.dlq --from-beginning --formatter-property print.headers=true`. `error` header માં કારણ હશે |
| Destination માં `_id` string અને numbers `Long` છે | Source connector `ExtendedJson` ને બદલે `SimplifiedJson`/`DefaultJson` વાપરે છે | Step 3.1 મુજબ `output.json.formatter` = `ExtendedJson` |
| Kafka console માં `Reconfiguration failed: No configuration found` | ફક્ત logging ની ચેતવણી | અવગણો |

---

## 12. Checklist

- [ ] Step 1: `mongo-cdc-sink-bronze` અને `mongo-cdc-source` delete કર્યા, JSON files `_old\` માં ખસેડી
- [ ] Step 2: `de_source_mongo` (3 partitions) અને `de_source_mongo.dlq` topics બનાવ્યા
- [ ] Step 3: `mongo-cdc-db-source.json` બનાવી, script નો `$names` બદલ્યો, connector `RUNNING`
- [ ] Step 4: એક જ topic માં બધા collections ના events દેખાય છે, અને `cdc.*` topic નથી બન્યો
- [ ] Step 5: `mongo-cdc-subscriber` project બનાવ્યો (pom, properties, 2 Java files)
- [ ] Step 6: Build થયું, subscriber ચાલે છે, `LAG = 0`
- [ ] Step 7: Insert, update, replace, delete અને નવું collection: બધા test pass, types (`ObjectId`, `Int32`) સચવાયા
