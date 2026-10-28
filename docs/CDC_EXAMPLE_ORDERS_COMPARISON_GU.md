# CDC Example: `ordes` collection – Source → Kafka → Destination સરખામણી

> એક document પર કરેલા **3 operations** (insert → replace → delete) source DB, Kafka topic અને destination (bronze) માં કેવા દેખાય છે, એની સરખામણી. જ્યાં mismatch છે ત્યાં એનું કારણ પણ લખ્યું છે.
> આખો setup: [MONGO_CDC_COMPLETE_SETUP_GU.md](MONGO_CDC_COMPLETE_SETUP_GU.md)
>
> બધો data **2026-10-28** ના ખરા test નો છે. ✔️ ચિહ્ન વાળી માહિતી machine પર ચકાસેલી છે.

---

## અનુક્રમણિકા

- [1. શું કર્યું (Timeline)](#1-શું-કર્યું-timeline)
- [2. Source: insert કરેલો document](#2-source-insert-કરેલો-document)
- [3. Kafka topic ના 3 messages](#3-kafka-topic-ના-3-messages)
- [4. Destination (bronze) માં મળેલો document](#4-destination-bronze-માં-મળેલો-document)
- [5. Field-by-field સરખામણી](#5-field-by-field-સરખામણી)
- [6. Mismatches અને કારણો](#6-mismatches-અને-કારણો)
- [7. નવા subscriber સાથે destination કેવું દેખાવું જોઈએ](#7-નવા-subscriber-સાથે-destination-કેવું-દેખાવું-જોઈએ)
- [8. સુધારવા શું કરવું](#8-સુધારવા-શું-કરવું)
- [9. સારાંશ](#9-સારાંશ)

---

## 1. શું કર્યું (Timeline)

| # | સમય (IST) | Source માં operation | Kafka `operationType` | Partition / Offset |
|---|---|---|---|---|
| 1 | 2026-10-28 10:34:20 | Document insert (`status: "PLACED"`) | `insert` | 2 / – |
| 2 | 2026-10-28 10:36:21 | Compass માં document edit કરીને save (`status: "Delevered"`) | `replace` | 2 / – |
| 3 | 2026-10-28 10:36:50 | Document delete | `delete` | 2 / – |

- Database: `de_source_mongo`, collection: **`ordes`**
- Document `_id`: `ObjectId("6ae18254641f36c74b6506ec")`
- ત્રણેય messages **એક જ partition (2)** માં છે, કારણ કે ત્રણેયનો key એ જ `_id` છે. એટલે એમનો ક્રમ સચવાયો છે ✅
- સમય messages ના `wallTime` પરથી લીધો છે.

> **Compass માં edit = `replace`, `update` નહીં.** Compass document ને આખો બદલીને save કરે છે, એટલે MongoDB એને `replace` event તરીકે નોંધે છે. `updateOne({...}, {$set: ...})` થી ફેરફાર કરો તો `update` event આવે.

---

## 2. Source: insert કરેલો document

**તમે લખેલો document** (`_id` આપ્યો નથી, એટલે MongoDB એ જાતે `ObjectId` બનાવ્યો):
```json
{
  "orderId": 1001,
  "status": "PLACED",
  "amount": 2499.50,
  "buyer": {
    "name": "Aarav Patel",
    "email": "aarav@example.com",
    "address": {
      "city": "Ahmedabad",
      "state": "Gujarat",
      "pin": "380015"
    }
  }
}
```

**Source માં save થયા પછી (BSON types સાથે):**

| Field | Value | BSON type |
|---|---|---|
| `_id` | `6ae18254641f36c74b6506ec` | ObjectId (MongoDB એ બનાવ્યો) |
| `orderId` | `1001` | Int32 |
| `status` | `"PLACED"` | String |
| `amount` | `2499.5` | Double |
| `buyer` | nested object | Object |
| `buyer.address.pin` | `"380015"` | String (quotes માં લખ્યું હતું) |

**Replace પછી:** ફક્ત `status` બદલાયું: `"PLACED"` → `"Delevered"`.
**Delete પછી:** Source માં document **નથી** (✔️ `de_source_mongo.ordes` count = 0).

---

## 3. Kafka topic ના 3 messages

Topic: `de_source_mongo` · Key (ત્રણેય માટે): `{"_id": {"$oid": "6ae18254641f36c74b6506ec"}}`

### 3.1 Message 1: `insert`
```json
{
  "_id": { "_data": "826AE18254000000012B042C0100296E5A10047D9FA13B6F7D4628B78ACB682CFA7946463C6F7065726174696F6E54797065003C696E736572740046646F63756D656E744B65790046645F696400646AE18254641F36C74B6506EC000004" },
  "operationType": "insert",
  "clusterTime": { "$timestamp": { "t": 1793163860, "i": 1 } },
  "wallTime": { "$date": { "$numberLong": "1793163860102" } },
  "fullDocument": {
    "_id": { "$oid": "6ae18254641f36c74b6506ec" },
    "orderId": { "$numberInt": "1001" },
    "status": "PLACED",
    "amount": { "$numberDouble": "2499.5" },
    "buyer": {
      "name": "Aarav Patel",
      "email": "aarav@example.com",
      "address": { "city": "Ahmedabad", "state": "Gujarat", "pin": "380015" }
    }
  },
  "ns": { "db": "de_source_mongo", "coll": "ordes" },
  "documentKey": { "_id": { "$oid": "6ae18254641f36c74b6506ec" } }
}
```

### 3.2 Message 2: `replace`
```json
{
  "_id": { "_data": "826AE182CD000000032B042C0100296E5A10047D9FA13B6F7D4628B78ACB682CFA7946463C6F7065726174696F6E54797065003C7265706C6163650046646F63756D656E744B65790046645F696400646AE18254641F36C74B6506EC000004" },
  "operationType": "replace",
  "clusterTime": { "$timestamp": { "t": 1793163981, "i": 3 } },
  "wallTime": { "$date": { "$numberLong": "1793163981504" } },
  "fullDocument": {
    "_id": { "$oid": "6ae18254641f36c74b6506ec" },
    "orderId": { "$numberInt": "1001" },
    "status": "Delevered",
    "amount": { "$numberDouble": "2499.5" },
    "buyer": {
      "name": "Aarav Patel",
      "email": "aarav@example.com",
      "address": { "city": "Ahmedabad", "state": "Gujarat", "pin": "380015" }
    }
  },
  "ns": { "db": "de_source_mongo", "coll": "ordes" },
  "documentKey": { "_id": { "$oid": "6ae18254641f36c74b6506ec" } }
}
```
`replace` માં `updateDescription` **નથી** હોતું (એ ફક્ત `update` માં આવે છે). આખો નવો document `fullDocument` માં હોય છે.

### 3.3 Message 3: `delete`
```json
{
  "_id": { "_data": "826AE182EA000000012B042C0100296E5A10047D9FA13B6F7D4628B78ACB682CFA7946463C6F7065726174696F6E54797065003C64656C6574650046646F63756D656E744B65790046645F696400646AE18254641F36C74B6506EC000004" },
  "operationType": "delete",
  "clusterTime": { "$timestamp": { "t": 1793164010, "i": 1 } },
  "wallTime": { "$date": { "$numberLong": "1793164010166" } },
  "ns": { "db": "de_source_mongo", "coll": "ordes" },
  "documentKey": { "_id": { "$oid": "6ae18254641f36c74b6506ec" } }
}
```
`fullDocument` **નથી**, કારણ કે document હવે રહ્યો જ નથી. ફક્ત `documentKey._id` થી ખબર પડે છે કે કયો document ગયો.

### 3.4 ત્રણેય messages એક નજરમાં

| Property | insert | replace | delete |
|---|---|---|---|
| `operationType` | `insert` | `replace` | `delete` |
| `fullDocument` | ✅ (`status: PLACED`) | ✅ (`status: Delevered`) | ❌ નથી |
| `updateDescription` | ❌ | ❌ | ❌ |
| `documentKey._id` | ✅ | ✅ | ✅ |
| `clusterTime.i` | 1 | **3** (એ second નું 3જું operation) | 1 |
| Partition | 2 | 2 | 2 |

✅ **Kafka messages માં કોઈ mismatch નથી.** ત્રણેય operations પૂરી માહિતી અને સાચા types સાથે આવ્યા છે.

---

## 4. Destination (bronze) માં મળેલો document

**તમે જોયેલો document** (`de_bronze.ordes`):
```json
{
  "_id": { "$oid": "6ae18254641f36c74b6506ec" },
  "orderId": 1001,
  "status": "Delevered",
  "amount": 2499.5,
  "buyer": {
    "name": "Aarav Patel",
    "email": "aarav@example.com",
    "address": { "city": "Ahmedabad", "state": "Gujarat", "pin": "380015" }
  }
}
```

**અત્યારની ખરી સ્થિતિ** (✔️ ચકાસેલું):

| ચકાસણી | પરિણામ |
|---|---|
| `de_source_mongo.ordes` documents | **0** |
| `de_bronze.ordes` documents | **0** (document **નીકળી ગયો** છે) |
| Subscriber consumer group `LAG` (ત્રણેય partitions) | **0** (ત્રણેય messages process થઈ ગયા છે) |
| ચાલતો subscriber (PID 9892) | **2026-10-28 00:41** એ start થયેલો, અને jar પણ **00:41** એ build થયેલું |

---

## 5. Field-by-field સરખામણી

Replace પછીની સ્થિતિ: Source (replace પછી) vs Kafka message 2 vs તમે જોયેલો destination document.

| Field | Source (BSON) | Kafka (Extended JSON) | Destination (જોયેલું) | Match? |
|---|---|---|---|---|
| `_id` | ObjectId `6ae18254...` | `{"$oid": "6ae18254..."}` | ObjectId `6ae18254...` | ✅ |
| `orderId` | Int32 `1001` | `{"$numberInt": "1001"}` | `1001` (Int32) | ✅ |
| `status` | `"Delevered"` | `"Delevered"` | `"Delevered"` | ✅ |
| `amount` | Double `2499.5` | `{"$numberDouble": "2499.5"}` | `2499.5` (Double) | ✅ (નીચેની નોંધ જુઓ) |
| `buyer.name` | `"Aarav Patel"` | `"Aarav Patel"` | `"Aarav Patel"` | ✅ |
| `buyer.email` | `"aarav@example.com"` | એ જ | એ જ | ✅ |
| `buyer.address.city/state/pin` | `Ahmedabad / Gujarat / "380015"` | એ જ | એ જ | ✅ |
| Fields નો ક્રમ | `_id, orderId, status, amount, buyer` | એ જ | એ જ | ✅ |
| `_op` | – | (`operationType` = `replace`) | ❌ **નથી** | ⚠️ Mismatch 1 |
| `_deleted` | – | – | ❌ **નથી** | ⚠️ Mismatch 1 |
| `_source_ts` | – | (`wallTime` = `1793163981504`) | ❌ **નથી** | ⚠️ Mismatch 1 |
| `_ingested_at` | – | – | ❌ **નથી** | ⚠️ Mismatch 1 |
| Delete પછી document | ❌ નથી | `delete` event ✅ | તમે જોયો ત્યારે ✅ હતો, હવે ❌ નથી | ⚠️ Mismatch 2 |

> **`2499.50` vs `2499.5` mismatch નથી.** Double type માં છેલ્લો `0` સચવાતો નથી. Value અને type (Double) બંને એ જ છે. Decimal ના ચોક્કસ digits જોઈતા હોય તો source માં `NumberDecimal("2499.50")` (Decimal128) વાપરો.

---

## 6. Mismatches અને કારણો

### ⚠️ Mismatch 1: Metadata fields (`_op`, `_deleted`, `_source_ts`, `_ingested_at`) નથી

**કારણ: ચાલતો subscriber જૂના code નો છે.**

| | સમય (2026-10-28) |
|---|---|
| ચાલતા subscriber નું jar build થયું | **00:41** |
| Subscriber start થયો (PID 9892) | **00:41** |
| Metadata + soft delete વાળો નવો code લખાયો | 00:41 **પછી** |

એટલે ચાલતો subscriber હજુ **જૂનું behavior** વાપરે છે:
- Document **metadata વગર** લખે છે
- Delete વખતે document **કાઢી નાખે છે** (hard delete)

Project ની files માં નવો code છે, પણ **ફરી build કરીને restart કર્યા વગર** એ લાગુ થતો નથી.

**અસર:** Spark (Bronze → Silver) `_ingested_at` પરથી નવો data શોધે છે. આ field ન હોય તો Spark આ documents **ક્યારેય વાંચશે નહીં**.

### ⚠️ Mismatch 2: Delete પછી પણ destination માં document દેખાયો

**કારણ: તમે destination **delete થયા પહેલાં** જોયો હતો, અથવા Compass refresh નહોતું થયું.**

- તમે જોયેલા document માં `status: "Delevered"` છે, એટલે કે એ **replace પછીની અને delete પહેલાંની** સ્થિતિ છે.
- અત્યારે `de_bronze.ordes` માં **0 documents** છે, અને subscriber ની `LAG = 0` છે. એટલે delete event પણ લાગુ થઈ ગયો છે ✔️
- જૂનો subscriber hard delete કરે છે, એટલે document સંપૂર્ણ **નીકળી ગયો**.

➡️ Pipeline એ delete **સાચી રીતે** લાગુ કર્યું છે. આ ફક્ત જોવાના સમયનો ફરક છે. Compass માં **Refresh** (🔄) દબાવીને ફરી જુઓ.

### ℹ️ Data quality નોંધ (pipeline ની ભૂલ નથી)

| શું | ક્યાં | નોંધ |
|---|---|---|
| Collection નું નામ **`ordes`** | Source | કદાચ `orders` લખવું હતું. Pipeline નામ **જેમ છે તેમ** આગળ લઈ જાય છે, એટલે bronze માં પણ `ordes` બને છે |
| `status: "Delevered"` | Source (Compass edit) | કદાચ `Delivered` લખવું હતું. Bronze source ની ચોક્કસ copy છે, એટલે spelling પણ એ જ રહે છે. આવી values silver layer ની validation માં પકડવી |

---

## 7. નવા subscriber સાથે destination કેવું દેખાવું જોઈએ

નવો subscriber (metadata + soft delete) ચલાવો, તો દરેક step પછી `de_bronze.ordes` આવો દેખાશે. `_source_ts` = દરેક message નો `wallTime`. `_ingested_at` = subscriber એ લખ્યો તે સમય, જે તમારા run માં અલગ હશે.

**Insert પછી:**
```json
{
  "_id": { "$oid": "6ae18254641f36c74b6506ec" },
  "orderId": 1001,
  "status": "PLACED",
  "amount": 2499.5,
  "buyer": { "name": "Aarav Patel", "email": "aarav@example.com",
             "address": { "city": "Ahmedabad", "state": "Gujarat", "pin": "380015" } },
  "_op": "insert",
  "_deleted": false,
  "_source_ts": 1793163860102,
  "_ingested_at": 1793163860120
}
```

**Replace પછી:**
```json
{
  "_id": { "$oid": "6ae18254641f36c74b6506ec" },
  "orderId": 1001,
  "status": "Delevered",
  "amount": 2499.5,
  "buyer": { "name": "Aarav Patel", "email": "aarav@example.com",
             "address": { "city": "Ahmedabad", "state": "Gujarat", "pin": "380015" } },
  "_op": "replace",
  "_deleted": false,
  "_source_ts": 1793163981504,
  "_ingested_at": 1793163981520
}
```

**Delete પછી (soft delete: document રહે છે):**
```json
{
  "_id": { "$oid": "6ae18254641f36c74b6506ec" },
  "orderId": 1001,
  "status": "Delevered",
  "amount": 2499.5,
  "buyer": { "name": "Aarav Patel", "email": "aarav@example.com",
             "address": { "city": "Ahmedabad", "state": "Gujarat", "pin": "380015" } },
  "_op": "delete",
  "_deleted": true,
  "_source_ts": 1793164010166,
  "_ingested_at": 1793164010180
}
```

| Step | જૂનો subscriber (હાલ ચાલે છે) | નવો subscriber |
|---|---|---|
| Insert | Document, metadata વગર | Document + `_op: insert`, `_deleted: false` |
| Replace | Document replace, metadata વગર | Document replace + `_op: replace` |
| Delete | Document **નીકળી જાય** | Document **રહે**, `_op: delete`, `_deleted: true` |
| Spark ને દેખાય? | ❌ (`_ingested_at` નથી) | ✅ |

> `_ingested_at` ની ઉપરની values ઉદાહરણ છે. બાકીની બધી values (`_id`, fields, `_source_ts`) તમારા ખરા Kafka messages પરથી છે.

---

## 8. સુધારવા શું કરવું

1. **Subscriber બંધ કરો:** subscriber વાળા terminal માં `Ctrl + C`.
2. **નવો code build કરો:**
   ```powershell
   cd D:\ClaudeProjects\dataEngineeringProjects\sparkProject\mongo-cdc-subscriber
   mvn -q clean package
   ```
3. **જૂના (metadata વગરના) bronze collections drop કરો** (`mongosh`). ફક્ત source ના નામવાળા collections; `mysql_*` ને અડવું નહીં:
   ```javascript
   const s = db.getSiblingDB("de_source_mongo"), b = db.getSiblingDB("de_bronze");
   s.getCollectionNames().forEach(c => { b[c].drop(); print("dropped de_bronze." + c); });
   b.ordes.drop();   // ordes source માં ખાલી થઈ ગયું હોય તો list માં ન પણ આવે
   ```
4. **Consumer group શરૂઆત પર લઈ જાઓ:**
   ```powershell
   cd C:\kafka
   .\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --group mongo-cdc-subscriber --topic de_source_mongo --reset-offsets --to-earliest --execute
   ```
5. **Subscriber ફરી start કરો:**
   ```powershell
   cd D:\ClaudeProjects\dataEngineeringProjects\sparkProject\mongo-cdc-subscriber
   java -jar target\mongo-cdc-subscriber-1.0.0-jar-with-dependencies.jar
   ```
6. **ચકાસો:** Topic ના ત્રણેય `ordes` messages ફરી લાગુ થશે, અને છેલ્લે document **soft deleted** સ્થિતિમાં હશે (વિભાગ 7 નું છેલ્લું JSON):
   ```javascript
   db.getSiblingDB("de_bronze").ordes.find({}, { status: 1, _op: 1, _deleted: 1, _source_ts: 1 })
   ```
   ➡️ `status: "Delevered"`, `_op: "delete"`, `_deleted: true`, `_source_ts: 1793164010166`

---

## 9. સારાંશ

| Layer | પરિણામ |
|---|---|
| Source → Kafka | ✅ ત્રણેય operations (insert, replace, delete) સાચા ક્રમમાં, એક જ partition માં, સાચા types સાથે |
| Kafka → Destination (data) | ✅ `_id`, `orderId` (Int32), `amount` (Double), nested `buyer`, fields નો ક્રમ: બધું source જેવું જ |
| Kafka → Destination (delete) | ✅ લાગુ થયું (હાલ bronze માં 0 documents). તમે delete પહેલાનો state જોયો હતો |
| Metadata fields | ⚠️ **નથી**, કારણ કે ચાલતો subscriber જૂના code નો છે. વિભાગ 8 પ્રમાણે rebuild + replay કરો |
| Data quality | ℹ️ `ordes` અને `Delevered` ની spelling source માંથી જ આવી છે |
