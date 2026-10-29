# CDC Example: `orders` collection – Source → Kafka → Destination સરખામણી

> એક document પર કરેલા **4 operations** (insert → update → replace → delete) source DB, Kafka topic અને destination (bronze) માં કેવા દેખાય છે, એની સરખામણી. જ્યાં mismatch છે ત્યાં એનું કારણ પણ લખ્યું છે.
> આખો setup: [MONGO_CDC_COMPLETE_SETUP_GU.md](MONGO_CDC_COMPLETE_SETUP_GU.md)
>
> બધો data **2026-10-28** ના ખરા test નો છે. ✔️ ચિહ્ન વાળી માહિતી machine પર ચકાસેલી છે.

---

## અનુક્રમણિકા

- [1. શું કર્યું (Timeline)](#1-શું-કર્યું-timeline)
- [2. Step 1: Insert](#2-step-1-insert)
- [3. Step 2: Update](#3-step-2-update)
- [4. Step 3: Replace](#4-step-3-replace)
- [5. Step 4: Delete](#5-step-4-delete)
- [6. ચારેય Kafka messages એક નજરમાં](#6-ચારેય-kafka-messages-એક-નજરમાં)
- [7. Mismatches અને કારણો](#7-mismatches-અને-કારણો)
- [8. નવા subscriber સાથે destination કેવું દેખાવું જોઈએ](#8-નવા-subscriber-સાથે-destination-કેવું-દેખાવું-જોઈએ)
- [9. સુધારવા શું કરવું](#9-સુધારવા-શું-કરવું)
- [10. સારાંશ](#10-સારાંશ)

---

## 1. શું કર્યું (Timeline)

| # | સમય (IST) | Source માં operation | Kafka `operationType` | Partition / Offset ✔️ |
|---|---|---|---|---|
| 1 | 2026-10-28 23:45:39 | Document insert (`status: "PLACED"`) | `insert` | 1 / 3 |
| 2 | 2026-10-28 23:48:27 | `updateOne`: `status` → `SHIPPED`, `buyer.address.city` → `Surat` | `update` | 1 / 4 |
| 3 | 2026-10-28 23:52:00 | Document replace (`status: "DELIVERED"`, `amount: 2299`, નવો email) | `replace` | 1 / 5 |
| 4 | 2026-10-28 23:53:36 | Document delete | `delete` | 1 / 6 |

- Database: `de_source_mongo`, collection: **`orders`**
- Document `_id`: `ObjectId("6ae23bcbbde98f5b55239830")`
- ચારેય messages **એક જ partition (1)** માં, offsets **3 → 4 → 5 → 6** ના ક્રમમાં છે, કારણ કે ચારેયનો key એ જ `_id` છે ✅
- સમય દરેક message ના `wallTime` પરથી લીધો છે.

---

## 2. Step 1: Insert

### Source (insert પછી)
```json
{
  "_id": { "$oid": "6ae23bcbbde98f5b55239830" },
  "orderId": 1001,
  "status": "PLACED",
  "amount": 2499.5,
  "buyer": {
    "name": "Aarav Patel",
    "email": "aarav@example.com",
    "address": { "city": "Ahmedabad", "state": "Gujarat", "pin": "380015" }
  }
}
```

### Kafka message (partition 1, offset 3)
Key: `{"_id": {"$oid": "6ae23bcbbde98f5b55239830"}}`
```json
{
  "_id": { "_data": "826AE23BCB000000012B042C0100296E5A1004F59EE5B698D64D26835270C3D1D93D94463C6F7065726174696F6E54797065003C696E736572740046646F63756D656E744B65790046645F696400646AE23BCBBDE98F5B55239830000004" },
  "operationType": "insert",
  "clusterTime": { "$timestamp": { "t": 1793211339, "i": 1 } },
  "wallTime": { "$date": { "$numberLong": "1793211339483" } },
  "fullDocument": {
    "_id": { "$oid": "6ae23bcbbde98f5b55239830" },
    "orderId": { "$numberInt": "1001" },
    "status": "PLACED",
    "amount": { "$numberDouble": "2499.5" },
    "buyer": {
      "name": "Aarav Patel",
      "email": "aarav@example.com",
      "address": { "city": "Ahmedabad", "state": "Gujarat", "pin": "380015" }
    }
  },
  "ns": { "db": "de_source_mongo", "coll": "orders" },
  "documentKey": { "_id": { "$oid": "6ae23bcbbde98f5b55239830" } }
}
```

### Destination (તમે જોયેલું)
```json
{
  "_id": { "$oid": "6ae23bcbbde98f5b55239830" },
  "orderId": 1001,
  "status": "PLACED",
  "amount": 2499.5,
  "buyer": {
    "name": "Aarav Patel",
    "email": "aarav@example.com",
    "address": { "city": "Ahmedabad", "state": "Gujarat", "pin": "380015" }
  }
}
```

### સરખામણી
| Field | Source | Kafka | Destination | Match? |
|---|---|---|---|---|
| `_id` | ObjectId | `{"$oid": ...}` | ObjectId | ✅ |
| `orderId` | Int32 `1001` | `{"$numberInt": "1001"}` | Int32 `1001` | ✅ |
| `status` | `PLACED` | `PLACED` | `PLACED` | ✅ |
| `amount` | Double `2499.5` | `{"$numberDouble": "2499.5"}` | Double `2499.5` | ✅ |
| `buyer` (nested) | Ahmedabad, `aarav@example.com` | એ જ | એ જ | ✅ |
| Fields નો ક્રમ | `_id, orderId, status, amount, buyer` | એ જ | એ જ | ✅ |
| Metadata (`_op`, `_deleted`, `_source_ts`, `_ingested_at`) | – | – | ❌ **નથી** | ⚠️ [Mismatch 1](#mismatch-1-metadata-fields-નથી) |

---

## 3. Step 2: Update

### Source માં ચલાવેલી query
```javascript
db.orders.updateOne(
  { orderId: 1001 },
  { $set: { status: "SHIPPED", "buyer.address.city": "Surat" } }
)
```

### Source (update પછી)
```json
{
  "_id": { "$oid": "6ae23bcbbde98f5b55239830" },
  "orderId": 1001,
  "status": "SHIPPED",
  "amount": 2499.5,
  "buyer": {
    "name": "Aarav Patel",
    "email": "aarav@example.com",
    "address": { "city": "Surat", "state": "Gujarat", "pin": "380015" }
  }
}
```

### Kafka message (partition 1, offset 4)
```json
{
  "_id": { "_data": "826AE23C73000000012B042C0100296E5A1004F59EE5B698D64D26835270C3D1D93D94463C6F7065726174696F6E54797065003C7570646174650046646F63756D656E744B65790046645F696400646AE23BCBBDE98F5B55239830000004" },
  "operationType": "update",
  "clusterTime": { "$timestamp": { "t": 1793211507, "i": 1 } },
  "wallTime": { "$date": { "$numberLong": "1793211507427" } },
  "ns": { "db": "de_source_mongo", "coll": "orders" },
  "documentKey": { "_id": { "$oid": "6ae23bcbbde98f5b55239830" } },
  "updateDescription": {
    "updatedFields": { "status": "SHIPPED", "buyer.address.city": "Surat" },
    "removedFields": [],
    "truncatedArrays": []
  },
  "fullDocument": {
    "_id": { "$oid": "6ae23bcbbde98f5b55239830" },
    "orderId": { "$numberInt": "1001" },
    "status": "SHIPPED",
    "amount": { "$numberDouble": "2499.5" },
    "buyer": {
      "name": "Aarav Patel",
      "email": "aarav@example.com",
      "address": { "city": "Surat", "state": "Gujarat", "pin": "380015" }
    }
  }
}
```

**આ message માં નવું શું છે:**

| Property | Value | અર્થ |
|---|---|---|
| `updateDescription.updatedFields` | `{"status": "SHIPPED", "buyer.address.city": "Surat"}` | **ફક્ત બદલાયેલા fields**. Nested field નું નામ **dotted path** (`buyer.address.city`) તરીકે આવે છે |
| `updateDescription.removedFields` | `[]` | `$unset` નથી કર્યું |
| `fullDocument` | આખો નવો document | `change.stream.full.document=updateLookup` ને કારણે મળ્યો |

> Insert/replace માં `fullDocument` `ns` ની પહેલાં આવે છે, જ્યારે update માં **છેલ્લે** (`updateDescription` પછી). આ MongoDB નો ક્રમ છે. Subscriber fields નામથી વાંચે છે, એટલે ક્રમથી કોઈ ફરક પડતો નથી.

### Destination (તમે જોયેલું)
```json
{
  "_id": { "$oid": "6ae23bcbbde98f5b55239830" },
  "orderId": 1001,
  "status": "PLACED",
  "amount": 2499.5,
  "buyer": {
    "name": "Aarav Patel",
    "email": "aarav@example.com",
    "address": { "city": "Ahmedabad", "state": "Gujarat", "pin": "380015" }
  }
}
```

### સરખામણી
| Field | Source | Kafka (`fullDocument`) | Destination (જોયેલું) | Match? |
|---|---|---|---|---|
| `status` | `SHIPPED` | `SHIPPED` | **`PLACED`** | ❌ [Mismatch 2](#mismatch-2-update-પછી-destination-માં-જૂની-values-દેખાઈ) |
| `buyer.address.city` | `Surat` | `Surat` | **`Ahmedabad`** | ❌ [Mismatch 2](#mismatch-2-update-પછી-destination-માં-જૂની-values-દેખાઈ) |
| બાકીના fields | | એ જ | એ જ | ✅ |
| Metadata | – | – | ❌ નથી | ⚠️ [Mismatch 1](#mismatch-1-metadata-fields-નથી) |

✅ **Source → Kafka સાચું છે.** Mismatch ફક્ત તમે જોયેલા destination માં છે (કારણ વિભાગ 7 માં).

---

## 4. Step 3: Replace

### Source (replace પછી)
```json
{
  "_id": { "$oid": "6ae23bcbbde98f5b55239830" },
  "orderId": 1001,
  "status": "DELIVERED",
  "amount": 2299,
  "buyer": {
    "name": "Aarav Patel",
    "email": "aarav.patel@example.com",
    "address": { "city": "Ahmedabad", "state": "Gujarat", "pin": "380015" }
  }
}
```

### Kafka message (partition 1, offset 5)
```json
{
  "_id": { "_data": "826AE23D48000000012B042C0100296E5A1004F59EE5B698D64D26835270C3D1D93D94463C6F7065726174696F6E54797065003C7265706C6163650046646F63756D656E744B65790046645F696400646AE23BCBBDE98F5B55239830000004" },
  "operationType": "replace",
  "clusterTime": { "$timestamp": { "t": 1793211720, "i": 1 } },
  "wallTime": { "$date": { "$numberLong": "1793211720863" } },
  "fullDocument": {
    "_id": { "$oid": "6ae23bcbbde98f5b55239830" },
    "orderId": { "$numberInt": "1001" },
    "status": "DELIVERED",
    "amount": { "$numberInt": "2299" },
    "buyer": {
      "name": "Aarav Patel",
      "email": "aarav.patel@example.com",
      "address": { "city": "Ahmedabad", "state": "Gujarat", "pin": "380015" }
    }
  },
  "ns": { "db": "de_source_mongo", "coll": "orders" },
  "documentKey": { "_id": { "$oid": "6ae23bcbbde98f5b55239830" } }
}
```
`replace` માં `updateDescription` **નથી**. આખો નવો document `fullDocument` માં છે.

### Destination (તમે જોયેલું)
```json
{
  "_id": { "$oid": "6ae23bcbbde98f5b55239830" },
  "orderId": 1001,
  "status": "DELIVERED",
  "amount": 2299,
  "buyer": {
    "name": "Aarav Patel",
    "email": "aarav.patel@example.com",
    "address": { "city": "Ahmedabad", "state": "Gujarat", "pin": "380015" }
  }
}
```

### સરખામણી
| Field | Source | Kafka | Destination | Match? |
|---|---|---|---|---|
| `status` | `DELIVERED` | `DELIVERED` | `DELIVERED` | ✅ |
| `amount` | **Int32** `2299` | `{"$numberInt": "2299"}` | **Int32** `2299` | ✅ (પણ type બદલાયો, [Note 3](#note-3-amount-નો-type-double--int32-બદલાયો)) |
| `buyer.email` | `aarav.patel@example.com` | એ જ | એ જ | ✅ |
| `buyer.address.city` | `Ahmedabad` | `Ahmedabad` | `Ahmedabad` | ✅ |
| Metadata | – | – | ❌ નથી | ⚠️ [Mismatch 1](#mismatch-1-metadata-fields-નથી) |

> Replace પછી `city` ફરી `Ahmedabad` છે, કારણ કે replace માં તમે આખો નવો document આપ્યો હતો, અને એમાં city `Ahmedabad` હતું. Update વાળો `Surat` આ replace થી બદલાઈ ગયો.

---

## 5. Step 4: Delete

### Kafka message (partition 1, offset 6)
```json
{
  "_id": { "_data": "826AE23DA8000000012B042C0100296E5A1004F59EE5B698D64D26835270C3D1D93D94463C6F7065726174696F6E54797065003C64656C6574650046646F63756D656E744B65790046645F696400646AE23BCBBDE98F5B55239830000004" },
  "operationType": "delete",
  "clusterTime": { "$timestamp": { "t": 1793211816, "i": 1 } },
  "wallTime": { "$date": { "$numberLong": "1793211816458" } },
  "ns": { "db": "de_source_mongo", "coll": "orders" },
  "documentKey": { "_id": { "$oid": "6ae23bcbbde98f5b55239830" } }
}
```
`fullDocument` **નથી**. ફક્ત `documentKey._id` થી ખબર પડે છે કે કયો document ગયો.

### Source અને Destination (✔️ ચકાસેલું)
| | Documents |
|---|---|
| `de_source_mongo.orders` | **0** |
| `de_bronze.orders` | **0**: document **નીકળી ગયો** (hard delete) |

---

## 6. ચારેય Kafka messages એક નજરમાં

| Property | insert | update | replace | delete |
|---|---|---|---|---|
| Offset (partition 1) | 3 | 4 | 5 | 6 |
| `fullDocument` | ✅ PLACED | ✅ SHIPPED, Surat | ✅ DELIVERED, 2299 | ❌ |
| `updateDescription` | ❌ | ✅ `status`, `buyer.address.city` | ❌ | ❌ |
| `documentKey._id` | ✅ | ✅ | ✅ | ✅ |
| `amount` નો type | Double | Double | **Int32** | – |

✅ **Kafka messages માં કોઈ mismatch નથી.** ચારેય operations સાચા ક્રમમાં, સાચા types સાથે, પૂરી માહિતી સાથે આવ્યા છે.

---

## 7. Mismatches અને કારણો

### Mismatch 1: Metadata fields નથી

`_op`, `_deleted`, `_source_ts` અને `_ingested_at` એક પણ step માં destination માં નથી, અને delete પછી document **નીકળી ગયો** (soft delete નહીં).

**કારણ: ચાલતો subscriber જૂના code નો છે.** ✔️

| | સમય |
|---|---|
| ચાલતા subscriber નું jar build | 2026-10-28 **00:41** |
| Subscriber start (PID 9892) | 2026-10-28 **00:41**, ત્યારથી સતત ચાલુ |
| Metadata + soft delete વાળો નવો code | 00:41 **પછી** લખાયો |

Project માં નવો code છે, પણ **jar ફરી build કરીને subscriber restart** કર્યા વગર એ લાગુ થતો નથી.

**અસર:** Spark (Bronze → Silver) `_ingested_at` પરથી નવો data શોધે છે, એટલે આ documents Spark ને **દેખાશે નહીં**. Delete પણ silver સુધી નહીં પહોંચે, કારણ કે document જ નીકળી ગયો છે.

### Mismatch 2: Update પછી destination માં જૂની values દેખાઈ

તમે જોયેલા destination માં `status: "PLACED"` અને `city: "Ahmedabad"` હતા, જ્યારે source અને Kafka માં `SHIPPED` અને `Surat` હતા.

**કારણ: મોટે ભાગે destination update લાગુ થયા પહેલાં જોવાયું, અથવા Compass refresh નહોતું થયું. Update ખરેખર લાગુ થયું હતું.** પુરાવા ✔️:

| પુરાવો | પરિણામ | અર્થ |
|---|---|---|
| Consumer group, partition 1 | `CURRENT-OFFSET = 7`, `LAG = 0` | Offset 4 (update) સહિત બધા messages process થયા |
| DLQ (`de_source_mongo.dlq`) | હજુ **3** messages (જૂના test ના) | Update message **DLQ માં નથી ગયો**, એટલે parse સફળ થયો |
| Subscriber process | PID 9892, 00:41 થી સતત ચાલુ | MongoDB write fail થાય તો subscriber **બંધ થઈ જાય** છે. એ બંધ નથી થયો, એટલે write સફળ થયો |
| પછીના messages | Replace અને delete બંને destination માં દેખાયા | Subscriber partition ના ક્રમમાં લાગુ કરે છે. Offset 4 છોડીને 5 અને 6 લાગુ થાય તે શક્ય નથી |

Update message માં `fullDocument` છે, એટલે subscriber **આખો document replace** કરે છે, અને એનાથી `SHIPPED`/`Surat` જ આવે.

➡️ Compass માં destination જોતી વખતે **Refresh (🔄)** દબાવો. Subscriber log માં `orders: 1 writes (matched=1, ...)` જેવી line પણ જોઈ શકો છો.

> Document હવે delete થઈ ગયો છે, એટલે update પછીની bronze સ્થિતિ હવે ફરી જોઈ શકાતી નથી. આ નિષ્કર્ષ ઉપરના પુરાવા પરથી છે.

### Note 3: `amount` નો type Double → Int32 બદલાયો

| Step | `amount` | Type |
|---|---|---|
| Insert, update | `2499.5` | **Double** |
| Replace | `2299` | **Int32** |

**કારણ: source માં જ type બદલાયો.** Replace વખતે `2299` (દશાંશ વગર) લખ્યું, એટલે MongoDB એ Int32 તરીકે save કર્યું. Kafka (`{"$numberInt": "2299"}`) અને destination એ ચોક્કસ copy કરી. **Pipeline ની ભૂલ નથી.**

**જોખમ:** એક જ field ના documents માં કોઈ જગ્યાએ Double અને કોઈ જગ્યાએ Int32 હોય, તો Spark/silver માં schema અને સરખામણીમાં તકલીફ થઈ શકે.

**ઉકેલ (source બાજુ):** Amount હંમેશા એક જ type માં લખો:
```javascript
// Double
db.orders.replaceOne({ orderId: 1001 }, { ..., amount: Double(2299) })     // mongosh 2.x
// અથવા પૈસા માટે વધુ સારું: Decimal128 (ચોક્કસ digits)
db.orders.replaceOne({ orderId: 1001 }, { ..., amount: NumberDecimal("2299.00") })
```
Silver layer માં `amount` ને એક જ type (જેમ કે `DECIMAL(12,2)`) માં cast કરવું પણ સારી પ્રથા છે.

---

## 8. નવા subscriber સાથે destination કેવું દેખાવું જોઈએ

નવો subscriber (metadata + soft delete) ચલાવો, તો દરેક step પછી `de_bronze.orders` આવો દેખાશે. `_source_ts` = દરેક message નો `wallTime` (ખરી values). `_ingested_at` = subscriber એ લખ્યો તે સમય (ઉદાહરણ value, તમારા run માં અલગ).

| Step | `status` | `amount` | `city` | `email` | `_op` | `_deleted` | `_source_ts` |
|---|---|---|---|---|---|---|---|
| Insert | `PLACED` | 2499.5 (Double) | Ahmedabad | aarav@example.com | `insert` | `false` | `1793211339483` |
| Update | `SHIPPED` | 2499.5 (Double) | **Surat** | aarav@example.com | `update` | `false` | `1793211507427` |
| Replace | `DELIVERED` | 2299 (Int32) | Ahmedabad | aarav.patel@example.com | `replace` | `false` | `1793211720863` |
| Delete | `DELIVERED` | 2299 (Int32) | Ahmedabad | aarav.patel@example.com | `delete` | **`true`** | `1793211816458` |

**Delete પછીનો આખો document (soft delete: document રહે છે):**
```json
{
  "_id": { "$oid": "6ae23bcbbde98f5b55239830" },
  "orderId": 1001,
  "status": "DELIVERED",
  "amount": 2299,
  "buyer": {
    "name": "Aarav Patel",
    "email": "aarav.patel@example.com",
    "address": { "city": "Ahmedabad", "state": "Gujarat", "pin": "380015" }
  },
  "_op": "delete",
  "_deleted": true,
  "_source_ts": 1793211816458,
  "_ingested_at": 1793211816470
}
```

| | જૂનો subscriber (હાલ ચાલે છે) | નવો subscriber |
|---|---|---|
| Insert / update / replace | Document, metadata વગર | Document + metadata |
| Delete | Document **નીકળી જાય** | Document **રહે**, `_deleted: true` |
| Spark ને દેખાય? | ❌ | ✅ |

---

## 9. સુધારવા શું કરવું

1. **Subscriber બંધ કરો:** subscriber વાળા terminal માં `Ctrl + C`.
2. **નવો code build કરો:**
   ```powershell
   cd D:\ClaudeProjects\dataEngineeringProjects\sparkProject\mongo-cdc-subscriber
   mvn -q clean package
   ```
3. **જૂના (metadata વગરના) bronze collections drop કરો** (`mongosh`). `mysql_*` ને અડવું નહીં:
   ```javascript
   const b = db.getSiblingDB("de_bronze");
   ["orders", "weather_observations", "student", "ordes"].forEach(c => { b[c].drop(); print("dropped de_bronze." + c); });
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
6. **ચકાસો:** Topic ના ચારેય `orders` messages ફરી લાગુ થશે, અને છેલ્લે document soft-deleted સ્થિતિમાં હશે (વિભાગ 8 નું JSON):
   ```javascript
   db.getSiblingDB("de_bronze").orders.find({}, { status: 1, amount: 1, _op: 1, _deleted: 1, _source_ts: 1 })
   ```
   ➡️ `status: "DELIVERED"`, `amount: 2299`, `_op: "delete"`, `_deleted: true`, `_source_ts: 1793211816458`

---

## 10. સારાંશ

| Layer | પરિણામ |
|---|---|
| Source → Kafka | ✅ ચારેય operations (insert, update, replace, delete) સાચા ક્રમમાં (offsets 3–6), એક જ partition માં, સાચા types સાથે |
| Kafka → Destination (data) | ✅ Insert અને replace ના values અને types source જેવા જ. Update પણ લાગુ થયો હતો (પુરાવા વિભાગ 7) |
| Kafka → Destination (delete) | ✅ લાગુ થયું (bronze માં 0 documents), પણ hard delete તરીકે |
| Metadata fields | ⚠️ **નથી**: ચાલતો subscriber જૂના code નો છે. વિભાગ 9 પ્રમાણે rebuild + replay |
| Update પછી જૂની values દેખાઈ | ⚠️ જોવાનો સમય / refresh. Pipeline એ update લાગુ કર્યો હતો |
| `amount` type | ℹ️ Replace માં source માં જ Double → Int32 બદલાયો. Pipeline એ ચોક્કસ copy કરી |
