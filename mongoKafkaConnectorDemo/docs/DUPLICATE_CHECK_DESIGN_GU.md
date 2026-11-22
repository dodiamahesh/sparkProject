# Duplicate Check: Design (mongo-cdc-subscriber)

> **Status:** ✅ **implement થયું** (2026-11-23). Scripts: `sample-data/00_cdc_duplicate_rule.sql`, `sample-data/<pipeline>/03_bronze_unique_indexes.js`. Commands: [DAILY_OPERATIONS_COMMANDS_GU.md §8.5](DAILY_OPERATIONS_COMMANDS_GU.md#85-duplicates-જોવા)
> English version: [DUPLICATE_CHECK_DESIGN_EN.md](DUPLICATE_CHECK_DESIGN_EN.md)
> સંબંધિત: [MONGO_CDC_COMPLETE_SETUP_GU.md §7.4.1](MONGO_CDC_COMPLETE_SETUP_GU.md) (collection mapping), [DAILY_OPERATIONS_COMMANDS_GU.md](DAILY_OPERATIONS_COMMANDS_GU.md)

---

## સારાંશ

**શું:** Subscriber, bronze_db માં **નવો** record લખતા પહેલાં ચકાસે કે એ જ "business key" વાળો **live** record bronze માં પહેલેથી છે કે નહીં. હોય તો નવો record bronze માં **નહીં** જાય, PostgreSQL ના `meta.meta_duplicate_record` table માં જશે.

**Key કોણ નક્કી કરે:** તમે. દરેક collection / table માટે fields ની list, PostgreSQL table `meta.cdc_duplicate_rule` માં (જેમ કે `name,email,phone`). **બધા** fields સરખા હોય તો જ duplicate. નવો rule = એક SQL row, code કે restart નહીં.

**કેવી રીતે (A + C):**

| | શું | કામ |
|---|---|---|
| **A** (મુખ્ય) | લખતા **પહેલાં** subscriber bronze માં query કરે (દરેક batch, દરેક collection માટે 1 query) | Duplicate પકડે, update થવા દે, rule તરત લાગુ |
| **C** (સલામતી) | Bronze માં rule ના fields પર **partial unique index**. DB પોતે duplicate રોકે (E11000), subscriber error પકડીને dump કરે | A ચૂકી જાય (race, ઘણા instances) ત્યારે પણ guarantee |

**મુખ્ય નિયમો:** ફક્ત **નવો document બનાવતા** writes ચકાસાય • **પહેલો આવેલો જીતે** • soft-deleted (`_deleted: true`) document કોઈને **રોકતો નથી** • update / delete હંમેશા થાય (એક અપવાદ: નીચે §5).

**મુખ્ય મુશ્કેલીઓ અને ઉકેલ** (વિગત §7):

| મુશ્કેલી | ઉકેલ |
|---|---|
| Reject થયેલો record પછી **update** થી bronze માં ઘૂસી જાય | "નવો document બનાવે" એવા દરેક write ચકાસો, ફક્ત insert નહીં |
| એક batch માં એ જ key ના 2 નવા records | Batch ની અંદરની keys memory માં યાદ રાખો |
| Kafka replay પર એ જ duplicate વારંવાર | એ જ `_id` = duplicate નહીં. `meta_duplicate_record` પર unique constraint + upsert |
| Index વગર મોટા collection પર query ધીમી | Index (C) હોય જ. ન હોય તો 200 ms થી ધીમી query પર warning |
| Unique index **missing field** ને `null` ગણે | Index માં દરેક field પર `$exists: true` |
| Bronze માં પહેલેથી duplicates હોય તો index બને જ નહીં | Setup script duplicates ની list બતાવીને અટકે, data delete ન કરે |

---

## 1. અંતિમ નિર્ણયો

| # | વિષય | નિર્ણય |
|---|---|---|
| 1 | Rule ક્યાં | અલગ table `meta.cdc_duplicate_rule` |
| 2 | Rule નો પ્રકાર | ફક્ત fields ની list (comma થી). ✗ `ALL_EXCEPT` નહીં |
| 3 | Duplicate ક્યારે | Rule ના **બધા** fields સરખા, અને bronze માં એ **live** (`_deleted: false`) હોય |
| 4 | Check ક્યારે | જે write bronze માં **નવો document** બનાવે: insert, અથવા update/replace જેનો `_id` bronze માં નથી |
| 5 | કોણ જીતે | પહેલો આવેલો (first wins) |
| 6 | Soft-deleted document | નવા record ને **રોકતો નથી** |
| 7 | Rejected `_id` નો update | Bronze માં નહીં. `meta_duplicate_record` ની એ જ row નો `object_payload` update |
| 8 | Rejected `_id` નો delete | Bronze માં stub નહીં. `meta_duplicate_record` ની row **એમ જ રહે** (history) |
| 9 | Field ખૂટે | Field record માં **છે જ નહીં**: check skip, normal insert + warning log. Field છે પણ value `null` છે: એ **compare થાય** (null = null), unique index જેવું જ (`$exists: true` માં null આવી જાય) |
| 10 | Value compare | Numbers numeric value થી (Int32 = Int64 = Double), strings exact, nested = dot-notation (`address.pincode`), arrays ક્રમ સાથે |
| 11 | Index (C) | **દરેક** active rule પર. Setup script બનાવે, subscriber **નહીં** |
| 12 | Index અને rule મેળ ન ખાય | Subscriber start વખતે warning (બંધ નહીં થાય) |
| 13 | ધીમી query | 200 ms થી વધુ = warning log |
| 14 | `meta_duplicate_record` columns | ફક્ત 5 (તમારા). Columns પછી `ALTER TABLE ... ADD COLUMN` થી ઉમેરી શકાય |
| 15 | PostgreSQL database | `shop_dw` (`subscriber.properties`: `mapping.jdbc.url`). એક વાર `CREATE DATABASE shop_dw` |

---

## 2. Tables (PostgreSQL, schema `meta`)

**`meta.cdc_duplicate_rule`:** કયા fields થી duplicate ચકાસવો

| Column | Type | અર્થ |
|---|---|---|
| `source` | VARCHAR | Source collection / table (= message નું `ns.coll`) |
| `db_type` | VARCHAR | `NOSQL` / `SQL` (source નો type, mapping table જેવું) |
| `fields` | TEXT | Comma થી fields: `name,email,phone,address.pincode` |
| `active` | BOOLEAN | `false` = check નહીં |

Primary key `(source, db_type)`. Rule ન હોય = check નહીં (આજનું વર્તન).

**`meta.meta_duplicate_record`:** reject થયેલા records

| Column | Type | અર્થ |
|---|---|---|
| `id_pk` | BIGSERIAL PK | |
| `record_id` | TEXT | Source `_id`, Extended JSON (`{"$oid":"6ae0…"}`) |
| `data_store_name` | VARCHAR | Source collection / table |
| `object_payload` | JSONB | આખો record (છેલ્લી copy) |
| `db_type` | VARCHAR | `NOSQL` / `SQL` |

Unique constraint `(db_type, data_store_name, record_id)`: replay પર નવી row નહીં, એ જ row update (`ON CONFLICT DO UPDATE`).

**Bronze (MongoDB):** rule ના fields પર partial unique index (C), ઉદાહરણ:
```javascript
db.customers.createIndex(
  { name: 1, email: 1, phone: 1 },
  { unique: true, partialFilterExpression: {
      _deleted: false, name: { $exists: true }, email: { $exists: true }, phone: { $exists: true } } })
```

---

## 3. દરેક batch માં flow

Kafka consumer **poll** થી કામ કરે છે: એક poll = 1 થી 500 messages (= "batch"). રોજના traffic માં મોટા ભાગે 1, copy / replay વખતે 500.

```
1. Mapping + duplicate rules વાંચો (PostgreSQL)
2. Messages parse → destination collection
3. Rule હોય એવા દરેક destination માટે:
   a. Batch ના _id → bronze માં કયા છે? (1 query)
   b. "નવો document બનાવે" એવા writes → rule ના fields ની values
   c. find({ _deleted: false, $or: [ {fields = values}, … ] })   (1 query)   ← A
      + batch ની અંદર પહેલાં આવેલી keys (memory)
   d. મળ્યું → duplicate list, bronze writes માંથી કાઢો
4. Bronze bulk write (ordered). E11000 આવે → એ write duplicate, બાકીના ફરી મોકલો  ← C
5. Duplicates → meta.meta_duplicate_record (upsert)
6. DLQ → 7. Kafka commit
```
Step 5 fail થાય (PostgreSQL બંધ) → commit નહીં → restart પછી એ જ batch ફરી. બંને બાજુ idempotent, કંઈ ખોવાય કે ડબલ ન થાય.

---

## 4. દરેક event પર શું થાય

ઉદાહરણ: A1 bronze માં live, B = A1 નો duplicate (ફક્ત `meta_duplicate_record` માં).

| Event | પરિણામ |
|---|---|
| નવો insert, key નવી | ✅ Bronze |
| નવો insert, key A1 જેવી | ❌ `meta_duplicate_record` |
| એ જ `_id` ફરી (replay, `copy_existing`) | ✅ Normal replace (duplicate નહીં) |
| A1 નો update (key એ જ / નવી unique key) | ✅ Bronze update |
| B નો update (A1 હજુ live) | ❌ Bronze નહીં, meta row નો payload update |
| B નો delete | Bronze માં કંઈ નહીં, meta row એમ જ |
| A1 નો delete | ✅ Soft delete. હવે A1 કોઈને રોકતો નથી |
| A1 delete પછી B નો update | ✅ B bronze માં (key free) |
| A1 delete પછી B નો **કોઈ event નહીં** | B ફક્ત meta table માં રહે. હાથે: source માં B ને touch કરો (`updatedAt` update) |

---

## 5. અપવાદ: update જે record ને બીજા જેવો બનાવે

P1 નો update એના fields ને A1 જેવા કરી દે: index (C) એને **રોકે**. Bronze માં P1 **જૂની version** પર રહે, update `meta_duplicate_record` માં જાય. P1 ના પછીના updates પણ રોકાશે, જ્યાં સુધી key અલગ ન થાય. (નિર્ણય: રોકો + dump, કારણ કે દરેક rule પર index છે.)

---

## 6. ભવિષ્યમાં (હમણાં નહીં)

| વિચાર | ક્યારે જોઈએ |
|---|---|
| Batch ની અંદર `_source_ts` પ્રમાણે sort ("first" = source માં પહેલો) | Partitions વચ્ચેનો ક્રમ મહત્વનો બને |
| Auto-promote: A1 delete થાય ત્યારે pending B આપોઆપ bronze માં | હાથે touch કરવું ભારે પડે |
| `meta_duplicate_record` માં `status`, `detected_at`, `duplicate_key`, topic/offset | Report / audit જોઈએ |

---

## 7. મુશ્કેલીઓ અને ઉકેલ (વિગત)

| # | મુશ્કેલી | ઉકેલ |
|---|---|---|
| 1 | Reject થયેલો B પછી **update** આવે, upsert એને bronze માં લાવી દે | "નવો document બનાવે" એવા writes (`_id` bronze માં નથી) ને insert ની જેમ ચકાસો |
| 2 | એક batch માં એ જ key ના 2 નવા | પહેલો હજુ લખાયો નથી, DB query નહીં બતાવે → batch ની keys memory માં |
| 3 | "First" = subscriber પાસે પહેલો પહોંચ્યો, source માં પહેલો નહીં (Kafka key = `_id`, અલગ partitions) | હમણાં સ્વીકાર્યું. પછી `_source_ts` sort (§6) |
| 4 | Kafka replay / restart | એ જ `_id` = replace, duplicate નહીં • meta table પર unique constraint + upsert |
| 5 | Index વગર collection scan, ધીમું (દરેક message પર આખું collection) | દરેક rule પર index (C) • 200 ms થી ધીમી query = warning |
| 6 | Unique index missing field ને `null` ગણે (email વગરના 2 = duplicate) | `partialFilterExpression` માં દરેક field પર `$exists: true` |
| 7 | Bronze માં પહેલેથી duplicates → unique index બને નહીં | Script duplicates શોધી list બતાવે, અટકે. સફાઈ તમે નક્કી કરો |
| 8 | Rule બદલ્યો, index જૂનો | Script થી index ફરી • start વખતે mismatch warning |
| 9 | Index update ને પણ રોકે (§5) | સ્વીકાર્યું: રોકો + dump |
| 10 | E11000 પર `ordered(true)` bulk write અટકે | Error વાળો write duplicate, એની પછીના writes ફરી મોકલો (loop). ક્રમ સચવાય |
| 11 | A1 delete થાય પણ B નો event ન આવે | હાથે touch (§4). Auto-promote પછી (§6) |

---

## 8. Implementation steps

| # | Step | File |
|---|---|---|
| 1 | `cdc_duplicate_rule` + `meta_duplicate_record` tables, sample rules (`products`: `product_code`, `customers`: `customer_code`) | `sample-data/00_cdc_collection_mapping.sql` |
| 2 | Rules વાંચવા | `DuplicateRules.java` (નવી) |
| 3 | A: query, batch memory, duplicate list | `DuplicateDetector.java` (નવી) |
| 4 | Meta table માં upsert | `DuplicateRecordStore.java` (નવી) |
| 5 | Flow માં જોડવું + C: E11000 loop | `ChangeEventApplier.java` |
| 6 | Start: rules log, index mismatch warning | `SubscriberApp.java` |
| 7 | Duplicates શોધવા, પછી partial unique indexes | `sample-data/00_create_bronze_collections.js` |
| 8 | Verify: source = bronze live + duplicates | `sample-data/99_verify_bronze.js` |
| 9 | Test: scratch DB, sales 01 ના P003 અને C005 → 2 duplicates; replay પછી પણ 2 | JUnit + scratch DB |
| 10 | Docs + change log | setup guide, DAILY_OPERATIONS, README |

---

## 9. તમારા પ્રશ્નો અને ટૂંકા જવાબ

| # | પ્રશ્ન | જવાબ |
|---|---|---|
| 1 | દરેક collection માટે unique key બનાવવી પડશે, અને દરેક record માટે DB check? | હા, key જોઈએ. પણ rule PostgreSQL table માં (code નહીં), અને check દરેક batch માં collection દીઠ 1 query |
| 2 | Consumer event-driven છે, તો "batch" ક્યાંથી? | Kafka poll એકસાથે 1 થી 500 messages આપે. રોજ મોટા ભાગે 1, copy / replay વખતે 500 |
| 3 | Batch માં first come first? Duplicate (B) નો પછી update / delete? | હા, first wins. B નો update / delete bronze માં નહીં, meta row માં (§4) |
| 4 | Duplicate એક key પર નહીં, ઘણા fields ની values થી? | હા: rule = fields ની list, બધા સરખા હોય તો જ duplicate |
| 5 | Update / delete વખતે check? | ના. ફક્ત નવો document બનાવે ત્યારે. Rejected `_id` ના events ફક્ત meta row માં |
| 6 | `_dup_hash` વગર બીજા વિકલ્પ? | A (સીધી query), B (અલગ registry), C (unique index), D (memory, ✗). પસંદ: A + C |
| 7 | Index (A માં) ફરજિયાત? ન હોય તો? | Results એ જ, ફક્ત ધીમું (collection scan). 1 selective field નો index પણ ચાલે |
| 8 | A અને C માં code-wise ફરક? | A: પહેલાં query, પછી ફક્ત બચેલા writes. C: બધું લખો, E11000 પકડો, બાકીના ફરી મોકલો |
| 9 | A અને C માં update / delete? | Delete બંનેમાં હંમેશા. Update: A બધા થવા દે, C બીજા જેવો બનાવતો update રોકે (§5) |
| 10 | A + C માં doubt? | Index ક્યાં / કોણ બનાવે, missing field, પહેલેથી duplicates, mismatch: બધા §1 અને §7 માં નક્કી |
| 11 | `ALL_EXCEPT` જોઈએ? | ના, કાઢી નાખ્યું. ફક્ત તમે આપેલા fields |
| 12 | ફક્ત મારા 5 columns રાખું તો ભવિષ્યમાં તકલીફ? | ચાલશે. Unique constraint (columns નહીં) રાખવો જરૂરી. બાકીના columns પછી ઉમેરી શકાય (§6) |
| 13 | `shop_dw` ની જરૂર શું? | ફરજિયાત નથી. કોઈ પણ PostgreSQL database ચાલે (`mapping.jdbc.url`). ETL ના default સાથે મેળ માટે `shop_dw` |
