# Duplicate Check: Design (mongo-cdc-subscriber)

> **Status:** ✅ **implemented** (2026-11-23). Scripts: `sample-data/00_cdc_duplicate_rule.sql`, `sample-data/<pipeline>/03_bronze_unique_indexes.js`. Commands: [DAILY_OPERATIONS_COMMANDS_GU.md §8.5](DAILY_OPERATIONS_COMMANDS_GU.md#85-duplicates-જોવા)
> Gujarati version: [DUPLICATE_CHECK_DESIGN_GU.md](DUPLICATE_CHECK_DESIGN_GU.md)
> Related: [MONGO_CDC_COMPLETE_SETUP_GU.md §7.4.1](MONGO_CDC_COMPLETE_SETUP_GU.md) (collection mapping), [DAILY_OPERATIONS_COMMANDS_GU.md](DAILY_OPERATIONS_COMMANDS_GU.md)

---

## Summary

**What:** Before the subscriber writes a **new** record to bronze_db, it checks whether a **live** record with the same "business key" already exists in bronze. If it does, the new record does **not** go to bronze; it goes to the PostgreSQL table `meta.meta_duplicate_record`.

**Who defines the key:** you. For each collection / table, a list of fields in the PostgreSQL table `meta.cdc_duplicate_rule` (for example `name,email,phone`). A record is a duplicate only when **all** fields are equal. A new rule = one SQL row, no code change, no restart.

**How (A + C):**

| | What | Purpose |
|---|---|---|
| **A** (main) | **Before** writing, the subscriber queries bronze (1 query per batch per collection) | Catches duplicates, lets updates through, rule changes apply immediately |
| **C** (safety net) | A **partial unique index** on the rule fields in bronze. The DB itself blocks duplicates (E11000); the subscriber catches the error and dumps the record | Guarantee even when A misses one (race, several instances) |

**Main rules:** only writes that **create a new document** are checked • **first one wins** • a soft-deleted (`_deleted: true`) document does **not block** anything • updates / deletes always go through (one exception: §5 below).

**Main complications and solutions** (details in §7):

| Complication | Solution |
|---|---|
| A rejected record sneaks into bronze later through an **update** | Check every write that "creates a new document", not only inserts |
| 2 new records with the same key in one batch | Remember the keys of the batch in memory |
| The same duplicate again and again on Kafka replay | Same `_id` = not a duplicate. Unique constraint + upsert on `meta_duplicate_record` |
| Slow query on a big collection without an index | Index (C) always exists. If missing: warning when a query takes more than 200 ms |
| A unique index treats a **missing field** as `null` | `$exists: true` on every field in the index |
| Index cannot be built when bronze already has duplicates | The setup script lists the duplicates and stops; it does not delete data |

---

## 1. Final decisions

| # | Topic | Decision |
|---|---|---|
| 1 | Where the rule lives | Separate table `meta.cdc_duplicate_rule` |
| 2 | Rule type | Only a list of fields (comma separated). ✗ No `ALL_EXCEPT` |
| 3 | When is it a duplicate | **All** rule fields are equal, and the bronze document is **live** (`_deleted: false`) |
| 4 | When is it checked | Every write that creates a **new document** in bronze: insert, or update/replace whose `_id` is not in bronze |
| 5 | Who wins | The first one to arrive (first wins) |
| 6 | Soft-deleted document | Does **not block** a new record |
| 7 | Update of a rejected `_id` | Not written to bronze. `object_payload` of the same row in `meta_duplicate_record` is updated |
| 8 | Delete of a rejected `_id` | No stub in bronze. The row in `meta_duplicate_record` **stays as it is** (history) |
| 9 | Field missing | Field **does not exist** in the record: check skipped, normal insert + warning log. A field that exists with value `null` **is** compared (null = null), same as the unique index (`$exists: true` includes null) |
| 10 | Value comparison | Numbers by numeric value (Int32 = Int64 = Double), strings exact, nested = dot notation (`address.pincode`), arrays in order |
| 11 | Index (C) | On **every** active rule. Created by the setup script, **not** by the subscriber |
| 12 | Index and rule do not match | Warning at subscriber start (it does not stop) |
| 13 | Slow query | More than 200 ms = warning log |
| 14 | `meta_duplicate_record` columns | Only the 5 you defined. More columns can be added later with `ALTER TABLE ... ADD COLUMN` |
| 15 | PostgreSQL database | `shop_dw` (`subscriber.properties`: `mapping.jdbc.url`). Once: `CREATE DATABASE shop_dw` |

---

## 2. Tables (PostgreSQL, schema `meta`)

**`meta.cdc_duplicate_rule`:** which fields define a duplicate

| Column | Type | Meaning |
|---|---|---|
| `source` | VARCHAR | Source collection / table (= `ns.coll` of the message) |
| `db_type` | VARCHAR | `NOSQL` / `SQL` (type of the source, same as the mapping table) |
| `fields` | TEXT | Comma-separated fields: `name,email,phone,address.pincode` |
| `active` | BOOLEAN | `false` = no check |

Primary key `(source, db_type)`. No rule = no check (today's behavior).

**`meta.meta_duplicate_record`:** rejected records

| Column | Type | Meaning |
|---|---|---|
| `id_pk` | BIGSERIAL PK | |
| `record_id` | TEXT | Source `_id`, Extended JSON (`{"$oid":"6ae0…"}`) |
| `data_store_name` | VARCHAR | Source collection / table |
| `object_payload` | JSONB | The whole record (latest copy) |
| `db_type` | VARCHAR | `NOSQL` / `SQL` |

Unique constraint `(db_type, data_store_name, record_id)`: a replay does not create a new row; the same row is updated (`ON CONFLICT DO UPDATE`).

**Bronze (MongoDB):** partial unique index on the rule fields (C), example:
```javascript
db.customers.createIndex(
  { name: 1, email: 1, phone: 1 },
  { unique: true, partialFilterExpression: {
      _deleted: false, name: { $exists: true }, email: { $exists: true }, phone: { $exists: true } } })
```

---

## 3. Flow per batch

The Kafka consumer works with **poll**: one poll = 1 to 500 messages (= "batch"). In daily traffic usually 1, during copy / replay 500.

```
1. Read mapping + duplicate rules (PostgreSQL)
2. Parse messages → destination collection
3. For every destination that has a rule:
   a. _id values of the batch → which ones exist in bronze? (1 query)
   b. Writes that "create a new document" → values of the rule fields
   c. find({ _deleted: false, $or: [ {fields = values}, … ] })   (1 query)   ← A
      + keys that came earlier in the same batch (memory)
   d. Found → duplicate list, remove from the bronze writes
4. Bronze bulk write (ordered). On E11000 → that write is a duplicate, send the rest again  ← C
5. Duplicates → meta.meta_duplicate_record (upsert)
6. DLQ → 7. Kafka commit
```
If step 5 fails (PostgreSQL down) → no commit → the same batch comes again after a restart. Both sides are idempotent, so nothing is lost or doubled.

---

## 4. What happens on each event

Example: A1 is live in bronze, B = duplicate of A1 (only in `meta_duplicate_record`).

| Event | Result |
|---|---|
| New insert, new key | ✅ Bronze |
| New insert, same key as A1 | ❌ `meta_duplicate_record` |
| Same `_id` again (replay, `copy_existing`) | ✅ Normal replace (not a duplicate) |
| Update of A1 (same key / new unique key) | ✅ Bronze updated |
| Update of B (A1 still live) | ❌ Not in bronze, payload of the meta row updated |
| Delete of B | Nothing in bronze, meta row stays |
| Delete of A1 | ✅ Soft delete. A1 no longer blocks anything |
| Update of B after A1 was deleted | ✅ B goes to bronze (key is free) |
| A1 deleted and **no event** for B afterwards | B stays only in the meta table. By hand: touch B in the source (update `updatedAt`) |

---

## 5. Exception: an update that makes a record equal to another one

An update of P1 makes its fields equal to A1: the index (C) **blocks** it. P1 stays on its **old version** in bronze, the update goes to `meta_duplicate_record`. Later updates of P1 are blocked too, until its key is different again. (Decision: block + dump, because every rule has an index.)

---

## 6. Later (not now)

| Idea | When needed |
|---|---|
| Sort by `_source_ts` inside a batch ("first" = first in the source) | When the order across partitions matters |
| Auto-promote: when A1 is deleted, move the pending B into bronze automatically | When touching by hand becomes a burden |
| `status`, `detected_at`, `duplicate_key`, topic/offset in `meta_duplicate_record` | When a report / audit is needed |

---

## 7. Complications and solutions (details)

| # | Complication | Solution |
|---|---|---|
| 1 | Rejected B gets an **update** later, the upsert would bring it into bronze | Check writes that "create a new document" (`_id` not in bronze) the same way as inserts |
| 2 | 2 new records with the same key in one batch | The first is not written yet, the DB query cannot see it → keep the keys of the batch in memory |
| 3 | "First" = first to reach the subscriber, not first in the source (Kafka key = `_id`, different partitions) | Accepted for now. Later: sort by `_source_ts` (§6) |
| 4 | Kafka replay / restart | Same `_id` = replace, not a duplicate • unique constraint + upsert on the meta table |
| 5 | Without an index: collection scan, slow (whole collection for every message) | Index (C) on every rule • query slower than 200 ms = warning |
| 6 | A unique index treats a missing field as `null` (2 customers without email = duplicate) | `$exists: true` on every field in `partialFilterExpression` |
| 7 | Bronze already has duplicates → unique index cannot be built | The script finds and lists the duplicates, then stops. You decide how to clean up |
| 8 | Rule changed, index still old | Rebuild the index with the script • mismatch warning at start |
| 9 | The index also blocks updates (§5) | Accepted: block + dump |
| 10 | On E11000 an `ordered(true)` bulk write stops | The failing write is the duplicate, send the writes after it again (loop). Order is kept |
| 11 | A1 deleted but no event for B | Touch by hand (§4). Auto-promote later (§6) |

---

## 8. Implementation steps

| # | Step | File |
|---|---|---|
| 1 | Tables `cdc_duplicate_rule` + `meta_duplicate_record`, sample rules (`products`: `product_code`, `customers`: `customer_code`) | `sample-data/00_cdc_collection_mapping.sql` |
| 2 | Read the rules | `DuplicateRules.java` (new) |
| 3 | A: query, batch memory, duplicate list | `DuplicateDetector.java` (new) |
| 4 | Upsert into the meta table | `DuplicateRecordStore.java` (new) |
| 5 | Wire into the flow + C: E11000 loop | `ChangeEventApplier.java` |
| 6 | At start: log rules, index mismatch warning | `SubscriberApp.java` |
| 7 | Find duplicates, then create partial unique indexes | `sample-data/00_create_bronze_collections.js` |
| 8 | Verify: source = bronze live + duplicates | `sample-data/99_verify_bronze.js` |
| 9 | Test: scratch DB, P003 and C005 of sales 01 → 2 duplicates; still 2 after a replay | JUnit + scratch DB |
| 10 | Docs + change log | setup guide, DAILY_OPERATIONS, README |

---

## 9. Your questions and short answers

| # | Question | Answer |
|---|---|---|
| 1 | Does every collection need a unique key, and a DB check for every record? | Yes, a key is needed. But the rule lives in a PostgreSQL table (not in code), and the check is 1 query per collection per batch |
| 2 | The consumer is event driven, so where does a "batch" come from? | A Kafka poll returns 1 to 500 messages at once. Usually 1 per day-to-day event, 500 during copy / replay |
| 3 | First come first inside a batch? What about a later update / delete of the duplicate (B)? | Yes, first wins. Updates / deletes of B do not go to bronze, only to the meta row (§4) |
| 4 | Duplicate based on the values of several fields, not one key? | Yes: rule = list of fields, duplicate only when all are equal |
| 5 | Is it checked on update / delete? | No. Only when a new document would be created. Events of a rejected `_id` only touch the meta row |
| 6 | Other options without `_dup_hash`? | A (direct query), B (separate registry), C (unique index), D (memory, ✗). Chosen: A + C |
| 7 | Is the index (in A) mandatory? What if it is missing? | Same results, only slower (collection scan). An index on 1 selective field also works |
| 8 | Difference between A and C in code? | A: query first, then write only the remaining writes. C: write everything, catch E11000, send the rest again |
| 9 | Update / delete in A and C? | Delete always goes through in both. Update: A lets all through, C blocks an update that makes a record equal to another one (§5) |
| 10 | Any doubts about A + C? | Where / who creates the index, missing fields, existing duplicates, mismatch: all decided in §1 and §7 |
| 11 | Is `ALL_EXCEPT` needed? | No, removed. Only the fields you define |
| 12 | Any future problem if I keep only my 5 columns? | Works. The unique constraint (not a column) is required. More columns can be added later (§6) |
| 13 | Why is `shop_dw` needed? | It is not mandatory. Any PostgreSQL database works (`mapping.jdbc.url`). `shop_dw` matches the ETL default |
