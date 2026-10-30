# 02 — Ingestion Layer (Source MongoDB → Kafka → Bronze)

> Builds on [01-architecture-and-structure.md](01-architecture-and-structure.md).
> Scope: Kafka Connect + MongoDB source connector, the shared CDC topic, the `etl-ingestion` service, Bronze storage and metadata, ordering/idempotency, DLQ, schema evolution, metrics and replay.

---

## 0. Summary

| Topic | Decision |
|---|---|
| Connectors | **One MongoDB source connector per source DB**. All captured collections → **one shared topic** `cdc.source.events` |
| Event format | Full change-event envelope as **canonical Extended JSON string** (keeps BSON types: ObjectId, Date, Decimal128) |
| Message key | `sourceService:collection:documentId`, built in the connector pipeline → per-document ordering |
| Topic | 24 partitions in prod (sized below), RF 3, `cleanup.policy=delete`, **14 days retention** in prod |
| Bronze store | **MongoDB**: one Bronze DB per source service, one Bronze collection per source collection, `_id` = source `_id` |
| Bronze shape | `{ _id, data: <source document>, _etl: <metadata> }`. Metadata is namespaced under `_etl`, so it never collides with source fields |
| Idempotency | **Version guard**: `sourceVersion` = MongoDB `clusterTime`. A write is applied only if it is newer than what Bronze already has |
| Delivery | **At-least-once + idempotent writes** (effectively once). No Kafka transactions needed |
| Errors | Data errors → DLQ immediately. Infra errors → blocking retry with backoff (never DLQ). Stale/duplicate events → skipped (metric only) |
| Schema Registry | **Not required in phase 1** (schemaless JSON + data contracts + drift detection). Optional in phase 2 with `TopicRecordNameStrategy` |
| Soft delete | Delete event → `_etl.isDeleted=true`, `_etl.deletedAt`, `syncSilver=PENDING`. The last known `data` is kept |

---

## 1. Source MongoDB Prerequisites

| Requirement | Why | Recommendation |
|---|---|---|
| Replica set or sharded cluster | Change streams do not work on a standalone server | Already true for most production Mongo |
| Connector user privileges | `find` + `changeStream` on the captured DBs (+ `listCollections` for snapshot) | Dedicated read-only user per connector, e.g. `cdc_connector_order` |
| **Oplog window** ➕ | The connector resumes from a resume token stored in the oplog. If the connector is down longer than the oplog window, the token is lost and a **re-snapshot** is needed | Size the oplog for **≥ 72 hours** of writes. Alert when the window drops below 48 h |
| Pre/post images (optional) ➕ | `updateLookup` returns the document **as it is now**, not as it was at the event time. Post-images give the exact state per event | MongoDB 6.0+: enable `changeStreamPreAndPostImages` on collections that need exact history (e.g. for SCD2 in Gold) |
| Time sync | `clusterTime` and `$$NOW` come from the server clocks | NTP on all Mongo nodes |

---

## 2. Kafka Connect & MongoDB Source Connector

### 2.1 Layout (from doc 01)

```
kafka-connect/
├── README.md                              # Shared topic, envelope, key format, connector → collection mapping
├── connectors/
│   ├── base/
│   │   ├── mongo-source-svc-order.json     # 1 connector per source DB
│   │   ├── mongo-source-svc-customer.json
│   │   └── mongo-source-svc-payment.json
│   └── overlays/
│       ├── dev/   mongo-source-svc-order.json   # only the keys that differ
│       ├── qa/    ...
│       ├── uat/   ...
│       └── prod/  ...
├── worker/
│   └── connect-worker.properties.example  # Config providers, override policy
├── topics/
│   └── topics.yml                          # cdc.source.events, dlq.ingestion, cdc.source.snapshot
└── scripts/
    ├── deploy-connectors.sh                # base + overlay → PUT /connectors/{name}/config
    └── deploy-topics.sh
```

**Why one connector per source DB:**
- The MongoDB source connector reads **one change stream per connector**. In practice it runs a single task, so one connector per DB gives parallelism across DBs.
- Failure isolation: one broken source DB does not stop CDC for the others.
- Separate credentials and resume tokens per microservice.

### 2.2 Base connector config

`kafka-connect/connectors/base/mongo-source-svc-order.json`

```json
{
  "name": "mongo-source-svc-order",
  "config": {
    "connector.class": "com.mongodb.kafka.connect.MongoSourceConnector",
    "tasks.max": "1",

    "connection.uri": "${file:/opt/kafka/secrets/mongo-svc-order.properties:uri}",
    "database": "order_db",

    "pipeline": "[{\"$match\": {\"ns.coll\": {\"$in\": [\"orders\", \"order_items\", \"returns\"]}, \"operationType\": {\"$in\": [\"insert\", \"update\", \"replace\", \"delete\"]}}}, {\"$addFields\": {\"sourceService\": \"svc-order\", \"envelopeVersion\": 1, \"cdcKey\": {\"$concat\": [\"svc-order:\", \"$ns.coll\", \":\", {\"$toString\": \"$documentKey._id\"}]}}}]",

    "topic.namespace.map": "{\"*\": \"${TOPIC_PREFIX}cdc.source.events\"}",

    "change.stream.full.document": "updateLookup",
    "publish.full.document.only": "false",

    "output.format.key": "schema",
    "output.schema.key": "{\"type\": \"record\", \"name\": \"CdcKey\", \"fields\": [{\"name\": \"cdcKey\", \"type\": \"string\"}]}",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",

    "output.format.value": "json",
    "output.json.formatter": "com.mongodb.kafka.connect.source.json.formatter.ExtendedJson",
    "value.converter": "org.apache.kafka.connect.storage.StringConverter",

    "startup.mode": "latest",

    "heartbeat.interval.ms": "60000",
    "heartbeat.topic.name": "${TOPIC_PREFIX}__mongodb_heartbeats",

    "poll.max.batch.size": "1000",
    "poll.await.time.ms": "1000",

    "errors.tolerance": "none",

    "producer.override.acks": "all",
    "producer.override.enable.idempotence": "true",
    "producer.override.compression.type": "zstd",
    "producer.override.max.request.size": "8388608"
  }
}
```

| Property | Value | Why |
|---|---|---|
| `pipeline` → `$match` | collection list + op types | Captures only the needed collections. Ignores `drop`/`rename`/`invalidate` (these are alerted on separately, see 13) |
| `pipeline` → `$addFields` | `sourceService`, `envelopeVersion`, `cdcKey` | Adds the metadata that Mongo does not provide, and builds the message key on the server side |
| `topic.namespace.map` | `{"*": "cdc.source.events"}` | Sends every collection of this DB to the shared topic. A hot/PII collection can be routed elsewhere by adding one entry, e.g. `{"order_db.returns": "cdc.source.events.returns", "*": "cdc.source.events"}` |
| `change.stream.full.document` | `updateLookup` | Update events carry the full current document, not just the delta. Bronze always gets the complete state |
| `publish.full.document.only` | `false` | **Mandatory.** Keeps `ns`, `operationType`, `documentKey`, `clusterTime`. Without it, delete events and metadata are lost |
| `output.format.key` + `output.schema.key` | `cdcKey` | Key = `svc-order:orders:65f1...`. The default key (resume token) would spread one document's events across partitions and break ordering |
| `output.json.formatter` | `ExtendedJson` | Canonical Extended JSON keeps exact BSON types, so Bronze (also Mongo) stores the same types as the source |
| `startup.mode` | `latest` (prod after initial load) / `copy_existing` (initial load) | See section 4 |
| `heartbeat.*` | 60 s | ➕ Keeps the resume token fresh for collections with few writes, so a quiet collection never loses its oplog position |
| `errors.tolerance` | `none` | Never drop CDC events silently at the source. The connector fails, alerts fire, and it resumes from the last token after the fix |
| `producer.override.*` | idempotent, `acks=all`, zstd | No loss or duplicates from producer retries; compression for large docs. Requires `connector.client.config.override.policy=All` on the worker |

> ⚠ **Verify in the PoC against your connector version:** `startup.mode` (1.9+; older versions use `copy.existing=true`), key projection behaviour with `output.schema.key`, and the max message size for your largest documents.

### 2.3 Message key

```
key   = {"cdcKey": "svc-order:orders:65f1a2b3c4d5e6f708091a2b"}
                     └ service ┘└ coll ┘└──── documentKey._id ────┘
```

- All events of one source document → same partition → **in order**.
- `sourceService` is part of the key ➕ (refinement of doc 01), because two microservices can have the same collection name and even the same `_id` values.
- Different documents have no ordering guarantee across each other, and none is needed. Cross-collection consistency (e.g. `orders` and `order_items`) is handled in Silver as late-arriving data (Prompt 03).

### 2.4 Event envelope examples (what the consumer receives)

**Insert / update / replace** (value, Extended JSON, shortened):

```json
{
  "_id": {"_data": "8265F1A2B3000000012B..."},
  "operationType": "update",
  "clusterTime": {"$timestamp": {"t": 1730190000, "i": 3}},
  "wallTime": {"$date": "2026-10-29T08:20:00.123Z"},
  "ns": {"db": "order_db", "coll": "orders"},
  "documentKey": {"_id": {"$oid": "65f1a2b3c4d5e6f708091a2b"}},
  "fullDocument": {
    "_id": {"$oid": "65f1a2b3c4d5e6f708091a2b"},
    "orderNo": "ORD-1001",
    "amount": {"$numberDecimal": "1499.00"},
    "status": "CONFIRMED",
    "items": [{"sku": "A1", "qty": 2}]
  },
  "updateDescription": {"updatedFields": {"status": "CONFIRMED"}, "removedFields": []},
  "sourceService": "svc-order",
  "envelopeVersion": 1,
  "cdcKey": "svc-order:orders:65f1a2b3c4d5e6f708091a2b"
}
```

**Delete** (no `fullDocument`):

```json
{
  "operationType": "delete",
  "clusterTime": {"$timestamp": {"t": 1730193600, "i": 1}},
  "ns": {"db": "order_db", "coll": "orders"},
  "documentKey": {"_id": {"$oid": "65f1a2b3c4d5e6f708091a2b"}},
  "sourceService": "svc-order",
  "envelopeVersion": 1,
  "cdcKey": "svc-order:orders:65f1a2b3c4d5e6f708091a2b"
}
```

Edge case: an `update` event can arrive with `fullDocument: null` when the document was deleted before the lookup ran. The consumer skips it (the delete event follows), records the metric `outcome=skipped_null_lookup`, and does not send it to the DLQ.

### 2.5 Per-environment overlays, secrets and deploy

**Overlay** (JSON merge on `config`). Only the differences go here.

`kafka-connect/connectors/overlays/dev/mongo-source-svc-order.json`
```json
{
  "config": {
    "connection.uri": "${file:/opt/kafka/secrets/mongo-svc-order.properties:uri}",
    "database": "order_db_dev",
    "poll.max.batch.size": "200",
    "heartbeat.interval.ms": "300000"
  }
}
```

`kafka-connect/connectors/overlays/prod/mongo-source-svc-order.json`
```json
{
  "config": {
    "poll.max.batch.size": "2000",
    "producer.override.linger.ms": "20"
  }
}
```

**Secrets:** Kafka Connect `ConfigProvider`. The real URI is never in Git; it is mounted from a K8s Secret or Vault agent.

```properties
# worker/connect-worker.properties.example
config.providers=file
config.providers.file.class=org.apache.kafka.common.config.provider.FileConfigProvider
connector.client.config.override.policy=All
```

**Deploy script** (idempotent: `PUT` creates or updates):

```bash
#!/usr/bin/env bash
# kafka-connect/scripts/deploy-connectors.sh <env> <connect-url>
set -euo pipefail
ENV="$1"; CONNECT_URL="$2"
export TOPIC_PREFIX="${TOPIC_PREFIX:-}"          # empty when each env has its own Kafka cluster
DIR="$(cd "$(dirname "$0")/.." && pwd)"

for base in "$DIR"/connectors/base/*.json; do
  name="$(basename "$base" .json)"
  overlay="$DIR/connectors/overlays/$ENV/$name.json"
  if [[ -f "$overlay" ]]; then merged="$(jq -s '.[0] * .[1]' "$base" "$overlay")"; else merged="$(cat "$base")"; fi
  # Substitute ONLY our own placeholders; ${file:...} secret references stay untouched for Connect
  config="$(jq '.config' <<<"$merged" | envsubst '${TOPIC_PREFIX}')"
  echo "Deploying $name to $ENV"
  curl -sf -X PUT -H 'Content-Type: application/json' \
       "$CONNECT_URL/connectors/$name/config" -d "$config" > /dev/null
done
```

➕ CI runs the same merge step with `--dry-run` and validates each config through `PUT /connector-plugins/MongoSourceConnector/config/validate`.

---

## 3. Shared Topic Design

### 3.1 Topic definitions

`kafka-connect/topics/topics.yml`
```yaml
topics:
  - name: cdc.source.events
    partitions: { dev: 3, qa: 6, uat: 12, prod: 24 }
    replicationFactor: { dev: 1, qa: 3, uat: 3, prod: 3 }
    config:
      cleanup.policy: delete
      retention.ms: { dev: 259200000, qa: 604800000, uat: 604800000, prod: 1209600000 }  # 3d / 7d / 7d / 14d
      min.insync.replicas: { dev: 1, default: 2 }
      max.message.bytes: 8388608          # 8 MB, sized from the largest real documents
      compression.type: producer

  - name: cdc.source.snapshot             # ➕ Isolated topic for big initial loads / re-snapshots (section 4)
    partitions: { dev: 3, default: 12 }
    config: { cleanup.policy: delete, retention.ms: 604800000, max.message.bytes: 8388608 }

  - name: dlq.ingestion
    partitions: { dev: 1, default: 6 }
    config: { cleanup.policy: delete, retention.ms: 2592000000 }   # 30 days: time to fix and replay

  - name: etl.events.bronze-loaded        # Optional, batch-level "data landed" signal for event-triggered runs
    partitions: { default: 3 }
    config: { cleanup.policy: delete, retention.ms: 604800000 }

  - name: __mongodb_heartbeats
    partitions: { default: 1 }
    config: { cleanup.policy: delete, retention.ms: 86400000 }
```

### 3.2 Partition count

```
partitions ≥ max( peak_events_per_sec / events_per_sec_per_consumer_thread ,  max_consumer_threads_needed )
             × headroom (2x)
```

Example: peak 3,000 events/s and one consumer thread with Mongo bulk writes doing ~300 events/s → 10 threads → ×2 headroom → **24 partitions** (a clean multiple of 2/3/4/6/8/12 for even spreading over pods).

➕ **Over-provision from the start.** Adding partitions later changes `hash(key) → partition`. For a short time, events of one document can then be split between the old and the new partition, so ordering is no longer guaranteed. The version guard (section 8) still prevents wrong data, but it is better to avoid this.

### 3.3 Retention: why `delete`, not `compact`

| Policy | Effect | Fit |
|---|---|---|
| `compact` | Keeps only the latest message per key | ❌ Loses the change history; time-based replay ("everything since Tuesday 10:00") becomes meaningless |
| `delete` (chosen) | Keeps everything for `retention.ms` | ✅ Replay from offset/timestamp within the window |

**Replay window (answers "what retention is needed"):**

```
retention ≥ time to DETECT a data-loss incident + time to FIX it + time to REPLAY, with a safety margin
```

| Env | Retention | Reasoning |
|---|---|---|
| prod | **14 days** (minimum 7) | Covers an incident found after a weekend plus investigation time |
| uat/qa | 7 days | Test cycles |
| dev | 3 days | Cost |

Storage estimate: `events/day × avg compressed size × retention days × RF`. Example: 5 M events/day × 4 KB × 14 × 3 ≈ **840 GB**.

Beyond Kafka retention, **Bronze is the long-term replay source** (Bronze → Silver → Gold rebuild). For a total Bronze loss older than the retention window, run a **re-snapshot** from the source (section 4.3). ➕ Kafka **tiered storage** (if your Kafka version/distribution supports it) can extend retention to 30–90 days cheaply.

### 3.4 Shared-topic trade-offs and mitigations

The trade-off table is in doc 01, section 2.4. The concrete mitigations in this design:

| Risk | Mitigation |
|---|---|
| Noisy neighbour (one collection floods the topic) | (1) Initial loads and re-snapshots go to the **separate `cdc.source.snapshot` topic**. (2) Partitions over-provisioned. (3) A permanently hot collection is moved to `cdc.source.events.<name>` via `topic.namespace.map`. The consumer subscribes by **pattern** `cdc\.source\.(events|snapshot).*`, so no code change is needed. |
| No per-collection lag in Kafka | Event-time lag metric per `service`/`collection` in the consumer (section 13) |
| Replay of one collection | Replay consumer with a **collection filter** (section 15). Bronze → Silver re-sync for longer horizons |
| ACL/PII per collection | Topic ACLs: only the Connect principal (write) and `etl-ingestion` (read). PII-heavy collections can be moved to their own topic with stricter ACLs. PII fields can be encrypted at Bronze write (section 16) |
| One schema per topic | Schemaless JSON in phase 1. `TopicRecordNameStrategy` in phase 2 (section 12) |

---

## 4. Initial Load (Full Snapshot) vs Streaming

### 4.1 Options

| Option | How | Pros | Cons | Use for |
|---|---|---|---|---|
| **A. `copy_existing` in the main connector** | Connector records the resume token, copies all documents, then streams from that token | Built-in ordering guarantee (copy first, then changes); zero extra code | Big collections flood the shared topic and delay live CDC for everyone | Small/medium collections (up to a few million docs) |
| **B. Separate snapshot connector → `cdc.source.snapshot`** ➕ | A temporary connector with `startup.mode=copy_existing`, a namespace regex for one collection, and the snapshot topic. The live connector keeps streaming | Live CDC is not delayed; the snapshot can be throttled | Snapshot and live events may interleave → needs the version guard with a snapshot timestamp (below) | Large collections, re-snapshots in prod |
| **C. Spark bulk copy (source Mongo → Bronze)** ➕ | Start the live connector first (from `latest`), then a Spark job reads the source collection and writes Bronze with the same version-guard rule | Fastest for very large (TB) collections; no Kafka load | Extra job type; reads load the source DB (use a secondary read preference) | Very large collections only |

**Recommendation:** A by default. B for large collections and recovery. C only when B is too slow.

### 4.2 Snapshot event versioning (needed for B and C)

Copied documents have no `clusterTime`. The snapshot pipeline adds a read timestamp:

```json
"startup.mode": "copy_existing",
"startup.mode.copy.existing.namespace.regex": "^order_db\\.orders$",
"startup.mode.copy.existing.pipeline": "[{\"$addFields\": {\"_snapshotTs\": \"$$NOW\"}}]",
"topic.namespace.map": "{\"*\": \"cdc.source.snapshot\"}"
```

The consumer then uses `sourceVersion = Timestamp(_snapshotTs seconds, 0)` and strips `_snapshotTs` from `data`. A live event in the same second has `increment ≥ 1`, so **live events always win ties**. This is an approximation based on server clocks, and it is safe because the snapshot only ever writes when it is newer.

> ⚠ Verify in the PoC how your connector version marks copied records (e.g. a `copyingData` flag in the event `_id`) so the consumer can set `cdcOperation=SNAPSHOT`.

### 4.3 Re-snapshot (recovery)

Used when the oplog position is lost (connector down longer than the oplog window), Bronze is corrupted beyond Kafka retention, or a new collection is added mid-life:

1. Deploy a temporary snapshot connector (option B) for the affected collection(s).
2. The consumer applies the events with the version guard. Documents that are already up to date are skipped, and missing or stale ones are fixed.
3. ➕ Documents deleted at the source while CDC was broken never get a delete event. After the snapshot, a **reconciliation step** marks Bronze documents not seen in the snapshot (`_etl.lastIngestedAt < snapshotStart`) as `isDeleted=true, deleteReason=RECONCILED_MISSING`.
4. Remove the temporary connector.

> Note: the user-triggered **"full load"** run mode is a different thing. It re-processes **Bronze → Silver** (or Silver → Gold) without touching the source. A re-snapshot is a source → Bronze recovery tool.

---

## 5. Bronze Storage Design

### 5.1 Mongo or EPAS for Bronze → **MongoDB (final)**

| | MongoDB (chosen) | EPAS `JSONB` |
|---|---|---|
| Fidelity | Exact BSON types; same shape as the source | Decimal128/ObjectId/dates must be mapped; nested arrays become JSON |
| Schema evolution | Automatic | Automatic in JSONB, but indexing and querying is clumsier |
| Upsert with version guard | Native (`updateOne` + `upsert` + filter) | `INSERT ... ON CONFLICT DO UPDATE WHERE` (also fine) |
| Spark read | Mongo Spark connector with partitioned reads | JDBC + JSON parsing |
| Ops | Second DB technology in the data platform | One technology everywhere |

### 5.2 Naming

| Item | Convention | Example |
|---|---|---|
| Bronze DB | `bronze_<service>` (one per source microservice) | `bronze_svc_order` |
| Bronze collection | same name as the source collection | `bronze_svc_order.orders` |
| `_id` | **source `_id`, unchanged** | `ObjectId("65f1...")` |

Why one DB per service: access control (PII services get separate grants), simpler ownership, and a dropped or misbehaving service never affects the others.

### 5.3 Document shape

```json
{
  "_id": {"$oid": "65f1a2b3c4d5e6f708091a2b"},
  "data": {
    "_id": {"$oid": "65f1a2b3c4d5e6f708091a2b"},
    "orderNo": "ORD-1001",
    "amount": {"$numberDecimal": "1499.00"},
    "status": "CONFIRMED",
    "items": [{"sku": "A1", "qty": 2}]
  },
  "_etl": {
    "envelopeVersion": 1,
    "sourceService": "svc-order",
    "sourceDb": "order_db",
    "sourceCollection": "orders",
    "sourceId": "65f1a2b3c4d5e6f708091a2b",
    "cdcOperation": "UPDATE",
    "sourceVersion": {"$numberLong": "7432128847286337539"},
    "sourceEventTs": {"$date": "2026-10-29T08:20:00.123Z"},
    "kafkaTopic": "cdc.source.events",
    "kafkaPartition": 7,
    "kafkaOffset": {"$numberLong": "1844221"},
    "ingestionBatchId": "ing-20261029T082001Z-7-1844100",
    "ingestedAt": {"$date": "2026-10-20T10:00:00.000Z"},
    "lastIngestedAt": {"$date": "2026-10-29T08:20:01.004Z"},
    "recordHash": "sha256:9f2c...",
    "changeCount": 4,
    "isDeleted": false,
    "deletedAt": null,
    "schemaDrift": [],
    "syncSilver": {
      "status": "PENDING",
      "syncedVersion": {"$numberLong": "7432128800000000001"},
      "updatedAt": {"$date": "2026-10-29T07:05:12.000Z"},
      "runId": "b2s_order-20261029T070000Z-01",
      "jobName": "b2s_order",
      "errorCode": null,
      "errorMessage": null,
      "retryCount": 0,
      "firstFailedAt": null,
      "quarantineRef": null,
      "targets": []
    }
  }
}
```

**Why `data` + `_etl` instead of flat columns:**
- Source documents may already have fields called `version`, `status`, `isDeleted` or `updatedAt`. A namespace avoids collisions.
- Spark reads `data.*` as the business payload and `_etl.*` as control fields. Easy to separate and strip.
- One index prefix (`_etl.`) for all control queries.

Alternative: flat fields with an `etl_` prefix (`etl_isDeleted`, ...). It works, but it mixes the payload and the metadata.

### 5.4 Optional change history ➕

Bronze keeps the **latest state** per document. If a document changes 5 times between two Silver runs, Silver sees only the last state. That is fine for SCD1, but **SCD2 in Silver/Gold would miss the intermediate versions**.

For collections that need full history, set `keepHistory: true` in the route config. The consumer then also **appends** each event to `bronze_<service>.<collection>__history`, which has a unique index on `(_id.sourceId, _id.sourceVersion)`, so it stays idempotent. Combine this with post-images (section 1) for exact states. This is decided per collection, because it costs storage.

---

## 6. Bronze Metadata Columns (Final List)

Your initial list mapped to the final names:

| Your name | Final name | Change |
|---|---|---|
| `isDelete` | `_etl.isDeleted` | Kept |
| `syncSilver` | `_etl.syncSilver.status` | Kept; a sub-document groups everything about the Silver sync |
| `errorMessageWhileSync` | `_etl.syncSilver.errorMessage` | Kept + `errorCode` added (machine-readable) |
| `syncUpdateDate` | `_etl.syncSilver.updatedAt` | Kept |

### 6.1 Full column list

**Set by `etl-ingestion` (on every event unless noted):**

| Field | Type | Purpose |
|---|---|---|
| `_id` | source type (ObjectId/string) | Same as source `_id`; upsert key |
| `data` | document | The full source document (last known state) |
| `_etl.envelopeVersion` | int | Version of the `_etl` structure itself, for future migrations ➕ |
| `_etl.sourceService` | string | Owning microservice |
| `_etl.sourceDb` | string | Source DB name |
| `_etl.sourceCollection` | string | Source collection name |
| `_etl.sourceId` | string | `_id` as string; used for joins, logs and ID-reprocess run mode |
| `_etl.cdcOperation` | string enum | `INSERT` / `UPDATE` / `REPLACE` / `DELETE` / `SNAPSHOT` (last applied operation) |
| `_etl.sourceVersion` | long | `clusterTime` as a 64-bit value (`seconds << 32 \| increment`). **Ordering + idempotency key** |
| `_etl.sourceEventTs` | date | Event time at the source (`wallTime`, or `clusterTime` seconds) |
| `_etl.kafkaTopic` / `kafkaPartition` / `kafkaOffset` | string / int / long | Exact Kafka location of the last applied event; used for debugging and replay |
| `_etl.ingestionBatchId` | string | Consumer batch id; links Bronze to ingestion logs |
| `_etl.ingestedAt` | date | First time the document reached Bronze (`$setOnInsert` semantics) |
| `_etl.lastIngestedAt` | date | Last time an event was applied. Used for the watermark and the reconciliation step |
| `_etl.recordHash` | string | SHA-256 of the canonical `data` (sorted keys, volatile fields excluded). **Detects no-op updates** ➕ |
| `_etl.changeCount` | int | Number of applied changes (`$inc`); helps spot hot documents |
| `_etl.schemaDrift` | array<string> | Unknown fields or type changes vs the data contract (section 12) |
| `_etl.isDeleted` | bool | Soft-delete flag |
| `_etl.deletedAt` | date | Source delete event time |
| `_etl.deleteReason` | string | `SOURCE_DELETE` / `RECONCILED_MISSING` ➕ |

**Set by the Spark Bronze → Silver job (write-back), reset by ingestion on real changes:**

| Field | Type | Purpose |
|---|---|---|
| `_etl.syncSilver.status` | string enum | `PENDING` / `SUCCESS` / `FAILED` / `PARTIAL` / `QUARANTINED` / `SKIPPED` |
| `_etl.syncSilver.syncedVersion` | long | The `sourceVersion` that was last synced successfully. Dirty check: `sourceVersion > syncedVersion` |
| `_etl.syncSilver.updatedAt` | date | When the status was last written (your `syncUpdateDate`) |
| `_etl.syncSilver.runId` | string | Run that wrote the status → links to `job_run_audit` |
| `_etl.syncSilver.jobName` | string | Job that wrote the status (your `lastSyncJobName`) |
| `_etl.syncSilver.errorCode` | string | From `error_code_catalog`, e.g. `DQ_NOT_NULL`, `SCHEMA_TYPE_MISMATCH` |
| `_etl.syncSilver.errorMessage` | string (max 2 KB) | Human-readable reason (your `errorMessageWhileSync`), truncated, **no PII values** |
| `_etl.syncSilver.retryCount` | int | Failed attempts since the last success; the retry limit uses it |
| `_etl.syncSilver.firstFailedAt` | date | Age of the failure, for SLA/alerting |
| `_etl.syncSilver.nextRetryAt` | date | Earliest time a FAILED record is retried (backoff, doc 07 §3.5) |
| `_etl.syncSilver.quarantineRef` | string | ID of the quarantine record (if `QUARANTINED`) |
| `_etl.syncSilver.targets` | array<{table, status, errorCode}> | Per-Silver-table result when one Bronze document is split into several Silver tables ➕ |

### 6.2 Decisions on your suggested columns

| Suggested | Decision | Reason |
|---|---|---|
| `sourceSystem` | → `sourceService` | Same meaning, matches the connector field |
| `version` | → `sourceVersion` (clusterTime) | A source-independent, monotonic order per document. An app-level `version` field (if the source has one) stays in `data` |
| `batchId` / `runId` | Both: `ingestionBatchId` (ingestion) + `syncSilver.runId` (Spark) | Different producers, different meanings |
| `isQuarantined` | ❌ Dropped → `syncSilver.status = QUARANTINED` + `quarantineRef` | A separate flag can disagree with the status. One source of truth |
| `syncGold` | ❌ **Not in Bronze** | Rule: each layer is written back by the **next** layer only. Gold sync status lives on **Silver** rows. Bronze → Gold traceability comes from `runId` + lineage |
| `PARTIAL` (record level) | Kept, with a precise meaning | A record is PARTIAL only when it is split into several Silver tables and some succeeded while others failed (`targets[]` shows which) |

➕ **Rule: one owning Bronze → Silver job per Bronze collection.** That job may write several Silver tables. With a second consumer job, a single `syncSilver` status becomes ambiguous. If this is ever really needed, `syncSilver` becomes a map keyed by job name (`_etl.syncSilver.<jobName>.*`), which costs extra indexes.

### 6.3 Who updates what: status lifecycle

```
                       ingestion: new event with a REAL change (hash changed, or delete/undelete)
          ┌───────────────────────────────────────────────────────────────────────────────┐
          ▼                                                                               │
   ┌─────────────┐  Spark B→S success   ┌─────────┐                                      │
   │   PENDING   │ ───────────────────► │ SUCCESS │ ─────────────────────────────────────┤
   └─────┬───────┘                      └─────────┘                                      │
         │ Spark: retryable error        ┌──────────┐  retryCount < max: picked by        │
         ├─────────────────────────────► │  FAILED  │  incremental / rerun-failed runs ───┤
         │                               └────┬─────┘                                     │
         │ Spark: non-fixable data error      │ retryCount ≥ max                          │
         ├─────────────────────────────► ┌────▼────────┐  fixed → READY_TO_REPROCESS      │
         │                               │ QUARANTINED │  (quarantine table) → reprocess ─┤
         │                               └─────────────┘                                  │
         │ Spark: some targets OK, some failed ┌─────────┐                                │
         └───────────────────────────────────► │ PARTIAL │ → rerun-failed re-processes ───┘
                                               └─────────┘   the whole doc (see doc 03)
```

**Ingestion rules on each event:**

| Situation | `data` / meta | `syncSilver` |
|---|---|---|
| New document | Insert | `status=PENDING`, `retryCount=0` |
| Update, `recordHash` **changed** | Update | Reset: `status=PENDING`, `retryCount=0`, error fields cleared |
| Update, `recordHash` **unchanged** (no-op update, e.g. only `updatedAt` touched) | Update Kafka/version metadata only | **Unchanged** → no useless re-processing ➕ |
| Delete | `isDeleted=true`, `deletedAt`, `data` kept | `status=PENDING` (Silver must propagate the delete) |
| Insert after delete (same `_id` re-created) | `isDeleted=false`, `deletedAt=null`, new `data` | `status=PENDING` |
| Stale / duplicate event (version ≤ stored) | Nothing | Nothing |

**➕ Race condition: optimistic write-back by Spark (critical).**
1. Spark reads document X at `sourceVersion = v5` (PENDING).
2. While Spark is processing, ingestion applies `v6` (PENDING again).
3. Spark finishes v5 and writes `SUCCESS` → **v6 would be marked synced but never processed.**

**Fix:** the Spark write-back filter always includes the version it read:

```javascript
// Bulk update issued by the Spark write-back step (one per record, sent in batches)
db.orders.updateOne(
  { _id: X, "_etl.sourceVersion": NumberLong(v5) },      // only if unchanged since read
  { $set: { "_etl.syncSilver.status": "SUCCESS",
            "_etl.syncSilver.syncedVersion": NumberLong(v5),
            "_etl.syncSilver.runId": "...", "_etl.syncSilver.updatedAt": ISODate() } }
)
// matchedCount = 0 → the document moved on; it stays PENDING and is picked up by the next run
```

The Silver merge is idempotent (Prompt 03), so processing v5 and then v6 is safe.

### 6.4 Bronze indexes and validator

```javascript
// db/bronze-mongo/indexes/orders.js (applied per Bronze collection by a generic script)
db.orders.createIndex(
  { "_etl.syncSilver.status": 1, "_etl.lastIngestedAt": 1 },
  { name: "ix_sync_pickup",
    partialFilterExpression: { "_etl.syncSilver.status": { $in: ["PENDING", "FAILED", "PARTIAL"] } } } // MongoDB 6.0+
);
db.orders.createIndex({ "_etl.sourceEventTs": 1 }, { name: "ix_event_ts" });          // backfill / date range
db.orders.createIndex({ "_etl.syncSilver.runId": 1 }, { name: "ix_sync_run" });        // rerun by run
db.orders.createIndex({ "_etl.sourceId": 1 }, { name: "ix_source_id" });               // ID reprocess
db.orders.createIndex({ "_etl.lastIngestedAt": 1 }, { name: "ix_last_ingested" });     // watermark / reconciliation
```

- The partial index keeps the "what must be synced" index small: SUCCESS documents (the vast majority) are not in it. On MongoDB < 6.0 (no `$in` in partial filters), use a normal compound index.
- ➕ A `$jsonSchema` validator makes the required `_etl` fields (`sourceService`, `sourceVersion`, `isDeleted`, `syncSilver.status`) mandatory, so a buggy handler cannot write incomplete metadata.

---

## 7. `etl-ingestion` Service Design (Spring Boot)

### 7.1 Processing pipeline

```
 Kafka batch (cdc.source.events + cdc.source.snapshot, pattern subscription)
      │
      ▼
 ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐  ┌───────────────┐  ┌──────────────┐
 │ Envelope     │─►│ Route        │─►│ CollectionHandler│─►│ Enricher chain│─►│ Collapse     │
 │ Parser       │  │ Resolver     │  │ (generic or      │  │ metadata,hash,│  │ last version │
 │ ExtJSON→BSON │  │ svc+db+coll  │  │  custom bean)    │  │ drift, PII    │  │ per _id      │
 └──────┬───────┘  └──────┬───────┘  └────────┬─────────┘  └──────┬────────┘  └──────┬───────┘
        │ parse error     │ unmapped          │ handler error     │                   ▼
        ▼                 ▼                   ▼                   ▼          ┌─────────────────┐
 ┌───────────────────────────────────────────────────────────────────┐      │ BronzeWriter    │
 │ DLQ publisher (dlq.ingestion) + metrics, AFTER the batch write     │◄─────│ bulkWrite with  │
 └───────────────────────────────────────────────────────────────────┘ data │ version guard   │
                                                                     errors └────────┬────────┘
                                         infra error (Mongo down/timeout)            │ ok
                                         → throw → DefaultErrorHandler retries        ▼
                                           whole batch with backoff (no DLQ)  commit offsets
                                                                              + bronze-loaded event
```

### 7.2 Package structure

```
etl-ingestion/src/main/java/com/company/etl/ingestion/
├── IngestionApplication.java
├── config/
│   ├── IngestionProperties.java          # @ConfigurationProperties for ingestion-mappings.yml
│   ├── KafkaConsumerConfig.java          # batch listener factory, error handler, backoff
│   └── MongoConfig.java                  # one MongoClient for Bronze, writeConcern=majority
├── consumer/
│   └── CdcBatchListener.java             # @KafkaListener(topicPattern=...), orchestrates the pipeline
├── parse/
│   ├── CdcEnvelope.java                  # Parsed event (record)
│   ├── CdcOperation.java                 # INSERT, UPDATE, REPLACE, DELETE, SNAPSHOT
│   └── CdcEnvelopeParser.java            # Extended JSON → BsonDocument → CdcEnvelope
├── router/
│   ├── RouteKey.java                     # (sourceService, db, collection)
│   ├── IngestionRoute.java               # Resolved route: target, handler, options
│   └── RouteResolver.java                # Lookup with defaults; unmapped → exception
├── handler/
│   ├── CollectionHandler.java            # SPI interface
│   ├── GenericCollectionHandler.java     # Default: config-driven, no code per collection
│   └── HandlerRegistry.java              # name → handler bean
├── enrich/
│   ├── Enricher.java                     # SPI interface
│   ├── MetadataEnricher.java             # _etl.* from envelope + Kafka coordinates
│   ├── RecordHashEnricher.java           # canonical SHA-256 excluding volatile fields
│   ├── SchemaDriftEnricher.java          # compares with contracts/*.yml
│   └── PiiProtectionEnricher.java        # optional field-level encryption
├── write/
│   ├── BronzeRecord.java
│   ├── BronzeWriter.java                 # bulkWrite + version guard + outcome per record
│   ├── HistoryWriter.java                # keepHistory: append-only __history collection
│   └── WriteOutcome.java                 # WRITTEN, STALE, NOOP_HASH, FAILED_DATA
├── error/
│   ├── IngestionErrorCode.java           # PARSE_ERROR, UNMAPPED_COLLECTION, HANDLER_ERROR, DOC_TOO_LARGE, ...
│   ├── NonRetryableIngestionException.java
│   ├── RetryableIngestionException.java
│   └── DlqPublisher.java
├── events/
│   └── BronzeLoadedPublisher.java        # throttled batch-level events (optional)
├── replay/
│   └── ReplayRunner.java                 # one-off replay mode (section 15)
└── metrics/
    └── IngestionMetrics.java             # Micrometer, tagged by service/collection
```

### 7.3 Route config: no code per collection

`etl-ingestion/src/main/resources/ingestion-mappings.yml`
```yaml
etl:
  ingestion:
    topicPattern: "cdc\\.source\\.(events|snapshot).*"
    groupId: etl-ingestion
    unmappedPolicy: DLQ                  # DLQ | IGNORE_WITH_METRIC
    defaults:
      handler: generic
      bronzeDatabase: "bronze_{sourceService}"   # svc-order → bronze_svc_order
      bronzeCollection: "{collection}"
      keepHistory: false
      hashExcludeFields: [updatedAt, lastModifiedAt, _class]   # volatile fields: no re-sync if only these change
      dropFields: []                     # e.g. large binary blobs not needed downstream
      piiFields: []                      # encrypted at Bronze write if PII protection is enabled
      contract: null

    routes:
      - { sourceService: svc-order,    db: order_db,    collection: orders,      keepHistory: true,
          contract: contracts/svc-order/orders.contract.yml }
      - { sourceService: svc-order,    db: order_db,    collection: order_items }
      - { sourceService: svc-order,    db: order_db,    collection: returns }
      - { sourceService: svc-customer, db: customer_db, collection: customers,
          piiFields: [email, phone, address.line1] }
      - { sourceService: svc-payment,  db: payment_db,  collection: payments,
          handler: paymentHandler,       # custom bean: e.g. normalizes legacy amount formats
          dropFields: [rawGatewayResponse] }
```

**Adding a new collection = one line here + the `$match` list of the connector.** Custom Java code is needed only for a special handler.

➕ **Guardrail:** handlers do **technical** normalization only (drop blobs, normalize `_id` type, protect PII). Business rules, cleansing and splitting belong in Silver. Otherwise Bronze stops being a faithful copy of the source, and replay/debugging get harder.

### 7.4 Code skeletons

```java
// parse/CdcOperation.java
public enum CdcOperation { INSERT, UPDATE, REPLACE, DELETE, SNAPSHOT;
    public boolean isDelete() { return this == DELETE; }
}
```

```java
// parse/CdcEnvelope.java
public record CdcEnvelope(
        String sourceService, String db, String collection,
        CdcOperation operation,
        BsonValue documentId, String sourceId,
        long sourceVersion,                 // clusterTime as long (seconds << 32 | inc)
        Instant eventTs,
        BsonDocument fullDocument,          // null for DELETE
        KafkaCoordinates kafka) {

    public record KafkaCoordinates(String topic, int partition, long offset) {}
    public RouteKey routeKey() { return new RouteKey(sourceService, db, collection); }
}
```

```java
// parse/CdcEnvelopeParser.java
@Component
public class CdcEnvelopeParser {

    public CdcEnvelope parse(ConsumerRecord<String, String> rec) {
        try {
            BsonDocument ev = BsonDocument.parse(rec.value());          // canonical Extended JSON → exact BSON types
            BsonDocument ns = ev.getDocument("ns");
            BsonValue id = ev.getDocument("documentKey").get("_id");
            CdcOperation op = resolveOperation(ev);
            long version = resolveVersion(ev, op);                        // clusterTime or snapshot timestamp
            BsonDocument full = ev.isDocument("fullDocument") ? ev.getDocument("fullDocument") : null;
            if (full != null) full.remove("_snapshotTs");

            return new CdcEnvelope(
                    ev.getString("sourceService").getValue(),
                    ns.getString("db").getValue(), ns.getString("coll").getValue(),
                    op, id, IdUtils.asString(id), version, resolveEventTs(ev, version), full,
                    new CdcEnvelope.KafkaCoordinates(rec.topic(), rec.partition(), rec.offset()));
        } catch (RuntimeException e) {
            throw new NonRetryableIngestionException(IngestionErrorCode.PARSE_ERROR, e.getMessage(), e);
        }
    }

    private long resolveVersion(BsonDocument ev, CdcOperation op) {
        if (ev.isTimestamp("clusterTime")) return ev.getTimestamp("clusterTime").getValue();
        if (op == CdcOperation.SNAPSHOT) {                                 // seconds of $$NOW, increment 0 → live events win ties
            long seconds = ev.getDocument("fullDocument").getDateTime("_snapshotTs").getValue() / 1000;
            return new BsonTimestamp((int) seconds, 0).getValue();
        }
        throw new NonRetryableIngestionException(IngestionErrorCode.MISSING_VERSION, "No clusterTime", null);
    }
    // resolveOperation(): maps insert/update/replace/delete; detects copied records → SNAPSHOT
    // resolveEventTs(): wallTime if present, else clusterTime seconds
}
```

```java
// handler/CollectionHandler.java — SPI for the rare custom case
public interface CollectionHandler {
    String name();                                            // referenced from ingestion-mappings.yml
    BronzeRecord toBronze(CdcEnvelope event, IngestionRoute route);
}
```

```java
// handler/GenericCollectionHandler.java — covers almost every collection
@Component
public class GenericCollectionHandler implements CollectionHandler {

    @Override public String name() { return "generic"; }

    @Override
    public BronzeRecord toBronze(CdcEnvelope e, IngestionRoute route) {
        BsonDocument data = e.operation().isDelete() ? null : e.fullDocument().clone();
        if (data != null) route.dropFields().forEach(f -> BsonPaths.remove(data, f));
        return BronzeRecord.from(e, route.target(), data);      // _etl block is filled by the enrichers
    }
}
```

```java
// enrich/Enricher.java
public interface Enricher {
    void enrich(BronzeRecord record, CdcEnvelope event, IngestionRoute route);
}

// enrich/RecordHashEnricher.java
@Component @Order(20)
public class RecordHashEnricher implements Enricher {
    @Override
    public void enrich(BronzeRecord r, CdcEnvelope e, IngestionRoute route) {
        if (r.data() == null) return;                           // delete: hash unchanged
        r.setRecordHash(CanonicalHash.sha256(r.data(), route.hashExcludeFields())); // sorted keys, UTF-8, excluded fields
    }
}
```

```java
// write/BronzeWriter.java — version guard + conditional syncSilver reset in ONE atomic update
@Component
@RequiredArgsConstructor
public class BronzeWriter {
    private final MongoClient mongo;
    private final IngestionMetrics metrics;

    public Map<BronzeRecord, WriteOutcome> write(BronzeTarget target, List<BronzeRecord> records) {
        MongoCollection<BsonDocument> coll = mongo.getDatabase(target.database())
                .getCollection(target.collection(), BsonDocument.class)
                .withWriteConcern(WriteConcern.MAJORITY);

        List<WriteModel<BsonDocument>> models = records.stream().map(this::toModel).toList();
        try {
            coll.bulkWrite(models, new BulkWriteOptions().ordered(false));
            return allWritten(records);
        } catch (MongoBulkWriteException ex) {
            return classify(records, ex);   // code 11000 → STALE (version guard), 10334/too large → FAILED_DATA
        } catch (MongoTimeoutException | MongoSocketException | MongoNotPrimaryException ex) {
            throw new RetryableIngestionException(ex);          // infra → retry the whole batch, never DLQ
        }
    }

    private WriteModel<BsonDocument> toModel(BronzeRecord r) {
        // Apply only if newer than the stored version (or the doc does not exist yet).
        // If a newer version exists, the filter does not match → upsert tries an insert with the same _id
        // → E11000 duplicate key → classified as STALE (expected, not an error).
        Bson filter = Filters.and(Filters.eq("_id", r.id()),
                Filters.or(Filters.lt("_etl.sourceVersion", r.sourceVersion()),
                           Filters.exists("_etl.sourceVersion", false)));

        // Aggregation-pipeline update: decides atomically whether syncSilver must be reset.
        Document realChange = new Document("$or", List.of(
                new Document("$ne", List.of("$_etl.recordHash", r.recordHash())),
                new Document("$ne", List.of("$_etl.isDeleted", r.isDelete())),
                new Document("$eq", List.of(new Document("$type", "$_etl.syncSilver.status"), "missing"))));

        Document set = new Document()
                .append("data", r.isDelete() ? "$data" : new Document("$literal", r.data()))
                .append("_etl", new Document("$mergeObjects", List.of(
                        new Document("$ifNull", List.of("$_etl", new Document())),
                        new Document("$literal", r.metadataForEvent()))))           // source/kafka/version/hash/delete fields
                .append("_etl.ingestedAt", new Document("$ifNull", List.of("$_etl.ingestedAt", "$$NOW")))
                .append("_etl.changeCount", new Document("$add", List.of(
                        new Document("$ifNull", List.of("$_etl.changeCount", 0)), 1)))
                .append("_etl.syncSilver", new Document("$cond", List.of(realChange,
                        new Document("$literal", SyncSilver.pendingReset()),          // PENDING, retryCount 0, errors cleared
                        "$_etl.syncSilver")));

        return new UpdateOneModel<>(filter, List.of(new Document("$set", set)),
                new UpdateOptions().upsert(true));
    }
}
```

> Note: in a `$set` pipeline stage, fields are evaluated against the **original** document, so `realChange` compares the stored hash with the new one before anything is overwritten. Two `$set` stages (merge `_etl` first, then dotted fields) avoid any ambiguity. Unit tests in `etl-testkit` (Testcontainers Mongo replica set) cover every row of the table in 6.3.

```java
// consumer/CdcBatchListener.java
@Component
@RequiredArgsConstructor
public class CdcBatchListener {
    private final CdcEnvelopeParser parser;
    private final RouteResolver routes;
    private final HandlerRegistry handlers;
    private final List<Enricher> enrichers;          // ordered via @Order
    private final BronzeWriter writer;
    private final HistoryWriter history;
    private final DlqPublisher dlq;
    private final BronzeLoadedPublisher loadedEvents;
    private final IngestionMetrics metrics;

    @KafkaListener(topicPattern = "${etl.ingestion.topicPattern}", groupId = "${etl.ingestion.groupId}",
                   containerFactory = "cdcBatchFactory")
    public void onBatch(List<ConsumerRecord<String, String>> batch, Acknowledgment ack) {
        String batchId = BatchIds.of(batch);
        List<Failed> dataFailures = new ArrayList<>();
        Map<BronzeTarget, Map<BsonValue, BronzeRecord>> byTarget = new LinkedHashMap<>();

        for (ConsumerRecord<String, String> rec : batch) {
            try {
                CdcEnvelope ev = parser.parse(rec);
                IngestionRoute route = routes.resolve(ev.routeKey());              // throws UNMAPPED_COLLECTION
                if (ev.operation() == CdcOperation.UPDATE && ev.fullDocument() == null) {
                    metrics.skipped(ev, "null_lookup"); continue;                  // delete follows
                }
                BronzeRecord br = handlers.get(route.handler()).toBronze(ev, route);
                br.setIngestionBatchId(batchId);
                enrichers.forEach(en -> en.enrich(br, ev, route));
                if (route.keepHistory()) history.stage(br);
                // collapse: keep only the highest version per _id inside this batch
                byTarget.computeIfAbsent(route.target(), t -> new HashMap<>())
                        .merge(br.id(), br, (a, b) -> a.sourceVersion() >= b.sourceVersion() ? a : b);
            } catch (NonRetryableIngestionException e) {
                dataFailures.add(new Failed(rec, e));
            }
        }

        // Infra failures throw RetryableIngestionException → DefaultErrorHandler re-seeks and retries the batch.
        byTarget.forEach((target, recs) -> {
            Map<BronzeRecord, WriteOutcome> out = writer.write(target, List.copyOf(recs.values()));
            out.forEach((r, o) -> { metrics.outcome(r, o); if (o == WriteOutcome.FAILED_DATA) dataFailures.add(Failed.of(r)); });
        });
        history.flush();

        dataFailures.forEach(f -> dlq.publish(f.record(), f.error(), batchId));   // after the write, so retries don't duplicate DLQ entries
        loadedEvents.record(byTarget);                                             // throttled, e.g. once per minute per collection
        ack.acknowledge();                                                         // at-least-once: commit only after Bronze write
    }
}
```

```java
// config/KafkaConsumerConfig.java (key parts)
@Bean
ConcurrentKafkaListenerContainerFactory<String, String> cdcBatchFactory(ConsumerFactory<String, String> cf,
                                                                        IngestionMetrics metrics) {
    var f = new ConcurrentKafkaListenerContainerFactory<String, String>();
    f.setConsumerFactory(cf);
    f.setBatchListener(true);
    f.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
    var backOff = new ExponentialBackOff(1_000, 2.0);
    backOff.setMaxInterval(60_000);                       // cap 1 min, retry FOREVER for infra errors
    var handler = new DefaultErrorHandler(backOff);      // no recoverer → never skips a batch on infra failure
    handler.setRetryListeners((rec, ex, attempt) -> metrics.infraRetry(attempt));
    f.setCommonErrorHandler(handler);
    return f;
}
```

### 7.5 `application.yml` (defaults; env overrides in `application-<env>.yml`)

```yaml
spring:
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP}
    consumer:
      group-id: etl-ingestion
      enable-auto-commit: false
      auto-offset-reset: earliest           # a new group never skips data
      max-poll-records: 500
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      properties:
        max.poll.interval.ms: 300000
        partition.assignment.strategy: org.apache.kafka.clients.consumer.CooperativeStickyAssignor
        fetch.max.bytes: 52428800
        max.partition.fetch.bytes: 8388608  # ≥ max.message.bytes
        isolation.level: read_committed
    producer:                                # DLQ + bronze-loaded events
      acks: all
      properties: { enable.idempotence: true, compression.type: zstd }
  data:
    mongodb:
      uri: ${BRONZE_MONGO_URI}               # from the secret manager

etl:
  ingestion:
    concurrency: 4                           # threads per pod; pods × concurrency ≤ partitions
  pii:
    enabled: false
    keyRef: ${PII_KMS_KEY_REF:}

management:
  endpoints.web.exposure.include: health,prometheus
  metrics.tags.application: etl-ingestion
```

### 7.6 The "producer" part of the custom consumer/producer

With the direct-write design (doc 01, Option A), the service **produces** only:
1. **DLQ messages** → `dlq.ingestion`
2. **Batch-level "bronze loaded" events** → `etl.events.bronze-loaded`, for example `{service, collection, count, maxSourceVersion, windowEnd}`. They are throttled (one per collection per minute), and Airflow can use them for event-triggered B→S runs.

If other teams later need the enriched stream, add an optional `EnrichedEventPublisher` that writes the `BronzeRecord` to `bronze.events`. The pipeline stays the same.

---

## 8. Ordering, Out-of-Order and Duplicate Events

**Where out-of-order or duplicate events come from:**

| Source of disorder/duplicates | Example |
|---|---|
| Connector restart | Re-emits events since the last committed resume token → duplicates |
| Consumer restart / rebalance | Re-reads since the last committed offset → duplicates |
| Kafka/DLQ replay | Old events arrive after newer ones are already applied |
| Snapshot + live overlap | Snapshot (option B/C) of an old state arrives after a live update |
| Partition increase / moving a collection to its own topic | Short window where one document's events are split across partitions/topics |

**One mechanism handles all of them: the version guard.**

```
apply(event)  ⇔  event.sourceVersion  >  bronze._etl.sourceVersion   (or the doc does not exist)
equal         →  duplicate → skip (metric: outcome=stale)
smaller       →  older     → skip (metric: outcome=stale)
```

- `sourceVersion` = `clusterTime`. It is strictly increasing per document within a replica set, and change streams on sharded clusters are also ordered by `clusterTime`.
- Kafka offsets are **not** used as the version: offsets are only comparable within one partition of one topic, and they break on replay, snapshot topics or topic moves.
- Every event carries the **full state** (`updateLookup`), so "last version wins" is always correct. No delta merging is needed.
- In-batch collapse (keep the highest version per `_id`) reduces write load for hot documents.

➕ **Multi-document source transactions** are not atomic in Bronze: each document is applied on its own. This is acceptable for an analytics pipeline. Silver handles parent/child timing (Prompt 03).

---

## 9. Offset Management and Delivery Semantics

| Aspect | Decision |
|---|---|
| Commit | Manual, **after** the Bronze bulk write (and the DLQ publish) succeeds for the whole batch |
| Auto commit | Disabled |
| New consumer group | `auto.offset.reset=earliest` |
| Rebalance | Cooperative sticky assignor; on revoke, in-flight batches finish and commit |
| Semantics | **At-least-once delivery + idempotent writes = effectively exactly-once results** |

**Why not Kafka exactly-once (transactions)?** Kafka EOS covers Kafka → Kafka. The sink here is MongoDB, outside the Kafka transaction. True EOS would mean storing offsets inside Mongo in the same transaction, which adds complexity and slows writes. The version guard already makes duplicates harmless.

➕ Optional: Kafka Connect exactly-once **source** support (KIP-618, Kafka 3.3+) reduces duplicates after connector restarts. It is nice to have, but not required because of the version guard.

---

## 10. Soft Delete (and Hard Delete)

**Flow:**
```
source: db.orders.deleteOne({_id: X})
  → change event {operationType: delete, documentKey: {_id: X}, clusterTime: T}
  → ingestion: version guard (T > stored) →
      $set _etl.isDeleted = true, _etl.deletedAt = eventTs, _etl.deleteReason = SOURCE_DELETE,
           _etl.cdcOperation = DELETE, _etl.sourceVersion = T, _etl.syncSilver = PENDING (reset)
      data stays as the last known state (useful for audit and for Silver to know WHAT was deleted)
  → Spark B→S: sees isDeleted=true → applies the delete strategy in Silver (soft delete flag, Prompt 03)
```

| Case | Handling |
|---|---|
| Delete for a document never seen in Bronze | Upsert a **tombstone** document: `data: null`, `isDeleted: true`. It protects against an older insert arriving later (version guard) |
| Re-insert with the same `_id` after a delete | Newer version → `isDeleted=false`, `deletedAt=null`, new data, `PENDING` |
| Delete missed while CDC was broken | Reconciliation after a re-snapshot (section 4.3) → `deleteReason=RECONCILED_MISSING` |

➕ **Hard delete / right to be forgotten (GDPR/DPDP):** soft delete keeps personal data forever, which can violate privacy law. Add a separate, audited **purge** process: a request (`sourceService`, `collection`, `sourceId`) → physical delete or anonymization in Bronze + `__history` + Silver + Gold + quarantine + DLQ. It is triggered from the ops API and logged in the control DB. Design detail: Prompt 11.

➕ **Retention for soft-deleted records:** optionally purge Bronze documents that are `isDeleted=true` and fully synced (`syncSilver=SUCCESS`) for longer than N days, if Bronze storage grows too much. This is configured per route.

---

## 11. Error Handling and DLQ

### 11.1 Error classification

| Error | Example | Class | Action |
|---|---|---|---|
| Parse error | Invalid JSON, missing `ns` | Data, non-retryable | DLQ `PARSE_ERROR` |
| Unmapped collection | New collection captured but no route | Config, non-retryable | DLQ `UNMAPPED_COLLECTION` (or ignore + metric, per `unmappedPolicy`) + alert |
| Missing version | No `clusterTime` and not a snapshot | Data | DLQ `MISSING_VERSION` |
| Handler/enricher exception | Custom handler bug, bad field type | Data/code | DLQ `HANDLER_ERROR` |
| Document too large / invalid for Mongo | Mongo error 10334, invalid field names | Data | DLQ `BRONZE_WRITE_REJECTED` |
| Stale/duplicate | E11000 from the version guard | Expected | Skip, metric `outcome=stale` |
| Mongo down / timeout / not primary / network | Infra | **Retryable** | Retry the whole batch with exponential backoff, **forever**; alert if stuck > 5 min. **Never DLQ** (otherwise an outage dumps everything into the DLQ) |
| Poison message (always crashes, e.g. OOM) | Pathological document | Data | Detected by `attempt > N` on the same offset → DLQ `POISON_MESSAGE` |

### 11.2 Flow diagram

```
                        ┌────────────────────────────┐
  cdc.source.events ───►│  CdcBatchListener          │
  cdc.source.snapshot   └─────────────┬──────────────┘
                                      │ per record
              ┌───────────────────────┼─────────────────────────────────────┐
              ▼                       ▼                                     ▼
       parse / route / handler     write to Bronze                    infra exception
       error (data)                (bulkWrite, version guard)         (Mongo down, timeout)
              │                       │                                     │
              │          ┌────────────┼──────────────┐                      ▼
              │          ▼            ▼              ▼            DefaultErrorHandler
              │       WRITTEN       STALE      FAILED_DATA        exponential backoff
              │       (metric)   (skip, metric)    │              (1s → 60s cap, forever)
              │                                    │              seek + re-poll batch
              ▼                                    ▼                     │
       ┌──────────────────────────────────────────────────┐              │ stuck > 5 min
       │  DlqPublisher → dlq.ingestion                     │              ▼
       │  value = original bytes, key = original key       │        ALERT (ops on-call)
       │  headers = error code, message, origin coords, … │
       └──────────────────────┬────────────────────────────┘
                              │ index (metadata only)
                              ▼
       ┌──────────────────────────────────────────────────┐
       │ etl_ctl.ingestion_dlq_index  (Prompt 05)          │  ← ops UI: search, filter, mark resolved
       │ dlq offset, error code, service, collection,     │
       │ sourceId, firstSeen, status (NEW/REPLAYED/IGNORED)│
       └──────────────────────┬────────────────────────────┘
                              │ after fix (code deploy / route added / data fixed at source)
                              ▼
       ┌──────────────────────────────────────────────────┐
       │ DLQ replay (ops API / CLI): select by error code, │
       │ collection, time → re-publish to cdc.source.events│
       │ with SAME key + header x-dlq-replay=<ticket>      │──► normal pipeline (version guard
       └──────────────────────────────────────────────────┘    protects against old data)
```

### 11.3 DLQ message format

| Part | Content |
|---|---|
| Key | Original key (`cdcKey`) → replay lands on the same partition |
| Value | Original bytes, unchanged |
| Headers | `x-error-code`, `x-error-class`, `x-error-message` (truncated, no PII), `x-exception-fingerprint` (hash of the stack trace), `x-origin-topic`, `x-origin-partition`, `x-origin-offset`, `x-source-service`, `x-source-collection`, `x-source-id`, `x-ingestion-batch-id`, `x-failed-at`, `x-attempts`, `x-app-version` |

### 11.4 DLQ replay rules

- Replay goes back into the **main topic** with the same key, so there is one code path for everything. The version guard makes replaying old events safe.
- The replay tool records the DLQ offsets it processed in `ingestion_dlq_index` (`REPLAYED` + ticket), so nothing is replayed twice by accident.
- Events that are not worth fixing are marked `IGNORED` with a reason (an audit trail, not a silent drop).
- ➕ An alert fires when the DLQ inflow rate exceeds a threshold or when any `UNMAPPED_COLLECTION` appears.

---

## 12. Schema Evolution and Schema Registry

### 12.1 What happens on schema changes

| Source change | Effect in ingestion/Bronze |
|---|---|
| New field | Stored automatically in `data`. Drift detection flags it (`schemaDrift: ["NEW_FIELD:discountCode"]`) |
| Removed field | Simply absent in new documents. Flagged if the contract marks it as required |
| Type change (e.g. `amount` string → decimal) | Stored as-is (Bronze is faithful). Flagged `TYPE_CHANGE:amount`. **Silver DQ** decides whether it is valid |
| Renamed collection / new collection | Not captured until added to the connector `$match` and the routes (a deliberate, reviewed change) |

Ingestion **never blocks** on schema drift: Bronze must stay complete. Drift is surfaced as a metric, a flag and an alert, and enforced in Silver (Prompt 03).

### 12.2 Schema Registry: needed?

| Option | Pros | Cons |
|---|---|---|
| **Phase 1 (chosen): schemaless canonical Extended JSON + data contracts + drift detection** | Works with polymorphic Mongo documents; no schema-inference fragility; zero friction for new fields | No broker-side compatibility enforcement; larger messages than Avro |
| Phase 2 (optional): Avro/JSON Schema + Schema Registry, `TopicRecordNameStrategy` (subject = `cdc.source.events-<recordName>`, one record name per collection), `BACKWARD` compatibility | Compatibility checks at produce time; compact; schema discovery for governance | Mongo documents are often heterogeneous (optional and polymorphic fields). The connector's schema inference (`output.schema.infer.value`) can produce unstable schemas and rejected events |

**Recommendation:** start with phase 1. Re-evaluate phase 2 when source teams can commit to strict document schemas (data contracts in CI). This **refines doc 01**: there, the Schema Registry was listed as a guardrail. Here the guardrail is implemented through **data contracts + drift detection**, and the registry becomes optional.

**Data contract example** (`contracts/svc-order/orders.contract.yml`):
```yaml
contract:
  sourceService: svc-order
  collection: orders
  owner: team-order
  version: 3
  fields:
    _id:      { type: objectId, required: true }
    orderNo:  { type: string,   required: true }
    amount:   { type: decimal,  required: true }
    status:   { type: string,   required: true, allowed: [NEW, CONFIRMED, SHIPPED, CANCELLED] }
    customerId: { type: objectId, required: true }
    email:    { type: string,   pii: true }
  unknownFields: WARN          # WARN | IGNORE
```

---

## 13. Metrics and Monitoring (per collection, although Kafka sees one topic)

**Micrometer metrics** (Prometheus), tags: `service`, `collection`, `op`, `outcome`, `topic`:

| Metric | Type | Purpose |
|---|---|---|
| `etl_ingest_events_total{service,collection,op,outcome}` | counter | Throughput per collection; outcome = `written` / `stale` / `noop_hash` / `dlq` / `skipped_null_lookup` |
| `etl_ingest_event_lag_seconds{service,collection}` | gauge | **Per-collection lag** = now − `sourceEventTs` of the last applied event |
| `etl_ingest_last_event_timestamp{service,collection}` | gauge | Freshness: detects a silent stop of CDC for one collection ➕ |
| `etl_ingest_dlq_total{service,collection,error_code}` | counter | DLQ inflow by reason |
| `etl_ingest_batch_write_seconds{database}` | histogram | Mongo bulk write latency |
| `etl_ingest_batch_size` | histogram | Records per poll |
| `etl_ingest_infra_retry_total` / `etl_ingest_infra_retry_active` | counter / gauge | Mongo/infra outage visibility |
| `etl_ingest_schema_drift_total{service,collection,kind}` | counter | New fields / type changes |
| Kafka consumer lag (per partition) | from Kafka lag exporter | Classic lag. Event-time lag above is the per-collection view |
| Connector status / task state | from the Connect REST API exporter | Connector FAILED, restarts |
| Mongo oplog window (source) | Mongo exporter | Must stay > 48 h |

**Alerts (examples):**

| Alert | Condition |
|---|---|
| CDC lag high | `etl_ingest_event_lag_seconds > 900` for 10 min (per collection; thresholds per route) |
| Collection silent | No events for a collection for longer than its expected max gap (configured per route, e.g. 6 h) |
| Connector failed | Any connector/task state `FAILED` |
| Infra retry stuck | `etl_ingest_infra_retry_active == 1` for 5 min |
| DLQ spike | DLQ rate > X/min or any `UNMAPPED_COLLECTION` |
| Oplog window low | < 48 h on any source |

➕ The source `drop` / `rename` / `dropDatabase` / `invalidate` events are filtered out of the data pipeline. A tiny second change stream (or the Mongo audit log) **alerts** on them, because a dropped source collection must never go unnoticed.

---

## 14. Backpressure, Scaling and Poison Messages

| Concern | Handling |
|---|---|
| Consumer slower than producer | Lag grows; scale pods. **Max parallelism = partitions** (24 in prod) |
| Auto-scaling ➕ | KEDA `kafka` scaler on consumer-group lag, min 2 / max `partitions ÷ concurrency` pods |
| Mongo overloaded | Bulk write latency rises → the listener naturally slows down (synchronous writes). If p99 > threshold, the `pause()`/`resume()` container API throttles the poll |
| Long batches | `max.poll.records=500` + bulk writes keep each poll far below `max.poll.interval.ms` |
| Hot document | In-batch collapse keeps only the last version → 1 write per batch per hot document |
| Big snapshot | Separate snapshot topic; optional separate consumer group/deployment with a lower priority |
| Poison message | Per-record exceptions go straight to the DLQ. For crashes outside the per-record `try` (OOM, native errors), Spring's `DefaultErrorHandler` delivery attempts header + a cap on the same offset → DLQ `POISON_MESSAGE` |
| Graceful shutdown | Finish the current batch, commit, then exit (`spring.lifecycle.timeout-per-shutdown-phase=60s`) |

---

## 15. Replay and Recovery

### 15.1 Recovery scenarios

| Scenario | Tool | Duplicates? |
|---|---|---|
| Bronze write bug for some hours (wrong data written) | **Replay from timestamp** with a collection filter and `--force-reapply`, after deploying the fix | No ⚠ see note below |
| Events stuck in the DLQ | **DLQ replay** (section 11.4) | No |
| Bronze collection dropped/corrupted, within Kafka retention | Replay from the earliest offset (or the timestamp of the last good backup) for that collection | No |
| Bronze lost beyond retention | Restore the Mongo backup + replay from the backup time, or a **re-snapshot** (section 4.3) | No |
| Connector lost its resume token (oplog rolled over) | Re-snapshot + reconciliation | No |

> ⚠ **Note on replaying to fix wrong data:** if a bug wrote wrong data with the **correct** `sourceVersion`, a replay is skipped as "not newer". For bug-fix replays, the replay tool runs with `--force-reapply`. It then uses `>=` instead of `>` in the version guard, so the same version is re-applied and the hash comparison resets `syncSilver` only where the data actually changes.

### 15.2 Replay runner

A replay never touches the live consumer group. It runs as a **one-off job** (K8s Job using the same image) with its own consumer group:

```bash
java -jar etl-ingestion.jar --spring.profiles.active=prod,replay \
  --etl.replay.ticket=INC-1042 \
  --etl.replay.groupId=etl-ingestion-replay-INC-1042 \
  --etl.replay.fromTimestamp=2026-10-27T00:00:00Z \
  --etl.replay.toTimestamp=2026-10-28T06:00:00Z \
  --etl.replay.filter.services=svc-order \
  --etl.replay.filter.collections=orders,order_items \
  --etl.replay.forceReapply=false
```

Behaviour:
1. `offsetsForTimes(fromTimestamp)` for every partition of the selected topic(s) → seek.
2. Consume until `toTimestamp` on every partition, then stop.
3. Events outside the filter are skipped cheaply (only the key/headers are inspected: `cdcKey` starts with `svc-order:orders:`).
4. The same pipeline (parse → route → handler → enrich → **version guard write**) runs.
5. A summary is written to the control DB (`ingestion_replay_audit`: ticket, range, counts written/stale/dlq) and is visible in the ops UI. The ops API triggers this job for the **replay run mode**.

Afterwards, the Bronze documents that changed are `PENDING`, so the normal Bronze → Silver incremental run (or a cascade rerun, Prompt 07) carries the fix downstream.

---

## 16. Security for the Ingestion Layer

| Area | Control |
|---|---|
| Kafka | TLS + SASL (SCRAM/OAuth). ACLs: Connect principal = write on `cdc.source.*`; `etl-ingestion` = read `cdc.source.*`, write `dlq.ingestion` and `etl.events.*`. No other consumers on the raw CDC topics |
| Secrets | Connector URIs via `ConfigProvider` files mounted from Vault/K8s Secrets; Spring apps via the secret manager (Prompt 08) |
| Source Mongo | Read-only CDC user per source DB with `changeStream` + `find` only |
| Bronze Mongo | `etl-ingestion` = readWrite on `bronze_*`; Spark = read + update `_etl.syncSilver.*` (custom role); humans = read via the ops API only |
| PII in Bronze ➕ | Optional `PiiProtectionEnricher`: field-level encryption of `piiFields` (deterministic encryption for join keys such as email, randomized for the rest), keys in KMS. Alternative: MongoDB client-side field-level encryption (CSFLE/Queryable Encryption, edition-dependent). Final choice in Prompt 11 |
| DLQ | Same sensitivity as the source data: same ACLs, error messages never contain field values |
| Audit | Connector deploys (CI), replay runs and DLQ actions are logged with user + ticket |

---

## 17. Testing

| Level | What |
|---|---|
| Unit | Parser (all op types, snapshot, Extended JSON types), route resolution, hash canonicalization, every row of the 6.3 rules table, error classification |
| Component | `BronzeWriter` against Testcontainers MongoDB **replica set**: version guard, stale E11000, tombstones, conditional `syncSilver` reset, `$literal` safety for fields starting with `$` |
| Integration | Testcontainers Kafka + Kafka Connect + Mongo replica set: real change events end to end, including deletes, restarts (duplicates) and snapshot + live overlap |
| Contract | Connector config validation in CI; `ingestion-mappings.yml` schema validation; every connector `$match` collection has a route (and vice versa) ➕ |
| Chaos/recovery | Kill a pod mid-batch, stop Mongo for 2 minutes, replay a range: Bronze must end up identical |

---

## 18. ➕ Added by Architect (summary)

| # | Addition | Why |
|---|---|---|
| B1 | Message key built in the connector pipeline (`cdcKey` = service:coll:id) | Per-document ordering; avoids `_id` collisions across services |
| B2 | Canonical Extended JSON value format | Exact BSON types end to end (Decimal128 money values stay exact) |
| B3 | Heartbeats + oplog window ≥ 72 h + alert | Prevents lost resume tokens and forced re-snapshots |
| B4 | Separate snapshot topic + snapshot timestamp versioning | Big loads don't starve live CDC; still ordering-safe |
| B5 | Version guard on `clusterTime` (single mechanism for duplicates, disorder, replay, snapshot) | Simple, provable idempotency |
| B6 | `recordHash` no-op detection | Avoids re-processing millions of "touch only" updates |
| B7 | Optimistic write-back filter on `sourceVersion` | Fixes the Spark-vs-ingestion race that would mark unprocessed changes as synced |
| B8 | `data` + `_etl` document shape, `envelopeVersion` | No field collisions; future-proof metadata |
| B9 | Optional `__history` collections | SCD2 needs intermediate versions that "latest state" Bronze would lose |
| B10 | One owning B→S job per Bronze collection | Keeps `syncSilver` unambiguous |
| B11 | Infra errors retried forever, never DLQ'd | An outage must not turn into thousands of DLQ entries or data loss |
| B12 | DLQ index in the control DB + replay with tickets | Searchable, auditable, no double replay |
| B13 | Tombstones + reconciliation (`RECONCILED_MISSING`) | Correct deletes even when CDC was broken |
| B14 | GDPR/DPDP purge path | Soft delete alone conflicts with privacy laws |
| B15 | Per-collection event-time lag + freshness alerts + drop/rename alerts | Visibility despite the single topic |
| B16 | Schema Registry → phase 2; data contracts + drift detection now | Fits schemaless Mongo without blocking ingestion |
| B17 | Partial index for the sync pick-up query | Small, fast index for incremental Bronze → Silver reads |
| B18 | Over-provisioned partitions | Repartitioning later temporarily breaks ordering |
| B19 | `--force-reapply` replay option | Fixes bad data written with a correct version |

---

## 19. Changes to Earlier Decisions (doc 01)

| Doc 01 said | Now | Reason |
|---|---|---|
| Message key = `collection + documentKey` | Key = `sourceService:collection:documentId` (`cdcKey`, built in the connector pipeline) | Avoids collisions across services; the key is a simple string |
| Schema Registry as a guardrail | Optional (phase 2). Phase 1 = data contracts + drift detection | Mongo documents are schemaless/polymorphic |
| Topic list: `cdc.source.events`, `dlq.ingestion`, `etl.events.bronze-loaded` | + `cdc.source.snapshot`, `__mongodb_heartbeats` | Snapshot isolation, resume-token freshness |
| Bronze = MongoDB (preliminary) | **Final:** MongoDB, `bronze_<service>.<collection>`, `{_id, data, _etl}` | See section 5 |

---

## 20. Key Decisions

1. **Connectors:** one MongoDB source connector per source DB; `$match` selects collections and op types; `$addFields` adds `sourceService`, `envelopeVersion`, `cdcKey`; `topic.namespace.map {"*": "cdc.source.events"}`; `publish.full.document.only=false`; `updateLookup`; canonical Extended JSON; heartbeats; `errors.tolerance=none`. Base + env overlay, deployed by an idempotent `PUT` script; secrets via `ConfigProvider`.
2. **Key:** `cdcKey = <sourceService>:<collection>:<documentId>` → per-document ordering.
3. **Topics:** `cdc.source.events` (prod 24 partitions, RF 3, delete policy, 14 days), `cdc.source.snapshot`, `dlq.ingestion` (30 days), `etl.events.bronze-loaded`, `__mongodb_heartbeats`.
4. **Initial load:** `copy_existing` by default; a separate snapshot connector/topic for large collections; Spark bulk copy only for very large ones; re-snapshot + reconciliation for recovery.
5. **Bronze:** MongoDB, `bronze_<service>.<collection>`, `_id` = source `_id`, document = `{_id, data, _etl}`; optional `__history` per collection.
6. **Metadata:** the `_etl` field list in section 6.1; ingestion owns the source/kafka/version/hash/delete fields; Spark owns `_etl.syncSilver.*`; no `syncGold` and no `isQuarantined` in Bronze.
7. **Idempotency:** version guard on `sourceVersion` (= `clusterTime`), conditional `syncSilver` reset by `recordHash`, optimistic write-back by Spark on `sourceVersion`.
8. **Delivery:** at-least-once, manual commit after the Bronze write, idempotent writes. No Kafka transactions.
9. **Errors:** data errors → `dlq.ingestion` (original bytes + rich headers) + `ingestion_dlq_index`; infra errors → infinite backoff + alert; stale events → skip + metric; DLQ replay to the main topic with tickets.
10. **Schema:** phase 1 = canonical JSON + data contracts + drift detection (never blocking); phase 2 = optional Schema Registry with `TopicRecordNameStrategy`.
11. **Monitoring:** per-collection throughput, event-time lag, freshness, DLQ, drift and infra-retry metrics; connector, oplog and drop/rename alerts.
12. **Replay:** one-off replay job with its own consumer group, time range + service/collection filters, optional `--force-reapply`, audited in the control DB.
13. **Soft delete:** `isDeleted`/`deletedAt`/`deleteReason`, tombstones for unknown documents, undelete on re-insert, plus a separate audited GDPR purge path.
