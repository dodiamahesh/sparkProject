# CDC Tool Comparison – Debezium કે બીજું? (ગુજરાતી)

> Medallion pipeline ના **Stage 1 (Source: MongoDB + MySQL) → Stage 2 (Bronze: MongoDB)** માટે Kafka Connect સાથે કયું tool વાપરવું, તેની સરખામણી.
> સંબંધિત: [MEDALLION_PIPELINE_PLAN_GU.md](MEDALLION_PIPELINE_PLAN_GU.md), [MEDALLION_SETUP_GUIDE_GU.md](MEDALLION_SETUP_GUIDE_GU.md)
>
> 📝 **નોંધ:** શરૂઆતમાં SQL source તરીકે EDB Postgres Advanced Server (EPAS) હતું. હવે તેની જગ્યાએ **MySQL** છે. આ document MySQL પ્રમાણે update કરેલું છે.

---

## અનુક્રમણિકા
1. [Debezium ફરજિયાત છે?](#1-debezium-ફરજિયાત-છે)
2. [બધા વિકલ્પો](#2-બધા-વિકલ્પો)
3. [Debezium વાપરો કે નહીં – સીધી સરખામણી](#3-debezium-વાપરો-કે-નહીં--સીધી-સરખામણી)
4. [વિકલ્પ A: Debezium વાપરો તો](#4-વિકલ્પ-a-debezium-વાપરો-તો)
5. [વિકલ્પ B: Debezium વગર (JDBC Source) તો](#5-વિકલ્પ-b-debezium-વગર-jdbc-source-તો)
6. [Feature-wise સરખામણી](#6-feature-wise-સરખામણી)
7. [અંતિમ નિર્ણય](#7-અંતિમ-નિર્ણય)
8. [મહત્વના શબ્દો](#8-મહત્વના-શબ્દો)

---

## 1. Debezium ફરજિયાત છે?

**ના.**
- **Kafka Connect** એક **framework** છે. તે DB અને Kafka ને જોડવા માટે connectors ચલાવે છે.
- **Debezium** તેના માટેના **source connectors નો એક set** છે (MongoDB, MySQL, PostgreSQL, Oracle...).

```
Kafka Connect (framework)
   ├─ Debezium connectors          ← એક વિકલ્પ
   ├─ MongoDB Kafka Connector      ← બીજો વિકલ્પ
   └─ JDBC Source Connector        ← ત્રીજો વિકલ્પ
```

---

## 2. બધા વિકલ્પો

| વિકલ્પ | Source | પ્રકાર | Delete પકડે? | ક્યારે વાપરવું |
|---|---|---|---|---|
| **Debezium** | Mongo + MySQL | સાચું CDC (DB નો change log વાંચે) | ✅ | Production-grade CDC ✅ **(પસંદ કરેલ)** |
| **MongoDB Kafka Connector (Source)** | Mongo | સાચું CDC (change streams) | ✅ | Mongo માટે official tool. આ જ plugin sink પણ છે |
| **JDBC Source Connector** (Confluent) | MySQL | Polling: થોડી સેકન્ડે `SELECT ... WHERE updated_at > ?` | ❌ (soft delete column જોઈએ) | CDC ન ચાલે ત્યારે fallback. Table માં `updated_at` જરૂરી |
| **Spark Structured Streaming** (Mongo connector) | Mongo | Change streams, **Kafka વગર** | ✅ | Kafka શીખવું ન હોય ત્યારે. MySQL માટે આવી સુવિધા નથી |
| **Flink CDC / custom Java app** | બંને | CDC | ✅ | વધારાનું tool અથવા વધુ code. અત્યારે જરૂર નથી |

---

## 3. Debezium વાપરો કે નહીં – સીધી સરખામણી

**Debezium ન વાપરો તો Mongo અને MySQL માટે અલગ tools વાપરવા પડે.** Debezium સિવાય એવું કોઈ એક tool નથી જે **બંને** DB નું સાચું CDC કરે.

| | **વિકલ્પ A: Debezium** | **વિકલ્પ B: Debezium વગર** |
|---|---|---|
| Mongo source | Debezium MongoDB connector | MongoDB Kafka Connector (source) |
| MySQL source | Debezium MySQL connector (binlog) | **JDBC Source Connector** (polling) |
| Bronze sink (બંનેમાં) | MongoDB Kafka Connector (sink) | MongoDB Kafka Connector (sink) |
| Plugins | 2 families (Debezium + MongoDB connector) | 2 (MongoDB connector + JDBC connector) |

```
વિકલ્પ A:
  Mongo ──(Debezium Mongo, change streams)──┐
                                            ├──► Kafka ──(MongoDB Sink)──► Bronze
  MySQL ──(Debezium MySQL, binlog)──────────┘

વિકલ્પ B:
  Mongo ──(MongoDB Source, change streams)──┐
                                            ├──► Kafka ──(MongoDB Sink)──► Bronze
  MySQL ──(JDBC Source, SELECT polling)─────┘
```

**Mongo બાજુ મોટો ફરક નથી.** બંને tools change streams વાંચે છે.
**ખરો ફરક MySQL બાજુ છે:** Debezium (binlog વાંચે) વિરુદ્ધ JDBC (query થી polling).

---

## 4. વિકલ્પ A: Debezium વાપરો તો

### ✅ ફાયદા
- **Delete પણ પકડાય.** Source માં row delete થાય તો bronze માં `__deleted="true"` આવે.
- **દરેક change પકડાય.** એક row 1 મિનિટમાં 5 વાર update થાય, તો પાંચેય events મળે.
- **Source tables માં ફેરફાર નહીં.** `updated_at` કે `is_deleted` columns ઉમેરવાની જરૂર નથી.
- **DB પર ઓછો ભાર.** વારંવાર `SELECT` નથી; MySQL નો binlog વંચાય છે.
- **Latency ઓછી.** લગભગ real-time.
- **જૂનો data પણ આવે.** `snapshot.mode=initial` connector શરૂ થાય તે પહેલાનો data પણ લાવે છે, gap વગર.
- **MySQL ને officially support કરે છે** ✅. EPAS વખતનું compatibility જોખમ હવે નથી.
- **તમારી MySQL પહેલેથી તૈયાર છે.** Binlog ON, `ROW`, `FULL` (ચકાસેલું).
- **બંને sources નું event format અને SMTs એકસરખા.**
- **Industry standard CDC tool.** શીખવાનું મૂલ્ય વધારે છે.

### ❌ ગેરફાયદા / ધ્યાન રાખવાનું
- **MySQL user ને replication privileges જોઈએ.** `REPLICATION SLAVE`, `REPLICATION CLIENT` વગેરે (setup guide B4).
- **Binlog expiry.** Connector `binlog_expire_logs_seconds` (તમારે 30 દિવસ) થી વધુ સમય બંધ રહે, તો જૂનો binlog ભૂંસાઈ જાય અને **ફરી snapshot** કરવો પડે.
- **શીખવા માટે થોડું વધુ.** Config અને concepts વધારે છે.

---

## 5. વિકલ્પ B: Debezium વગર (JDBC Source) તો

### ✅ ફાયદા
- **MySQL માં કોઈ ખાસ setup નહીં.** ફક્ત read access વાળો user જોઈએ.
- **સમજવામાં સરળ.** થોડી સેકન્ડે એક query ચાલે છે, એટલું જ.

### ❌ ગેરફાયદા
- **Delete પકડાતા નથી ❌.** Source માં **soft delete** (`is_deleted` column) ફરજિયાત છે.
- **દરેક table માં `updated_at` column ફરજિયાત.** Index સાથે, અને update થાય ત્યારે આપોઆપ બદલાય (`ON UPDATE CURRENT_TIMESTAMP`).
- **વચ્ચેના changes ગુમ થાય.** બે polling વચ્ચે row 5 વાર બદલાય, તો ફક્ત **છેલ્લી value** મળે.
- **DB પર સતત ભાર.** દર થોડી સેકન્ડે દરેક table પર query ચાલે છે.
- **Latency વધારે.** Polling interval જેટલી.
- **Clock અને transaction ની સમસ્યા.** લાંબા transactions ને કારણે કોઈ row ક્યારેક miss થઈ શકે.
- **License.** Confluent Community License: વાપરવા free છે, પણ Apache open source નથી.

---

## 6. Feature-wise સરખામણી

| Feature | વિકલ્પ A (Debezium) | વિકલ્પ B (MongoDB Source + JDBC Source) |
|---|---|---|
| Insert પકડે | ✅ | ✅ |
| Update પકડે | ✅ દરેક update | ⚠️ ફક્ત છેલ્લી value |
| Delete પકડે | ✅ | Mongo ✅ / MySQL ❌ (soft delete જોઈએ) |
| Source table માં ફેરફાર | ❌ જરૂર નથી | MySQL: `updated_at` + `is_deleted` |
| MySQL server config | Binlog ROW/FULL (તમારે પહેલેથી ✅) + replication user | કંઈ નહીં (read user) |
| જૂનો (existing) data | ✅ Initial snapshot, gap વગર | ✅ પહેલી query માં (`updated_at` જોઈએ) |
| Source DB પર ભાર | ઓછો (log વાંચે) | વધુ (સતત polling) |
| Latency | < 1 સેકન્ડ | Polling interval |
| Official support | ✅ MySQL + MongoDB | ✅ |
| Operational જોખમ | Binlog expiry → ફરી snapshot | Missed rows (long transactions) |
| Event format | બંને sources નું એકસરખું | બે અલગ formats |
| License | Apache 2.0 | MongoDB: Apache 2.0 / JDBC: Confluent Community License |
| શીખવાનું મૂલ્ય | ⭐⭐⭐ | ⭐⭐ |

---

## 7. અંતિમ નિર્ણય

### ✅ વિકલ્પ A: બંને sources માટે Debezium
| Source | Connector |
|---|---|
| MongoDB | **Debezium MongoDB connector** (બંને sources નું format એકસરખું રાખવા) |
| MySQL | **Debezium MySQL connector** |
| Bronze sink | **MongoDB Kafka Connector (sink)** |

**કારણો:**
1. **Delete પકડવા.** નહીંતર silver અને gold માં deleted data પણ ગણાતો રહે.
2. **Source tables માં ફેરફાર ન કરવા.**
3. **Connector શરૂ થાય તે પહેલાનો જૂનો data** initial snapshot થી gap વગર આવે.
4. **MySQL officially supported છે**, અને તમારી MySQL પહેલેથી તૈયાર છે. જોખમ ઓછું છે.

### Plan B
Debezium MySQL કોઈ કારણસર ન ચાલે, તો **JDBC Source Connector** + `updated_at` (`ON UPDATE CURRENT_TIMESTAMP`) + `is_deleted` soft delete.

---

## 8. મહત્વના શબ્દો

| શબ્દ | અર્થ |
|---|---|
| **CDC (Change Data Capture)** | DB માં થતા insert/update/delete ને event તરીકે પકડવા |
| **Change Streams** | MongoDB ની સુવિધા, જે changes ને stream તરીકે આપે છે (replica set જરૂરી) |
| **Binlog (Binary Log)** | MySQL નો change log. દરેક ફેરફાર અહીં નોંધાય છે. Debezium તેને વાંચે છે |
| **`binlog_format=ROW` / `binlog_row_image=FULL`** | Binlog માં આખી row (પહેલાની અને પછીની values) નોંધાય. Debezium માટે જરૂરી |
| **Initial Snapshot** | Connector પહેલી વાર શરૂ થાય ત્યારે હાલનો બધો data વાંચવો, પછી live changes |
| **Offset** | Connector ક્યાં સુધી વાંચી ગયો તેની નોંધ (Kafka ના `connect-offsets` topic માં) |
| **Polling** | વારંવાર query ચલાવીને નવો data શોધવો |
| **Soft Delete** | Row ને ખરેખર delete કરવાને બદલે deleted તરીકે mark કરવી |
| **SMT (Single Message Transform)** | Kafka Connect ની અંદર message માં નાનો ફેરફાર (envelope કાઢવું, field ઉમેરવું, topic નું નામ બદલવું) |
| **DLQ (Dead Letter Queue)** | ખરાબ messages માટેનો અલગ topic, જેથી આખો connector અટકે નહીં |
