# ETL પ્લેટફોર્મ — સંપૂર્ણ Architecture નો વિગતવાર સારાંશ (ગુજરાતીમાં)

> આ document આપણી આખી ચર્ચા (docs 01 થી 12) નો **એક જગ્યાએ, સરળ ગુજરાતીમાં** સારાંશ છે.
> Technical શબ્દો (Kafka, Spark, Bronze, merge વગેરે) જાણી જોઈને **English માં જ** રાખ્યા છે, કારણ કે code, tools અને team ની વાતચીતમાં એ જ શબ્દો વપરાશે.
> કોઈ પણ વિષયની પૂરી વિગત માટે કૌંસમાં આપેલ document જુઓ, જેમ કે (doc 03).

---

## 0. ટૂંકા શબ્દો (Abbreviations) અને તેનો અર્થ

| ટૂંકું રૂપ | પૂરું નામ | સરળ અર્થ |
|---|---|---|
| ETL | Extract, Transform, Load | data કાઢવો, સાફ/બદલવો, બીજે મૂકવો |
| CDC | Change Data Capture | source database માં થતો દરેક ફેરફાર (insert/update/delete) પકડવો |
| DB | Database | જ્યાં data સંગ્રહાય |
| EPAS | EDB Postgres Advanced Server | PostgreSQL આધારિત આપણો database (Silver, Gold, control) |
| B2S / S2G | Bronze to Silver / Silver to Gold | job ના બે મુખ્ય પ્રકાર |
| DQ | Data Quality | data સાચો છે કે નહીં તેની તપાસ |
| DLQ | Dead Letter Queue | જે message process ન થઈ શકે તેનું "problem box" Kafka topic |
| SK | Surrogate Key | Gold માં આપણે બનાવેલો નંબર (dimension row ની ઓળખ) |
| SCD | Slowly Changing Dimension | dimension ના ફેરફારનો ઇતિહાસ કેવી રીતે રાખવો તેના નિયમ |
| DAG | Directed Acyclic Graph | Airflow નો workflow |
| PII | Personally Identifiable Information | નામ, email, phone, ID નંબર જેવી વ્યક્તિગત માહિતી |
| RBAC | Role-Based Access Control | role પ્રમાણે permission |
| SSO | Single Sign-On | કંપનીના એક જ login થી બધા tools |
| CI/CD | Continuous Integration / Continuous Delivery | આપમેળે build, test અને deploy |
| SLA | Service Level Agreement | વચન, જેમ કે "સવારે 8 સુધી data તૈયાર" |
| RPO / RTO | Recovery Point / Time Objective | આફતમાં કેટલો data ગુમાવી શકાય / કેટલા સમયમાં પાછું ચાલુ થવું |
| K8s | Kubernetes | container platform |
| OM | OpenMetadata | data catalog અને governance tool |
| MVP | Minimum Viable Product | પહેલું નાનું પણ ખરેખર ઉપયોગી version |
| PoC | Proof of Concept | કંઈક ચાલશે કે નહીં તે સાબિત કરવાનો નાનો પ્રયોગ |

---

## 1. આખું પ્લેટફોર્મ એક નજરે

> **સરળ શબ્દોમાં:**
> આપણી 20+ microservices પોતાનો data MongoDB માં લખે છે. એ data માં થતો **દરેક ફેરફાર** Kafka Connect પકડે છે અને **એક જ Kafka topic** માં મોકલે છે. એક નાની **ingestion service** એ ફેરફાર વાંચીને **Bronze** (MongoDB) માં મૂળ જેવી જ નકલ સાચવે છે, અને સાથે ટ્રેકિંગ માહિતી (`_etl`) ઉમેરે છે.
> પછી **Spark jobs** (બધા એક જ reusable framework થી બનેલા, દરેકની એક YAML file) Bronze નો data **સાફ કરીને, તોડીને** **Silver** (EPAS) માં મૂકે છે, અને પછી reports માટે **star schema** માં **Gold** (EPAS) બનાવે છે.
> દરેક record ને ખબર હોય છે કે તે સફળ થયો કે નહીં, અને દરેક run નો હિસાબ રહે છે. **Airflow** નક્કી કરે છે કે job ક્યારે ચાલે, **Ops UI** થી લોકો સુરક્ષિત રીતે run/rerun/repair કરી શકે, **OpenMetadata** બતાવે છે કે કયો data ક્યાંથી આવ્યો, અને **monitoring** કંઈ ખોટું થાય ત્યારે જાણ કરે છે.

```
 ┌────────────────── હાલની 20+ Spring Boot microservices (MongoDB) ──────────────────┐
 └──────────────┬──────────────────────────────────────────────────────────────────┘
                │ change streams (દરેક ફેરફાર)
                ▼
 Kafka Connect (દરેક source DB માટે 1 Mongo connector)
                │
                ▼
 એક જ topic: cdc.source.events   (key = service:collection:id → એક document ના ફેરફાર ક્રમમાં)
                │
                ▼
 ingestion-service (Spring Boot): metadata થી route → _etl ઉમેરો → version guard → soft delete → upsert
                │
                ▼
 BRONZE  (MongoDB)  bronze_<service>.<collection>   { _id, data, _etl }
                │  Spark B2S job: pending records વાંચો → તોડો → સાફ કરો → DQ → duplicate દૂર → merge
                ▼
 SILVER  (EPAS)  silver_<domain>.*   સાફ, typed, તોડેલા tables (natural keys)
                │  Spark S2G jobs: dimensions (SCD) → facts → aggregates
                ▼
 GOLD    (EPAS)  gold_core.* (shared dimensions) + gold_<area>.* (facts, aggregates)  → BI dashboards

 આજુબાજુ (cross-cutting):
   etl_ctl (EPAS, Silver/Gold વાળો જ DB) : jobs નું register, runs, locks, bookmarks, quarantine, DQ, હિસાબ
   Airflow                               : દરેક job નો એક DAG, Spark ને K8s પર ચાલુ કરે
   ops-service (Spring Boot)             : runs, rerun, approvals, quarantine, DLQ, trace, audit
   OpenMetadata                          : catalog, lineage, DQ results, glossary, PII tags
   Prometheus + Grafana + Loki           : metrics, dashboards, alerts, logs
   Vault                                 : બધા passwords (ક્યારેય Git માં નહીં)
```

---

## 2. Layer પ્રમાણે વિગત

### 2.1 Source → Kafka (doc 02)

> **સરળ શબ્દોમાં:** source MongoDB માં કોઈ order બને, બદલાય કે delete થાય, તો MongoDB નું "change stream" એ ફેરફાર જણાવે છે. Kafka Connect નો Mongo connector એ ફેરફારને Kafka message બનાવીને મોકલે છે.

| મુદ્દો | નિર્ણય | કારણ |
|---|---|---|
| Connector કેટલા | દરેક source DB માટે 1 | એક DB બગડે તો બીજા પર અસર નહીં; અલગ credentials |
| Topic કેટલા | **બધા collections માટે એક જ** `cdc.source.events` | સરળ ops; નવા collection માટે નવો topic નહીં |
| Message key | `svc-order:orders:<_id>` | એક document ના બધા ફેરફાર એક જ partition માં, **ક્રમમાં** |
| Message ની અંદર | આખો change event + `sourceService`, operation, document key, સમય | delete અને metadata ગુમાય નહીં |
| Format | Canonical Extended JSON | ObjectId, Date, Decimal128 જેવા types બરાબર સચવાય |
| Prod settings | 24 partitions, replication 3, retention **14 દિવસ** | 14 દિવસ સુધી replay કરી શકાય |
| મોટા collection નો પહેલો load | અલગ `cdc.source.snapshot` topic | live ફેરફારો ધીમા ન પડે |
| જોખમ: એક ભારે collection બીજાને ધીમા કરે | config બદલીને એ collection ને પોતાના topic માં મોકલી શકાય | code બદલવો ન પડે |

### 2.2 Ingestion service → Bronze (doc 02)

> **સરળ શબ્દોમાં:** godown ના loading dock જેવું. માલ (data) ઉતારે, ગણીને નોંધ કરે (`_etl`), અને rack પર મૂકે (Bronze). માલ સાફ કરવાનું કામ અહીં **નથી**, એ Silver નું કામ છે.

- **Routing:** message ના metadata (service, db, collection) જોઈને સાચો handler પસંદ થાય. નવા collection માટે **ફક્ત એક config line**, code નહીં.
- **Bronze document નો આકાર:** `{ _id, data: <મૂળ document>, _etl: <ટ્રેકિંગ માહિતી> }`. `_etl` અલગ રાખ્યું છે, જેથી source ના fields સાથે નામનો ટકરાવ ન થાય.
- **મુખ્ય `_etl` fields:** `sourceService`, `sourceCollection`, `sourceId`, `cdcOperation`, `sourceVersion` (ક્રમ માટે), `kafkaPartition/Offset`, `recordHash`, `isDeleted`, `deletedAt`, અને Spark લખે તે `syncSilver.{status, errorCode, errorMessage, retryCount, nextRetryAt, runId, updatedAt}`.
- **Version guard (સૌથી મહત્વનો નિયમ):** નવો event ત્યારે જ લખાય, જ્યારે એ Bronze માં પડેલા version કરતાં **નવો** હોય. ડુપ્લિકેટ, જૂના ક્રમના, replay થયેલા, બધા events આ એક નિયમથી સુરક્ષિત થાય.
- **Soft delete:** source માં delete થાય તો Bronze માં `isDeleted = true` થાય, data રહે.
- **Errors:** ખરાબ message → DLQ (પછી ઠીક કરીને replay). DB down → રાહ જોઈને ફરી પ્રયાસ, DLQ માં **ક્યારેય નહીં**.
- **Delivery:** at-least-once + idempotent writes. પરિણામ "exactly once" જેવું જ.

### 2.3 Bronze → Silver (doc 03)

> **સરળ શબ્દોમાં:** Bronze નો કાચો માલ લઈને **સાફ કરો, ચકાસો, અલગ-અલગ ખાનામાં ગોઠવો**. એક order document (items, discounts, payments સાથે) Silver માં 4–5 tables બને.

**Steps (ક્રમમાં):** read → filter → **split** (tables માં તોડવું) → standardize (trim, upper, type cast) → **DQ** → duplicate દૂર → lookup → status નક્કી → delete → staging માં COPY → **એક transaction માં merge** → Bronze માં status પાછો લખવો → હિસાબ (reconcile).

| મુદ્દો | નિર્ણય |
|---|---|
| કયા records વાંચવા | Bronze ના `syncSilver.status` = PENDING / PARTIAL / (FAILED અને retry નો સમય થયો હોય) |
| Table division | 1:1 sub-document → parent માં flatten; array → child table; array ની અંદર array → grandchild table |
| DQ rules | YAML માં લખાય, Spark expressions માં બદલાય, એક જ pass માં દરેક row પર `_errors` |
| Type cast | `try_cast` વાપરવું, કારણ કે Spark ખરાબ value ને ચૂપચાપ NULL બનાવી દે છે |
| Merge | `INSERT … ON CONFLICT`, version + row-hash guard સાથે. Rerun સુરક્ષિત, જૂનો data નવાને ક્યારેય overwrite ન કરે |
| Bronze write-back | Silver commit **પછી જ**. શરત: "Bronze document વાંચ્યા પછી બદલાયો ન હોય" |
| Circuit breaker | ઘણા records એક સાથે fail થાય (દા.ત. 60%) તો rollback, કારણ કે મોટે ભાગે code bug હોય |
| Silver modeling | Natural keys; `silver_ref` માં codes ની mapping; જરૂર હોય ત્યાં SCD2 ઇતિહાસ **Silver માં જ** પકડવો |

**Record ના status:**

| Status | અર્થ |
|---|---|
| SUCCESS | બરાબર થયું |
| PARTIAL | parent બરાબર, પણ કોઈ child table fail થયું |
| FAILED | ફરી પ્રયાસથી ઠીક થઈ શકે (દા.ત. customer હજુ આવ્યો નથી) → backoff સાથે retry |
| QUARANTINED | data/code ઠીક કરવો પડે → repair shelf પર |
| SKIPPED | જાણી જોઈને છોડ્યું (filter, duplicate) અને કારણ નોંધ્યું |

### 2.4 Silver → Gold (doc 04)

> **સરળ શબ્દોમાં:** Silver = સારી રીતે ગોઠવેલો store room. Gold = showroom, જ્યાં data reports ને જોઈએ તેવા આકારમાં ગોઠવાયેલો હોય. વચ્ચે **fact** (જે બન્યું, જેમ કે વેચાણ) અને આજુબાજુ **dimensions** (કોણ, શું, ક્યાં, ક્યારે).

| મુદ્દો | નિર્ણય |
|---|---|
| Gold ક્યાં | ફક્ત EPAS (BI tools ને SQL, joins, group by જોઈએ) |
| Schema | Star schema; shared dimensions `gold_core` માં |
| Job ના પ્રકાર | 4: dimension, fact, snapshot, aggregate (બધા YAML થી) |
| Gold ને નવો Silver data કેવી રીતે ખબર પડે | **Bookmark system:** Silver row બદલાય ત્યારે નવો change number મળે; દરેક Gold job નો પોતાનો bookmark |
| Silver પર status | summary `_sync_gold_status` (બધા Gold jobs પૂરા થાય ત્યારે SUCCESS) + દરેક job ની પોતાની retry list |
| Surrogate key | EPAS પોતે બનાવે (identity); `-1` = Unknown, `-2` = Not applicable; ક્યારેય બદલાય નહીં |
| SCD | દરેક column માટે Type 1 (overwrite), Type 2 (નવી row, ઇતિહાસ), Type 3 (પાછલી value column) |
| Dimension મોડો આવે | placeholder ("inferred member") બને; પછી સાચો data એ જ row માં ભરાય |
| Fact ના પ્રકાર | transactional (bill book), periodic snapshot (રોજનો photo), accumulating (parcel tracking) |
| Aggregates | ફક્ત બદલાયેલા દિવસો ("dirty dates") ફરી ગણવા, SQL થી EPAS ની અંદર |
| Delete | dimension ક્યારેય delete નહીં; fact માં SOFT / HARD / REVERSAL; BI ફક્ત `v_*` views વાંચે |

### 2.5 Control tables `etl_ctl` (doc 05)

> **સરળ શબ્દોમાં:** factory નો **control room**: machines નું register, દરેક shift ની logbook, "machine ચાલુ છે" નું board, bookmark, repair shelf, quality report, સ્ટોક હિસાબ.

**સૌથી મહત્વનો નિર્ણય:** `etl_ctl` એ Silver/Gold વાળા **એક જ EPAS database** માં છે. કારણ: Gold job ને data, bookmark અને status **એક જ transaction** માં લખવા પડે, અને બે અલગ databases વચ્ચે એક transaction શક્ય નથી.

| Table | એક row એટલે |
|---|---|
| `job_registry` (+ `job_source`, `job_target`, `job_dependency`, `job_config_version`) | એક job (Git ની YAML પરથી આપમેળે) |
| `job_run_audit` | એક run (કોણે, કેમ, ક્યારે, કેટલા records, પરિણામ) |
| `job_step_audit` | run નું એક step (સમય) |
| `job_lock` | હાલમાં ચાલતો job (10 મિનિટ lease + heartbeat) |
| `watermark` (+ history) | bookmark |
| `quarantine_record` (+ history) | repair માટે રાહ જોતો record |
| `record_error_log` | દરેક ભૂલની diary (90 દિવસ) |
| `dq_results` | દરેક rule નું પરિણામ |
| `reconciliation` | દરેક હિસાબની તપાસ |
| `error_code_catalog` | ભૂલના codes, અને retry થી ફાયદો થાય કે નહીં (`retryable`) |
| `run_request` | manual run ની વિનંતી (approval સાથે) |
| `pii_access_log` | કોણે PII જોયો |
| બાકી | gold sync, DLQ index, replay audit, purge request, retention policy |

**Staging tables:** ફક્ત Spark → EPAS merge માટે, દરેક target table દીઠ એક `UNLOGGED` table. TEMP table ન ચાલે, કારણ કે Spark ના ઘણા executors અલગ-અલગ connections વાપરે છે.

---

## 3. Framework, Run Modes અને Status (doc 06, 07)

### 3.1 Reusable framework

> **સરળ શબ્દોમાં:** **એક kitchen, ઘણી recipes.** Kitchen = framework code, જે એક જ વાર લખાય. Recipe = દરેક job ની YAML file. 101 મો job ઉમેરવો = 101 મી YAML લખવી, નવો code નહીં.

- **એક જ Spark JAR** અને એક જ `EtlJobRunner` (`--job --mode --env --from --to --ids --dry-run ...`).
- **Config ના 5 સ્તર:** defaults → env job-defaults → job YAML → env overrides → CLI override.
- **Design patterns:** Template Method (fixed lifecycle), Strategy (બદલી શકાય એવા ભાગ), Factory (નામથી શોધવું), Builder (steps ગોઠવવા), Decorator (દરેક step નો સમય આપમેળે), Null Object (dry-run માં writers કંઈ નહીં લખે).
- **Spark ની અંદર Spring નહીં**, ફક્ત plain Java. Spring ફક્ત હંમેશા ચાલતી services માં.
- **Plugin:** YAML થી ન થઈ શકે તેવું ખાસ કામ નાના Java class માં. 3 jobs ને એ જ plugin જોઈએ, તો framework માં ઉમેરી દેવું.

### 3.2 Run modes

| Mode | ક્યારે | Bookmark આગળ વધે? |
|---|---|---|
| INCREMENTAL | રોજનું schedule (1 કલાક / 5 કલાક / દૈનિક) | ✅ |
| FULL_LOAD | પહેલી વાર, અથવા logic બદલાય ત્યારે | ✅ |
| RERUN_JOB | આખો run fail થયો | ✅ |
| RERUN_FAILED_RECORDS | ફક્ત fail થયેલા records હમણાં જ | ❌ |
| REPROCESS_QUARANTINE | repair થયેલા records | ❌ |
| BACKFILL | એક તારીખ ગાળો ફરીથી | ❌ |
| REPLAY | data ગુમાયા પછી ફરી બનાવવું | ❌ |
| ID_REPROCESS | અમુક ખાસ IDs | ❌ |
| + dry-run flag | કોઈ પણ mode, કંઈ લખ્યા વગર ફક્ત તપાસ | ❌ ક્યારેય નહીં |

**Trigger** (કોણે શરૂ કર્યું) અલગ column છે: SCHEDULED, MANUAL, DEPENDENCY, EVENT, CASCADE, AIRFLOW_RETRY, API.

### 3.3 Job નો status

| Status | અર્થ |
|---|---|
| SUCCESS | બધું બરાબર |
| PARTIAL | job પૂરો થયો અને સારો data સચવાયો, પણ અમુક records માં ભૂલ. Default માં **એક પણ ખરાબ record → PARTIAL** |
| FAILED | infra error, બહુ વધારે ભૂલો (circuit breaker), અથવા હિસાબ ન મળ્યો → rollback |
| SKIPPED | જાણી જોઈને ન ચાલ્યો (lock, upstream fail) |
| CANCELLED | કોઈએ રોક્યો |

- **Retry ના 4 સ્તર:** SQL statement → Spark task → આખો job (ફક્ત retryable errors) → record (backoff: 0 / 1 / 4 / 12 / 24 કલાક, પછી quarantine).
- **Dependency:** upstream PARTIAL → downstream ચાલે. Upstream FAILED → 60 મિનિટ રાહ, પછી SKIPPED (16 નહીં, ફક્ત 1 alert).
- **Quarantine:** record પર flag (machine માટે) + quarantine item (માણસ માટે). સાચો data પોતાની layer માં જ રહે; quarantine માં **masked** નકલ. Prod માં fix કરનાર અને approve કરનાર **બે અલગ વ્યક્તિ**.
- **Idempotency:** કોઈ પણ job કોઈ પણ સમયે ફરી ચલાવો, ડુપ્લિકેટ નહીં બને, અને કોઈ પણ જગ્યાએ crash થાય તો data સાચો રહે.

---

## 4. Airflow, Environments અને Secrets (doc 08, 09)

### 4.1 Airflow

> **સરળ શબ્દોમાં:** Airflow = orchestra નો conductor. પોતે વગાડતો નથી, ફક્ત કહે છે કે કોણ ક્યારે વગાડે.

- **દરેક job નો એક DAG**, એક Python factory file આપમેળે બનાવે (CI ની JSON file પરથી).
- Spark ને **Spark Operator** (K8s) થી ચાલુ કરે. રાહ જોતી વખતે Airflow worker free રહે (deferrable).
- પરિણામ **`job_run_audit` પરથી** વાંચે (ops API દ્વારા), exit code પર આધાર નહીં.
- **Dependencies = Datasets** (WhatsApp group જેવું): "table X update થયું" → તેને સાંભળતા jobs જાગે. Sources નું **OR** વાપરવું, AND ની જાળથી બચવા.
- **PARTIAL કેવી રીતે દેખાય:** `run_etl` લીલો (data સચવાયો) + `quality_gate` લાલ (બધું સાફ નથી).
- `catchup=False`, કારણ કે આપણા jobs "જે બાકી છે તે બધું" process કરે છે. Airflow નું backfill નહીં, આપણો BACKFILL mode.
- Uat/prod માં manual run **ફક્ત ops API થી** (approval સાથે).

### 4.2 Environments (dev / qa / uat / prod)

- **Code એક જ વાર build થાય**; ફક્ત settings બદલાય. બધા settings Git માં `config/environments/<env>/` માં.
- Job YAML માં ક્યારેય host name નહીં, ફક્ત નામ (`datasource: bronze_mongo`, `profile: medium`).
- Spark માટે **T-shirt sizes** (small / medium / large), દરેક env પોતાનો અર્થ નક્કી કરે.
- Dev માં schedules default **paused**; dev/qa માં synthetic data, uat માં masked data, real PII ફક્ત prod માં.

### 4.3 Secrets

**Vault → External Secrets Operator → K8s Secret → pods માં files.** Passwords ક્યારેય Git, image, command-line argument, Airflow XCom કે logs માં નહીં. Airflow ને DB passwords મળતા જ નથી.

---

## 5. Governance, Lineage, Data Contracts (doc 10)

> **સરળ શબ્દોમાં:** મોટી library જેવું: catalog (કયા tables છે), librarian (owner), dictionary (glossary), તાળાવાળો રૂમ (PII), bibliography (lineage), quality stamp (DQ), publisher સાથે કરાર (data contract).

- **Tool:** OpenMetadata. Lineage OpenLineage standard માં છે, અને બધી governance માહિતી Git માં code તરીકે છે. એટલે કાલે DataHub પર જવું હોય તો rewrite નહીં.
- **Lineage:** Spark નો automatic listener આપણા COPY અને EPAS SQL merge જોઈ શકતો નથી. એટલે framework **YAML પરથી જાતે** table અને column lineage જાહેર કરે.
- **PII tags:** data contract માં નક્કી થાય, અને lineage સાથે આગળ જાય (masked હોય તો `Masked` tag).
- **DQ results** OpenMetadata માં test results તરીકે દેખાય. એક publisher મોકલે, એટલે OM બંધ હોય તો પણ ETL અટકે નહીં.
- **Data contract:** microservice team અને data team વચ્ચે લેખિત કરાર (fields, types, PII, freshness, ફેરફારની નીતિ). બંને teams approve કરે. Microservice ના પોતાના CI માં contract check, જેથી breaking change ત્યાં જ પકડાય.

---

## 6. Production Readiness (doc 11)

| વિષય | ટૂંકમાં |
|---|---|
| Monitoring | Prometheus + Grafana + Alertmanager (હાલનો stack વાપરવો), 9 dashboards |
| Alerts | 27 alerts, દરેકનો owner અને runbook. Job ના alerts Airflow થી, infra ના Prometheus થી |
| Logs | Loki (અથવા હાલનું ELK), `runId` થી શોધ |
| Record trace | "ORD-1001 ક્યાં અટક્યો?" → એક જ screen માં Source → Bronze → Silver → Gold ની સફર |
| Security | SSO, RBAC, least privilege, TLS, disk encryption, PII masking, audit, GDPR/DPDP purge |
| Reconciliation | દરેક run નો હિસાબ + રોજ એક cross-layer હિસાબ (Source = Bronze = Silver = Gold) |
| Testing | Unit → golden dataset → integration (Testcontainers) → રાત્રે real EPAS પર → regression diff |
| CI/CD | એક versioned release; dev/qa આપમેળે, uat/prod approval સાથે; DB migrations ફક્ત "ઉમેરો" (expand/contract) |
| Disaster recovery | EPAS/Mongo backups (PITR), દર મહિને restore test, Kafka replay, 8 scenarios |
| Ops UI | runs, rerun chain, બધા run modes, approvals, quarantine, DLQ, trace, audit |
| Runbooks | 19 તૈયાર ઉકેલો (problem → તપાસ → ઉપાય → ખાતરી) |
| Go-live | 2–3 sources નો pilot, 2 અઠવાડિયા shadow mode, gated checklist |

---

## 7. હાલની microservice માં fit કરવું કે અલગ બનાવવું?

> **સરળ શબ્દોમાં:** **ETL નો કોઈ code તમારી હાલની microservices ની અંદર નહીં મૂકવો.** બધું એક **અલગ repo (`data-platform`)** માં રાખવું. પણ **infrastructure** (Kafka cluster, Kafka Connect, Kubernetes, monitoring, Vault, CI/CD tool) હાલનું જ **ફરી વાપરવું**.

### 7.1 શા માટે હાલની microservice માં નહીં

| કારણ | સમજૂતી |
|---|---|
| **Library નો ટકરાવ** | Spark અને Spring Boot ના Jackson, Netty, logging libraries ના versions અલગ છે. એક project માં ભેગા કરો તો runtime errors આવે |
| **Release નો ચક્ર અલગ** | ETL configs રોજ બદલાય, microservices નહીં. ETL ના ફેરફાર માટે business service redeploy કરવી પડે તે જોખમી છે |
| **Scaling અલગ** | Ingestion Kafka partitions પ્રમાણે scale થાય, business service user traffic પ્રમાણે |
| **નિષ્ફળતાની અસર** | ETL માં ભૂલ થાય તો order/payment જેવી business service બગડવી ન જોઈએ |
| **Ownership** | Data team પોતાનો code પોતે release કરી શકે, બીજી team ની રાહ વગર |
| **ભાષાઓ** | ETL માં Java + Python (Airflow) + SQL + YAML છે; service repo માં એ ભેળવવું યોગ્ય નથી |

### 7.2 હાલની microservices / repo માં શું આવશે (ફક્ત આટલું)

| શું | ક્યાં | કેમ |
|---|---|---|
| Data contract check (એક નાનો Maven plugin / CLI step) | દરેક source microservice ના **CI pipeline** માં | Breaking change તેમના build માં જ પકડાય |
| Contract ની approval (CODEOWNERS) | `data-platform` repo ની `contracts/` folder | Source team પોતાના data ના કરારની co-owner |
| (વૈકલ્પિક) Kafka Connect configs | જો તમારી team ઇચ્છે તો હાલના infra repo ના `infra/kafka/connect/` માં | Default: `data-platform` repo માં |
| **કોઈ code ફેરફાર નહીં** | — | CDC MongoDB ના change streams થી ચાલે છે, microservice ને કંઈ બદલવાની જરૂર નથી |

### 7.3 ફરી વાપરવા જેવું હાલનું infrastructure

| હાલનું | ETL માટે ઉપયોગ |
|---|---|
| Kafka cluster | નવા topics: `cdc.source.events`, `cdc.source.snapshot`, `dlq.ingestion`, `etl.events.bronze-loaded`, `etl.lineage.events` |
| Kafka Connect cluster (હોય તો) | Mongo source connectors |
| Kubernetes | નવા namespaces `etl-dev/qa/uat/prod` + Spark માટે અલગ node pool |
| Monitoring (Prometheus/Grafana), logging (ELK/Loki) | એ જ stack, નવા dashboards અને alerts |
| Vault / secret manager, SSO | એ જ |
| CI/CD tool (GitHub Actions / GitLab / Jenkins) | એ જ tool, નવી pipeline |
| Config ની રીત (Config Server વાપરતા હો તો) | ETL ની Spring services પણ એ જ રીતે |

---

## 8. ઓછામાં ઓછી services: શું ચાલશે (runtime)

> **સરળ શબ્દોમાં:** નવી **હંમેશા ચાલતી** Spring Boot services ફક્ત **2** છે. Spark jobs "service" નથી, એ ચાલે, કામ કરે અને બંધ થાય. બાકી બધા તૈયાર tools છે.

| # | ઘટક | પ્રકાર | નવું? | કેટલી copies |
|---|---|---|---|---|
| 1 | **`ingestion-service`** | Spring Boot, 24×7 Kafka consumer | ✅ નવી service | 2–6 pods (partitions પ્રમાણે) |
| 2 | **`ops-service`** | Spring Boot, REST API + UI backend + governance publisher + Kafka→Airflow event bridge | ✅ નવી service | 2 pods |
| 3 | `spark-jobs` (એક image) | Batch, Airflow ચાલુ કરે, પૂરું થાય એટલે બંધ | ✅ નવો code, service નહીં | job દીઠ પોતાના pods |
| 4 | Airflow | તૈયાર tool | નવું install (હોય તો હાલનું) | scheduler + triggerer + web |
| 5 | Spark Operator | તૈયાર tool | નવું install | 1 |
| 6 | OpenMetadata | તૈયાર tool | નવું install | server + ingestion |
| 7 | Kafka, Connect, EPAS, MongoDB, Vault, Prometheus, Grafana, Loki | હાલનું infrastructure | ફરી વાપરવું | — |

**બે services કેમ, એક કેમ નહીં?**

| | બે અલગ services (✅ ભલામણ) | બંનેને એક service માં ભેગી કરવી |
|---|---|---|
| Ingestion 24×7 ચાલવું જોઈએ | Ops API ના deploy થી ingestion અટકતું નથી | Ops UI નો નાનો ફેરફાર deploy કરો તો CDC પણ અટકે |
| Scaling | Ingestion partitions પ્રમાણે, ops API traffic પ્રમાણે | બંને એક સાથે scale કરવા પડે |
| ભૂલની અસર | Ops API માં memory leak થાય તો ingestion સુરક્ષિત | એક ભૂલ બંનેને પાડે |

➕ **વચ્ચેનો રસ્તો (જો microservices ઓછી રાખવી ખૂબ જ જરૂરી હોય):** બંનેનો code **એક જ Maven module/એક જ Docker image** માં રાખો, પણ **બે અલગ K8s Deployments** તરીકે ચલાવો (Spring profile `ingestion` અને `ops`). Code અને build એક, runtime અલગ. શરૂઆત **બે અલગ modules** થી કરવી વધારે સ્વચ્છ છે, એટલે ભલામણ એ જ છે.

---

## 9. અંતિમ Folder Structure (સરળ, પણ standard)

> **સરળ શબ્દોમાં:** પહેલાના docs માં 10 Java modules હતા. અહીં એને **5 modules** માં ભેગા કર્યા છે, પણ standard જાળવીને: દરેક module નું કામ સ્પષ્ટ, Spark અને Spring ક્યારેય એક module માં નહીં, અને package boundaries ArchUnit tests થી ચકાસાય. જરૂર પડે ત્યારે પછી ફરી અલગ કરી શકાય.

### 9.1 Modules નું mapping (જૂના નામ → નવા નામ)

| નવો module | અંદર શું (જૂના docs ના modules) | Spark? | Spring? |
|---|---|---|---|
| `platform-core` | `etl-common` + `etl-config` + `etl-control` + `etl-governance` (models, enums, errors, YAML loading/validation, secrets interface, `etl_ctl` DAOs, OpenMetadata client) | ❌ | ❌ |
| `spark-jobs` | `etl-core` + `etl-jobs` + `etl-spark-app` (framework, steps, DQ engine, merge, SCD, lineage, plugins, shaded JAR) | ✅ | ❌ |
| `ingestion-service` | `etl-ingestion` | ❌ | ✅ |
| `ops-service` | `etl-ops-api` (+ governance publisher, event bridge) | ❌ | ✅ |
| `testkit` | `etl-testkit` (Testcontainers, local SparkSession, data generators) | test | test |

Dependency નો નિયમ: `spark-jobs`, `ingestion-service`, `ops-service` → `platform-core` પર આધાર રાખે. **`platform-core` કોઈના પર નહીં.** Spring વાળા modules `spark-jobs` પર ક્યારેય આધાર ન રાખે.

### 9.2 આખું folder tree

```
data-platform/                                   ← નવો, અલગ repo
├── pom.xml                                      # parent POM: 5 modules, versions એક જ જગ્યાએ (BOM), enforcer rules
├── README.md                                    # શરૂઆત કેવી રીતે કરવી
├── CODEOWNERS                                   # કયો folder કોની team નો (contracts: source team + data team)
├── docker-compose.local.yml                     # laptop પર Kafka, Mongo, Postgres, Airflow
├── .env.local.example                           # local secrets નો નમૂનો (સાચી file Git માં નહીં)
│
├── docs/                                        # architecture docs (01–13), adr/, runbooks/
│   ├── adr/                                     # દરેક મોટા નિર્ણયનું કારણ
│   └── runbooks/                                # R01–R19 problem ઉકેલો
│
├── platform-core/                               # ☕ શુદ્ધ Java library (Spark નહીં, Spring નહીં)
│   └── src/main/java/com/company/etl/core/
│       ├── model/                               # RunContext, JobConfig records, RunMode, JobStatus, SyncStatus
│       ├── error/                               # EtlException tree, ErrorCode
│       ├── config/                              # YAML load + 5-layer merge + JSON Schema validation + placeholders
│       ├── secrets/                             # SecretResolver (file / env var)
│       ├── control/                             # etl_ctl DAOs: registry, audit, lock, watermark, quarantine, dq, recon
│       ├── governance/                          # OpenMetadata client, lineage model, publishers, contract checker
│       └── util/                                # hashing, UTC time, ids
│
├── spark-jobs/                                  # ⚡ Spark framework + plugins → એક shaded JAR
│   ├── pom.xml                                  # Spark = provided; shade plugin + ServicesResourceTransformer
│   └── src/main/java/com/company/etl/spark/
│       ├── runner/                              # EtlJobRunner (main), ComponentFactory, JobTypeRegistry, exit codes
│       ├── pipeline/                            # AbstractEtlJob, Step, TimedStep, PipelineBuilder, job types
│       ├── pipeline/steps/                      # Read, Split, Standardize, Dq, Dedupe, Enrich, Classify, Stage, Merge, ...
│       ├── runmode/                             # ReadFilter, ReadPlan, RunModePolicy (દરેક mode ની એક class)
│       ├── io/                                  # Mongo/EPAS readers, COPY/JDBC loaders, filter translators
│       ├── merge/                               # guarded upsert, SCD1/SCD2, fact merge, partition replace, shadow swap
│       ├── dq/                                  # DQ engine + rule types
│       ├── transform/                           # standardize, dedupe, lookups
│       ├── modeling/                            # table splitter, SCD detection, inferred members, junk dims
│       ├── writeback/                           # Bronze write-back, Silver summary
│       ├── lineage/  metrics/  logging/         # OpenLineage emitter, Pushgateway, MDC
│       └── plugins/<domain>/                    # 🔌 ખાસ business code (ફક્ત જરૂર હોય ત્યારે), ServiceLoader થી
│
├── ingestion-service/                           # 🌱 Spring Boot: Kafka CDC → Bronze
│   └── src/main/
│       ├── java/com/company/etl/ingestion/      # consumer, router, handler, enrich, write, dlq, replay, metrics
│       └── resources/
│           ├── application.yml, application-{dev,qa,uat,prod}.yml
│           └── ingestion-mappings.yml           # (service, db, collection) → handler → Bronze collection
│
├── ops-service/                                 # 🌱 Spring Boot: Ops API + UI backend
│   └── src/main/
│       ├── java/com/company/etl/ops/            # runs, run-requests, jobs, quarantine, dlq, trace, privacy, audit,
│       │                                        # security (SSO/RBAC), governance publisher, event bridge
│       └── resources/application*.yml
│   (UI frontend: ops-service/ui/ અથવા તમારી કંપનીનો હાલનો internal-UI repo)
│
├── testkit/                                     # 🧪 Testcontainers, local SparkSession, synthetic data, edge cases
│
├── pipelines/                                   # 📄 ★ દરેક job ની YAML (config-driven હૃદય)
│   ├── _schema/job-config.schema.json
│   ├── _defaults/job-defaults.yml
│   ├── _platform/                               # maintenance, xlayer_recon, purge, governance_publisher
│   ├── bronze-to-silver/<domain>/b2s_*.yml
│   └── silver-to-gold/{core,<area>}/s2g_*.yml  + kpis.yml
├── dq-rules/                                    # DQ rule sets (_common, <domain>, gold/...)
├── contracts/<service>/<collection>.contract.yml # data contracts
│
├── config/environments/                         # ⚙ env settings (secrets નહીં)
│   ├── base/   datasources, spark, kafka, airflow, observability, features (.yml)
│   ├── dev/ qa/ uat/ prod/   (એ જ files + job-defaults.yml + job-overrides.yml)
│
├── kafka-connect/                               # 🔗 connectors (base + overlays/<env>), topics.yml, deploy scripts
├── db/                                          # 🗄 Flyway migrations
│   ├── control/   (V1–V23)                      # etl_ctl
│   ├── silver/    gold/                         # tables, partitions, views, seeds (silver_ref, dim_date)
│   └── bronze-mongo/                            # Mongo indexes, validators
│
├── orchestration/airflow/                       # 🌀 Python
│   ├── dags/                                    # etl_job_dags.py (factory), manual_run, cascade, maintenance, sla, replay
│   ├── etl_airflow/                             # factory, EtlSparkOperator, triggers, wait_upstream, quality gate, callbacks
│   └── tests/
│
├── governance/openmetadata/                     # 🏛 ingestion configs, domains, glossary, classifications, PII rules
├── monitoring/                                  # 📊 grafana dashboards, prometheus alerts, alertmanager routes
├── deploy/                                      # 🚀 Dockerfiles, Helm charts + values-<env>, Spark templates, ExternalSecrets
├── tools/                                       # config-cli (render/validate/diff), governance-cli
├── scripts/                                     # local helper scripts, synthetic data
└── .github/workflows/  (અથવા Jenkinsfile / .gitlab-ci.yml)   # PR checks, release, deploy
```

**આ structure standard કેમ છે:**
- Maven multi-module, દરેક module નું એક જ કામ, versions એક જ જગ્યાએ.
- Spark અને Spring હંમેશા અલગ modules માં (library ટકરાવ નહીં).
- Code (`*-service`, `spark-jobs`) અને config (`pipelines`, `config`, `contracts`) અલગ folders માં.
- DB schema Flyway migrations થી, infra Helm/Docker થી, monitoring JSON/YAML થી: **બધું Git માં, હાથથી કંઈ નહીં**.
- Boundaries ArchUnit + Maven Enforcer થી build માં ચકાસાય.

---

## 10. કેવી રીતે બનાવવું: ક્રમ (Roadmap)

> **સરળ શબ્દોમાં:** ઘર માળ પ્રમાણે બનાવો. પહેલાં **પાયો** (પ્રયોગ અને infra), પછી **નાનું પણ પૂરું ઘર** (MVP), પછી **વધુ રૂમ** (scale out), અને છેલ્લે **finishing** (optimization). (અંદાજ: 4–5 engineers ની team.)

| તબક્કો | સમય (અંદાજ) | મુખ્ય કામ | પૂરું ક્યારે ગણવું |
|---|---|---|---|
| **Phase 0: પાયો અને PoCs** | 3–4 અઠવાડિયા | નીચેના 4 ખુલ્લા પ્રશ્નો નક્કી કરવા; Mongo connector PoC; Spark on K8s + COPY + merge PoC; OpenMetadata + EPAS PoC; namespaces, topics, DBs, Vault, Airflow; repo skeleton (§9) + CI | બધા PoC પાસ, dev/qa infra તૈયાર |
| **Phase 1: MVP (pilot)** | 10–12 અઠવાડિયા | 2–3 sources (દા.ત. customers, orders, products) end-to-end; ingestion-service; framework + B2S; 2 dimensions + 1 fact + 1 aggregate; બધા control tables; INCREMENTAL / FULL_LOAD / RERUN / dry-run; Airflow factory; ops-service (minimum); મૂળ dashboards અને alerts; backups | 2 અઠવાડિયા shadow mode, હિસાબ હંમેશા મળે, business owner ની મંજૂરી |
| **Phase 2: Scale out** | 3–4 મહિના | domain પ્રમાણે 10–30+ jobs; બાકીના run modes (backfill, replay, ID, quarantine reprocess); cascade + event triggers; આખું ops UI (trace, approvals, quarantine); બધા alerts; cross-layer હિસાબ; lineage + DQ OpenMetadata માં; contract check microservices ના CI માં; DR drill | બધા run modes UI થી, OM માં lineage + DQ દેખાય |
| **Phase 3: Optimize** | સતત | MDM crosswalk, cost optimization, dynamic DB credentials, schema registry, DR site, self-service | — |

**MVP માં જાણી જોઈને નહીં (પછી ઉમેરવું, design તૈયાર છે):** MDM, schema registry, cascade DAG, event triggers, accumulating snapshots, PII reveal, purge automation, regression diff, DR site.

---

## 11. MVP પહેલાં નક્કી કરવાના 4 મહત્વના પ્રશ્નો 🔴

| # | પ્રશ્ન | કેમ મહત્વનો | ભલામણ |
|---|---|---|---|
| 1 | **કોઈ microservice EPAS માં data રાખે છે?** (tech stack માં sources "MongoDB + EPAS" લખ્યું છે) | આપણી design ફક્ત Mongo CDC cover કરે છે | જો હા: Debezium PostgreSQL connector → એ જ shared topic, ingestion માં બીજો parser |
| 2 | **Business time zone** | Timestamps UTC માં છે; સવારે 2 વાગ્યે (IST) મુકાયેલો order UTC માં **આગલો દિવસ** ગણાય | Date keys `Asia/Kolkata` (business zone) માં, timestamps UTC માં |
| 3 | **Data volume** (રોજના documents, size, વૃદ્ધિ) | Partitions, Spark sizes, DB size, ખર્ચ બધું તેના પર આધારિત | 1–2 અઠવાડિયા માપવું |
| 4 | **Multi-tenancy / branch access** | એક tenant/branch ના users બીજાનો data ન જુએ | જો હા: `tenant_id` દરેક layer માં + EPAS row-level security |

---

## 12. Docs ક્યાં શું છે

| જાણવું હોય | વાંચો |
|---|---|
| આખું structure, repo, modules | doc 01 |
| MongoDB → Kafka → Bronze | doc 02 |
| Bronze → Silver (split, DQ, merge, write-back) | doc 03 |
| Facts, dimensions, SCD, aggregates | doc 04 |
| Control/audit/staging tables, retention | doc 05 |
| Framework અને નવો job કેવી રીતે ઉમેરવો | doc 06 |
| Run modes, status, retry, quarantine, rerun API | doc 07 |
| Environment config અને secrets | doc 08 |
| Airflow DAGs | doc 09 |
| Catalog, lineage, PII, contracts | doc 10 |
| Monitoring, security, CI/CD, DR, ops UI, runbooks | doc 11 |
| અંતિમ સારાંશ, ટકરાવના ઉકેલ, gaps, roadmap (English) | doc 12 |
| આ document (ગુજરાતી સારાંશ + folder structure) | doc 13 |
