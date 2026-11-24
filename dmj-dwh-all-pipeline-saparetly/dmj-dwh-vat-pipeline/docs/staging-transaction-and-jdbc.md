# Staging layer, transaction ane JdbcTemplate (JPA kem nahi)

Aa project ma data PostgreSQL ma kevi rite save thaay chhe, staging layer kem chhe, te kaadhi shakaay ke nahi,
"all-or-nothing" transaction kevi rite banaavvu, ane JPA ne badle `JdbcTemplate` kem vaapriye chhiye:
aa badhi charcha ane nirnay ek jagya e.

---

## 1. Aaje data kevi rite save thaay chhe

Project ma **JPA nathi**. Be j rasta chhe: **Spark-JDBC** ane **Spring `JdbcTemplate`**.

```
MongoDB ──(Mongo Spark Connector)──> Spark DataFrame
                                          │
                                          │ Spark-JDBC (executors, parallel)
                                          ▼
                                 etl_staging.stg_<table>_<run>_<n>
                                          │
                                          │ JdbcTemplate: INSERT ... SELECT ... ON CONFLICT DO UPDATE
                                          ▼
                                 silver.* / curated.*      (pachhi staging table DROP)
```

| Kaam | Kevi rite | Class |
|---|---|---|
| Spark data ne staging ma lakhvu | Spark-JDBC `rows.write().format("jdbc").mode(Overwrite)` | `PostgresWriter.writeToStaging()` |
| Staging thi silver ma merge (upsert) | `JdbcTemplate`: `INSERT ... SELECT ... ON CONFLICT DO UPDATE` | `PostgresWriter.upsert()` |
| SCD2 dimension | `JdbcTemplate`: 3 SQL ek `TransactionTemplate` ma | `Scd2Writer` |
| Lookups / surrogate keys vaanchva | Spark-JDBC read | `PostgresReader` |
| `meta_pipeline` (+ `last_run_time` = watermark) | `JdbcTemplate`: `INSERT ... ON CONFLICT DO UPDATE`, `UPDATE` | `MetaPipelineRepository` |
| `meta_pipeline_run` | `JdbcTemplate`: `INSERT ... SELECT ... RETURNING`, `UPDATE` | `MetaPipelineRunRepository` |
| `meta_asset`, `meta_lineage`, `meta_column_lineage` | `JdbcTemplate`: `INSERT ... ON CONFLICT` (expression index) | `MetaAssetRepository`, `MetaLineageRepository`, `MetaColumnLineageRepository` |
| `meta_error_master` + `meta_rejected_record` | Spark-JDBC thi staging, pachhi `JdbcTemplate` 2 SQL ek transaction ma (record dith ek row, `error_ids` array) | `RejectedRecordRepository` |

`JdbcTemplate` = application nu connection (Spring `DataSource`, Hikari pool), plain SQL.
Spark-JDBC = Spark nu potanu connection, dar partition alag, dar partition potani rite commit kare.

### Staging kem chhe?

Spark nu JDBC writer fakt **append** (navi rows umero) ke **overwrite** (aakhu table badlo) kari shake chhe.
"Row hoy to update, na hoy to insert" (UPSERT) te nathi kari shaktu. Etle:

1. Spark data parallel ma ek temporary staging table ma lakhe (jhadpi, silver ne aDtu nathi).
2. Ek SQL statement staging ne silver ma merge kare (set-based, Postgres andar, jhadpi).
3. Staging table DROP.

### Aaje job "all-or-nothing" chhe?

**Na, puri rite nahi.** Dar table no load potani rite commit thaay chhe (silver ni ek table, SCD2, rejected records,
watermark: alag alag). Pan **watermark sauthi chhelle khase chhe**, ane badha load UPSERT chhe. Etle job vachche fail
thaay to aavti run e j window fari vaanche ane e j rows fari lakhe: data **chhevte saachu** thai jaay chhe
(eventually consistent), pan fail thayeli run pachhi thodo samay silver ma adhuru update dekhaai shake.

---

## 2. Staging kaadhi ne seedhu silver ma INSERT / UPDATE / DELETE thaay?

**Haa, insert ane update badhi jobs maate shakya chhe.** Rasto: dar Spark partition (`foreachPartition`) potanu JDBC
connection khole ane batch ma `INSERT ... ON CONFLICT (key) DO UPDATE` seedhu silver table ma moklave.

| Job / table | Staging vagar seedhu silver ma? |
|---|---|
| koi pan plain dim / fact (SCD1) | Haa, ek row = ek upsert (aaje `vat-return-pipeline` aa j rite kare chhe) |
| SCD2 dimension (`Scd2Writer`) | Shakya, pan agharu: dar badlaayela record maate juni version close + navi insert. Aaje aa aakha set maate ek transaction ma thaay chhe; staging vagar dar partition alag commit kare, etle failure par adhuru rahi shake (aavti run sudhaare) |
| meta: rejected records | Haa: naano data, driver par laavi `JdbcTemplate` batch thi |

**Shu gumaavsho:**

- **Atomic load:** aaje ek table no load ek SQL = badhu ke kai nahi. Seedhu lakhva ma dar partition alag commit kare.
- **Moti volume ni speed:** row-by-row batch hajaaro thi thoda laakh rows sudhi barabar; karodo rows maate
  staging + ek set-based SQL ghanu jhadpi.

### DELETE: alag prashn

Staging kaadhvathi delete **nathi** malto. Aaje MongoDB ma delete thayelo document job ne kyaarey dekhaato j nathi
(job fakt hayaat documents vaanche chhe). Silver ma delete maate source e delete ni jaan aapvi pade:

- **Soft-delete (suchan):** source ma `isDeleted: true` + navo `updatedAt`. Job tene vaanchi `DELETE` (athva
  `is_active = false`) kare. Staging hoy ke na hoy, badhi jobs maate chaale.
- **MongoDB change streams:** delete event seedha male, pan timestamp-watermark design thi alag architecture.

---

## 3. "Badhu SUCCESS athva kai j nahi" transaction: shakya chhe?

**Mukhya vaat:** database transaction **ek j connection** par chaale. Spark executors parallel ma alag alag connections
thi lakhe chhe, etle Spark-JDBC nu lakhaan aapna transaction no bhaag kyaarey na bani shake.

### Rasto 1: staging vagar, driver par ek connection, ek transaction

1. Spark validate + transform kare (aaje jevu j). Dataset `cache()` karo.
2. Lakhti vakhte data driver par laavo `toLocalIterator()` thi (ek vakhte ek partition, 5,000 na batch ma).
3. Application nu ek `TransactionTemplate` (ek Hikari connection):
   badha tables nu upsert → SCD2 → rejected records → watermark.
4. COMMIT = badhu saathe dekhaay. Kyay pan error = ROLLBACK = silver / meta ma kai j badlaav nahi.

- Badhi jobs maate chaale, SCD2 sahit (ek transaction ma hovathi SCD2 saral bane).
- Lakhvanu ek j thread ma: moti volume ma dhimu.

### Rasto 2: Spark parallel seedhu silver ma, ane atomic

Two-phase commit (Postgres `PREPARE TRANSACTION`, `max_prepared_transactions`) joie. Complex, ane phasaayela
(prepared pan commit na thayela) transactions no risk. **Suchavta nathi.**

### Rasto 3 (moti volume maate best): staging raakho, chhello merge ek transaction ma

1. Spark badha tables staging ma parallel lakhe (jhadpi, silver ne aDtu nathi).
2. Application na ek connection par **ek transaction**: badha merge (`INSERT ... ON CONFLICT`, SCD2 SQL) +
   rejected records + watermark.
3. COMMIT ke ROLLBACK.

Aaje na code ma staging chhe j; fakt dar table na alag commit ne badle chhello bhaag ek transaction ma lapetvo pade.

| | Rasto 1 (staging vagar) | Rasto 3 (staging + ek transaction) |
|---|---|---|
| All-or-nothing | Haa | Haa |
| Badhi jobs | Haa | Haa |
| SCD2 | Saral | Chaale |
| Moti volume | Dhimu (ek thread) | Jhadpi (parallel + SQL merge) |
| Code | Vadhaare saral | Thodu ochhu saral |

---

## 4. Ek job ma 20M rows hoy to? (Rasto 1 par prashno)

### Badha 20M driver ni memory ma rahe?

**Na.** `toLocalIterator()` ek vakhte **ek partition** laave chhe. Driver par ek partition + ek batch j rahe;
batch Postgres ma moklaay ke memory chhuti.
Example: 20M rows ÷ 200 partitions = dar partition ~1 laakh rows → driver ne fakt aatli memory joie.

**Commit pehla data kya rahe?** **PostgreSQL ma**, driver ma nahi. Postgres uncommitted rows potaana table pages ane
WAL ma lakhi raakhe; COMMIT thaay tyaare j bija ne dekhaay (MVCC).

Dhyaan raakho:

- Pehla `cache()`: nahi to `toLocalIterator` dar partition maate aakhi lineage (Mongo read sahit) fari chalaave.
- Partitions naana raakho (`repartition(200)`): sauthi moto partition driver ni memory ma samaavo joie.

### Kayu connection: Spark-JDBC ke application nu?

**Application nu**: Spring `DataSource` (Hikari) → `JdbcTemplate` + `TransactionTemplate`, driver JVM ma
(aapnu application j Spark driver chhe).
Spark-JDBC (`df.write.format("jdbc")`) executors par chaale, dar partition potanu connection + potano commit:
te aapna transaction ma joDai **na** shake.

Rasto 1 ma aakhi job darmiyaan pool nu ek connection pakdaayelu rahe, etle ek saathe chaalti jobs ni sankhya
Hikari pool size par aadhaar raakhe.

### To 20M maate Rasto 1 kem saaro nathi?

| Mudda | Asar |
|---|---|
| Samay | Ek thread, ~20-50 hajaar rows/second → 20M maate ~7-17 minute; Spark executors bekaar |
| Lock | Transaction chaale tyaa sudhi update thayela rows locked; te j rows ne aDti biji job / query raah jove |
| Postgres bhaar | Moto WAL, vadhaare disk; update thi "dead tuples" (pachhi VACUUM); replica hoy to replication lag |
| Failure | Rollback Postgres ma jhadpi, pan 15 minute nu kaam fari karvu pade |

**Nirnay:** laakh-be laakh rows sudhi Rasto 1 chaale. 20M jevi moti job maate **Rasto 3**: staging raakho, Spark
parallel ma staging lakhe, ane chhello merge ek transaction ma.

---

## 5. Spark ETL ma JPA kem nahi, `JdbcTemplate` j kem? (strong reasons)

### 1) Classpath takraar: JPA aa project ma chaali j na shake

Spring Boot 3 nu Hibernate 6 HQL parser maate **ANTLR 4.13** vaapre chhe. Spark 3.5 no SQL parser **ANTLR 4.9**
thi generate thayelo chhe. Banne ek classpath par mukiye to ek bajuno parser runtime par fail thaay
(ATN deserialize error). Bijo vikalp nathi:
Hibernate 5 `javax` vaapre (Boot 3 `jakarta` maange), ane ANTLR shade karvu e bhaare, toDi naakhe evo jugaad chhe.
**Aa ek j kaaran pooratu chhe.**

### 2) ETL set-based chhe, JPA ek-ek entity vaaru chhe

ETL nu kaam "aakha set ne ek SQL ma" chhe: `INSERT ... SELECT ... ON CONFLICT`, `DELETE ... USING`,
SCD2 na 3 set-based statements. JPA ek-ek object `persist` / `merge` kare:
laakho rows maate laakho statements, ane persistence context (first-level cache) ma laakho objects bhegaa thaay
(memory vadhe, `flush()` / `clear()` jaate karvu pade). Set-based SQL Postgres andar ek j vaar chaale.

### 3) PostgreSQL na features joie: JPA ma te native SQL j bane

`ON CONFLICT DO UPDATE` (upsert), `RETURNING pipeline_run_id`, `DISTINCT ON`, `CAST(... AS JSONB)`,
`WITH removed AS (DELETE ... RETURNING ...)`: aa badhu JPQL ma nathi. JPA vaaprie to pan `@Query(nativeQuery = true)`
lakhvu pade: etle JPA no faydo shunya, ane upar thi ek layer.

### 4) Staging tables na naam dynamic chhe

Staging table dar run ane dar table maate navu bane chhe: `etl_staging.stg_silver_dim_taxpayer_42_3`.
JPA entity ek **fix** table sathe map thaay; runtime par banta ane DROP thata tables maate entity bani j na shake.
`JdbcTemplate` ma table naam SQL string ma mukvu e j pooratu chhe.

### 5) Spark na data `Row` chhe, entity nahi; ane executors par Spring nathi

DataFrame `Row` objects aape chhe. JPA maate dar `Row` ne entity ma badalvu pade: vadhaarano code, vadhaarani
memory, koi faydo nahi. Ane `EntityManager` / Spring beans fakt driver JVM ma chhe: Spark executors (cluster ma bija
machine par) tene vaapri na shake, kem ke te serializable nathi. Executors thi lakhvu hoy to Spark-JDBC ke
`foreachPartition` ma plain JDBC j chaale.

### 6) Chokkas control ane anumaan laayak vartan

`JdbcTemplate` ma je SQL lakhyu te j chaale: chhupa `flush`, dirty checking, lazy loading, N+1 queries,
cascade jevu kai nathi. ETL ma "kayu SQL, kyaare, kayaa transaction ma" chokkas khabar hovu joie,
khaas karine watermark chhelle khasvu joie te jevi ordering maate.

### 7) Debug saral

Code ma je SQL chhe te j `psql` ma copy karine chalaavi shakaay. Hibernate nu generate thayelu SQL shodhvu ke
samajvu na pade.

### 8) Meta tables naana ane saral chhe: ORM ni jarur j nathi

`meta_pipeline`, `meta_pipeline_run`, `meta_asset`, `meta_lineage`: thoda columns, koi relationship / object graph nahi,
caching nahi. ORM (relationships, lazy loading, caching) na kaam ni ahi koi jarur nathi. Aapde tables pan jaate
banavta nathi (`03_postgres_tables.sql`), etle `ddl-auto` jevi suvidha pan kaam ni nathi.

### 9) Ochhi dependency, jhadpi startup

Hibernate metamodel, entity scanning, dialect, second-level cache config: aamaanu kai j nahi.
`spring-boot-starter-jdbc` naanu ane Spark sathe takraar vagar.

### JPA kyaare saaru?

CRUD application (forms, REST, object graph, relationships) maate JPA saaru chhe. **Bulk ETL** maate nahi.
Repository style j joie to **Spring Data JDBC** (Hibernate / ANTLR vagar) `meta_pipeline` /
`meta_pipeline_run` maate vaapri shakaay. Pan upsert, composite key ane set-based kaam maate tema pan `@Query` ma
SQL j lakhvu pade, etle aa project ma `JdbcTemplate` j sauthi saral ane saacho rasto chhe.

---

## 6. Saar (nirnay)

| Prashn | Javaab |
|---|---|
| Aaje save kevi rite? | Spark-JDBC → staging → `JdbcTemplate` merge → silver. Meta tables: `JdbcTemplate`. JPA nathi. |
| Staging kaadhi shakaay? | Haa, insert / update badhi jobs maate. SCD2 agharu. Moti volume ma dhimu. |
| Delete? | Staging thi sambandh nathi: source ma soft-delete (`isDeleted` + `updatedAt`) joie. |
| All-or-nothing transaction? | Haa, badhi jobs maate: ek connection, ek transaction (Rasto 1 ke Rasto 3). |
| 20M rows? | Driver memory ma ek partition j rahe. Pan Rasto 1 dhimu: **Rasto 3** (staging + ek final transaction). |
| JPA kem nahi? | ANTLR takraar (chaale j nahi), set-based ETL, Postgres features, dynamic staging tables, Spark `Row`, control / debug. |
