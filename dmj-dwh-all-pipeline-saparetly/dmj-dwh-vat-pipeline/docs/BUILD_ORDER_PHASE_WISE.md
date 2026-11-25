# vat-return-pipeline: files lakhvano kram (phase wise) + darek phase pachhi test

Aa file biji system ma project **haath thi lakhva** mate che. Files ne **13 phase** (0 thi 12) ma vahenchi che.
Darek phase pachhi project **compile thay che** (ane jya test che tya test pass thay che), etle tame
tya j atki ne check kari shako ke atyar sudhi nu barabar lakhayu che ke nahi.

> ✅ Aa kram **verify karelo che**: khali folder ma phase-by-phase files umeri ne darek phase pachhi
> `mvn test-compile` / `mvn test` chalavyu, ane badha phase pass thaya. Phase no kram badalso to
> compile error aavi shake (koi file pehla ni phase ni file vapre che).

**Kul:** 83 Java files (79 main + 4 test), 3 YAML, 1 `pom.xml`, 3 sample-data scripts, 2 Airflow files.
`[120]` = file ma aashare ketli lines che (tunka comments sathe).

**Package structure** (`com.tcs.dmj.dwh`): `exception/` = badhi custom exceptions (ek j jagya e),
`service/` = fakt service interfaces + `impl/` + `dto/`, `web/rest/errors/` = exceptions → HTTP response.

---

## Phase no saransh

| Phase | Shu banse | Files | Test |
|---|---|---|---|
| 0 | Maven project + config | 4 | `mvn test-compile` (dependencies download) |
| 1 | PostgreSQL tables + MongoDB sample data | 3 | `psql` / `mongosh` thi count check |
| 2 | Paya na types (enum, `exception/`, domain, config) | 23 | `mvn test-compile` |
| 3 | Data quality engine (sanitize + rules + reject) | 7 + 1 test | `DataQualityEngineTest` (1 test) |
| 4 | **VAT na rules + mapping** (pipeline nu dil) | 4 + 1 test | `VatReturnPipelineRulesTest` (3 tests) |
| 5 | Meta tables na repositories (JDBC) | 7 | `mvn test-compile` |
| 6 | Run context + PostgreSQL writers | 9 + 2 tests | `DirectPostgresWriterTest`, `SoftDeleteWriterTest` (3 tests) |
| 7 | MongoDB extract engine | 2 | `mvn test-compile` |
| 8 | Pipeline framework + `VatReturnPipeline` | 7 | `mvn test-compile` |
| 9 | Services (run, commit, lineage, lock) | 13 | `mvn test-compile` |
| 10 | REST API + batch mode + `main` | 7 | **aakhi pipeline end-to-end** chalavo |
| 11 | Airflow (optional) | 3 | DAG parse check |
| 12 | Docs (optional) | 4 | — |

---

## Shuru karta pehla (ek j vaar)

| Jaruri | Version / setting |
|---|---|
| Java | 17 (`java -version`) |
| Maven | 3.9+ (`mvn -v`) |
| PostgreSQL | database `silver` (default `localhost:5433`, user `postgres`) |
| MongoDB | `localhost:27017`, database `bronze_db` (scripts jate banave che) |
| Tools | `psql`, `mongosh`, `curl` |

Folder banavo (base package `com.tcs.dmj.dwh`):

```text
dmj-dwh-vat-pipeline/
├── pom.xml
├── src/main/java/com/tcs/dmj/dwh/
│   ├── batch/  config/  connector/source/  connector/target/  core/  domain/  enums/  exception/
│   ├── pipeline/vatreturn/  repository/  service/constant/  service/dto/  service/impl/
│   ├── utils/  validation/  web/rest/errors/
├── src/main/resources/
├── src/test/java/com/tcs/dmj/dwh/
│   ├── connector/target/  pipeline/vatreturn/  validation/
├── sample-data/vat-return-pipeline/
├── airflow/dags/
└── docs/
```

> ⚠️ **Git nu dhyan rakho:** aaje repo ni `.gitignore` ma `target/` lakhelu che. E Maven no build folder
> ignore kare che, **pan sathe `connector/target/` package ne pan** ignore kari nakhe che (7 main + 2 test files
> git ma jati j nathi). Navi system ma project folder (jya `pom.xml` che) ma `.gitignore` banavo ane emaa
> `/target/` lakho (aagal `/` = fakt e folder no Maven build folder). Repo ma biji koi jagya e saado `target/`
> rule na rakhvo, nahi to aa files GitHub par nahi pahoche.

PowerShell ma ek test chalavva: `mvn test "-Dtest=DataQualityEngineTest"` (quotes sathe).

---

## Phase 0: Maven project + configuration

| # | File | Kaam |
|---|---|---|
| 1 | `pom.xml` [162] | Spring Boot 3.5, Spark 3.5 (Scala 2.12), Mongo Spark connector, PostgreSQL driver, Java 17 `--add-opens` flags |
| 2 | `src/main/resources/application.yml` [76] | common settings: Spark config, `application.dwh.*` (mongo, postgres, quality, batch) |
| 3 | `src/main/resources/application-dev.yml` [31] | local DB URL / user / password (env variable thi override) |
| 4 | `src/main/resources/application-prod.yml` [33] | prod: badhu env variables thi |

**Test**

```powershell
mvn -q test-compile
```

✔️ Error vagar puru thay (pehli vaar dependencies download thay, thodi minute lage). Haju koi Java file nathi.
`application-dev.yml` ma tamaro PostgreSQL port / password check karo.

---

## Phase 1: Database tables + sample data

| # | File | Kaam |
|---|---|---|
| 5 | `sample-data/vat-return-pipeline/03_postgres_tables.sql` [546] | PART 1 meta tables, PART 2 VAT star schema (+ 4 seeded reference dims), PART 3 `meta_error_master` seed (69 errors) |
| 6 | `sample-data/vat-return-pipeline/01_mongo_full_load.js` [175] | `bronze_db.vat_return_coll`: 22 documents (21 vanchase + 1 test document) |
| 7 | `sample-data/vat-return-pipeline/02_mongo_incremental.js` [122] | pehli run **pachhi** na badlav (update / insert / delete / soft delete) |

**Test** (02 haju **na** chalavo)

```powershell
& "C:\Program Files\PostgreSQL\18\bin\psql.exe" -h localhost -p 5433 -U postgres -d silver -f sample-data\vat-return-pipeline\03_postgres_tables.sql
mongosh "mongodb://localhost:27017" sample-data\vat-return-pipeline\01_mongo_full_load.js
```

```sql
SELECT count(*) FROM meta.meta_error_master;    -- 69 (khali database par)
SELECT count(*) FROM silver.dim_tax_form;       -- 2
SELECT count(*) FROM silver.dim_filing_channel; -- 3
```

```js
// mongosh
use bronze_db
db.vat_return_coll.countDocuments()             // 22
```

---

## Phase 2: Paya na types (koi logic nahi)

| # | File | Kaam |
|---|---|---|
| 8 | `enums/AssetType.java` [12] | lineage asset no prakar (TABLE, COLLECTION ...) |
| 9 | `enums/ErrorType.java` [14] | CORRUPT, VALIDATION, ORPHAN, DUPLICATE (**kram mahatvano**: pehli = sauthi gambhir) |
| 10 | `enums/LayerName.java` [9] | RAW / CURATED ... |
| 11 | `enums/MappingType.java` [14] | column lineage: DIRECT, RENAMED, LOOKUP ... |
| 12 | `enums/PipelineStatus.java` [21] | RUNNING / SUCCESS / SUCCESS_WITH_FAILED / FAILED + `of(...)` |
| 13 | `enums/PipelineType.java` [8] | RAW_TO_CURATED ... |
| 14 | `enums/Platform.java` [9] | MONGODB, EDB_PAS ... |
| 15 | `service/constant/SparkTypes.java` [17] | MONEY = decimal(18,2), DATE, TIMESTAMP, INT |
| 16 | `utils/ErrorMessages.java` [34] | exception → error_code / short message |
| 17 | `exception/EtlException.java` [13] | badhi custom exceptions no base (unchecked) |
| 18 | `exception/DataQualityGateException.java` [16] | reject gate fail |
| 19 | `exception/PipelineAlreadyRunningException.java` [8] | same pipeline biji jagya e chalu (HTTP 409) |
| 20 | `exception/PipelineNotFoundException.java` [10] | ajanyu pipeline naam (HTTP 404) |
| 21 | `exception/PipelineRunNotFoundException.java` [8] | ajanyo run id (HTTP 404) |
| 22 | `exception/PipelineConfigurationException.java` [13] | code / DB setup khotu: naam, table / column nathi, khoto load mode |
| 23 | `exception/SoftDeletesNotAppliedException.java` [10] | source na delete `applyDeletes` vagar rahi gaya |
| 24 | `domain/MetaAsset.java` [43] | lineage asset (collection / table) |
| 25 | `domain/MetaPipeline.java` [17] | `meta.meta_pipeline` ni ek row |
| 26 | `domain/MetaPipelineRun.java` [24] | `meta.meta_pipeline_run` ni ek row (REST response pan aa j) |
| 27 | `domain/RejectedRecord.java` [23] | `meta.meta_rejected_record` ni ek row |
| 28 | `service/dto/RejectedRecordSummaryDTO.java` [6] | |
| 29 | `config/ApplicationProperties.java` [36] | `application.dwh.*` typed record |
| 30 | `config/SparkConfig.java` [28] | ek j `SparkSession` bean |

(Path `src/main/java/com/tcs/dmj/dwh/` thi shuru thay che; aagal na phase ma pan.)

**Test:** `mvn -q test-compile` → error vagar.

---

## Phase 3: Data quality engine

| # | File | Kaam |
|---|---|---|
| 31 | `validation/QualityColumns.java` [75] | technical column names (`_id`, `_raw_record`, errors struct) |
| 32 | `validation/ValidationRule.java` [21] | ek rule = error type + "kyare kharab" condition + message |
| 33 | `validation/ReferenceRule.java` [8] | ORPHAN rule (value bija table ma hovi joie) |
| 34 | `validation/QualityResult.java` [8] | valid + rejected datasets |
| 35 | `validation/DataSanitizer.java` [132] | cleaning recipe: trim, upper, `fromJson`, `mapValues` ... |
| 36 | `validation/DataQualityRules.java` [148] | rule list: `required`, `mustBeDecimal`, `mustBeOneOf`, `custom` ... |
| 37 | `validation/DataQualityEngine.java` [124] | rules lagave → valid / rejected + DUPLICATE check |
| T1 | `src/test/java/.../validation/DataQualityEngineTest.java` [100] | engine no unit test (Spark local, DB vagar) |

**Test**

```powershell
mvn test "-Dtest=DataQualityEngineTest"
```

✔️ `Tests run: 1, Failures: 0, Errors: 0`

---

## Phase 4: VAT na rules + mapping (pipeline nu dil)

| # | File | Kaam |
|---|---|---|
| 38 | `connector/source/SourceDefinition.java` [150] | ek collection no "data contract": fields, business key, watermark, soft delete, reject % |
| 39 | `connector/target/DateDimension.java` [50] | `silver.dim_date` rows + `dateKey()` (20261014) |
| 40 | `pipeline/vatreturn/VatReturnSources.java` [192] | **shu vanchvu** (`VAT_RETURNS`), **kevi rite saaf karvu** (`sanitizer()`), **kya rules** (`rules()`) |
| 41 | `pipeline/vatreturn/VatReturnSchemaMapper.java` [147] | String rows → typed rows → `dim_taxpayer`, `dim_tax_period`, `fact_vat_return` rows |
| T2 | `src/test/java/.../pipeline/vatreturn/VatReturnPipelineRulesTest.java` [179] | VAT sanitize + rules + mapping test |

**Test**

```powershell
mvn test "-Dtest=VatReturnPipelineRulesTest"
```

✔️ `Tests run: 3, Failures: 0` — aa 3 test check kare che:
1. messy values saaf thay (`"A"` → `Amended`, `"2,80,000"` → `280000`, nested `crtDt` → `filedAt`)
2. 6 kharab returns darek potani sachi error sathe reject thay
3. sacho return fact columns ma map thay (`period_key 202609`, `net_payable -20000.00` ...)

Aa phase pass = VAT business logic barabar che. Database haju jaruri nathi.

---

## Phase 5: Meta tables na repositories (JDBC)

| # | File | Kaam |
|---|---|---|
| 42 | `connector/source/LoadWindow.java` [21] | incremental window: `updatedAt > last_run_time AND <= run start` |
| 43 | `repository/MetaPipelineRepository.java` [130] | pipeline register, `last_run_time` vanchvu / aagal khasedvu, name check (≤ 30, `[A-Za-z0-9._-]`) |
| 44 | `repository/MetaPipelineRunRepository.java` [139] | run row: create, finish, interrupted runs FAILED |
| 45 | `repository/MetaAssetRepository.java` [71] | lineage assets |
| 46 | `repository/MetaLineageRepository.java` [43] | table lineage edges |
| 47 | `repository/MetaColumnLineageRepository.java` [53] | column lineage |
| 48 | `repository/RejectedRecordRepository.java` [293] | rejected rows save (staging + direct), nava messages → `meta_error_master` |

**Test:** `mvn -q test-compile` → error vagar.

---

## Phase 6: Run context + PostgreSQL writers

| # | File | Kaam |
|---|---|---|
| 49 | `connector/source/SourceBatch.java` [41] | ek collection nu parinam: valid, rejected, deleted + counts |
| 50 | `core/JobMetrics.java` [67] | run na counters |
| 51 | `core/PipelineContext.java` [105] | ek run ni badhi vastu: id, window end, cache, lineage |
| 52 | `connector/target/SoftDeleteColumns.java` [47] | table ma `is_deleted` / `deleted_at` che ke nahi |
| 53 | `connector/target/PostgresWriter.java` [98] | staging thi UPSERT (`directLoad=false` pipelines) + `job_run_id`, `loaded_at` constants |
| 54 | `connector/target/DirectPostgresWriter.java` [137] | **VAT vapre che**: staging vagar `INSERT ... ON CONFLICT`, `KeyLookup` (surrogate key SQL ma) |
| 55 | `connector/target/PostgresReader.java` [64] | Spark thi table vanchvu (ORPHAN lookup) |
| 56 | `connector/target/Scd2Writer.java` [127] | SCD2 dimension (VAT ma nathi vaparatu, framework mate) |
| 57 | `connector/target/SoftDeleteWriter.java` [104] | soft delete UPDATE (direct + staging) |
| T3 | `src/test/java/.../connector/target/DirectPostgresWriterTest.java` [23] | upsert SQL check |
| T4 | `src/test/java/.../connector/target/SoftDeleteWriterTest.java` [28] | soft delete SQL check |

**Test**

```powershell
mvn test "-Dtest=DirectPostgresWriterTest,SoftDeleteWriterTest"
```

✔️ `Tests run: 3, Failures: 0` (1 + 2)

---

## Phase 7: MongoDB extract engine

| # | File | Kaam |
|---|---|---|
| 58 | `connector/source/MongoSourceReader.java` [64] | Mongo collection → Spark (window `$match` MongoDB ma j chale) |
| 59 | `connector/source/SourceProcessor.java` [210] | WINDOW → EXTRACT → SANITIZE → SOFT DELETE split → VALIDATE → DEDUP → REJECT GATE |

**Test:** `mvn -q test-compile` → error vagar.

---

## Phase 8: Pipeline framework + VAT pipeline

| # | File | Kaam |
|---|---|---|
| 60 | `core/ColumnLineage.java` [16] | ek field → column mapping |
| 61 | `core/EtlPipeline.java` [52] | pipeline no contract: `name()`, `sources()`, `run()`, `directLoad()` ... |
| 62 | `core/LineageCollector.java` [219] | Spark plan mathi table lineage jate shodhe |
| 63 | `core/PipelineRegistry.java` [42] | badhi `@Component` pipelines jate register |
| 64 | `core/PipelineToolkit.java` [21] | record: pipeline ne jaruri services ek object ma |
| 65 | `core/AbstractEtlPipeline.java` [131] | building blocks: `extractAndValidate`, `loadDirect`, `applyDeletes` ... |
| 66 | `pipeline/vatreturn/VatReturnPipeline.java` [115] | **VAT recipe**: extract+validate → dim_taxpayer → dim_tax_period → dim_date → fact → soft delete |

**Test:** `mvn -q test-compile` → error vagar. (Phase 4 na test fari chalavi shako: `mvn test "-Dtest=VatReturnPipelineRulesTest"`.)

---

## Phase 9: Services

| # | File | Kaam |
|---|---|---|
| 67 | `service/SourceCommitService.java` [9] | interface |
| 68 | `service/LineageService.java` [10] | interface |
| 69 | `service/DwhPipelineService.java` [18] | interface |
| 70 | `service/DwhPipelineRunService.java` [14] | interface |
| 71 | `service/RejectedRecordService.java` [15] | interface |
| 72 | `service/dto/SourceInfoDTO.java` [25] | API ma source ni mahiti |
| 73 | `service/dto/PipelineInfoDTO.java` [17] | API ma pipeline ni mahiti |
| 74 | `service/impl/PipelineRunLock.java` [87] | PostgreSQL advisory lock (same pipeline be vaar ek saathe nahi) |
| 75 | `service/impl/SourceCommitServiceImpl.java` [69] | commit phase: rejects save + `last_run_time` aagal |
| 76 | `service/impl/LineageServiceImpl.java` [136] | safal run pachhi lineage lakhe |
| 77 | `service/impl/DwhPipelineServiceImpl.java` [227] | **run no main flow**: lock → run row → transaction → pipeline.run → commit → status |
| 78 | `service/impl/DwhPipelineRunServiceImpl.java` [32] | run history |
| 79 | `service/impl/RejectedRecordServiceImpl.java` [31] | rejected records list / summary |

**Test:** `mvn -q test-compile` → error vagar.

---

## Phase 10: REST API + batch mode + main → aakhi pipeline chalavo

| # | File | Kaam |
|---|---|---|
| 80 | `web/rest/errors/ApiError.java` [7] | error JSON |
| 81 | `web/rest/errors/ExceptionTranslator.java` [62] | 404 (pipeline nathi), 409 (already running), 503 ... |
| 82 | `web/rest/DwhPipelineResource.java` [38] | `GET /api/admin/dwh/pipelines`, `POST .../{name}/run` |
| 83 | `web/rest/DwhPipelineRunResource.java` [39] | `GET /api/admin/dwh/pipeline-runs` |
| 84 | `web/rest/DwhRejectedRecordResource.java` [41] | `GET /api/admin/dwh/rejected-records` (+ `/summary`) |
| 85 | `batch/PipelineBatchRunner.java` [89] | `--run-pipeline=<name>`: ek pipeline chalavi ne exit 0 / 1 |
| 86 | `DmjDwhApp.java` [38] | `main`: REST service ke batch mode |

### Test A: badha unit tests

```powershell
mvn test
```

✔️ 4 test classes, `Tests run: 7, Failures: 0`.

### Test B: pehli run (Phase 1 no data)

```powershell
mvn spring-boot:run
# biji terminal ma:
curl.exe http://localhost:8080/api/admin/dwh/pipelines
curl.exe -X POST http://localhost:8080/api/admin/dwh/pipelines/vat-return-pipeline/run
```

✔️ Log ma `Registered ETL pipeline 'vat-return-pipeline'`. Run no response:
`pipelineStatus = SUCCESS_WITH_FAILED`, `totalRecords = 21`, `successRecords = 14`, `failedRecords = 7`.

```sql
SELECT count(*) FROM silver.dim_taxpayer;          -- 6
SELECT count(*) FROM silver.dim_tax_period;        -- 3
SELECT count(*) FROM silver.fact_vat_return;       -- 14
SELECT business_key, error_messages FROM meta.meta_rejected_record
 WHERE pipeline_name = 'vat-return-pipeline' ORDER BY 1;   -- 7 rows (603, 615, 616, 617, 618, 619, 620)
SELECT pipeline_name, last_run_time FROM meta.meta_pipeline; -- last_run_time bharayelu
```

### Test C: incremental run

```powershell
mongosh "mongodb://localhost:27017" sample-data\vat-return-pipeline\02_mongo_incremental.js
curl.exe -X POST http://localhost:8080/api/admin/dwh/pipelines/vat-return-pipeline/run
```

✔️ `totalRecords = 7` (5 valid + 1 rejected + 1 soft delete), `successRecords = 6`, `failedRecords = 1`,
`SUCCESS_WITH_FAILED`.

```sql
SELECT count(*) FROM silver.dim_taxpayer;                          -- 7
SELECT count(*) FROM silver.fact_vat_return;                       -- 17
SELECT ack_no FROM silver.fact_vat_return WHERE is_deleted;        -- 120000000614
SELECT taxpayer_name FROM silver.dim_taxpayer WHERE tin = 'P0000000731';   -- Asante Trading Company Ltd
```

### Test D: kai badlav vagar fari run

```powershell
curl.exe -X POST http://localhost:8080/api/admin/dwh/pipelines/vat-return-pipeline/run
```

✔️ `totalRecords = 0`, `SUCCESS` (incremental: navu kai nathi).

### Test E: batch mode (Airflow jevu)

```powershell
mvn -q package -DskipTests
java -jar target\dmj-dwh-1.0.0.jar --run-pipeline=vat-return-pipeline
echo $LASTEXITCODE
```

✔️ Log ni chhelli line `DWH_RESULT pipeline=vat-return-pipeline ... exitCode=0`, ane `$LASTEXITCODE` = `0`.
(Batch mode ma web server nathi, etle REST service chalu hoy to pan chale. Fakt e j samaye REST thi pan
aa j pipeline chalti hoy to batch `exit 1` "already running" aape.)

**Fari shunya thi test karva:** `03` fari chalavo (safe che), `01` fari chalavo, ane
`UPDATE meta.meta_pipeline SET last_run_time = NULL WHERE pipeline_name = 'vat-return-pipeline';`
(nahi to `01` na juna `updatedAt` vanchata nathi). Badhu khali karvu hoy to silver na VAT tables `TRUNCATE ... CASCADE` karo.

---

## Phase 11: Airflow (optional)

| # | File | Kaam |
|---|---|---|
| 87 | `airflow/dags/dmj_dwh_dags.py` [102] | YAML mathi darek pipeline mate ek DAG banave |
| 88 | `airflow/dags/dmj_dwh_pipelines.yaml` [50] | `vat-return-pipeline`: schedule `0 3 * * *`, jar path, env |
| 89 | `airflow/README.md` [79] | setup steps |

**Test:** Airflow worker par `airflow dags list | grep vat-return-pipeline`
(Airflow na hoy to `python -c "import ast; ast.parse(open('airflow/dags/dmj_dwh_dags.py').read())"` thi syntax check).

---

## Phase 12: Docs (optional)

| File | Shu che |
|---|---|
| `sample-data/vat-return-pipeline/vat-return-pipeline.md` | VAT design, badha rules, sample data na expected results |
| `sample-data/README.md` | sample data scripts, meta tables, soft delete |
| `docs/PIPELINE_GUIDE.md` | aakhi pipeline samjuti + navo rule / collection / pipeline kevi rite umervo |
| `docs/staging-transaction-and-jdbc.md`, `docs/airflow-execution-jar-running-on-multiple-jvm.md` | design charcha (staging vs direct, ghani JVM) |

---

## Error aave to

| Lakshan | Karan / upay |
|---|---|
| `cannot find symbol` compile vakhte | e class pachhi ni phase ma che, ke package / file naam ma typo. Upar no kram jao. |
| Test ma `IllegalAccessError` / `InaccessibleObjectException` | `pom.xml` ma `spark.jvm.add-opens` ane surefire `argLine` chuki gaya |
| Spark `ClassNotFoundException ... janino` | `pom.xml` ma `<janino.version>3.1.9</janino.version>` |
| Start-up: `Meta tables not found in PostgreSQL` | Phase 1 nu `03_postgres_tables.sql` chalavo |
| Run `FAILED`, `Reject gate failed` | 30% thi vadhu kharab rows: sample data / rules check karo |
| Run `FAILED`, `null value in column "taxpayer_key"` | fact nu `KeyLookup` dimension ma row na madi (dim load pehla thavu joie) |
| Pehli run `totalRecords = 0` | `last_run_time` pehla thi set che: NULL karo (upar joyo) |
