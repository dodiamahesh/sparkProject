# dmj-dwh: build order, phase by phase

Use this list to type the project by hand on another computer. Write the files **in this order**. Every phase
**compiles on its own** (this was checked by copying the files phase by phase into an empty project), and each
phase ends with a **test** you can run before you start the next phase.

**What the project is:** a Spring Boot + Spark ETL. It reads 4 MongoDB collections (`products`, `customers`,
`orders`, `payments`), cleans and validates them, and loads a star schema into PostgreSQL (`curated.*` +
`silver.dim_date`). Bad rows go to `meta.meta_rejected_record`. Every run is incremental (`last_run_time`).

**Only one pipeline:** `sales-pipeline`. It writes **directly** into the target tables. There are **no staging
tables** and no `etl_staging` schema: rows are sent with batched `INSERT ... ON CONFLICT DO UPDATE`, and each
table is one transaction.

Paths: Java files are under `src/main/java/com/tcs/dmj/dwh/`, tests under `src/test/java/com/tcs/dmj/dwh/`.
In the tables below, **(main)** and **(test)** mean those two folders.

---

## 0. Before you start (new computer)

| Need | Version / note |
|---|---|
| Java | 17 (`java -version`) |
| Maven | 3.9.x (`mvn -version`) |
| PostgreSQL | 14 or newer (this computer: 18). A database named `silver`. Default URL `localhost:5433`, user `postgres`, password `root`. Change them with the env vars `POSTGRES_URL`, `POSTGRES_USER`, `POSTGRES_PASSWORD` |
| MongoDB | default `mongodb://localhost:27017`, database `bronze_db` (env `MONGO_URI`, `MONGO_DATABASE`) |
| mongosh, psql | to load the sample data and check the results |
| Windows only | set `HADOOP_HOME` to a folder with `bin\winutils.exe` (like `C:\hadoop` on this computer), otherwise Spark logs Hadoop errors on Windows |

Project folder: `dmj-dwh/`. Base Java package: `com.tcs.dmj.dwh`.

---

## Phase overview

| Phase | What you build | Files | How you test it |
|---|---|---|---|
| 1 | Maven project + configuration (4 profiles) | 6 | `mvn dependency:resolve` |
| 2 | PostgreSQL tables + MongoDB sample data | 3 | psql / mongosh counts |
| 3 | Enums, domain records, exceptions, small helpers | 19 | `mvn compile` |
| 4 | Data-quality engine (sanitize + validate) | 8 | unit test `DataQualityEngineTest` |
| 5 | Sales rules + star-schema mapping | 5 | unit test `SalesPipelineRulesTest` |
| 6 | Spring Boot + Spark start-up | 3 | app starts, `/actuator/health` = UP |
| 7 | Meta-table repositories (JDBC) | 7 | `mvn compile`, app starts |
| 8 | Run context + MongoDB reader | 5 | `mvn compile`, app starts |
| 9 | PostgreSQL writers (direct, no staging) | 7 | unit tests `PostgresWriterTest`, `SoftDeleteWriterTest` |
| 10 | Pipeline framework (base class, registry, lineage) | 6 | `mvn compile` |
| 11 | The sales pipeline | 1 | app starts, log shows the pipeline |
| 12 | Services (run a pipeline, commit, lineage) | 13 | app registers the pipeline in `meta.meta_pipeline` |
| 13 | REST API + first full run + incremental run | 5 | `POST .../sales-pipeline/run`, check the tables |
| 14 | Batch mode (Airflow `--run-pipeline`) | 2 | `java -jar ... --run-pipeline=sales-pipeline`, exit code 0 |
| 15 | Airflow DAG (optional) | 3 | DAG `sales-pipeline` visible in Airflow |

---

## Phase 1: Maven project + configuration

| # | File | What it is |
|---|---|---|
| 1 | `pom.xml` | Spring Boot 3.5 parent, Java 17, Spark 3.5.9 (Scala 2.12), Mongo Spark connector, PostgreSQL driver, the `--add-opens` flags for Spark |
| 2 | `src/main/resources/application.yml` | common settings: Spark config, `application.dwh.*` (postgres `batch-size`, quality gate, batch mode) |
| 3 | `src/main/resources/application-offDev.yml` | **offDev** (laptop, default profile): Spark `local[*]`, local PostgreSQL 5433 + MongoDB 27017 |
| 4 | `src/main/resources/application-dev.yml` | **dev** server: Spark standalone cluster (client mode), small resources, dev DB hosts |
| 5 | `src/main/resources/application-test.yml` | **test** / QA: same cluster set-up, medium resources, INFO logs |
| 6 | `src/main/resources/application-prod.yml` | **prod**: cluster, large resources, every host / secret from environment variables |

Which profile is used: `--spring.profiles.active=<offDev|dev|test|prod>` or the env var `SPRING_PROFILES_ACTIVE`.
Nothing given = `offDev`. A profile file only adds / overrides keys of `application.yml`.

**Test**
```powershell
mvn -q dependency:resolve      # downloads all libraries; must end without errors
```

---

## Phase 2: Database tables + sample data

| # | File | What it is |
|---|---|---|
| 7 | `sample-data/sales-pipeline/03_postgres_tables.sql` | ALL PostgreSQL tables: `meta.*`, `silver.dim_date`, `curated.*` star schema, error master seed |
| 8 | `sample-data/sales-pipeline/01_mongo_full_load.js` | MongoDB data for the FIRST run (good rows + bad rows on purpose) |
| 9 | `sample-data/sales-pipeline/02_mongo_incremental.js` | changes for the SECOND run (update, insert, soft delete). Write it now, run it in phase 13 |

**Test**
```powershell
psql -h localhost -p 5433 -U postgres -d silver -f sample-data\sales-pipeline\03_postgres_tables.sql
psql -h localhost -p 5433 -U postgres -d silver -c "\dt meta.*" -c "\dt curated.*" -c "\dt silver.*"
#   meta: 7 tables (meta_pipeline, meta_pipeline_run, meta_asset, meta_lineage, meta_column_lineage,
#         meta_error_master, meta_rejected_record)
#   curated: dim_product, dim_customer, dim_order_status, dim_payment_method, fact_sales, fact_payment
#   silver: dim_date

mongosh "mongodb://localhost:27017" sample-data\sales-pipeline\01_mongo_full_load.js
mongosh "mongodb://localhost:27017/bronze_db" --eval "['products','customers','orders','payments'].forEach(c => print(c, db[c].countDocuments()))"
#   the counts must match the "read" numbers at the top of 01_mongo_full_load.js (24 / 23 / 24 / 23)
```
The SQL script is safe to run again (`IF NOT EXISTS` / `ON CONFLICT`).

---

## Phase 3: Basic building blocks (no logic)

Small files that everything else uses. Write them in this order.

| # | File | What it is |
|---|---|---|
| 10 | (main) `enums/AssetType.java` | TABLE / VIEW / COLLECTION ... (lineage asset type) |
| 11 | (main) `enums/ErrorType.java` | CORRUPT / VALIDATION / ORPHAN / DUPLICATE |
| 12 | (main) `enums/LayerName.java` | SOURCE / RAW / CURATED (lineage layer) |
| 13 | (main) `enums/MappingType.java` | DIRECT / RENAMED / DERIVED ... (column lineage) |
| 14 | (main) `enums/PipelineStatus.java` | RUNNING / SUCCESS / SUCCESS_WITH_FAILED / FAILED (+ `of(success, rejected)`) |
| 15 | (main) `enums/PipelineType.java` | SOURCE_TO_RAW / RAW_TO_CURATED / CURATED_TO_AGGREGATED |
| 16 | (main) `enums/Platform.java` | MONGODB / EDB_PAS / FILESYSTEM (lineage platform) |
| 17 | (main) `service/constant/SparkTypes.java` | shared Spark types (MONEY = decimal(18,2), RATE, DATE, TIMESTAMP, INT) |
| 18 | (main) `domain/MetaAsset.java` | record: one lineage asset |
| 19 | (main) `domain/MetaPipeline.java` | record: row of `meta.meta_pipeline` |
| 20 | (main) `domain/MetaPipelineRun.java` | record: row of `meta.meta_pipeline_run` |
| 21 | (main) `domain/RejectedRecord.java` | record: row of `meta.meta_rejected_record` |
| 22 | (main) `service/EtlException.java` | base runtime exception |
| 23 | (main) `service/DataQualityGateException.java` | too many bad rows in one collection |
| 24 | (main) `service/PipelineNotFoundException.java` | unknown pipeline name |
| 25 | (main) `service/PipelineRunNotFoundException.java` | unknown run id |
| 26 | (main) `service/PipelineAlreadyRunningException.java` | same pipeline already running |
| 27 | (main) `utils/ErrorMessages.java` | short error code / message of an exception (saved in the run row) |
| 28 | (main) `service/dto/RejectedRecordSummaryDTO.java` | record for the rejected-records summary API |

**Test**
```powershell
mvn -q compile                 # no output = OK
```

---

## Phase 4: Data-quality engine

Pure Spark code, no Spring and no database. Tested with a small in-memory dataset.

| # | File | What it is |
|---|---|---|
| 29 | (main) `validation/QualityColumns.java` | names of the technical columns (`_id`, `_raw_record`, errors ...) + `oneRowPerError` |
| 30 | (main) `validation/ValidationRule.java` | one rule: fail condition + error type + field + message |
| 31 | (main) `validation/ReferenceRule.java` | one ORPHAN rule (value must exist in another table) |
| 32 | (main) `validation/QualityResult.java` | result: valid rows + rejected rows |
| 33 | (main) `validation/DataSanitizer.java` | cleaning steps: trim, upper/lower, phone, JSON fields, map values ... |
| 34 | (main) `validation/DataQualityRules.java` | rule builder: `required`, `mustBeDecimal`, `mustMatch`, `mustExistIn` ... |
| 35 | (main) `validation/DataQualityEngine.java` | runs all rules on a dataset, splits valid / rejected |
| 36 | (test) `validation/DataQualityEngineTest.java` | unit test of the engine |

**Test**
```powershell
mvn -q test "-Dtest=DataQualityEngineTest"
#   Tests run: 1, Failures: 0, Errors: 0
```

---

## Phase 5: Sales rules + star-schema mapping

The business logic of the sales pipeline, still without databases.

| # | File | What it is |
|---|---|---|
| 37 | (main) `connector/source/SourceDefinition.java` | data contract of one collection: fields, business key, timestamp column, soft delete, reject %, owner, SLA |
| 38 | (main) `connector/target/DateDimension.java` | builds `silver.dim_date` rows and the `date_key` (yyyyMMdd) |
| 39 | (main) `pipeline/sales/SalesSources.java` | the 4 sources + their sanitizers and rules (J2-S01 ... J2-F03) |
| 40 | (main) `pipeline/sales/SalesStarSchemaMapper.java` | Mongo rows to dim / fact rows, items[] explode, SCD2 point-in-time lookup |
| 41 | (test) `pipeline/sales/SalesPipelineRulesTest.java` | unit test of the sales rules and mapping |

**Test**
```powershell
mvn -q test "-Dtest=SalesPipelineRulesTest"
#   Tests run: 4, Failures: 0, Errors: 0
```

---

## Phase 6: Spring Boot + Spark start-up

| # | File | What it is |
|---|---|---|
| 42 | (main) `config/ApplicationProperties.java` | typed `application.dwh.*` settings (spark, mongo, postgres, quality, batch) |
| 43 | (main) `config/SparkConfig.java` | creates the `SparkSession` bean from the active profile: `local[*]` or the cluster, adds the executor jars, stops with a clear message when a needed env var (e.g. `SPARK_DRIVER_HOST`) is missing |
| 44 | (main) `DmjDwhApp.java` | **TEMPORARY short version** (below). You replace it with the real file in phase 14 |

Temporary `DmjDwhApp.java` (the real one needs `PipelineBatchRunner`, which comes in phase 14):
```java
package com.tcs.dmj.dwh;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class DmjDwhApp {
    public static void main(String[] args) {
        SpringApplication.run(DmjDwhApp.class, args);
    }
}
```

**Test**
```powershell
mvn -q spring-boot:run
#   log: 'The following 1 profile is active: "offDev"'
#   log: "Starting SparkSession (master=local[*], app=dmj-dwh, driver.host=localhost, executor jars=0)"
#   log: "Started DmjDwhApp"
# in a 2nd terminal:
curl.exe http://localhost:8080/actuator/health
#   {"status":"UP"}   (UP also means the PostgreSQL connection works)
# stop the app with Ctrl+C

# profile check: dev WITHOUT its env vars must stop with a clear message
mvn -q spring-boot:run "-Dspring-boot.run.profiles=dev"
#   "Spark setting spark.driver.host = ${SPARK_DRIVER_HOST} is not set ..."
```

---

## Phase 7: Meta-table repositories (plain JDBC)

| # | File | What it is |
|---|---|---|
| 45 | (main) `connector/source/LoadWindow.java` | incremental window: `last_run_time < updated_at <= run start` |
| 46 | (main) `repository/MetaPipelineRepository.java` | register pipelines, read / move `last_run_time` |
| 47 | (main) `repository/MetaPipelineRunRepository.java` | create / finish run rows, find runs |
| 48 | (main) `repository/MetaAssetRepository.java` | lineage assets |
| 49 | (main) `repository/MetaLineageRepository.java` | table lineage edges |
| 50 | (main) `repository/MetaColumnLineageRepository.java` | column lineage |
| 51 | (main) `repository/RejectedRecordRepository.java` | saves rejected records (directly, one row per record + error ids) and reads them |

**Test:** `mvn -q compile`, then `mvn -q spring-boot:run` still starts (Ctrl+C).

---

## Phase 8: Run context + MongoDB reader

| # | File | What it is |
|---|---|---|
| 52 | (main) `core/JobMetrics.java` | counters of one run (read, valid, rejected, deleted, loaded) |
| 53 | (main) `connector/source/SourceBatch.java` | result of one source: valid / rejected / deleted rows |
| 54 | (main) `core/PipelineContext.java` | everything of ONE run: id, window end, metrics, batches, cache, lineage |
| 55 | (main) `connector/source/MongoSourceReader.java` | reads one collection with Spark, only the incremental window |
| 56 | (main) `connector/source/SourceProcessor.java` | read, sanitize, split soft deletes, validate, de-duplicate, reject gate |

**Test:** `mvn -q compile`, app still starts.

---

## Phase 9: PostgreSQL writers (DIRECT, no staging)

This is the part that changed: everything is written straight into the target table.

| # | File | What it is |
|---|---|---|
| 57 | (main) `connector/target/SoftDeleteColumns.java` | knows which tables have `is_deleted` / `deleted_at` |
| 58 | (main) `connector/target/PostgresReader.java` | reads a table with Spark (lookups, surrogate keys) |
| 59 | (main) `connector/target/PostgresWriter.java` | **direct upsert**: rows streamed to the driver, batched `INSERT ... VALUES (?) ON CONFLICT DO UPDATE`, one transaction per table |
| 60 | (main) `connector/target/Scd2Writer.java` | **direct SCD2**: per batch 3 statements (SCD1 update, close old version, insert new version), one transaction |
| 61 | (main) `connector/target/SoftDeleteWriter.java` | marks deleted rows (`is_deleted = TRUE`), SCD2: closes the current version |
| 62 | (test) `connector/target/PostgresWriterTest.java` | checks the upsert SQL and the 3 SCD2 statements |
| 63 | (test) `connector/target/SoftDeleteWriterTest.java` | checks the soft-delete SQL |

**Test**
```powershell
mvn -q test "-Dtest=PostgresWriterTest,SoftDeleteWriterTest"
#   PostgresWriterTest: Tests run: 2   SoftDeleteWriterTest: Tests run: 2   (no failures)
```

---

## Phase 10: Pipeline framework

| # | File | What it is |
|---|---|---|
| 64 | (main) `core/ColumnLineage.java` | record: one field-to-field lineage mapping |
| 65 | (main) `core/EtlPipeline.java` | interface every pipeline implements: `name()`, `sources()`, `targetTables()`, `run()` |
| 66 | (main) `core/LineageCollector.java` | finds table lineage automatically from the Spark plan of every load |
| 67 | (main) `core/PipelineRegistry.java` | all `EtlPipeline` beans by name |
| 68 | (main) `core/PipelineToolkit.java` | bundles reader / writers / processor for the pipeline constructor |
| 69 | (main) `core/AbstractEtlPipeline.java` | base class: `extractAndValidate`, `load`, `loadScd2`, `applyDeletes`, `addSurrogateKey` ... |

**Test:** `mvn -q compile`. (Do not start the app yet: there is no pipeline bean until phase 11.)

---

## Phase 11: The sales pipeline

| # | File | What it is |
|---|---|---|
| 70 | (main) `pipeline/sales/SalesPipeline.java` | the recipe: products -> customers (SCD2) -> orders/lines -> payments -> soft deletes |

**Test**
```powershell
mvn -q spring-boot:run
#   log: "Registered ETL pipeline 'sales-pipeline' (SalesPipeline)"   then Ctrl+C
```

---

## Phase 12: Services (run orchestration)

| # | File | What it is |
|---|---|---|
| 71 | (main) `service/dto/SourceInfoDTO.java` | one source, for the API |
| 72 | (main) `service/dto/PipelineInfoDTO.java` | one pipeline, for the API |
| 73 | (main) `service/SourceCommitService.java` | interface |
| 74 | (main) `service/LineageService.java` | interface |
| 75 | (main) `service/DwhPipelineService.java` | interface |
| 76 | (main) `service/DwhPipelineRunService.java` | interface |
| 77 | (main) `service/RejectedRecordService.java` | interface |
| 78 | (main) `service/impl/PipelineRunLock.java` | PostgreSQL advisory lock: the same pipeline never runs twice at once |
| 79 | (main) `service/impl/SourceCommitServiceImpl.java` | after a successful run: save rejects, move `last_run_time` |
| 80 | (main) `service/impl/LineageServiceImpl.java` | writes `meta_asset`, `meta_lineage`, `meta_column_lineage` |
| 81 | (main) `service/impl/DwhPipelineServiceImpl.java` | runs a pipeline end to end + registers pipelines at start-up |
| 82 | (main) `service/impl/DwhPipelineRunServiceImpl.java` | read run history |
| 83 | (main) `service/impl/RejectedRecordServiceImpl.java` | read rejected records |

**Test**
```powershell
mvn -q spring-boot:run          # wait for "Started DmjDwhApp", then Ctrl+C
psql -h localhost -p 5433 -U postgres -d silver -c "SELECT pipeline_name, active_flag, last_run_time FROM meta.meta_pipeline"
#   sales-pipeline | Y | (null)
```

---

## Phase 13: REST API + first full run + incremental run

| # | File | What it is |
|---|---|---|
| 84 | (main) `web/rest/errors/ApiError.java` | JSON error body |
| 85 | (main) `web/rest/errors/ExceptionTranslator.java` | exceptions to HTTP status (404, 409 ...) |
| 86 | (main) `web/rest/DwhPipelineResource.java` | `GET /api/admin/dwh/pipelines`, `POST /api/admin/dwh/pipelines/{name}/run` |
| 87 | (main) `web/rest/DwhPipelineRunResource.java` | `GET /api/admin/dwh/pipeline-runs`, `GET .../pipeline-runs/{id}` |
| 88 | (main) `web/rest/DwhRejectedRecordResource.java` | `GET /api/admin/dwh/rejected-records`, `GET .../rejected-records/summary` |

**Test 1: first run** (MongoDB has the 01 data from phase 2)
```powershell
mvn -q spring-boot:run
# 2nd terminal:
curl.exe http://localhost:8080/api/admin/dwh/pipelines
curl.exe -X POST http://localhost:8080/api/admin/dwh/pipelines/sales-pipeline/run
#   status SUCCESS_WITH_FAILED (the bad sample rows are rejected on purpose)
```
```sql
-- psql -h localhost -p 5433 -U postgres -d silver
SELECT count(*) FROM curated.dim_customer;                    -- 20
SELECT count(*), sum(line_amount) FROM curated.fact_sales;    -- 30 | 249500.00
SELECT count(*) FROM curated.fact_payment;                    -- 20
SELECT source_collection, count(*) FROM meta.meta_rejected_record GROUP BY 1;
--   products 4, customers 3, orders 4, orders.items 2, payments 3
SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'etl_staging';   -- 0 rows: no staging
```
```powershell
curl.exe "http://localhost:8080/api/admin/dwh/rejected-records/summary"
```

**Test 2: incremental run**
```powershell
mongosh "mongodb://localhost:27017" sample-data\sales-pipeline\02_mongo_incremental.js
curl.exe -X POST http://localhost:8080/api/admin/dwh/pipelines/sales-pipeline/run
#   status SUCCESS (nothing rejected)
```
```sql
SELECT count(*) FROM curated.dim_customer;                    -- 22
SELECT customer_code, city, tier, valid_from, valid_to, is_current
  FROM curated.dim_customer WHERE customer_code = 'C003' ORDER BY valid_from;
--   2 rows: Mumbai/Gold closed (N), Pune/Platinum current (Y)        (SCD2)
SELECT count(*) FROM curated.fact_sales;                      -- 32
SELECT order_no, line_no, is_deleted FROM curated.fact_sales WHERE order_no = 'O1019';   -- 2 lines, is_deleted = true
SELECT count(*) FROM curated.fact_payment;                    -- 21
```

**Test 3: run again without changes**
```powershell
curl.exe -X POST http://localhost:8080/api/admin/dwh/pipelines/sales-pipeline/run
#   reads 0 documents, SUCCESS, table counts stay the same (idempotent)
```

---

## Phase 14: Batch mode (used by Airflow)

| # | File | What it is |
|---|---|---|
| 89 | (main) `batch/PipelineBatchRunner.java` | `--run-pipeline=<name>`: runs ONE pipeline, no web server, exit code 0 / 1 |
| 90 | (main) `DmjDwhApp.java` | **replace** the temporary version with the real one (batch mode + `System.exit`) |

**Test**
```powershell
mvn -q clean package
#   target\dmj-dwh-1.0.0.jar  +  target\spark-jars\ (6 jars: mongo connector + driver, bson, postgresql)
java -jar target\dmj-dwh-1.0.0.jar --run-pipeline=sales-pipeline                  # offDev (default)
echo $LASTEXITCODE
#   log line: DWH_RESULT pipeline=sales-pipeline ... status=SUCCESS ... exitCode=0
#   $LASTEXITCODE = 0
java -jar target\dmj-dwh-1.0.0.jar --run-pipeline=does-not-exist
echo $LASTEXITCODE                                       # 1
```

On a cluster environment the same jar runs with its profile, for example on the dev server (Linux):
```bash
export SPARK_DRIVER_HOST=$(hostname -I | awk '{print $1}') POSTGRES_PASSWORD=...
java -jar /opt/dmj-dwh/dmj-dwh-1.0.0.jar --spring.profiles.active=dev --run-pipeline=sales-pipeline
#   log: "Starting SparkSession (master=spark://dev-spark-master:7077, ..., executor jars=6)"
```

---

## Phase 15 (optional): Airflow

| # | File | What it is |
|---|---|---|
| 91 | `airflow/dags/dmj_dwh_pipelines.yaml` | the pipeline list (only `sales-pipeline`) + one block per environment (offDev / dev / test / prod): Spring profile, java options, schedules on/off, env vars |
| 92 | `airflow/dags/dmj_dwh_dags.py` | builds one DAG per YAML entry for THIS Airflow's environment (`DMJ_DWH_ENV` or Variable `dmj_dwh_env`) |
| 93 | `airflow/README.md` | setup per environment (env setting, connection `dmj_dwh_postgres`, variable `dmj_dwh_mongo_uri`, network) |

**Test:** set `DMJ_DWH_ENV=dev` (or the Variable `dmj_dwh_env`) on the Airflow, copy the jar + `spark-jars` to the
paths in the YAML, copy both `dags/` files to the Airflow `dags/` folder. The DAG `sales-pipeline` appears with the
tag `dev`. Trigger it: the task log shows `--spring.profiles.active=dev` and `DWH_RESULT ... exitCode=0`.
Without `DMJ_DWH_ENV` the Airflow UI shows an import error that names the setting.

---

## Docs (optional, any time)

| File | What it is |
|---|---|
| `sample-data/README.md` | how runs, meta tables, soft delete and the direct load work |
| `docs/airflow-execution-jar-running-on-multiple-jvm.md` | why one finished Airflow job (`System.exit`) does not stop the others |
| `.vscode/settings.json` | VS Code Java setting (`updateBuildConfiguration: automatic`) |

---

## Tips

- After each phase run `mvn -q compile`. When it fails, the error names the file and line. Compare it with the original.
- Spark tests and the app need the `--add-opens` flags. They are already in `pom.xml` (surefire, spring-boot plugin,
  jar manifest). If you copy `pom.xml` exactly, nothing else is needed.
- To start from zero again: drop the `curated` tables' rows (or run the 03 script on an empty database) and set
  `UPDATE meta.meta_pipeline SET last_run_time = NULL;`, then load `01_mongo_full_load.js` again.
