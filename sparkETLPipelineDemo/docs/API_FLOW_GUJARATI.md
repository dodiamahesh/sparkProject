# API End Point Flow – ગુજરાતીમાં સંપૂર્ણ સમજૂતી

આ document માં `api-requests.http` ના દરેક end point (1 થી 11, અને bonus 12 થી 15) માટે સમજાવ્યું છે કે:

* request **કયા controller** માં જાય છે
* પછી **કયા service / class / interface** માં જાય છે અને ત્યાં **શું કામ** થાય છે
* data **ક્યાંથી આવે છે** (MongoDB / PostgreSQL / Java code)
* data **ક્યાં save થાય છે**
* response માં **શું પાછું આવે છે**

> Class ના નામ, method ના નામ, table ના નામ English માં જ રાખ્યા છે, જેથી code માં સીધું શોધી શકો (VS Code માં `Ctrl + P` → class નું નામ લખો).

---

## 0. પહેલાં આખું ચિત્ર સમજો (Big Picture)

```
                Postman / api-requests.http
                          │  HTTP request
                          ▼
   ┌──────────────────── api (Controller layer) ────────────────────┐
   │ EtlPipelineController   EtlJobController   RejectedRecordController │
   │ DevDataController (dev)  SampleDataIngestionController            │
   └──────────────────────────────┬──────────────────────────────────┘
                                  ▼
   ┌──────────────────── framework (reusable engine) ────────────────┐
   │ job/      EtlJobService, SourceCommitService, JobLockManager     │
   │ pipeline/ PipelineRegistry, EtlPipeline, AbstractEtlPipeline ... │
   │ source/   MongoSourceReader, SourceProcessor, MongoFlagUpdater   │
   │ quality/  DataSanitizer, DataQualityRules, DataQualityEngine     │
   │ target/   PostgresWriter, PostgresReader, DateDimension          │
   │ audit/    StepTracker, JobRunRepository, JobStepRepository,      │
   │           RejectedRecordRepository                               │
   └──────────────────────────────┬──────────────────────────────────┘
                                  ▼
   ┌──────────────────── pipelines (business logic) ─────────────────┐
   │ payment/      PaymentPipeline, PaymentSource, PaymentStarSchemaMapper │
   │ transaction/  TransactionPipeline, TransactionSources, ...Mapper      │
   └──────────────────────────────────────────────────────────────────┘
        │ read                                  │ write
        ▼                                       ▼
   MongoDB  bronze_db                    PostgreSQL  silver
   (payments, customers, ...)            silver.*      -> star schema (dim_*, fact_*)
                                         etl_audit.*   -> job history, steps, rejected
                                         etl_staging.* -> temporary tables (upsert માટે)
```

**યાદ રાખવાનો નિયમ:**
* `api` = ફક્ત request લે છે અને response આપે છે (કોઈ logic નહીં)
* `framework` = દરેક pipeline માટે common કામ (read, validate, load, audit) – આમાં payment કે customer નું નામ પણ નથી
* `pipelines` = ફક્ત business logic (કયું collection, કયા rules, કયા table)

### Database માં કયું table શું રાખે છે

| જગ્યા | Table / Collection | શું રાખે છે |
|---|---|---|
| MongoDB `bronze_db` | `payments`, `customers`, `accounts`, ... (10 collections) | raw source data + `processedFlag` (N / Y / E / D) |
| PostgreSQL `silver` | `silver.dim_*`, `silver.fact_*` | સાફ (valid) data, star schema માં |
| PostgreSQL `silver` | `etl_audit.etl_job_run` | દરેક job run ની એક row (status, counts, time, error) |
| PostgreSQL `silver` | `etl_audit.etl_job_step` | job ના દરેક step ની row (ક્યાં fail થયું, કેટલો time, કેટલા records) |
| PostgreSQL `silver` | `etl_audit.etl_rejected_record` | ખરાબ records + કારણ + original JSON |
| PostgreSQL `silver` | `etl_staging.stg_*` | upsert માટેના temporary tables (કામ પતે એટલે delete) |

> દરેક table માં job પછી data **કેવો દેખાય છે** (સાચા 10-10 rows સાથે), તે આ file ના છેલ્લા ભાગમાં **"ઉદાહરણ data"** section માં છે.

### Error આવે તો કોણ handle કરે?

દરેક controller ની exception [GlobalExceptionHandler.java](../src/main/java/com/demo/etl/api/GlobalExceptionHandler.java) પકડે છે અને સરખો JSON error આપે છે:

| Exception | HTTP | ક્યારે |
|---|---|---|
| `PipelineNotFoundException`, `JobRunNotFoundException` | 404 | ખોટું pipeline નામ / ખોટો job id |
| `IllegalArgumentException` | 400 | ખોટો `loadType` / `mode` / file નામ |
| `PipelineAlreadyRunningException` | 409 | એ જ pipeline પહેલેથી ચાલે છે |
| બીજી `EtlException` | 503 | job queue full |
| બીજી કોઈપણ `Exception` | 500 | અણધારી ભૂલ |

> ધ્યાન રાખો: **job fail થાય તો HTTP error નથી આવતો.** Request સફળ થઈ, પણ job ની `status = FAILED` આવે છે અને `errorMessage` માં કારણ હોય છે.

---

## 1. List all pipelines

```http
GET {{host}}/api/etl/pipelines
```

### સવાલ: આ data ક્યાંથી આવે છે?
**કોઈ database માંથી નહીં!** આ data **Java code** માંથી આવે છે. દરેક pipeline class પોતે પોતાનું નામ, description, collections અને tables method માં return કરે છે.

### Step by step flow

**Step 1 – Application start થાય ત્યારે (request પહેલાં જ):**
1. Spring Boot `com.demo.etl` package અને તેના બધા sub-packages scan કરે છે (કારણ કે [SparkEtlApplication.java](../src/main/java/com/demo/etl/SparkEtlApplication.java) આ package માં છે).
2. તેને `@Component` વાળી બે class મળે છે: [PaymentPipeline.java](../src/main/java/com/demo/etl/pipelines/payment/PaymentPipeline.java) અને [TransactionPipeline.java](../src/main/java/com/demo/etl/pipelines/transaction/TransactionPipeline.java).
3. આ બંને `AbstractEtlPipeline` ને extend કરે છે, અને `AbstractEtlPipeline` interface [EtlPipeline.java](../src/main/java/com/demo/etl/framework/pipeline/EtlPipeline.java) ને implement કરે છે. એટલે Spring ની નજરમાં બંને `EtlPipeline` છે.
4. [PipelineRegistry.java](../src/main/java/com/demo/etl/framework/pipeline/PipelineRegistry.java) ના constructor માં `List<EtlPipeline> pipelines` છે. **Spring પોતે** બધા `EtlPipeline` beans ની list બનાવીને અહીં આપે છે.
5. Registry દરેક pipeline ને `name()` ના આધારે એક `Map` માં મૂકે છે:
   `"payment-pipeline" → PaymentPipeline`, `"transaction-pipeline" → TransactionPipeline`.
   બે pipeline નું નામ સરખું હોય તો app start જ નહીં થાય (`IllegalStateException`).
6. Log માં દેખાય છે: `Registered ETL pipeline 'payment-pipeline' (PaymentPipeline)`.

**Step 2 – જ્યારે તમે request મોકલો:**
1. Request [EtlPipelineController.java](../src/main/java/com/demo/etl/api/EtlPipelineController.java) ના `listPipelines()` method માં આવે છે.
2. Controller `pipelineRegistry.getAllPipelines()` બોલાવે છે, એટલે Map માંથી બધી pipelines મળે છે.
3. દરેક pipeline માટે [PipelineInfo.java](../src/main/java/com/demo/etl/api/dto/PipelineInfo.java) (record) બને છે:

| Response field | ક્યાંથી આવે છે |
|---|---|
| `name` | pipeline નો `name()` method |
| `description` | pipeline નો `description()` method |
| `sourceCollections` | pipeline નો `sourceCollections()` method |
| `targetTables` | pipeline નો `targetTables()` method |
| `fullLoadTruncateTables` | pipeline નો `fullLoadTruncateTables()` method |
| `loadTypes` | [LoadType.java](../src/main/java/com/demo/etl/framework/pipeline/LoadType.java) enum ની values |
| `running` | [JobLockManager.java](../src/main/java/com/demo/etl/framework/job/JobLockManager.java) – અત્યારે ચાલે છે કે નહીં (memory માં) |

4. Spring આ list ને JSON માં ફેરવીને પાછી મોકલે છે.

**ક્યાં save થાય છે?** ક્યાંય નહીં. આ ફક્ત read-only માહિતી છે.

### નવી pipeline બનાવવી હોય તો registration ક્યાં કરવું?

**ક્યાંય નહીં!** કોઈ config file, કોઈ list, કોઈ `if-else` માં નામ ઉમેરવાની જરૂર નથી. ફક્ત આ 3 શરત પૂરી કરો:

1. Class `com.demo.etl` ના કોઈ sub-package માં હોય (દા.ત. `com.demo.etl.pipelines.loan`)
2. Class ઉપર `@Component` હોય
3. Class `AbstractEtlPipeline` ને extend કરે અને `name()` unique હોય

App restart કરો એટલે `PipelineRegistry` તેને આપમેળે શોધી લેશે, અને તે આ end point માં દેખાશે અને end point 2 થી run થશે. વિગત માટે છેલ્લો section **"નવી pipeline કેવી રીતે ઉમેરવી"** જુઓ.

---

## 2. Example 1 – payment-pipeline INCREMENTAL run (રાહ જોઈને result)

```http
POST {{host}}/api/etl/pipelines/payment-pipeline/run?loadType=INCREMENTAL
```

આ સૌથી મહત્વનો end point છે, કારણ કે આખો ETL અહીં ચાલે છે.
**Data ક્યાંથી:** MongoDB `bronze_db.payments`. **Data ક્યાં:** PostgreSQL `silver.*` + `etl_audit.*`, અને MongoDB માં flag update.

### Step 1 – Controller
[EtlPipelineController.java](../src/main/java/com/demo/etl/api/EtlPipelineController.java) → `runPipeline(...)`
* URL માંથી `pipelineName = "payment-pipeline"`
* `loadType` → `LoadType.from("INCREMENTAL")` (ખોટું હોય તો 400)
* `async` default `false` → result આવે ત્યાં સુધી રાહ જુએ છે
* Header `X-Triggered-By` ન હોય તો `"api"`
* પછી `EtlJobService.runPipeline(...)` બોલાવે છે

### Step 2 – Job શરૂ કરવો
[EtlJobService.java](../src/main/java/com/demo/etl/framework/job/EtlJobService.java) → `runPipeline(...)`
1. `pipelineRegistry.getPipeline("payment-pipeline")` → `PaymentPipeline` object મળે (ન મળે તો 404)
2. `lockManager.acquire(...)` → એ જ pipeline પહેલેથી ચાલતી હોય તો 409
3. `jobRunRepository.createRun(...)` → **SAVE:** `etl_audit.etl_job_run` માં નવી row, `status = RUNNING`. નવો `job_run_id` (દા.ત. 1) પાછો મળે
4. `async = false` હોવાથી સીધું `execute(...)` બોલાવે છે

### Step 3 – execute(): job નો આખો lifecycle
`EtlJobService.execute(...)`:
1. `new PipelineContext(jobRunId, name, loadType)` – [PipelineContext.java](../src/main/java/com/demo/etl/framework/pipeline/PipelineContext.java) **આ એક run ની "થેલી"** છે: id, load type, counters ([JobMetrics.java](../src/main/java/com/demo/etl/framework/pipeline/JobMetrics.java)), process થયેલા sources, cache થયેલો data
2. `spark.sparkContext().setJobGroup(...)` – Spark log માં આ job ના બધા કામ એક નામ નીચે
3. `loadType == FULL` હોય તો fact tables truncate (end point 4 માં સમજાવ્યું છે). INCREMENTAL માં નહીં
4. **`pipeline.run(context)`** → `PaymentPipeline.run(...)` (Step 4)
5. **`commitService.commit(context)`** → (Step 9)
6. `jobRunRepository.finishRun(...)` → **SAVE:** `etl_job_run` row update: `status`, `end_time`, `duration_ms`, બધા counts
7. ભૂલ આવે તો `catch` → `status = FAILED`, `error_message`, `error_detail` (stack trace) save
8. `finally` → Spark cache ખાલી (`releaseResources`), lock છોડે

### Step 4 – PaymentPipeline.run(): business flow
[PaymentPipeline.java](../src/main/java/com/demo/etl/pipelines/payment/PaymentPipeline.java) ફક્ત "શું કરવું" કહે છે, "કેવી રીતે" નું કામ framework કરે છે:

```java
SourceBatch payments = extractAndValidate(context, PaymentSource.PAYMENTS, PaymentSource.sanitizer(), PaymentSource.rules());
Dataset<Row> typedPayments = toTypedPayments(payments.valid());
loadDateDimension(...);  load(... dim_payer ...);  load(... dim_payment_method ...);  load(... dim_biller ...);
addSurrogateKey(...) x3;  load(... fact_payment ...);
```

* `PaymentSource.PAYMENTS` ([PaymentSource.java](../src/main/java/com/demo/etl/pipelines/payment/PaymentSource.java)) = [SourceDefinition.java](../src/main/java/com/demo/etl/framework/source/SourceDefinition.java): collection `payments`, fields ની list, business key `paymentId`, latest column `updatedAt`
* `PaymentSource.sanitizer()` = સાફ કરવાના steps
* `PaymentSource.rules()` = validation rules

### Step 5 – EXTRACT (MongoDB માંથી read)
`AbstractEtlPipeline.extractAndValidate(...)` → [SourceProcessor.java](../src/main/java/com/demo/etl/framework/source/SourceProcessor.java) → `process(...)`
1. [MongoSourceReader.java](../src/main/java/com/demo/etl/framework/source/MongoSourceReader.java) → `read(source, INCREMENTAL)`
   * Spark `format("mongodb")` થી `bronze_db.payments` વાંચે છે
   * **બધા fields String તરીકે** વાંચે છે, જેથી `"12abc"` જેવી ખરાબ value પણ વાંચી શકાય અને report થઈ શકે
   * INCREMENTAL હોવાથી MongoDB ની અંદર જ filter: `{"$match": {"processedFlag": {"$in": ["N", null]}}}` → ફક્ત નવા documents આવે
2. `_raw_record` column ઉમેરે છે = original document નો JSON (સાફ કરતા **પહેલાં**), rejected table માટે
3. `context.cache(...)` → Spark data memory માં freeze કરે છે.
   **કેમ?** Spark lazy છે. Cache ન કરીએ તો પછી ફરી MongoDB વાંચે, અને ત્યાં સુધીમાં flags બદલાઈ ગયા હોય તો data અલગ મળે.
4. [StepTracker.java](../src/main/java/com/demo/etl/framework/audit/StepTracker.java) `"Extract payments (INCREMENTAL)"` step ચલાવે છે → **SAVE:** `etl_audit.etl_job_step` માં row (start time, record count = 100, SUCCESS)

> **StepTracker દરેક step માટે આ જ કામ કરે છે:** શરૂઆતમાં row insert (`RUNNING`), સફળ થાય તો `SUCCESS` + count + time, ભૂલ આવે તો `FAILED` + કારણ, અને પછી `EtlStepException("Step 'X' failed: ...")` throw કરે છે. એટલે job ના error માં સીધું દેખાય છે કે **કયા step** માં fail થયું.

### Step 6 – SANITIZE + VALIDATE + DUPLICATE CHECK
હજુ `SourceProcessor.process(...)` ની અંદર, step `"Validate payments"`:
1. **Sanitize** – [DataSanitizer.java](../src/main/java/com/demo/etl/framework/quality/DataSanitizer.java) `apply(...)`:
   `" inr "` → `"INR"`, `"1,499.00"` → `"1499.00"`, `"+91-98765..."` → `"98765..."`, ખાલી string → `null`
2. **Validate** – [DataQualityEngine.java](../src/main/java/com/demo/etl/framework/quality/DataQualityEngine.java) `check(...)`, rules [DataQualityRules.java](../src/main/java/com/demo/etl/framework/quality/DataQualityRules.java) માંથી:
   * દરેક rule એક Spark condition છે જે record **ખરાબ** હોય ત્યારે true થાય
   * એક record ના **બધા** failed rules ભેગા થાય છે: `[CORRUPT] amount is not a valid number (value: abc) | ...`
   * Error type: `CORRUPT` (ખોટો data type), `VALIDATION` (business rule), `ORPHAN` (reference નથી મળતો) – [ErrorType.java](../src/main/java/com/demo/etl/framework/quality/ErrorType.java)
3. **Duplicate check** – જે records pass થયા તેમાં `paymentId` બે વાર હોય તો `updatedAt` સૌથી નવો હોય તે રાખે છે, બાકીના `DUPLICATE`
4. Result: [QualityResult.java](../src/main/java/com/demo/etl/framework/quality/QualityResult.java) → `valid` (82) અને `rejected` (18), બંને cache
5. [SourceBatch.java](../src/main/java/com/demo/etl/framework/source/SourceBatch.java) બને છે (valid, rejected, counts) → `context.addSourceBatch(...)` → commit વખતે વાપરવા માટે યાદ રાખે છે
6. Counters update: `JobMetrics` માં source, valid, rejected, duplicate

> **ધ્યાન:** આ step માં rejected records **save નથી થતા**. તે છેલ્લે commit (Step 9) માં save થાય છે, જેથી job વચ્ચે fail થાય તો કશું અધૂરું save ન થાય.

### Step 7 – TRANSFORM
[PaymentStarSchemaMapper.java](../src/main/java/com/demo/etl/pipelines/payment/PaymentStarSchemaMapper.java)
* `toTypedPayments(...)` → MongoDB નામ (camelCase, String) → PostgreSQL નામ (snake_case, સાચા types): `paymentId` → `payment_id`, `amount` → `NUMERIC(18,2)`, `paymentDate` → `TIMESTAMP`
* `toDimPayer(...)` → એક payer ની એક જ row (latest details) – [SparkUtils.java](../src/main/java/com/demo/etl/framework/common/SparkUtils.java) `latestRecordPerKey`
* `toDimPaymentMethod(...)`, `toDimBiller(...)`, `toFactPayment(...)`

### Step 8 – LOAD (PostgreSQL માં upsert)
`AbstractEtlPipeline.load(...)` → StepTracker step `"Load silver.dim_payer"` → [PostgresWriter.java](../src/main/java/com/demo/etl/framework/target/PostgresWriter.java) `upsert(...)`:
1. Business key પર `dropDuplicates`
2. `job_run_id` અને `loaded_at` columns આપમેળે ઉમેરે છે
3. `writeToStaging(...)` → Spark JDBC થી **temporary table** માં લખે છે: `etl_staging.stg_silver_dim_payer_<jobId>_<n>`
4. `buildUpsertSql(...)` → એક SQL:
   ```sql
   INSERT INTO silver.dim_payer (...) SELECT ... FROM etl_staging.stg_...
   ON CONFLICT (payer_id) DO UPDATE SET payer_name = EXCLUDED.payer_name, ...
   ```
   નવું હોય તો insert, પહેલેથી હોય તો update. **એટલે job બે વાર ચલાવો તો પણ duplicate નહીં થાય.**
5. Staging table drop

**Order કેમ મહત્વનો:** પહેલાં `dim_date`, `dim_payer`, `dim_payment_method`, `dim_biller`, પછી fact. કારણ કે fact ને dimension ની **surrogate key** (`payer_key` વગેરે) જોઈએ છે.

* `loadDateDimension(...)` → [DateDimension.java](../src/main/java/com/demo/etl/framework/target/DateDimension.java) દરેક તારીખ માટે row: `date_key = 20260315`, day name, month, quarter, weekend...
* `addSurrogateKey(...)` → [PostgresReader.java](../src/main/java/com/demo/etl/framework/target/PostgresReader.java) dimension table વાંચે છે (`payer_key`, `payer_id`) અને fact સાથે join કરીને `payer_key` ઉમેરે છે

**SAVE:** `silver.dim_date`, `silver.dim_payer`, `silver.dim_payment_method`, `silver.dim_biller`, `silver.fact_payment`

### Step 9 – COMMIT (ફક્ત બધું load સફળ થાય પછી)
[SourceCommitService.java](../src/main/java/com/demo/etl/framework/job/SourceCommitService.java) `commit(...)`, દરેક SourceBatch માટે 3 steps:

| Step | શું કરે છે | SAVE ક્યાં |
|---|---|---|
| `Save rejected records [payments]` | rejected rows staging માં, પછી [RejectedRecordRepository.java](../src/main/java/com/demo/etl/framework/audit/RejectedRecordRepository.java) `upsertFromStaging` – નવું હોય તો insert, પહેલેથી હોય તો `attempt_count + 1` | `etl_audit.etl_rejected_record` |
| `Remove fixed records from rejected table [payments]` | જે records પહેલાં rejected હતા પણ હવે valid છે, તેની row **delete** | `etl_audit.etl_rejected_record` |
| `Update MongoDB flags [payments]` | [MongoFlagUpdater.java](../src/main/java/com/demo/etl/framework/source/MongoFlagUpdater.java): valid → `Y`, rejected → `E`, duplicate → `D`, સાથે `etlJobRunId`, `etlProcessedAt` | MongoDB `bronze_db.payments` |

**Flags છેલ્લે કેમ?** Job વચ્ચે fail થાય તો MongoDB માં કશું બદલાયું નથી, એટલે next run એ જ documents ફરી process કરશે (અને upsert ના લીધે safe છે).

### Step 10 – Response
`EtlJobService.getJobRun(id)` → `etl_job_run` + `etl_job_step` વાંચીને [JobRunDetails.java](../src/main/java/com/demo/etl/framework/job/JobRunDetails.java) → **HTTP 200**:
```json
{ "job": { "jobRunId": 1, "status": "SUCCESS_WITH_REJECTS", "sourceCount": 100, "validCount": 82,
           "rejectedCount": 18, "duplicateCount": 3, "loadedCount": 223, ... },
  "steps": [ { "stepOrder": 1, "stepName": "Extract payments (INCREMENTAL)", "recordCount": 100, ... }, ... ] }
```

---

## 3. Example 2 – transaction-pipeline (background માં, async)

```http
POST {{host}}/api/etl/pipelines/transaction-pipeline/run?loadType=INCREMENTAL&async=true
X-Triggered-By: mahesh
```

End point 2 જેવો જ flow છે. ફરક ફક્ત આટલો છે:

### ફરક 1 – async = true
* `EtlJobService.runPipeline(...)` job ને [AsyncConfig.java](../src/main/java/com/demo/etl/config/AsyncConfig.java) ના thread pool (`etlJobExecutor`, `etl-job-` threads) ને આપી દે છે
* Response **તરત** આવે છે: **HTTP 202**, `status = RUNNING`, અને `jobRunId`
* પછી job ની status end point 5 (`GET /api/etl/jobs/{id}`) થી જુઓ
* Pool ની queue full હોય તો job `FAILED` ("Job queue is full") અને HTTP 503
* `X-Triggered-By: mahesh` → `etl_job_run.triggered_by` માં save થાય છે (કોણે run કર્યું)
* Job ચાલુ હોય ત્યારે ફરી trigger કરો તો `JobLockManager` → **HTTP 409**

### ફરક 2 – ઘણા collections, order સાથે
[TransactionPipeline.java](../src/main/java/com/demo/etl/pipelines/transaction/TransactionPipeline.java) `run(...)`:

| # | Source (MongoDB) | Rules ([TransactionSources.java](../src/main/java/com/demo/etl/pipelines/transaction/TransactionSources.java)) | Target (PostgreSQL) |
|---|---|---|---|
| 1 | `customers` | `customerRules()` | `silver.dim_customer` |
| 2 | `accounts` | `accountRules(customerIds)` – customerId `dim_customer` માં હોવો જ જોઈએ | `silver.dim_account` |
| 3 | `merchants` | `merchantRules()` | `silver.dim_merchant` |
| 4 | `transactions` | `transactionRules(customerIds, accountIds, merchantIds)` | `silver.dim_date` + `silver.fact_transaction` |

### ફરક 3 – ORPHAN check
* `readTable("silver.dim_customer", "customer_id")` → PostgreSQL માંથી બધા customer ids (આ run + પહેલાંના runs)
* `mustExistIn("customerId", customerIds, ...)` → `DataQualityEngine` left join કરે છે; id ન મળે તો `[ORPHAN] customerId not found in dim_customer (value: CUST-9901)`
* એટલે dimensions **પહેલાં** load થવા જરૂરી છે

Mapping [TransactionStarSchemaMapper.java](../src/main/java/com/demo/etl/pipelines/transaction/TransactionStarSchemaMapper.java) માં થાય છે. Commit વખતે **4 collections માટે** 3-3 steps ચાલે છે (કુલ 25 steps).

---

## 4. FULL load

```http
POST {{host}}/api/etl/pipelines/payment-pipeline/run?loadType=FULL
```

End point 2 જેવો જ flow, આ ફરક સાથે:

1. **Truncate** – `EtlJobService.execute(...)` માં, `pipeline.run()` **પહેલાં**:
   `pipeline.fullLoadTruncateTables()` → `PaymentPipeline` માં `List.of("silver.fact_payment")` → `PostgresWriter.truncate(...)` → step `"Truncate silver.fact_payment (FULL load)"`
2. **બધા documents** – `MongoSourceReader.flagFilter(FULL)` = `null` → કોઈ filter નહીં, flag `N`, `Y`, `E`, `D` બધા વંચાય
3. **Dimensions truncate કેમ નથી થતા?** `dim_date` જેવા dimensions બીજી pipeline પણ વાપરે છે. તેમને ખાલી કરીએ તો બીજી pipeline ના facts તૂટી જાય. એટલે dimensions હંમેશા upsert થાય છે
4. Commit માં બધા documents ના flags ફરીથી set થાય છે, અને પહેલાં rejected હતા પણ હવે valid છે તે rejected table માંથી delete થાય છે

**ક્યારે વાપરવું:** rule બદલ્યો હોય, data ગડબડ થયો હોય, અથવા fact table શરૂઆતથી બનાવવો હોય ત્યારે.

---

## 5. Job status + steps

```http
GET {{host}}/api/etl/jobs/1
```

### Flow
1. [EtlJobController.java](../src/main/java/com/demo/etl/api/EtlJobController.java) → `getJobRun(1)`
2. `EtlJobService.getJobRun(1)`:
   * [JobRunRepository.java](../src/main/java/com/demo/etl/framework/audit/JobRunRepository.java) `findById` → `SELECT * FROM etl_audit.etl_job_run WHERE job_run_id = ?`
   * [JobStepRepository.java](../src/main/java/com/demo/etl/framework/audit/JobStepRepository.java) `findByJobRunId` → `SELECT * FROM etl_audit.etl_job_step WHERE job_run_id = ? ORDER BY step_order`
   * id ન મળે તો `JobRunNotFoundException` → **404**
3. `JobRunDetails(job, steps)` → JSON

**Data ક્યાંથી:** PostgreSQL `etl_audit` (જે end point 2/3/4/10 એ લખ્યો હતો). **Save:** કશું નહીં.

### Response ના મુખ્ય fields

| Field | અર્થ |
|---|---|
| `status` | `RUNNING`, `SUCCESS`, `SUCCESS_WITH_REJECTS` (load થયું પણ થોડા reject), `FAILED` |
| `startTime`, `endTime`, `durationMs` | ક્યારે શરૂ, ક્યારે પૂરું, કેટલા ms |
| `sourceCount` | MongoDB માંથી કેટલા વાંચ્યા |
| `validCount` / `rejectedCount` / `duplicateCount` | કેટલા સારા / ખરાબ / duplicate (duplicate એ rejected નો ભાગ છે) |
| `loadedCount` | silver tables માં કુલ કેટલી rows insert/update થઈ |
| `resolvedCount` | પહેલાં rejected હતા, હવે સુધરી ગયા |
| `errorMessage` | fail થાય તો કારણ, દા.ત. `Step 'Load silver.dim_biller' failed: ...` |
| `steps[]` | દરેક step: નામ, status, record count, time, error |

---

## 6. Job history

```http
GET {{host}}/api/etl/jobs?pipelineName=payment-pipeline&limit=20
```

### Flow
1. `EtlJobController.listJobRuns(pipelineName, status, limit)`
2. `limit` ને 1 થી 500 વચ્ચે રાખે છે (કોઈ 1 લાખ માગે તો પણ 500 જ)
3. `JobRunRepository.findRecent(...)` → dynamic SQL બનાવે છે:
   ```sql
   SELECT * FROM etl_audit.etl_job_run WHERE 1 = 1
     AND pipeline_name = ?          -- pipelineName આપ્યું હોય તો જ
   ORDER BY job_run_id DESC LIMIT ?
   ```
4. Response = job runs ની list, સૌથી નવો પહેલો (steps વગર, steps માટે end point 5)

બધા filters optional છે. `GET /api/etl/jobs` એકલું આપો તો બધી pipelines ના છેલ્લા 20 jobs આવે.

---

## 7. ફક્ત failed jobs

```http
GET {{host}}/api/etl/jobs?status=FAILED
```

End point 6 જેવો જ flow. ફરક: SQL માં `AND status = ?` ઉમેરાય છે (value upper case થાય છે, એટલે `failed` પણ ચાલે).

**FAILED ક્યારે save થાય છે?**
1. Job ના કોઈ step માં exception → `EtlJobService.execute()` નો `catch` → `finishRun(FAILED, errorMessage, errorDetail)`
2. App ચાલતા job વખતે બંધ થઈ ગઈ (crash) → job `RUNNING` માં અટકી જાય. એટલે app ફરી start થાય ત્યારે `EtlJobService.closeInterruptedJobs()` (`ApplicationReadyEvent`) → `JobRunRepository.failInterruptedRuns()` આવા jobs ને `FAILED` કરે છે ("Application stopped while the job was running")

---

## 8. Rejected records

```http
GET {{host}}/api/etl/rejected-records?pipelineName=payment-pipeline&errorType=CORRUPT
```

### Flow
1. [RejectedRecordController.java](../src/main/java/com/demo/etl/api/RejectedRecordController.java) → `listRejectedRecords(pipelineName, collection, errorType, limit)`
2. `limit` 1 થી 1000
3. `RejectedRecordRepository.find(...)` → `SELECT * FROM etl_audit.etl_rejected_record WHERE ...` (filters optional)
4. [RejectedRecord.java](../src/main/java/com/demo/etl/framework/audit/RejectedRecord.java) ની list

**Data ક્યાંથી:** `etl_audit.etl_rejected_record`, જે end point 2 ના **Step 9 (commit)** માં save થયો હતો.

### દરેક rejected record માં શું છે

| Field | અર્થ |
|---|---|
| `sourceCollection`, `sourceId` | કયું collection, MongoDB `_id` (એ document સીધો MongoDB માં શોધી શકો) |
| `businessKey` | દા.ત. `PAY-0078` |
| `errorType` | `CORRUPT` / `VALIDATION` / `ORPHAN` / `DUPLICATE` |
| `errorReason` | **બધા** failed rules ` \| ` થી અલગ, ખરાબ value સાથે |
| `rawRecord` | original document (સાફ કરતા પહેલાંનો) JSON |
| `attemptCount` | કેટલી વાર reject થયો |
| `firstJobRunId` / `lastJobRunId` | પહેલી વાર / છેલ્લી વાર કયા job માં reject થયો |

---

## 9. Rejected summary

```http
GET {{host}}/api/etl/rejected-records/summary
```

### Flow
1. `RejectedRecordController.summary()`
2. `RejectedRecordRepository.summary()`:
   ```sql
   SELECT pipeline_name, source_collection, error_type, COUNT(*) AS record_count
     FROM etl_audit.etl_rejected_record
    GROUP BY pipeline_name, source_collection, error_type
   ```
3. [RejectedRecordSummary.java](../src/main/java/com/demo/etl/framework/audit/RejectedRecordSummary.java) ની list

**ઉપયોગ:** dashboard માટે, "કયા collection માં કયા પ્રકારની ભૂલ કેટલી છે" તરત દેખાય.

---

## 10. RETRY_REJECTED – સુધારેલા records ફરી process કરવા

```http
POST {{host}}/api/etl/pipelines/payment-pipeline/run?loadType=RETRY_REJECTED
```

### પહેલાં તમે શું કરો
MongoDB માં ખરાબ document સુધારો (તેનો flag હજી `E` છે):
```js
db.payments.updateOne({paymentId: "PAY-0078"}, {$set: {amount: 499.00}})
```

### Flow
End point 2 જેવો જ flow, આ ફરક સાથે:
1. `MongoSourceReader.flagFilter(RETRY_REJECTED)` → `{"$match": {"processedFlag": "E"}}` → **ફક્ત rejected documents** વંચાય (`D` duplicate નહીં, કારણ કે duplicate સુધારવાનો હોતો જ નથી)
2. એ જ sanitize + validate થાય
3. **સુધરેલો record (PAY-0078):**
   * valid → silver tables માં load થાય
   * Commit step `Remove fixed records...` → `deleteResolvedFromStaging` → **rejected table માંથી તેની row DELETE**
   * MongoDB flag `E` → `Y`
   * `resolvedCount = 1`
4. **હજી ખરાબ records:**
   * ફરી reject → `upsertFromStaging` → `attempt_count + 1`, નવું કારણ, `last_job_run_id` update
   * flag `E` જ રહે

**ORPHAN માટે:** missing customer MongoDB `customers` માં ઉમેરો (flag `N`), પહેલાં `INCREMENTAL` ચલાવો (customer `dim_customer` માં જાય), પછી `RETRY_REJECTED` (transaction હવે valid).

---

## 11. DEV ONLY – Reset (શરૂઆતથી test કરવા)

```http
POST {{host}}/api/dev/reset?cleanPostgres=true
```

### Flow
1. [DevDataController.java](../src/main/java/com/demo/etl/devdata/DevDataController.java) → `reset(cleanPostgres)`
   * Class ઉપર `@Profile("dev")` છે, એટલે **prod profile માં આ end point અસ્તિત્વમાં જ નથી** (404)
2. [SampleDataLoader.java](../src/main/java/com/demo/etl/devdata/SampleDataLoader.java) `load(true)`:
   * `src/main/resources/sample-data/*.json` ની દરેક file (file નું નામ = collection નું નામ)
   * MongoDB collection **drop**, પછી JSON ના બધા documents **insert** → બધા flags પાછા `N`
3. `cleanPostgres = true` હોય તો:
   * `information_schema.tables` માંથી `silver`, `etl_audit`, `etl_staging` ના બધા tables શોધે છે
   * `TRUNCATE TABLE ... RESTART IDENTITY CASCADE` → બધા tables ખાલી, ids ફરી 1 થી
4. Response: collection દીઠ કેટલા documents load થયા + કયા tables ખાલી થયા

**Data ક્યાંથી:** project ની JSON files. **Save:** MongoDB `bronze_db` (ફરીથી), PostgreSQL ખાલી.

> App start થાય ત્યારે પણ `SampleDataLoader` (`ApplicationRunner`) ચાલે છે, પણ ત્યારે ફક્ત **ખાલી** collections ભરે છે (`etl.sample-data.load-on-startup: true` હોય તો જ).

---

## Bonus: 12 થી 15

### 12. Health
```http
GET {{host}}/actuator/health
```
Spring Boot Actuator (આપણો code નથી). MongoDB અને PostgreSQL connection ચાલે છે કે નહીં, તે `UP` / `DOWN` બતાવે છે. Setting: `application.yml` → `management.endpoints.web.exposure.include`.

### 13 થી 15. Data ingestion (JSON files → MongoDB)
```http
GET  {{host}}/api/ingestion/sample-data/files
POST {{host}}/api/ingestion/sample-data?mode=REPLACE
POST {{host}}/api/ingestion/sample-data/payments.json?mode=REPLACE
```
[SampleDataIngestionController.java](../src/main/java/com/demo/etl/ingestion/SampleDataIngestionController.java) → [SampleDataIngestionService.java](../src/main/java/com/demo/etl/ingestion/SampleDataIngestionService.java). દરેક file માટે 5 steps:

1. **RESOLVE** – `payments.json` → collection `payments`
2. **READ** – JSON array parse
3. **PREPARE** – collection બનાવો (`REPLACE` હોય તો પહેલાં drop, `SKIP_IF_EXISTS` હોય અને data હોય તો અહીં જ અટકો)
4. **INSERT** – 500-500 ના batch માં
5. **VERIFY** – collection માં count ગણો

એક file fail થાય તો પણ બાકીની files ચાલુ રહે છે. HTTP 200 = બધી OK, 207 = થોડી fail, 500 = બધી fail.

---

## નવી pipeline કેવી રીતે ઉમેરવી (દા.ત. `loans` collection માંથી `loan-pipeline`)

### Step 1 – PostgreSQL tables (Flyway)
નવી file: `src/main/resources/db/migration/V8__create_loan_star_schema.sql`
* નામ `V8__...` જ હોવું જોઈએ (V1–V7 પહેલેથી છે). **જૂની V1–V7 files ક્યારેય બદલશો નહીં**, Flyway checksum error આપશે
* Dimension: `loan_type_key BIGSERIAL PRIMARY KEY`, business key ઉપર `UNIQUE` (upsert માટે જરૂરી)
* Fact: business key (`loan_id`) ઉપર `UNIQUE`
* દરેક table માં: `job_run_id BIGINT, loaded_at TIMESTAMP` (`PostgresWriter` આપમેળે ભરે છે)

App start થાય એટલે Flyway આ SQL આપમેળે ચલાવશે.

### Step 2 – નવું package અને 3 classes
`com.demo.etl.pipelines.loan` (payment package copy કરીને બદલો):

| Class | શું લખવું | Copy ક્યાંથી |
|---|---|---|
| `LoanSource` | `SourceDefinition` (collection, fields, business key), `sanitizer()`, `rules()` | `PaymentSource` |
| `LoanStarSchemaMapper` | `toTypedLoans`, `toDimLoanType`, `toFactLoan` | `PaymentStarSchemaMapper` |
| `LoanPipeline` | `@Component`, `extends AbstractEtlPipeline`, `name()`, `run()` | `PaymentPipeline` |

```java
@Component                                         // ← Spring ને દેખાય એ માટે (આ જ "registration" છે)
public class LoanPipeline extends AbstractEtlPipeline {

    public LoanPipeline(PipelineToolkit toolkit) { super(toolkit); }

    @Override public String name() { return "loan-pipeline"; }          // ← URL માં આ નામ, unique
    @Override public String description() { return "loans -> dim_loan_type, fact_loan"; }
    @Override public List<String> sourceCollections() { return List.of("loans"); }
    @Override public List<String> targetTables() { return List.of("silver.dim_loan_type", "silver.fact_loan"); }
    @Override public List<String> fullLoadTruncateTables() { return List.of("silver.fact_loan"); }

    @Override
    public void run(PipelineContext context) {
        SourceBatch loans = extractAndValidate(context, LoanSource.LOANS, LoanSource.sanitizer(), LoanSource.rules());
        Dataset<Row> typed = LoanStarSchemaMapper.toTypedLoans(loans.valid());
        loadDateDimension(context, typed, "disbursement_date");
        load(context, LoanStarSchemaMapper.toDimLoanType(typed), "silver.dim_loan_type", "loan_type");
        Dataset<Row> facts = addSurrogateKey(typed, "silver.dim_loan_type", "loan_type_key", "loan_type");
        load(context, LoanStarSchemaMapper.toFactLoan(facts), "silver.fact_loan", "loan_id");
    }
}
```

### Step 3 – Registration?
**કંઈ કરવાનું નથી.** App restart કરો:
* Log માં: `Registered ETL pipeline 'loan-pipeline' (LoanPipeline)`
* `GET /api/etl/pipelines` (end point 1) માં દેખાશે
* `POST /api/etl/pipelines/loan-pipeline/run?loadType=INCREMENTAL` થી ચાલશે

### તમને "મફત" મળતી વસ્તુઓ (એક પણ line લખ્યા વગર)
* Job tracking (`etl_job_run`) અને step tracking (`etl_job_step`)
* Rejected records save, retry, સુધરે તો delete
* MongoDB flags (`Y` / `E` / `D`)
* FULL / INCREMENTAL / RETRY_REJECTED
* Lock (double run નહીં), async mode, error handling

### કયા rules વાપરી શકાય (`DataQualityRules`)

| Method | Error type | ઉદાહરણ |
|---|---|---|
| `mustBeDecimal`, `mustBeInteger`, `mustBeDate`, `mustBeTimestamp` | CORRUPT | `"12abc"`, `"2026-13-45"` |
| `required` | VALIDATION | field નથી / ખાલી |
| `mustBePositive`, `mustNotBeNegative`, `mustBeBetween` | VALIDATION | amount ≤ 0, rate 45% |
| `mustBeOneOf`, `mustMatch`, `mustBeValidEmail`, `mustNotBeInFuture` | VALIDATION | ખોટો status, ખોટું email |
| `mustExistIn` | ORPHAN | customer નથી મળતો |
| `custom` | તમે નક્કી કરો | `from == to` account |

`loans` માટે કયા rules જોઈએ અને કયા records ખરાબ છે, તે [SAMPLE_DATA_REPORT.md](SAMPLE_DATA_REPORT.md) માં લખેલું છે. પ્રેક્ટિસ માટે સરસ exercise છે.

---

## ઉદાહરણ data: job ચાલ્યા પછી દરેક table માં શું save થાય છે (10-10 rows)

નીચેનો **બધો data સાચો છે** (કાલ્પનિક નથી). Sample data લોડ કરીને નીચેના 10 jobs એક પછી એક ચલાવ્યા, અને દરેક table માંથી 10 rows અહીં copy કરી છે. તમારા machine પર IDs, `_id` અને time અલગ હશે, પણ counts અને flow આ જ રહેશે.

| Job | શું કર્યું | કયા end point થી |
|---|---|---|
| 1 | `payment-pipeline` પહેલી વાર, INCREMENTAL | 2 |
| 2 | `transaction-pipeline` પહેલી વાર, INCREMENTAL | 3 |
| 3 | `payment-pipeline` ફરી INCREMENTAL (નવો data નથી) | 2 |
| 4 | MongoDB માં `PAY-0078` નો amount સુધાર્યો, પછી RETRY_REJECTED | 10 |
| 5 | MongoDB માં નવો customer `CUST-9901` ઉમેર્યો, પછી `transaction-pipeline` INCREMENTAL | 3 |
| 6 | `transaction-pipeline` RETRY_REJECTED (orphan transaction હવે valid) | 10 |
| 7 | `payment-pipeline` FULL load | 4 |
| 8 | `silver.dim_biller` table જાણી જોઈને તોડ્યું, `PAY-0001` બદલ્યો, INCREMENTAL → **FAILED** | 2 |
| 9 | Table પાછું સાચું કર્યું, ફરી INCREMENTAL → SUCCESS | 2 |
| 10 | `transaction-pipeline` FULL load | 4 |

> લાંબી values (`error_reason`, `raw_record`, `error_message`) વાંચવામાં સરળ રહે એ માટે `...` થી ટૂંકી કરી છે. Database માં પૂરી value હોય છે.

---

### A. `etl_audit.etl_job_run` – દરેક job ની એક row

**કોણ લખે છે:** `JobRunRepository.createRun()` (job શરૂ થાય ત્યારે, `RUNNING`) અને `JobRunRepository.finishRun()` (job પૂરો થાય ત્યારે). **ક્યાં દેખાય:** end point 5, 6, 7.

| job_run_id | pipeline_name | load_type | status | triggered_by | start_time | end_time | duration_ms | source_count | valid_count | rejected_count | duplicate_count | loaded_count | resolved_count | error_message |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | payment-pipeline | INCREMENTAL | SUCCESS_WITH_REJECTS | mahesh | 2026-11-03 00:55:09 | 2026-11-03 00:55:26 | 16931 | 100 | 82 | 18 | 3 | 223 | 0 | null |
| 2 | transaction-pipeline | INCREMENTAL | SUCCESS_WITH_REJECTS | mahesh | 2026-11-03 00:55:26 | 2026-11-03 00:55:38 | 12153 | 400 | 332 | 68 | 10 | 403 | 0 | null |
| 3 | payment-pipeline | INCREMENTAL | SUCCESS | scheduler | 2026-11-03 00:55:38 | 2026-11-03 00:55:42 | 3795 | 0 | 0 | 0 | 0 | 0 | 0 | null |
| 4 | payment-pipeline | RETRY_REJECTED | SUCCESS_WITH_REJECTS | mahesh | 2026-11-03 00:55:43 | 2026-11-03 00:55:48 | 4973 | 15 | 1 | 14 | 0 | 5 | 1 | null |
| 5 | transaction-pipeline | INCREMENTAL | SUCCESS | scheduler | 2026-11-03 00:55:48 | 2026-11-03 00:55:55 | 6960 | 1 | 1 | 0 | 0 | 1 | 0 | null |
| 6 | transaction-pipeline | RETRY_REJECTED | SUCCESS_WITH_REJECTS | mahesh | 2026-11-03 00:55:55 | 2026-11-03 00:56:03 | 8399 | 58 | 1 | 57 | 0 | 2 | 1 | null |
| 7 | payment-pipeline | FULL | SUCCESS_WITH_REJECTS | mahesh | 2026-11-03 00:56:04 | 2026-11-03 00:56:09 | 5004 | 100 | 83 | 17 | 3 | 225 | 0 | null |
| 8 | payment-pipeline | INCREMENTAL | FAILED | scheduler | 2026-11-03 00:56:10 | 2026-11-03 00:56:14 | 3348 | 1 | 1 | 0 | 0 | 3 | 0 | Step 'Load silver.dim_biller' failed: PSQLException: ERROR: relation "silver.dim_biller" does not exist   Posi... |
| 9 | payment-pipeline | INCREMENTAL | SUCCESS | mahesh | 2026-11-03 00:56:14 | 2026-11-03 00:56:19 | 4419 | 1 | 1 | 0 | 0 | 5 | 0 | null |
| 10 | transaction-pipeline | FULL | SUCCESS_WITH_REJECTS | mahesh | 2026-11-03 00:56:19 | 2026-11-03 00:56:28 | 9484 | 401 | 334 | 67 | 10 | 406 | 0 | null |

**આ table કેવી રીતે વાંચવું:**
* **Job 1:** 100 વાંચ્યા = 82 valid + 18 rejected (તેમાંથી 3 duplicate). `loaded_count = 223` = બધા silver tables માં લખાયેલી rows (79 dim_date + 33 dim_payer + 14 dim_payment_method + 15 dim_biller + 82 fact_payment).
* **Job 3:** `source_count = 0`, કારણ કે job 1 એ બધા flags `Y` / `E` / `D` કરી દીધા હતા. INCREMENTAL ફક્ત `N` વાંચે છે.
* **Job 4:** RETRY_REJECTED એ ફક્ત 15 `E` documents વાંચ્યા (18 rejected − 3 duplicate). 1 સુધર્યો, એટલે `resolved_count = 1`.
* **Job 5:** ફક્ત નવો customer (1 record) વંચાયો.
* **Job 8:** `FAILED`, અને `error_message` માં **કયો step** અને **કેમ** fail થયો તે દેખાય છે. `error_detail` column માં પૂરો stack trace છે (અહીં બતાવ્યો નથી).
* **Job 9:** job 8 fail થયો ત્યારે MongoDB flag બદલાયો નહોતો, એટલે એ જ record ફરી વંચાયો અને load થયો.
* **Job 10:** `source_count = 401`, કારણ કે job 5 માં નવો customer ઉમેર્યો હતો.

---

### B. `etl_audit.etl_job_step` – job નો દરેક step

**કોણ લખે છે:** `StepTracker.track()` → `JobStepRepository.startStep()` / `finishStep()`. **ક્યાં દેખાય:** end point 5 (`steps[]`).

**Job 1 ના બધા 10 steps** (સફળ job):

| job_step_id | job_run_id | step_order | step_name | status | record_count | start_time | duration_ms | error_message |
|---|---|---|---|---|---|---|---|---|
| 1 | 1 | 1 | Extract payments (INCREMENTAL) | SUCCESS | 100 | 2026-11-03 00:55:12 | 3614 | null |
| 2 | 1 | 2 | Validate payments | SUCCESS | 82 | 2026-11-03 00:55:15 | 5148 | null |
| 3 | 1 | 3 | Load silver.dim_date | SUCCESS | 79 | 2026-11-03 00:55:21 | 1161 | null |
| 4 | 1 | 4 | Load silver.dim_payer | SUCCESS | 33 | 2026-11-03 00:55:22 | 570 | null |
| 5 | 1 | 5 | Load silver.dim_payment_method | SUCCESS | 14 | 2026-11-03 00:55:22 | 590 | null |
| 6 | 1 | 6 | Load silver.dim_biller | SUCCESS | 15 | 2026-11-03 00:55:23 | 487 | null |
| 7 | 1 | 7 | Load silver.fact_payment | SUCCESS | 82 | 2026-11-03 00:55:24 | 973 | null |
| 8 | 1 | 8 | Save rejected records [payments] | SUCCESS | 18 | 2026-11-03 00:55:25 | 324 | null |
| 9 | 1 | 9 | Remove fixed records from rejected table [payments] | SUCCESS | 0 | 2026-11-03 00:55:25 | 306 | null |
| 10 | 1 | 10 | Update MongoDB flags [payments] | SUCCESS | 100 | 2026-11-03 00:55:25 | 477 | null |

* Step 1–2 = **EXTRACT + VALIDATE** (`SourceProcessor`)
* Step 3–7 = **LOAD** (`AbstractEtlPipeline.load()` → `PostgresWriter.upsert()`). Dimensions પહેલાં, fact છેલ્લે.
* Step 8–10 = **COMMIT** (`SourceCommitService`). Step 10 માં 100 = 82 `Y` + 15 `E` + 3 `D`.

**Job 8 ના steps** (fail થયેલો job):

| job_step_id | job_run_id | step_order | step_name | status | record_count | duration_ms | error_message |
|---|---|---|---|---|---|---|---|
| 117 | 8 | 1 | Extract payments (INCREMENTAL) | SUCCESS | 1 | 81 | null |
| 118 | 8 | 2 | Validate payments | SUCCESS | 1 | 2047 | null |
| 119 | 8 | 3 | Load silver.dim_date | SUCCESS | 1 | 309 | null |
| 120 | 8 | 4 | Load silver.dim_payer | SUCCESS | 1 | 307 | null |
| 121 | 8 | 5 | Load silver.dim_payment_method | SUCCESS | 1 | 194 | null |
| 122 | 8 | 6 | Load silver.dim_biller | FAILED | null | 316 | PSQLException: ERROR: relation "silver.dim_biller" does not exist   Position: 13 |

* Step 6 `FAILED` થયો, એટલે **તેના પછીના steps ચાલ્યા જ નહીં** (fact load, rejected save, MongoDB flag update). એટલે MongoDB માં `PAY-0001` નો flag `N` જ રહ્યો, અને job 9 એ તેને ફરી process કર્યો.
* `record_count = null`, કારણ કે step પૂરો જ ન થયો.

---

### C. `etl_audit.etl_rejected_record` – ખરાબ records

**કોણ લખે છે:** `SourceCommitService.saveRejectedRecords()` → `RejectedRecordRepository.upsertFromStaging()`. **કોણ delete કરે છે:** `deleteResolvedFromStaging()` (record સુધરી જાય ત્યારે). **ક્યાં દેખાય:** end point 8, 9.

| rejected_record_id | pipeline_name | source_collection | source_id | business_key | error_type | error_reason | raw_record | attempt_count | first_job_run_id | last_job_run_id |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | payment-pipeline | payments | 6ae8e38bc52c74ce996a5071 | PAY-0009 | CORRUPT | [CORRUPT] paymentDate is not a valid timestamp (expected yyyy-MM-ddTHH:mm:s... | {"amount": "7020.7", "status": "SUCCESS", "pa... | 3 | 1 | 7 |
| 3 | payment-pipeline | payments | 6ae8e38bc52c74ce996a5078 | PAY-0016 | CORRUPT | [CORRUPT] paymentDate is not a valid timestamp (expected yyyy-MM-ddTHH:mm:s... | {"amount": "9656.11", "status": "SUCCESS", "p... | 3 | 1 | 7 |
| 8 | payment-pipeline | payments | 6ae8e38bc52c74ce996a509c | PAY-0052 | CORRUPT | [CORRUPT] amount is not a valid number (value: true) | {"amount": "true", "status": "PENDING", "paye... | 3 | 1 | 7 |
| 2 | payment-pipeline | payments | 6ae8e38bc52c74ce996a5076 | PAY-0014 | VALIDATION | [VALIDATION] status must be one of [SUCCESS, FAILED, PENDING] (value: DONE) | {"amount": "6968.3", "status": "DONE", "payer... | 3 | 1 | 7 |
| 4 | payment-pipeline | payments | 6ae8e38bc52c74ce996a507b | PAY-0019 | VALIDATION | [VALIDATION] amount must be greater than 0 (value: -99.0) | {"amount": "-99.0", "status": "SUCCESS", "pay... | 3 | 1 | 7 |
| 5 | payment-pipeline | payments | 6ae8e38bc52c74ce996a507e | PAY-0022 | VALIDATION | [VALIDATION] payerId is required | {"amount": "7713.81", "status": "SUCCESS", "c... | 3 | 1 | 7 |
| 68 | transaction-pipeline | transactions | 6ae8e38bc52c74ce996a5143 | TXN-000019 | ORPHAN | [ORPHAN] merchantId not found in dim_merchant (value: MER-9901) | {"amount": "74172.32", "status": "SUCCESS", "... | 3 | 2 | 10 |
| 69 | transaction-pipeline | transactions | 6ae8e38bc52c74ce996a5146 | TXN-000022 | ORPHAN | [ORPHAN] customerId not found in dim_customer (value: CUST-9902) | {"amount": "25135.04", "status": "SUCCESS", "... | 3 | 2 | 10 |
| 77 | transaction-pipeline | transactions | 6ae8e38bc52c74ce996a515d | TXN-000045 | ORPHAN | [ORPHAN] accountId not found in dim_account (value: ACC-9901) | {"amount": "74234.37", "status": "SUCCESS", "... | 3 | 2 | 10 |
| 16 | payment-pipeline | payments | 6ae8e38bc52c74ce996a508a | PAY-0034 | DUPLICATE | [DUPLICATE] another record with paymentId = PAY-0034 was kept (latest updat... | {"amount": "6016.79", "status": "FAILED", "pa... | 2 | 1 | 7 |

* **`source_id`** = MongoDB નો `_id`. એ document MongoDB માં સીધો શોધી શકો: `db.payments.find({_id: ObjectId("...")})`
* **`raw_record`** = original document (સાફ કરતા પહેલાંનો), એટલે ખબર પડે કે source માં શું આવ્યું હતું
* **`attempt_count`:**
  * payments ના rejected = **3** (job 1, 4 RETRY, 7 FULL, ત્રણેયમાં reject થયા). `first_job_run_id = 1`, `last_job_run_id = 7`
  * DUPLICATE = **2** (job 1 અને 7). Job 4 (RETRY_REJECTED) duplicate ને વાંચતો જ નથી, કારણ કે તેનો flag `D` છે
  * ORPHAN = **3** (job 2, 6, 10)
* **`PAY-0078` અહીં નથી!** તે job 4 માં સુધરી ગયો, એટલે તેની row **delete** થઈ ગઈ. આ જ રીતે `CUST-9901` વાળો transaction job 6 માં delete થયો.

---

### D. `silver.*` – Example 1 (`payment-pipeline`) નું star schema

**કોણ લખે છે:** `PostgresWriter.upsert()` (staging table → `INSERT ... ON CONFLICT`). Mapping `PaymentStarSchemaMapper` માં.

#### `silver.dim_date` (બંને pipelines share કરે છે) – `DateDimension.fromColumn()`

| date_key | full_date | day_of_month | month | month_name | quarter | year | day_of_week | day_name | is_weekend | job_run_id |
|---|---|---|---|---|---|---|---|---|---|---|
| 20250106 | 2025-01-06 | 6 | 1 | January | 1 | 2025 | 2 | Monday | f | 7 |
| 20250111 | 2025-01-11 | 11 | 1 | January | 1 | 2025 | 7 | Saturday | t | 7 |
| 20250113 | 2025-01-13 | 13 | 1 | January | 1 | 2025 | 2 | Monday | f | 7 |
| 20250117 | 2025-01-17 | 17 | 1 | January | 1 | 2025 | 6 | Friday | f | 7 |
| 20250118 | 2025-01-18 | 18 | 1 | January | 1 | 2025 | 7 | Saturday | t | 10 |
| 20250122 | 2025-01-22 | 22 | 1 | January | 1 | 2025 | 4 | Wednesday | f | 10 |
| 20250123 | 2025-01-23 | 23 | 1 | January | 1 | 2025 | 5 | Thursday | f | 7 |
| 20250202 | 2025-02-02 | 2 | 2 | February | 1 | 2025 | 1 | Sunday | t | 10 |
| 20250205 | 2025-02-05 | 5 | 2 | February | 1 | 2025 | 4 | Wednesday | f | 7 |
| 20250207 | 2025-02-07 | 7 | 2 | February | 1 | 2025 | 6 | Friday | f | 7 |

* `date_key` = `yyyyMMdd` number. Fact tables આ key થી date સાથે join થાય છે
* `day_of_week`: 1 = Sunday ... 7 = Saturday. `is_weekend` = Saturday / Sunday

#### `silver.dim_payer` – `toDimPayer()` (એક payer = એક row)

| payer_key | payer_id | payer_name | payer_email | payer_phone | payer_city | job_run_id |
|---|---|---|---|---|---|---|
| 1 | PYR-001 | Rahul Agarwal | rahul.agarwal1@yahoo.in | 9668912176 | Surat | 7 |
| 2 | PYR-002 | Hetal Desai | hetal.desai2@rediffmail.com | 9754105643 | Ahmedabad | 7 |
| 3 | PYR-003 | Parth Chauhan | parth.chauhan3@gmail.com | 9858674900 | Vadodara | 7 |
| 4 | PYR-004 | Yash Gupta | yash.gupta4@gmail.com | 8424976970 | Bengaluru | 7 |
| 5 | PYR-005 | Isha Desai | isha.desai5@yahoo.in | 7069795356 | Delhi | 7 |
| 6 | PYR-007 | Harsh Reddy | harsh.reddy7@outlook.com | 9185316706 | Surat | 7 |
| 7 | PYR-008 | Foram Iyer | foram.iyer8@yahoo.in | 9405499208 | Delhi | 7 |
| 8 | PYR-009 | Neha Singh | neha.singh9@rediffmail.com | 7284292676 | Mumbai | 9 |
| 9 | PYR-010 | Anjali Rao | anjali.rao10@outlook.com | 7372082032 | Indore | 7 |
| 10 | PYR-011 | Ravi Kapoor | ravi.kapoor11@gmail.com | 9931061597 | Pune | 7 |

* `payer_key` = **surrogate key** (database બનાવે છે, `BIGSERIAL`). `payer_id` = **business key** (source માંથી). Upsert `payer_id` પર થાય છે
* એક payer ના ઘણા payments હોય છે, પણ dimension માં એક જ row (સૌથી નવી details) હોય છે

#### `silver.dim_payment_method` – `toDimPaymentMethod()` (method + provider નો combination)

| payment_method_key | payment_method | payment_provider | job_run_id |
|---|---|---|---|
| 1 | CARD | MASTERCARD | 7 |
| 2 | WALLET | MOBIKWIK | 7 |
| 3 | WALLET | AMAZONPAY | 7 |
| 4 | WALLET | FREECHARGE | 7 |
| 5 | CARD | RUPAY | 7 |
| 6 | UPI | PAYTM | 7 |
| 7 | UPI | BHIM | 7 |
| 8 | CARD | VISA | 7 |
| 9 | NETBANKING | HDFC | 7 |
| 10 | UPI | GPAY | 9 |

#### `silver.dim_biller` – `toDimBiller()`

| biller_key | biller_name | biller_category | job_run_id |
|---|---|---|---|
| 1 | AMC Water Tax | WATER | 7 |
| 2 | Adani Electricity | ELECTRICITY | 7 |
| 3 | Adani Total Gas | GAS | 7 |
| 4 | Airtel Postpaid | MOBILE | 7 |
| 5 | Airtel Xstream | BROADBAND | 7 |
| 6 | Dish TV | DTH | 7 |
| 7 | Gujarat Gas | GAS | 7 |
| 8 | HDFC Life | INSURANCE | 7 |
| 9 | Jio Prepaid | MOBILE | 7 |
| 10 | JioFiber | BROADBAND | 9 |

#### `silver.fact_payment` – `toFactPayment()` (એક payment = એક row)

| payment_key | payment_id | payer_key | payment_method_key | biller_key | date_key | amount | currency | payment_status | reference_number | payment_ts | job_run_id |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 84 | PAY-0001 | 8 | 10 | 10 | 20251004 | 800.00 | INR | PENDING | REF1175249130 | 2025-10-04 00:40:04 | 9 |
| 85 | PAY-0002 | 21 | 4 | 5 | 20250925 | 564.87 | INR | SUCCESS | REF1086694394 | 2025-09-25 05:57:23 | 7 |
| 86 | PAY-0003 | 32 | 7 | 2 | 20260415 | 6553.73 | INR | FAILED | REF1650671141 | 2026-04-15 16:34:56 | 7 |
| 87 | PAY-0004 | 5 | 11 | 13 | 20260815 | 10379.90 | INR | SUCCESS | REF1786082662 | 2026-08-15 03:31:41 | 7 |
| 88 | PAY-0005 | 27 | 3 | 11 | 20250802 | 10032.93 | INR | SUCCESS | REF1602645802 | 2025-08-02 02:49:30 | 7 |
| 89 | PAY-0006 | 24 | 6 | 4 | 20251128 | 2929.40 | INR | SUCCESS | REF1297917169 | 2025-11-28 06:29:21 | 7 |
| 90 | PAY-0007 | 33 | 1 | 9 | 20260731 | 340.34 | INR | SUCCESS | REF1738911028 | 2026-07-31 02:06:27 | 7 |
| 91 | PAY-0008 | 26 | 10 | 11 | 20260327 | 4635.14 | INR | SUCCESS | REF1655551004 | 2026-03-27 08:35:14 | 7 |
| 92 | PAY-0010 | 26 | 6 | 13 | 20250111 | 9842.01 | INR | SUCCESS | REF1188943698 | 2025-01-11 06:59:03 | 7 |
| 93 | PAY-0011 | 8 | 7 | 2 | 20250903 | 4747.10 | INR | SUCCESS | REF1356824540 | 2025-09-03 15:55:04 | 7 |

* **Fact માં નામ નથી, ફક્ત keys છે:** `payer_key = 8` નો અર્થ `dim_payer` ની row 8. આ `addSurrogateKey()` એ join કરીને ઉમેર્યું છે
* **`payment_key` 84 થી કેમ શરૂ થાય?** Job 7 (FULL) એ `TRUNCATE` કર્યું અને બધું ફરી insert કર્યું. `TRUNCATE` ID counter reset નથી કરતું, એટલે નવી keys 84, 85... થી શરૂ થઈ. આ સામાન્ય છે (business key `payment_id` એ જ રહે છે)
* **`PAY-0001` માં `job_run_id = 9`, બાકીના માં 7:** job 9 એ ફક્ત `PAY-0001` (બદલાયેલો record) update કર્યો. આ **INCREMENTAL upsert** છે
* `PAY-0009` અહીં નથી, કારણ કે તે rejected છે (table C જુઓ)

---

### E. `silver.*` – Example 2 (`transaction-pipeline`) નું star schema

Mapping `TransactionStarSchemaMapper` માં.

#### `silver.dim_customer` – `customers` collection માંથી

| customer_key | customer_id | full_name | email | phone | date_of_birth | gender | city | state | kyc_status | job_run_id |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | CUST-0001 | Nikhil Agarwal | nikhil.agarwal1@gmail.com | 8164452831 | 1965-01-06 | M | Rajkot | Gujarat | VERIFIED | 10 |
| 2 | CUST-0002 | Deepak Nair | deepak.nair2@rediffmail.com | 6925986413 | 1975-08-19 | M | Indore | Madhya Pradesh | PENDING | 10 |
| 3 | CUST-0003 | Rohan Trivedi | rohan.trivedi3@outlook.com | 8777902340 | 1966-03-17 | M | Chennai | Tamil Nadu | VERIFIED | 10 |
| 24 | CUST-0004 | Sneha Nair | sneha.nair4@yahoo.in | 8514822445 | 1970-07-23 | F | Ahmedabad | Gujarat | VERIFIED | 10 |
| 71 | CUST-0005 | Jay Trivedi | jay.trivedi5@gmail.com | 9058706460 | 1971-06-21 | M | Lucknow | Uttar Pradesh | PENDING | 10 |
| 72 | CUST-0007 | Krupa Pandya | krupa.pandya7@gmail.com | 7287558099 | 1966-03-30 | F | Rajkot | Gujarat | PENDING | 10 |
| 4 | CUST-0008 | Krupa Mehta | krupa.mehta8@yahoo.in | 9242502303 | 1995-06-09 | F | Kolkata | West Bengal | VERIFIED | 10 |
| 46 | CUST-0010 | Asha Verma | asha.verma10@yahoo.in | 7752369787 | 1962-09-28 | F | Bengaluru | Karnataka | VERIFIED | 10 |
| 25 | CUST-0011 | Nikhil Sharma | nikhil.sharma11@yahoo.in | 8937248076 | 1981-10-17 | M | Hyderabad | Telangana | VERIFIED | 10 |
| 5 | CUST-0012 | Parth Bhatt | parth.bhatt12@rediffmail.com | 7610966464 | 1977-09-21 | M | Rajkot | Gujarat | VERIFIED | 10 |

* `full_name` source માં નથી. Mapper `firstName + " " + lastName` થી બનાવે છે
* Sanitizer ના લીધે names proper case, email lower case અને phone 10 digit છે

#### `silver.dim_account` – `accounts` collection માંથી

| account_key | account_id | customer_id | account_type | branch_code | ifsc_code | currency | account_status | current_balance | opened_date | job_run_id |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | ACC-0001 | CUST-0046 | CURRENT | BR-020 | ICIC0432848 | INR | ACTIVE | 84904.69 | 2018-11-14 | 10 |
| 2 | ACC-0002 | CUST-0029 | CURRENT | BR-004 | UTIB0718560 | INR | ACTIVE | 321815.39 | 2023-06-27 | 10 |
| 19 | ACC-0003 | CUST-0091 | CURRENT | BR-001 | KKBK0961802 | INR | ACTIVE | 496887.84 | 2020-06-10 | 10 |
| 75 | ACC-0004 | CUST-0062 | SAVINGS | BR-020 | HDFC0328664 | INR | DORMANT | 228054.03 | 2024-09-08 | 10 |
| 3 | ACC-0005 | CUST-0008 | CURRENT | BR-024 | HDFC0199116 | INR | ACTIVE | 235691.00 | 2017-06-19 | 10 |
| 40 | ACC-0007 | CUST-0038 | CURRENT | BR-027 | KKBK0864923 | INR | DORMANT | 332234.02 | 2019-12-21 | 10 |
| 4 | ACC-0008 | CUST-0004 | CURRENT | BR-010 | UTIB0821104 | INR | ACTIVE | 241504.42 | 2015-10-14 | 10 |
| 5 | ACC-0009 | CUST-0004 | NRI | BR-005 | HDFC0885852 | USD | ACTIVE | 291303.58 | 2023-04-02 | 10 |
| 76 | ACC-0010 | CUST-0091 | SALARY | BR-023 | HDFC0516632 | INR | ACTIVE | 346942.67 | 2021-03-05 | 10 |
| 41 | ACC-0011 | CUST-0092 | CURRENT | BR-018 | HDFC0015491 | INR | ACTIVE | 189897.23 | 2022-03-02 | 10 |

#### `silver.dim_merchant` – `merchants` collection માંથી

| merchant_key | merchant_id | merchant_name | category | mcc_code | city | merchant_status | onboarded_date | job_run_id |
|---|---|---|---|---|---|---|---|---|
| 18 | MER-0001 | Apollo Pharmacy Indore | HEALTHCARE | 5912 | Indore | ACTIVE | 2020-06-14 | 10 |
| 19 | MER-0002 | Bharat Petroleum Vadodara | FUEL | 5541 | Vadodara | ACTIVE | 2021-03-19 | 10 |
| 1 | MER-0003 | HP Petrol Pump Rajkot | FUEL | 5541 | Rajkot | ACTIVE | 2022-09-02 | 10 |
| 44 | MER-0004 | Bharat Petroleum Jaipur | FUEL | 5541 | Jaipur | ACTIVE | 2019-11-19 | 10 |
| 2 | MER-0005 | Barbeque Nation Delhi | DINING | 5812 | Delhi | ACTIVE | 2024-11-29 | 10 |
| 20 | MER-0006 | Apollo Pharmacy Delhi | HEALTHCARE | 5912 | Delhi | ACTIVE | 2021-03-14 | 10 |
| 45 | MER-0007 | BookMyShow Rajkot | ENTERTAINMENT | 7832 | Rajkot | ACTIVE | 2018-09-27 | 10 |
| 21 | MER-0009 | PVR Cinemas Kolkata | ENTERTAINMENT | 7832 | Kolkata | ACTIVE | 2019-11-06 | 10 |
| 46 | MER-0010 | PVR Cinemas Surat | ENTERTAINMENT | 7832 | Surat | ACTIVE | 2020-10-23 | 10 |
| 71 | MER-0012 | Swiggy Vadodara | DINING | 5812 | Vadodara | ACTIVE | 2024-02-15 | 10 |

#### `silver.fact_transaction` – `transactions` collection માંથી

| transaction_key | transaction_id | customer_key | account_key | merchant_key | date_key | transaction_type | channel | amount | currency | transaction_status | transaction_ts | job_run_id |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 76 | TXN-000001 | 75 | 12 | 65 | 20260920 | DEBIT | ONLINE | 4353.18 | INR | SUCCESS | 2026-09-20 10:51:09 | 10 |
| 77 | TXN-000002 | 10 | 82 | 17 | 20251017 | CREDIT | UPI | 73607.76 | INR | SUCCESS | 2025-10-17 14:43:34 | 10 |
| 78 | TXN-000003 | 36 | 6 | 87 | 20260428 | DEBIT | NETBANKING | 69907.67 | INR | PENDING | 2026-04-28 12:34:40 | 10 |
| 79 | TXN-000005 | 5 | 61 | 87 | 20251013 | DEBIT | ONLINE | 39416.78 | INR | SUCCESS | 2025-10-13 10:12:55 | 10 |
| 80 | TXN-000006 | 66 | 68 | 49 | 20250516 | DEBIT | POS | 31052.91 | INR | SUCCESS | 2025-05-16 19:43:51 | 10 |
| 81 | TXN-000007 | 55 | 7 | 69 | 20250620 | DEBIT | ATM | 32660.22 | INR | SUCCESS | 2025-06-20 08:27:32 | 10 |
| 82 | TXN-000008 | 66 | 22 | 85 | 20250927 | CREDIT | NETBANKING | 59096.71 | USD | PENDING | 2025-09-27 17:25:17 | 10 |
| 83 | TXN-000009 | 73 | 23 | 47 | 20260625 | DEBIT | ATM | 29424.33 | USD | SUCCESS | 2026-06-25 21:44:03 | 10 |
| 84 | TXN-000010 | 13 | 26 | 28 | 20260523 | DEBIT | NETBANKING | 40147.80 | INR | SUCCESS | 2026-05-23 06:47:05 | 10 |
| 85 | TXN-000011 | 51 | 2 | 50 | 20260306 | DEBIT | POS | 33038.76 | INR | SUCCESS | 2026-03-06 09:12:38 | 10 |

* `customer_key`, `account_key`, `merchant_key` = ત્રણ dimensions ની surrogate keys
* Orphan transactions (`TXN-000019`, `TXN-000022`, `TXN-000045` ...) અહીં નથી. તે rejected table માં છે

---

### F. MongoDB `bronze_db.payments` – job પછી document માં શું બદલાય છે

**કોણ લખે છે:** `SourceCommitService.updateSourceFlags()` → `MongoFlagUpdater`. ત્રણ fields set થાય છે: `processedFlag`, `etlJobRunId`, `etlProcessedAt`. બાકીનો document જેમ હતો તેમ જ રહે છે.

**Job પહેલાં** (sample data, દરેક document): `"processedFlag": "N"`, અને `etlJobRunId` / `etlProcessedAt` fields હોતા જ નથી.

**Job 10 પછી** (5 `Y`, 3 `E`, 2 `D` ના ઉદાહરણ):

| _id | paymentId | amount | status | processedFlag | etlJobRunId | etlProcessedAt (UTC) |
|---|---|---|---|---|---|---|
| 6ae8e38bc52c74ce996a5069 | PAY-0001 | 800 | PENDING | **Y** | 9 | 2026-11-02 19:26:19 |
| 6ae8e38bc52c74ce996a506a | PAY-0002 | 564.87 | SUCCESS | **Y** | 7 | 2026-11-02 19:26:08 |
| 6ae8e38bc52c74ce996a506b | PAY-0003 | 6553.73 | FAILED | **Y** | 7 | 2026-11-02 19:26:08 |
| 6ae8e38bc52c74ce996a506c | PAY-0004 | 10379.9 | SUCCESS | **Y** | 7 | 2026-11-02 19:26:08 |
| 6ae8e38bc52c74ce996a506d | PAY-0005 | 10032.93 | SUCCESS | **Y** | 7 | 2026-11-02 19:26:08 |
| 6ae8e38bc52c74ce996a5071 | PAY-0009 | 7020.7 | SUCCESS | **E** | 7 | 2026-11-02 19:26:08 |
| 6ae8e38bc52c74ce996a5076 | PAY-0014 | 6968.3 | DONE | **E** | 7 | 2026-11-02 19:26:08 |
| 6ae8e38bc52c74ce996a5078 | PAY-0016 | 9656.11 | SUCCESS | **E** | 7 | 2026-11-02 19:26:08 |
| 6ae8e38bc52c74ce996a50ca | PAY-0025 | 11913.44 | SUCCESS | **D** | 7 | 2026-11-02 19:26:08 |
| 6ae8e38bc52c74ce996a508a | PAY-0034 | 6016.79 | FAILED | **D** | 7 | 2026-11-02 19:26:08 |

* `Y` = silver માં load થઈ ગયો, next INCREMENTAL વખતે નહીં વંચાય
* `E` = rejected. Document સુધારીને RETRY_REJECTED ચલાવો
* `D` = duplicate. ક્યારેય retry નહીં થાય (FULL માં ફરી duplicate તરીકે જ પકડાશે)
* `PAY-0001` માં `etlJobRunId = 9` = છેલ્લે job 9 એ process કર્યો
* `etlProcessedAt` MongoDB માં **UTC** માં save થાય છે (19:26 UTC = 00:56 India time)
* **Source application માટે નિયમ:** document બદલો ત્યારે `processedFlag` ને પાછો `"N"` કરો, તો જ next INCREMENTAL તેને લેશે

---

### G. `etl_staging.*` – temporary tables

દરેક `load()` અને દરેક commit step વખતે `PostgresWriter.writeToStaging()` એક table બનાવે છે, દા.ત. `etl_staging.stg_silver_dim_payer_1_2` (= table નામ + job id + ક્રમ નંબર). SQL upsert પછી **તરત drop** થઈ જાય છે, એટલે job પૂરો થયા પછી આ schema હંમેશા **ખાલી** હોય છે. Job ચાલતો હોય ત્યારે જ થોડી seconds માટે દેખાય.

---

### H. 10 jobs પછી દરેક table માં કુલ rows

| table_name | rows |
|---|---|
| silver.dim_date | 141 |
| silver.dim_payer | 33 |
| silver.dim_payment_method | 14 |
| silver.dim_biller | 15 |
| silver.fact_payment | 83 |
| silver.dim_customer | 86 |
| silver.dim_account | 85 |
| silver.dim_merchant | 88 |
| silver.fact_transaction | 75 |
| etl_audit.etl_job_run | 10 |
| etl_audit.etl_job_step | 158 |
| etl_audit.etl_rejected_record | 84 |

* `dim_customer = 86` = 85 valid (sample data) + 1 નવો (`CUST-9901`)
* `fact_payment = 83` = 82 + 1 (સુધરેલો `PAY-0078`)
* `fact_transaction = 75` = 74 + 1 (orphan transaction જે customer ઉમેર્યા પછી valid થયો)
* `etl_rejected_record = 84` = 86 rejected (18 + 68) − 2 સુધરી ગયા

---

## ઝડપી યાદી: કયો end point કયા class / table માં જાય

### દરેક end point શું કરે છે (ટૂંકમાં)

#### 1. `GET /api/etl/pipelines`: બધી pipelines ની યાદી
**ટૂંકમાં:** project માં કઈ કઈ ETL pipelines છે તે બતાવે છે.
* `PipelineRegistry` માંથી બધી `@Component` pipelines ની માહિતી આપે છે: નામ, description, source collections, target tables
* દરેક pipeline અત્યારે ચાલે છે કે નહીં (`running`) તે `JobLockManager` માંથી બતાવે છે
* Database માં કશું વાંચતું કે લખતું નથી. Data Java code માંથી આવે છે

#### 2. `POST /api/etl/pipelines/payment-pipeline/run`: payment pipeline ચલાવો (INCREMENTAL)
**ટૂંકમાં:** MongoDB `payments` ના **નવા** documents (`processedFlag = N`) ને silver star schema માં load કરે છે.
* Job શરૂ થાય: `etl_job_run` માં `RUNNING` row બને
* Extract → sanitize → validate → duplicate check → transform → dimensions અને fact માં upsert
* ખરાબ records `etl_rejected_record` માં જાય, MongoDB flags `Y` / `E` / `D` થાય
* Job પૂરો થાય ત્યારે response માં status અને counts મળે (job પૂરો થાય ત્યાં સુધી રાહ જુએ છે)
* એ જ pipeline પહેલેથી ચાલતી હોય તો **409**

#### 3. `POST /api/etl/pipelines/transaction-pipeline/run?async=true`: transaction pipeline background માં ચલાવો
**ટૂંકમાં:** 4 collections (customers, accounts, merchants, transactions) ને star schema માં load કરે છે, અને **તરત જવાબ આપે છે**.
* Response **202** અને `jobRunId` તરત મળે. Job thread pool માં ચાલુ રહે છે
* Customers → accounts → merchants → transactions ના order માં load થાય
* ORPHAN check: transaction નો customer, account અથવા merchant ન મળે તો reject
* Result જોવા માટે end point 5 (`/api/etl/jobs/{id}`) વાપરો

#### 4. `...?loadType=FULL`: આખો data ફરીથી load કરો
**ટૂંકમાં:** fact table ખાલી કરીને MongoDB ના **બધા** documents (flag ગમે તે હોય) ફરી load કરે છે.
* પહેલાં `fullLoadTruncateTables()` વાળા fact tables `TRUNCATE` થાય
* Dimensions truncate નથી થતા, ફક્ત upsert થાય છે
* હવે valid થયેલા જૂના rejected records rejected table માંથી delete થાય
* Rule બદલ્યો હોય અથવા data શરૂઆતથી બનાવવો હોય ત્યારે વાપરો

#### 5. `GET /api/etl/jobs/{id}`: એક job ની પૂરી વિગત
**ટૂંકમાં:** એક job નું status, counts, time અને **દરેક step** ની વિગત બતાવે છે.
* `etl_job_run` (job) અને `etl_job_step` (steps) માંથી વાંચે છે
* Job fail થયો હોય તો કયા step માં અને કેમ, તે દેખાય છે
* Id ન મળે તો **404**. કશું save કરતું નથી

#### 6. `GET /api/etl/jobs?pipelineName=...`: job history
**ટૂંકમાં:** jobs ની યાદી આપે છે, સૌથી નવો પહેલો.
* `pipelineName`, `status`, `limit` filters optional છે (`limit` વધુમાં વધુ 500)
* Steps નથી આપતું. Steps માટે end point 5 વાપરો
* ફક્ત `etl_job_run` વાંચે છે

#### 7. `GET /api/etl/jobs?status=FAILED`: ફક્ત fail થયેલા jobs
**ટૂંકમાં:** end point 6 જેવું જ, પણ ફક્ત `FAILED` jobs બતાવે છે.
* `errorMessage` માં fail થવાનું કારણ દેખાય છે
* App crash થઈ હોય અને job `RUNNING` માં અટક્યો હોય, તો app ફરી start થાય ત્યારે તે job પણ `FAILED` બની જાય છે અને અહીં દેખાય છે

#### 8. `GET /api/etl/rejected-records`: ખરાબ records ની યાદી
**ટૂંકમાં:** કયા records reject થયા, **કેમ** થયા અને original data શું હતો, તે બતાવે છે.
* `etl_rejected_record` માંથી વાંચે છે. Filters: `pipelineName`, `collection`, `errorType`, `limit`
* દરેક record માં MongoDB `_id`, business key, error type, કારણ, `rawRecord` અને `attemptCount` હોય છે
* આ જોઈને MongoDB માં data સુધારી શકાય

#### 9. `GET /api/etl/rejected-records/summary`: rejected records ના counts
**ટૂંકમાં:** pipeline, collection અને error type દીઠ કેટલા ખરાબ records છે, તે count બતાવે છે.
* `GROUP BY` query, dashboard માટે ઉપયોગી
* દા.ત. "payments માં CORRUPT 5, VALIDATION 10"

#### 10. `...?loadType=RETRY_REJECTED`: સુધારેલા records ફરી ચલાવો
**ટૂંકમાં:** ફક્ત rejected (`processedFlag = E`) documents ફરી process કરે છે.
* પહેલાં MongoDB માં ખરાબ document સુધારો, પછી આ end point ચલાવો
* સુધરેલો record silver માં load થાય, rejected table માંથી **delete** થાય, flag `E` → `Y` બને (`resolvedCount` વધે)
* હજી ખરાબ હોય તો rejected table માં જ રહે, અને `attempt_count` 1 વધે
* Duplicate (`D`) records આમાં વંચાતા નથી

#### 11. `POST /api/dev/reset?cleanPostgres=true`: શરૂઆતથી test કરવા reset (ફક્ત dev)
**ટૂંકમાં:** MongoDB માં sample data ફરી લોડ કરે છે, અને PostgreSQL ના બધા tables ખાલી કરે છે.
* બધા collections drop કરીને `sample-data/*.json` ફરી insert કરે છે. બધા flags પાછા `N`
* `cleanPostgres=true` હોય તો `silver`, `etl_audit`, `etl_staging` ના tables `TRUNCATE ... RESTART IDENTITY` થાય
* ફક્ત `dev` profile માં ચાલે છે. Prod માં **404**

#### 12. `GET /actuator/health`: app ચાલે છે કે નહીં
**ટૂંકમાં:** app, MongoDB અને PostgreSQL ના connection `UP` છે કે `DOWN`, તે બતાવે છે. આ Spring Boot નું end point છે, આપણો code નથી.

#### 13. `GET /api/ingestion/sample-data/files`: sample files ની યાદી
**ટૂંકમાં:** `sample-data` folder માં કઈ JSON files છે, તેના નામ ની યાદી આપે છે. File નું નામ = collection નું નામ (`payments.json` → `payments`). કશું insert કરતું નથી.

#### 14. `POST /api/ingestion/sample-data?mode=REPLACE`: બધી files MongoDB માં નાખો
**ટૂંકમાં:** બધી JSON files એક પછી એક વાંચીને તેના collection માં insert કરે છે.
* દરેક file માટે 5 steps: RESOLVE → READ → PREPARE → INSERT (500 ના batch) → VERIFY
* `mode`: `REPLACE` (પહેલાં drop), `APPEND` (ઉમેરો), `SKIP_IF_EXISTS` (data હોય તો છોડી દો)
* એક file fail થાય તો પણ બાકીની files ચાલુ રહે. **200** = બધી OK, **207** = થોડી fail, **500** = બધી fail

#### 15. `POST /api/ingestion/sample-data/{fileName}?mode=REPLACE`: એક જ file MongoDB માં નાખો
**ટૂંકમાં:** end point 14 જેવું જ, પણ ફક્ત એક file માટે (દા.ત. `payments.json` → `payments` collection).

### કયો end point કયા class / table માં જાય

| # | End point | Controller | Service / Class | Read | Write |
|---|---|---|---|---|---|
| 1 | `GET /api/etl/pipelines` | EtlPipelineController | PipelineRegistry, JobLockManager | Java code | – |
| 2 | `POST .../payment-pipeline/run` | EtlPipelineController | EtlJobService → PaymentPipeline → framework | MongoDB `payments` | silver.*, etl_audit.*, Mongo flags |
| 3 | `POST .../transaction-pipeline/run?async=true` | EtlPipelineController | EtlJobService (thread pool) → TransactionPipeline | MongoDB 4 collections + silver dims | silver.*, etl_audit.*, Mongo flags |
| 4 | `...?loadType=FULL` | EtlPipelineController | EtlJobService (truncate) → pipeline | MongoDB બધા documents | silver.* (fact truncate + reload) |
| 5 | `GET /api/etl/jobs/{id}` | EtlJobController | EtlJobService, JobRunRepository, JobStepRepository | etl_job_run, etl_job_step | – |
| 6 | `GET /api/etl/jobs?pipelineName=` | EtlJobController | JobRunRepository | etl_job_run | – |
| 7 | `GET /api/etl/jobs?status=FAILED` | EtlJobController | JobRunRepository | etl_job_run | – |
| 8 | `GET /api/etl/rejected-records` | RejectedRecordController | RejectedRecordRepository | etl_rejected_record | – |
| 9 | `GET /api/etl/rejected-records/summary` | RejectedRecordController | RejectedRecordRepository | etl_rejected_record | – |
| 10 | `...?loadType=RETRY_REJECTED` | EtlPipelineController | EtlJobService → pipeline → SourceCommitService | MongoDB flag `E` | silver.*, rejected (delete/update), flags |
| 11 | `POST /api/dev/reset` | DevDataController | SampleDataLoader, JdbcTemplate | sample-data/*.json | MongoDB (reload), PostgreSQL (truncate) |
