# 🗂️ Project files: dmj-dwh

> **111 files**: 85 Java (☕), plus scripts, SQL, docs and config. Base package `com.tcs.dmj.dwh`. The `target` build folder, `.git` and `.vscode` are left out. Paths are relative to the project folder.

**Legend:** ☕ Java · 🍃 Mongo script · 🐘 PostgreSQL · 📝 Markdown · ⚙️ YAML · 📦 Maven

---

### 📂 src\main\java\com\tcs\dmj\dwh  ·  _1 file_

- ☕ `src\main\java\com\tcs\dmj\dwh\DmjDwhApp.java`

### 📂 src\main\java\com\tcs\dmj\dwh\config  ·  _2 files_

- ☕ `src\main\java\com\tcs\dmj\dwh\config\ApplicationProperties.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\config\SparkConfig.java`

### 📂 src\main\java\com\tcs\dmj\dwh\connector\source  ·  _5 files_

- ☕ `src\main\java\com\tcs\dmj\dwh\connector\source\LoadWindow.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\connector\source\MongoSourceReader.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\connector\source\SourceBatch.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\connector\source\SourceDefinition.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\connector\source\SourceProcessor.java`

### 📂 src\main\java\com\tcs\dmj\dwh\connector\target  ·  _7 files_

- ☕ `src\main\java\com\tcs\dmj\dwh\connector\target\DateDimension.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\connector\target\DirectPostgresWriter.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\connector\target\PostgresReader.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\connector\target\PostgresWriter.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\connector\target\Scd2Writer.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\connector\target\SoftDeleteColumns.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\connector\target\SoftDeleteWriter.java`

### 📂 src\main\java\com\tcs\dmj\dwh\domain  ·  _2 files_

- ☕ `src\main\java\com\tcs\dmj\dwh\domain\JobRun.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\domain\RejectedRecord.java`

### 📂 src\main\java\com\tcs\dmj\dwh\enums  ·  _2 files_

- ☕ `src\main\java\com\tcs\dmj\dwh\enums\ErrorType.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\enums\JobStatus.java`

### 📂 src\main\java\com\tcs\dmj\dwh\pipeline  ·  _6 files_

- ☕ `src\main\java\com\tcs\dmj\dwh\pipeline\AbstractEtlPipeline.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\pipeline\EtlPipeline.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\pipeline\JobMetrics.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\pipeline\PipelineContext.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\pipeline\PipelineRegistry.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\pipeline\PipelineToolkit.java`

### 📂 src\main\java\com\tcs\dmj\dwh\pipeline\fulfillment  ·  _3 files_

- ☕ `src\main\java\com\tcs\dmj\dwh\pipeline\fulfillment\FulfillmentPipeline.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\pipeline\fulfillment\FulfillmentSources.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\pipeline\fulfillment\FulfillmentStarSchemaMapper.java`

### 📂 src\main\java\com\tcs\dmj\dwh\pipeline\order  ·  _3 files_

- ☕ `src\main\java\com\tcs\dmj\dwh\pipeline\order\EcommerceOrderPipeline.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\pipeline\order\EcommerceOrderSchemaMapper.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\pipeline\order\EcommerceOrderSources.java`

### 📂 src\main\java\com\tcs\dmj\dwh\pipeline\payment  ·  _3 files_

- ☕ `src\main\java\com\tcs\dmj\dwh\pipeline\payment\PaymentPipeline.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\pipeline\payment\PaymentSource.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\pipeline\payment\PaymentStarSchemaMapper.java`

### 📂 src\main\java\com\tcs\dmj\dwh\pipeline\sales  ·  _3 files_

- ☕ `src\main\java\com\tcs\dmj\dwh\pipeline\sales\SalesPipeline.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\pipeline\sales\SalesSources.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\pipeline\sales\SalesStarSchemaMapper.java`

### 📂 src\main\java\com\tcs\dmj\dwh\pipeline\transaction  ·  _3 files_

- ☕ `src\main\java\com\tcs\dmj\dwh\pipeline\transaction\TransactionPipeline.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\pipeline\transaction\TransactionSources.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\pipeline\transaction\TransactionStarSchemaMapper.java`

### 📂 src\main\java\com\tcs\dmj\dwh\pipeline\vatreturn  ·  _3 files_

- ☕ `src\main\java\com\tcs\dmj\dwh\pipeline\vatreturn\VatReturnPipeline.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\pipeline\vatreturn\VatReturnSchemaMapper.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\pipeline\vatreturn\VatReturnSources.java`

### 📂 src\main\java\com\tcs\dmj\dwh\repository  ·  _3 files_

- ☕ `src\main\java\com\tcs\dmj\dwh\repository\JobRunRepository.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\repository\RejectedRecordRepository.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\repository\WatermarkRepository.java`

### 📂 src\main\java\com\tcs\dmj\dwh\service  ·  _9 files_

- ☕ `src\main\java\com\tcs\dmj\dwh\service\DataQualityGateException.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\service\DwhJobService.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\service\DwhPipelineService.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\service\EtlException.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\service\JobRunNotFoundException.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\service\PipelineAlreadyRunningException.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\service\PipelineNotFoundException.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\service\RejectedRecordService.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\service\SourceCommitService.java`

### 📂 src\main\java\com\tcs\dmj\dwh\service\constant  ·  _1 file_

- ☕ `src\main\java\com\tcs\dmj\dwh\service\constant\SparkTypes.java`

### 📂 src\main\java\com\tcs\dmj\dwh\service\dto  ·  _3 files_

- ☕ `src\main\java\com\tcs\dmj\dwh\service\dto\PipelineInfoDTO.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\service\dto\RejectedRecordSummaryDTO.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\service\dto\SourceInfoDTO.java`

### 📂 src\main\java\com\tcs\dmj\dwh\service\impl  ·  _5 files_

- ☕ `src\main\java\com\tcs\dmj\dwh\service\impl\DwhJobServiceImpl.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\service\impl\DwhPipelineServiceImpl.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\service\impl\JobLockManager.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\service\impl\RejectedRecordServiceImpl.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\service\impl\SourceCommitServiceImpl.java`

### 📂 src\main\java\com\tcs\dmj\dwh\utils  ·  _2 files_

- ☕ `src\main\java\com\tcs\dmj\dwh\utils\ErrorMessages.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\utils\SparkUtils.java`

### 📂 src\main\java\com\tcs\dmj\dwh\validation  ·  _7 files_

- ☕ `src\main\java\com\tcs\dmj\dwh\validation\DataQualityEngine.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\validation\DataQualityRules.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\validation\DataSanitizer.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\validation\QualityColumns.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\validation\QualityResult.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\validation\ReferenceRule.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\validation\ValidationRule.java`

### 📂 src\main\java\com\tcs\dmj\dwh\web\rest  ·  _3 files_

- ☕ `src\main\java\com\tcs\dmj\dwh\web\rest\DwhJobResource.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\web\rest\DwhPipelineResource.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\web\rest\DwhRejectedRecordResource.java`

### 📂 src\main\java\com\tcs\dmj\dwh\web\rest\errors  ·  _2 files_

- ☕ `src\main\java\com\tcs\dmj\dwh\web\rest\errors\ApiError.java`
- ☕ `src\main\java\com\tcs\dmj\dwh\web\rest\errors\ExceptionTranslator.java`

### 📂 src\main\resources  ·  _3 files_

- ⚙️ `src\main\resources\application-dev.yml`
- ⚙️ `src\main\resources\application-prod.yml`
- ⚙️ `src\main\resources\application.yml`

### 📂 src\test\java\com\tcs\dmj\dwh\connector\target  ·  _2 files_

- ☕ `src\test\java\com\tcs\dmj\dwh\connector\target\DirectPostgresWriterTest.java`
- ☕ `src\test\java\com\tcs\dmj\dwh\connector\target\SoftDeleteWriterTest.java`

### 📂 src\test\java\com\tcs\dmj\dwh\pipeline\fulfillment  ·  _1 file_

- ☕ `src\test\java\com\tcs\dmj\dwh\pipeline\fulfillment\FulfillmentStarSchemaMapperTest.java`

### 📂 src\test\java\com\tcs\dmj\dwh\pipeline\order  ·  _1 file_

- ☕ `src\test\java\com\tcs\dmj\dwh\pipeline\order\EcommerceOrderSchemaMapperTest.java`

### 📂 src\test\java\com\tcs\dmj\dwh\pipeline\sales  ·  _1 file_

- ☕ `src\test\java\com\tcs\dmj\dwh\pipeline\sales\SalesPipelineRulesTest.java`

### 📂 src\test\java\com\tcs\dmj\dwh\pipeline\vatreturn  ·  _1 file_

- ☕ `src\test\java\com\tcs\dmj\dwh\pipeline\vatreturn\VatReturnPipelineRulesTest.java`

### 📂 src\test\java\com\tcs\dmj\dwh\validation  ·  _1 file_

- ☕ `src\test\java\com\tcs\dmj\dwh\validation\DataQualityEngineTest.java`

### 📂 docs  ·  _2 files_

- 📝 `docs\PROJECT_STRUCTURE.md`
- 📝 `docs\staging-transaction-and-jdbc.md`

### 📂 (project root)  ·  _1 file_

- 📦 `pom.xml`

### 📂 sample-data  ·  _1 file_

- 📝 `sample-data\README.md`

### 📂 sample-data\ecommerce-fulfillment-pipeline  ·  _3 files_

- 🍃 `sample-data\ecommerce-fulfillment-pipeline\01_mongo_full_load.js`
- 🍃 `sample-data\ecommerce-fulfillment-pipeline\02_mongo_incremental.js`
- 🐘 `sample-data\ecommerce-fulfillment-pipeline\03_postgres_tables.sql`

### 📂 sample-data\ecommerce-order-pipeline  ·  _3 files_

- 🍃 `sample-data\ecommerce-order-pipeline\01_mongo_full_load.js`
- 🍃 `sample-data\ecommerce-order-pipeline\02_mongo_incremental.js`
- 🐘 `sample-data\ecommerce-order-pipeline\03_postgres_tables.sql`

### 📂 sample-data\payment-pipeline  ·  _3 files_

- 🍃 `sample-data\payment-pipeline\01_mongo_full_load.js`
- 🍃 `sample-data\payment-pipeline\02_mongo_incremental.js`
- 🐘 `sample-data\payment-pipeline\03_postgres_tables.sql`

### 📂 sample-data\sales-pipeline  ·  _3 files_

- 🍃 `sample-data\sales-pipeline\01_mongo_full_load.js`
- 🍃 `sample-data\sales-pipeline\02_mongo_incremental.js`
- 🐘 `sample-data\sales-pipeline\03_postgres_tables.sql`

### 📂 sample-data\transaction-pipeline  ·  _3 files_

- 🍃 `sample-data\transaction-pipeline\01_mongo_full_load.js`
- 🍃 `sample-data\transaction-pipeline\02_mongo_incremental.js`
- 🐘 `sample-data\transaction-pipeline\03_postgres_tables.sql`

### 📂 sample-data\vat-return-pipeline  ·  _4 files_

- 🍃 `sample-data\vat-return-pipeline\01_mongo_full_load.js`
- 🍃 `sample-data\vat-return-pipeline\02_mongo_incremental.js`
- 🐘 `sample-data\vat-return-pipeline\03_postgres_tables.sql`
- 📝 `sample-data\vat-return-pipeline\vat-return-pipeline.md`
