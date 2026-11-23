# 🗂️ Project files: ecommersDataETLPipeline

> **106 files**: 79 Java (☕), plus scripts, SQL, docs and config. The `target` build folder and `.git` are left out. Paths are relative to the project folder.

**Legend:** ☕ Java · 🍃 Mongo script · 🐘 PostgreSQL · 📝 Markdown · ⚙️ YAML · 📦 Maven · 🔧 JSON

---

### 📂 src\main\java\com\demo\etl  ·  _1 file_

- ☕ `src\main\java\com\demo\etl\SparkEtlApplication.java`

### 📂 src\main\java\com\demo\etl\api  ·  _4 files_

- ☕ `src\main\java\com\demo\etl\api\EtlJobController.java`
- ☕ `src\main\java\com\demo\etl\api\EtlPipelineController.java`
- ☕ `src\main\java\com\demo\etl\api\GlobalExceptionHandler.java`
- ☕ `src\main\java\com\demo\etl\api\RejectedRecordController.java`

### 📂 src\main\java\com\demo\etl\api\dto  ·  _3 files_

- ☕ `src\main\java\com\demo\etl\api\dto\ApiError.java`
- ☕ `src\main\java\com\demo\etl\api\dto\PipelineInfo.java`
- ☕ `src\main\java\com\demo\etl\api\dto\SourceInfo.java`

### 📂 src\main\java\com\demo\etl\config  ·  _2 files_

- ☕ `src\main\java\com\demo\etl\config\EtlProperties.java`
- ☕ `src\main\java\com\demo\etl\config\SparkConfig.java`

### 📂 src\main\java\com\demo\etl\framework\audit  ·  _7 files_

- ☕ `src\main\java\com\demo\etl\framework\audit\JobRun.java`
- ☕ `src\main\java\com\demo\etl\framework\audit\JobRunRepository.java`
- ☕ `src\main\java\com\demo\etl\framework\audit\JobStatus.java`
- ☕ `src\main\java\com\demo\etl\framework\audit\RejectedRecord.java`
- ☕ `src\main\java\com\demo\etl\framework\audit\RejectedRecordRepository.java`
- ☕ `src\main\java\com\demo\etl\framework\audit\RejectedRecordSummary.java`
- ☕ `src\main\java\com\demo\etl\framework\audit\WatermarkRepository.java`

### 📂 src\main\java\com\demo\etl\framework\common  ·  _3 files_

- ☕ `src\main\java\com\demo\etl\framework\common\ErrorMessages.java`
- ☕ `src\main\java\com\demo\etl\framework\common\SparkTypes.java`
- ☕ `src\main\java\com\demo\etl\framework\common\SparkUtils.java`

### 📂 src\main\java\com\demo\etl\framework\exception  ·  _5 files_

- ☕ `src\main\java\com\demo\etl\framework\exception\DataQualityGateException.java`
- ☕ `src\main\java\com\demo\etl\framework\exception\EtlException.java`
- ☕ `src\main\java\com\demo\etl\framework\exception\JobRunNotFoundException.java`
- ☕ `src\main\java\com\demo\etl\framework\exception\PipelineAlreadyRunningException.java`
- ☕ `src\main\java\com\demo\etl\framework\exception\PipelineNotFoundException.java`

### 📂 src\main\java\com\demo\etl\framework\job  ·  _3 files_

- ☕ `src\main\java\com\demo\etl\framework\job\EtlJobService.java`
- ☕ `src\main\java\com\demo\etl\framework\job\JobLockManager.java`
- ☕ `src\main\java\com\demo\etl\framework\job\SourceCommitService.java`

### 📂 src\main\java\com\demo\etl\framework\pipeline  ·  _6 files_

- ☕ `src\main\java\com\demo\etl\framework\pipeline\AbstractEtlPipeline.java`
- ☕ `src\main\java\com\demo\etl\framework\pipeline\EtlPipeline.java`
- ☕ `src\main\java\com\demo\etl\framework\pipeline\JobMetrics.java`
- ☕ `src\main\java\com\demo\etl\framework\pipeline\PipelineContext.java`
- ☕ `src\main\java\com\demo\etl\framework\pipeline\PipelineRegistry.java`
- ☕ `src\main\java\com\demo\etl\framework\pipeline\PipelineToolkit.java`

### 📂 src\main\java\com\demo\etl\framework\quality  ·  _8 files_

- ☕ `src\main\java\com\demo\etl\framework\quality\DataQualityEngine.java`
- ☕ `src\main\java\com\demo\etl\framework\quality\DataQualityRules.java`
- ☕ `src\main\java\com\demo\etl\framework\quality\DataSanitizer.java`
- ☕ `src\main\java\com\demo\etl\framework\quality\ErrorType.java`
- ☕ `src\main\java\com\demo\etl\framework\quality\QualityColumns.java`
- ☕ `src\main\java\com\demo\etl\framework\quality\QualityResult.java`
- ☕ `src\main\java\com\demo\etl\framework\quality\ReferenceRule.java`
- ☕ `src\main\java\com\demo\etl\framework\quality\ValidationRule.java`

### 📂 src\main\java\com\demo\etl\framework\source  ·  _5 files_

- ☕ `src\main\java\com\demo\etl\framework\source\LoadWindow.java`
- ☕ `src\main\java\com\demo\etl\framework\source\MongoSourceReader.java`
- ☕ `src\main\java\com\demo\etl\framework\source\SourceBatch.java`
- ☕ `src\main\java\com\demo\etl\framework\source\SourceDefinition.java`
- ☕ `src\main\java\com\demo\etl\framework\source\SourceProcessor.java`

### 📂 src\main\java\com\demo\etl\framework\target  ·  _7 files_

- ☕ `src\main\java\com\demo\etl\framework\target\DateDimension.java`
- ☕ `src\main\java\com\demo\etl\framework\target\DirectPostgresWriter.java`
- ☕ `src\main\java\com\demo\etl\framework\target\PostgresReader.java`
- ☕ `src\main\java\com\demo\etl\framework\target\PostgresWriter.java`
- ☕ `src\main\java\com\demo\etl\framework\target\Scd2Writer.java`
- ☕ `src\main\java\com\demo\etl\framework\target\SoftDeleteColumns.java`
- ☕ `src\main\java\com\demo\etl\framework\target\SoftDeleteWriter.java`

### 📂 src\main\java\com\demo\etl\pipelines\fulfillment  ·  _3 files_

- ☕ `src\main\java\com\demo\etl\pipelines\fulfillment\FulfillmentPipeline.java`
- ☕ `src\main\java\com\demo\etl\pipelines\fulfillment\FulfillmentSources.java`
- ☕ `src\main\java\com\demo\etl\pipelines\fulfillment\FulfillmentStarSchemaMapper.java`

### 📂 src\main\java\com\demo\etl\pipelines\order  ·  _3 files_

- ☕ `src\main\java\com\demo\etl\pipelines\order\EcommerceOrderPipeline.java`
- ☕ `src\main\java\com\demo\etl\pipelines\order\EcommerceOrderSchemaMapper.java`
- ☕ `src\main\java\com\demo\etl\pipelines\order\EcommerceOrderSources.java`

### 📂 src\main\java\com\demo\etl\pipelines\payment  ·  _3 files_

- ☕ `src\main\java\com\demo\etl\pipelines\payment\PaymentPipeline.java`
- ☕ `src\main\java\com\demo\etl\pipelines\payment\PaymentSource.java`
- ☕ `src\main\java\com\demo\etl\pipelines\payment\PaymentStarSchemaMapper.java`

### 📂 src\main\java\com\demo\etl\pipelines\sales  ·  _3 files_

- ☕ `src\main\java\com\demo\etl\pipelines\sales\SalesPipeline.java`
- ☕ `src\main\java\com\demo\etl\pipelines\sales\SalesSources.java`
- ☕ `src\main\java\com\demo\etl\pipelines\sales\SalesStarSchemaMapper.java`

### 📂 src\main\java\com\demo\etl\pipelines\transaction  ·  _3 files_

- ☕ `src\main\java\com\demo\etl\pipelines\transaction\TransactionPipeline.java`
- ☕ `src\main\java\com\demo\etl\pipelines\transaction\TransactionSources.java`
- ☕ `src\main\java\com\demo\etl\pipelines\transaction\TransactionStarSchemaMapper.java`

### 📂 src\main\java\com\demo\etl\pipelines\vatreturn  ·  _3 files_

- ☕ `src\main\java\com\demo\etl\pipelines\vatreturn\VatReturnPipeline.java`
- ☕ `src\main\java\com\demo\etl\pipelines\vatreturn\VatReturnSchemaMapper.java`
- ☕ `src\main\java\com\demo\etl\pipelines\vatreturn\VatReturnSources.java`

### 📂 src\main\resources  ·  _3 files_

- ⚙️ `src\main\resources\application-dev.yml`
- ⚙️ `src\main\resources\application-prod.yml`
- ⚙️ `src\main\resources\application.yml`

### 📂 src\test\java\com\demo\etl\framework\quality  ·  _1 file_

- ☕ `src\test\java\com\demo\etl\framework\quality\DataQualityEngineTest.java`

### 📂 src\test\java\com\demo\etl\framework\target  ·  _2 files_

- ☕ `src\test\java\com\demo\etl\framework\target\DirectPostgresWriterTest.java`
- ☕ `src\test\java\com\demo\etl\framework\target\SoftDeleteWriterTest.java`

### 📂 src\test\java\com\demo\etl\pipelines\fulfillment  ·  _1 file_

- ☕ `src\test\java\com\demo\etl\pipelines\fulfillment\FulfillmentStarSchemaMapperTest.java`

### 📂 src\test\java\com\demo\etl\pipelines\order  ·  _1 file_

- ☕ `src\test\java\com\demo\etl\pipelines\order\EcommerceOrderSchemaMapperTest.java`

### 📂 src\test\java\com\demo\etl\pipelines\sales  ·  _1 file_

- ☕ `src\test\java\com\demo\etl\pipelines\sales\SalesPipelineRulesTest.java`

### 📂 src\test\java\com\demo\etl\pipelines\vatreturn  ·  _1 file_

- ☕ `src\test\java\com\demo\etl\pipelines\vatreturn\VatReturnPipelineRulesTest.java`

### 📂 (project root)  ·  _1 file_

- 📦 `pom.xml`

### 📂 .vscode  ·  _1 file_

- 🔧 `.vscode\settings.json`

### 📂 docs  ·  _2 files_

- 📝 `docs\PROJECT_STRUCTURE.md`
- 📝 `docs\staging-transaction-and-jdbc.md`

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
