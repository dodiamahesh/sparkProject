# 🤖 Copilot mate step-by-step prompts: dmj-dwh project farithi banavo

Aa file ma **copy-paste karva layak prompts** che. Biji system par GitHub Copilot Chat (basic model) ne aa prompts aapo to e aaj project jevu j structure ane code banavi aapse.

- **Prompts English ma che.** Basic model English ma vadhare saachu code aape che.
- **Samjuti Gujarati ma che.**

> ⚡ **Shortcut:** jo biji system par code **copy kari shakay** (git push, zip, pen-drive), to e j sauthi saaru che. 100% same code male ane koi bhool na thay. Aa prompts tyare kaam ma aave jyare fakt Copilot j hoy.

---

## 0. Pehla aa vaanchi lo (kevi rite vaparvu)

| # | Niyam | Kem |
|---|---|---|
| 1 | Darek **navi chat** ni sharuaat ma **Prompt 0 (Master Context)** paste karo | Basic model lambi chat ma juni vaato bhooli jaay che |
| 2 | **Ek phase = ek navi chat.** Ek chat ma 3-4 prompts thi vadhare na aapo | Chat lambi thay to code bagde che |
| 3 | Darek prompt pachhi Copilot je code aape e **exact path par file banavi ne paste karo** | Darek code block ni pehli line ma path lakhelo hase |
| 4 | Darek phase pachhi **`mvn -q compile`** chalavo | Bhool turant pakdai jaay |
| 5 | Error aave to **Prompt F (Fix)** vaparo | Aakhi file farithi na mangavo |
| 6 | Jawab vachche kapaai jaay to **Prompt C (Continue)** vaparo | Basic model ni output limit nani che |
| 7 | VS Code Copilot Chat ma **`#file:src/.../X.java`** lakhi ne juni file attach kari shakay | Model ne sacha signatures dekhay. Prompt ma lakhela hoy topan attach karvu vadhare saaru |
| 8 | Bijo package / project naam joie to **aa aakhi file ma** `com.tcs.dmj.dwh` ane `dmj-dwh` **find-replace** karo, pachhi vaparo | Badha prompts ma ek j naam rahe |

**Order badalso nahi.** Darek prompt agaD na prompts ni classes vapre che.

```
Phase 1  Setup            : pom.xml, application.yml            (sidhu copy, prompt nahi)
Phase 2  Basic classes     : enums, constants, exceptions, utils, config        P2 - P4
Phase 3  Database          : meta tables SQL                     (sidhu copy)
Phase 4  Domain + validation : records, DTO, rules, sanitizer, quality engine   P5 - P9
Phase 5  Source + repository : SourceDefinition, repositories                   P10 - P13
Phase 6  Framework engine  : context, reader, processor, writers, lineage       P14 - P19
Phase 7  Core + service    : EtlPipeline, AbstractEtlPipeline, services         P20 - P23
Phase 8  REST + batch      : controllers, errors, Airflow runner, main          P24 - P25
Phase 9  Pehli pipeline    : sales-pipeline (SQL, sources, mapper, pipeline, data) P26 - P29
Phase 10 Optional          : tests, Airflow DAG                                 P30 - P31
Templates                  : navi pipeline, fix, continue                       N, F, C
```

---

## Prompt 0: MASTER CONTEXT (darek navi chat ma pehla aa paste karo)

````text
You are helping me build a Java project step by step. I will ask for a few files at a time.

STRICT RULES
1. Output COMPLETE files only: package line, all imports, full code. Never write "...", "// same as before",
   "// TODO", "implement here" or any placeholder.
2. One code block per file. The FIRST line inside each code block is a comment with the file path, for example:
   // src/main/java/com/tcs/dmj/dwh/enums/ErrorType.java
3. Use EXACTLY the class names, method names, parameter order, constants and SQL text I give you.
   Do not rename anything, do not add extra public methods, do not invent new classes.
4. Java 17. Use a record where I say "record". Constructor injection only (no @Autowired fields). No Lombok.
   No JPA / Hibernate: database access is Spring JdbcTemplate only.
5. Spark functions: use  import static org.apache.spark.sql.functions.*;
6. Logging: private static final Logger log = LoggerFactory.getLogger(X.class);  (SLF4J)
7. Short English Javadoc on every class, comments only where the logic is not obvious.
8. If your answer would be too long, stop after a COMPLETE file and write exactly: CONTINUE NEEDED.
   When I answer "continue" you write the next files.
9. Do not explain the code after it unless I ask. Only the files.

PROJECT
Name: dmj-dwh (groupId com.tcs.dmj, base package com.tcs.dmj.dwh)
Stack: Spring Boot 3.5, Java 17, Apache Spark 3.5 (Scala 2.12), MongoDB Spark Connector 10.x, PostgreSQL, Maven.
What it does: an ETL / data-warehouse application.
  MongoDB (bronze, raw collections)  --Spark-->  sanitize, validate, de-duplicate  --Spark-->  PostgreSQL star schema
  (dimensions + facts) + meta tables (pipeline registry, run history, rejected records, lineage).
One jar, two modes:
  REST service:  POST /api/admin/dwh/pipelines/{pipelineName}/run
  Batch mode:    java -jar dmj-dwh-1.0.0.jar --run-pipeline={pipelineName}   (used by Airflow, exit code 0 / 1)
Every run is INCREMENTAL: a pipeline reads documents with updated_at > meta.meta_pipeline.last_run_time and
<= the start time of the run. last_run_time moves forward ONLY after the whole run succeeded.

PACKAGES (base com.tcs.dmj.dwh)
  DmjDwhApp                      main class
  batch                          PipelineBatchRunner (Airflow batch mode)
  config                         ApplicationProperties, SparkConfig
  connector.source               LoadWindow, SourceDefinition, SourceBatch, MongoSourceReader, SourceProcessor
  connector.target               PostgresWriter, PostgresReader, Scd2Writer, SoftDeleteWriter, SoftDeleteColumns,
                                 DirectPostgresWriter, DateDimension
  core                           EtlPipeline, AbstractEtlPipeline, PipelineContext, PipelineRegistry, PipelineToolkit,
                                 JobMetrics, LineageCollector, ColumnLineage
  domain                         MetaAsset, MetaPipeline, MetaPipelineRun, RejectedRecord   (records)
  enums                          AssetType, ErrorType, LayerName, MappingType, PipelineStatus, PipelineType, Platform
  pipeline.<name>                one folder per pipeline: <Name>Pipeline, <Name>Sources, <Name>StarSchemaMapper
  repository                     JdbcTemplate SQL classes for the meta tables
  service                        service interfaces + exceptions;  service.impl = implementations
  service.constant               SparkTypes;  service.dto = DTO records
  utils                          ErrorMessages, SparkUtils
  validation                     DataSanitizer, DataQualityRules, DataQualityEngine, QualityColumns, ValidationRule,
                                 ReferenceRule, QualityResult
  web.rest                       REST controllers;  web.rest.errors = ApiError, ExceptionTranslator

KEY DESIGN RULES (keep them in every file)
- Every MongoDB field is read as STRING. Casting happens later (bad values become CORRUPT errors, never crashes).
- Technical Spark columns start with "_" (_id, _raw_record, _dq_errors ...).
- Rejected records and last_run_time are written at the END of a run (commit phase), never in the middle.
- Writes to PostgreSQL use a staging table: Spark writes etl_staging.stg_..., then one SQL
  INSERT ... SELECT ... ON CONFLICT (business key) DO UPDATE, then the staging table is dropped.
- Every silver/curated table has the audit columns job_run_id BIGINT and loaded_at TIMESTAMP.

Reply only "READY" now. Then wait for my next message.
````

---

## Phase 1: Setup (prompt nahi, sidhu copy karo)

Basic model ne `pom.xml` ma version ane exclusion ni bhool thay che, etle aa files sidhi copy karo.

**Folder banavo:** `dmj-dwh/` ane andar `src/main/java/com/tcs/dmj/dwh/`, `src/main/resources/`, `src/test/java/com/tcs/dmj/dwh/`, `sample-data/`.

### `pom.xml`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>3.5.16</version>
        <relativePath/>
    </parent>

    <groupId>com.tcs.dmj</groupId>
    <artifactId>dmj-dwh</artifactId>
    <version>1.0.0</version>
    <name>dmj-dwh</name>

    <properties>
        <java.version>17</java.version>
        <scala.binary.version>2.12</scala.binary.version>
        <spark.version>3.5.9</spark.version>
        <mongo-spark-connector.version>10.7.0</mongo-spark-connector.version>
        <!-- Spark 3.5 needs Janino 3.1.9; Spring Boot manages a newer one that breaks Spark code generation -->
        <janino.version>3.1.9</janino.version>
        <spark.jvm.add-opens>
            --add-opens=java.base/java.lang=ALL-UNNAMED
            --add-opens=java.base/java.lang.invoke=ALL-UNNAMED
            --add-opens=java.base/java.lang.reflect=ALL-UNNAMED
            --add-opens=java.base/java.io=ALL-UNNAMED
            --add-opens=java.base/java.net=ALL-UNNAMED
            --add-opens=java.base/java.nio=ALL-UNNAMED
            --add-opens=java.base/java.util=ALL-UNNAMED
            --add-opens=java.base/java.util.concurrent=ALL-UNNAMED
            --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED
            --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED
            --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
            --add-opens=java.base/sun.nio.cs=ALL-UNNAMED
            --add-opens=java.base/sun.security.action=ALL-UNNAMED
            --add-opens=java.base/sun.util.calendar=ALL-UNNAMED
        </spark.jvm.add-opens>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-actuator</artifactId>
        </dependency>
        <!-- JdbcTemplate only. No JPA: Hibernate 6 needs ANTLR 4.13, Spark 3.5 needs ANTLR 4.9 -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-jdbc</artifactId>
        </dependency>
        <dependency>
            <groupId>org.postgresql</groupId>
            <artifactId>postgresql</artifactId>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-core_${scala.binary.version}</artifactId>
            <version>${spark.version}</version>
            <exclusions>
                <exclusion>
                    <groupId>org.apache.logging.log4j</groupId>
                    <artifactId>log4j-slf4j2-impl</artifactId>
                </exclusion>
            </exclusions>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_${scala.binary.version}</artifactId>
            <version>${spark.version}</version>
            <exclusions>
                <exclusion>
                    <groupId>org.apache.logging.log4j</groupId>
                    <artifactId>log4j-slf4j2-impl</artifactId>
                </exclusion>
            </exclusions>
        </dependency>
        <dependency>
            <groupId>org.mongodb.spark</groupId>
            <artifactId>mongo-spark-connector_${scala.binary.version}</artifactId>
            <version>${mongo-spark-connector.version}</version>
        </dependency>
        <dependency>
            <groupId>org.mongodb</groupId>
            <artifactId>bson</artifactId>
        </dependency>
        <!-- Spark 3.5 still uses javax.servlet internally; Spring Boot 3 uses jakarta -->
        <dependency>
            <groupId>javax.servlet</groupId>
            <artifactId>javax.servlet-api</artifactId>
            <version>4.0.1</version>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
                <configuration>
                    <jvmArguments>${spark.jvm.add-opens}</jvmArguments>
                </configuration>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-jar-plugin</artifactId>
                <configuration>
                    <archive>
                        <manifestEntries>
                            <Add-Opens>java.base/java.lang java.base/java.lang.invoke java.base/java.lang.reflect java.base/java.io java.base/java.net java.base/java.nio java.base/java.util java.base/java.util.concurrent java.base/java.util.concurrent.atomic java.base/jdk.internal.ref java.base/sun.nio.ch java.base/sun.nio.cs java.base/sun.security.action java.base/sun.util.calendar</Add-Opens>
                        </manifestEntries>
                    </archive>
                </configuration>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-surefire-plugin</artifactId>
                <configuration>
                    <argLine>${spark.jvm.add-opens}</argLine>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>
```

### `src/main/resources/application.yml`

```yaml
spring:
  application:
    name: dmj-dwh
  profiles:
    active: dev
  jackson:
    serialization:
      write-dates-as-timestamps: false

server:
  port: 8080

management:
  endpoints:
    web:
      exposure:
        include: health,info

application:
  dwh:
    spark:
      app-name: ${spring.application.name}
      master: local[*]
      config:
        "[spark.sql.shuffle.partitions]": "4"
        "[spark.ui.enabled]": "false"
        # invalid casts return NULL instead of throwing: the data-quality rules rely on this
        "[spark.sql.ansi.enabled]": "false"
        "[spark.sql.legacy.timeParserPolicy]": "CORRECTED"
        "[spark.sql.session.timeZone]": "Asia/Kolkata"
    mongo:
      uri: ${spring.data.mongodb.uri}
      database: ${spring.data.mongodb.database}
    postgres:
      url: ${spring.datasource.url}
      username: ${spring.datasource.username}
      password: ${spring.datasource.password}
      staging-schema: etl_staging
      batch-size: 1000
    quality:
      reject-gate-min-records: 10
    batch:
      fail-on-rejected-records: ${DWH_FAIL_ON_REJECTED_RECORDS:false}

logging:
  level:
    root: INFO
    org.apache.spark: WARN
    org.sparkproject: WARN
    org.apache.hadoop: ERROR
    org.mongodb.driver: WARN
    org.apache.parquet: WARN
```

### `src/main/resources/application-dev.yml`

```yaml
spring:
  datasource:
    url: ${POSTGRES_URL:jdbc:postgresql://localhost:5433/silver?reWriteBatchedInserts=true}
    username: ${POSTGRES_USER:postgres}
    password: ${POSTGRES_PASSWORD:root}
  data:
    mongodb:
      uri: ${MONGO_URI:mongodb://localhost:27017}
      database: ${MONGO_DATABASE:bronze_db}

application:
  dwh:
    spark:
      master: local[*]
      config:
        "[spark.driver.host]": "localhost"
        "[spark.driver.bindAddress]": "127.0.0.1"

logging:
  level:
    com.tcs.dmj.dwh: DEBUG
```

> Biji system par PostgreSQL port / password alag hoy to `application-dev.yml` ma badlo.

✅ **Check:** `mvn -q dependency:resolve`. Badhi jar download thavi joie.

---

## Phase 2: Basic classes (navi chat → Prompt 0 → P2, P3, P4)

### P2: enums + SparkTypes

````text
Create these 8 files.

Package com.tcs.dmj.dwh.enums (plain enums, a one-line Javadoc on each constant group):
1. AssetType: TABLE, VIEW, MATERIALIZED_VIEW, COLLECTION, API, DATASET, REPORT
2. ErrorType: CORRUPT, VALIDATION, ORPHAN, DUPLICATE
   Javadoc: the ORDER matters, errors of a rejected record are sorted in this order (most serious first).
   CORRUPT = wrong data type ("12abc" for an amount), VALIDATION = business rule broken, ORPHAN = referenced
   key does not exist, DUPLICATE = same business key twice in one batch.
3. LayerName: SOURCE, RAW, CURATED, AGGREGATED
4. MappingType: DIRECT, RENAMED, DERIVED, AGGREGATED, LOOKUP, CONSTANT, CONDITIONAL, MASKED, HASHED
5. PipelineStatus: RUNNING, SUCCESS, SUCCESS_WITH_FAILED, FAILED
   plus: public static PipelineStatus of(long processedCount, long rejectedCount)
         rejectedCount == 0 -> SUCCESS; else processedCount == 0 -> FAILED; else SUCCESS_WITH_FAILED
6. PipelineType: SOURCE_TO_RAW, RAW_TO_CURATED, CURATED_TO_AGGREGATED
7. Platform: MONGODB, EDB_PAS, FILESYSTEM, API

Package com.tcs.dmj.dwh.service.constant:
8. final class SparkTypes, private constructor, public static final org.apache.spark.sql.types.DataType constants:
   MONEY = DataTypes.createDecimalType(18, 2)
   RATE = DataTypes.createDecimalType(9, 4)
   TIMESTAMP = DataTypes.TimestampType
   DATE = DataTypes.DateType
   INT = DataTypes.IntegerType
````

### P3: exceptions + utils

````text
Create these 8 files.

Package com.tcs.dmj.dwh.service:
1. EtlException extends RuntimeException: constructors (String message) and (String message, Throwable cause).
2. DataQualityGateException extends EtlException: field String collection; constructor (String collection, String message);
   getter getCollection(). Javadoc: a source failed the reject gate (too many rejected rows); the job stops before the
   collection is loaded and last_run_time does not move.
3. PipelineAlreadyRunningException extends EtlException: constructor (String pipelineName), message:
   "Pipeline '" + pipelineName + "' is already running. Wait for it to finish and try again."
4. PipelineNotFoundException extends EtlException: constructor (String pipelineName, java.util.Collection<String> availablePipelines),
   message: "Pipeline '" + pipelineName + "' not found. Available pipelines: " + availablePipelines
5. PipelineRunNotFoundException extends EtlException: constructor (long pipelineRunId), message: "Pipeline run " + id + " not found"

Package com.tcs.dmj.dwh.utils (final classes, private constructor, static methods):
6. ErrorMessages, MAX_MESSAGE_LENGTH = 1000:
   - String rootCauseMessage(Throwable e): deepest cause -> simpleClassName + ": " + message, truncated
   - String shortMessage(Throwable e): e.getMessage() or the simple class name when null, truncated
   - String errorCode(Throwable e): top simple class name; if the root cause class differs: "Top/Root"
     (example "UncategorizedSQLException/PSQLException")
   - private rootCause(Throwable) walks getCause() until null or self
   - private truncate(text, max): longer -> first (max - 3) chars + "..."
7. SparkUtils:
   - static Dataset<Row> latestRecordPerKey(Dataset<Row> rows, String keyColumn, String orderByColumn):
     row_number() over Window.partitionBy(col(keyColumn)).orderBy(col(orderByColumn).desc_nulls_last()) into a column
     "_row_number", keep = 1, drop the column.
````

### P4: config

````text
Create these 2 files in package com.tcs.dmj.dwh.config.

1. ApplicationProperties: a record annotated @ConfigurationProperties(prefix = "application.dwh") with components
   (Spark spark, Mongo mongo, Postgres postgres, Quality quality, Batch batch).
   Compact constructor: if batch == null then batch = new Batch(false).
   Nested records:
     record Batch(boolean failOnRejectedRecords)
     record Quality(int rejectGateMinRecords)
     record Spark(String appName, String master, java.util.Map<String, String> config)
     record Mongo(String uri, String database)
     record Postgres(String url, String username, String password, String stagingSchema, int batchSize)
   These map application.yml keys: application.dwh.spark.app-name / master / config, mongo.uri / database,
   postgres.url / username / password / staging-schema / batch-size, quality.reject-gate-min-records,
   batch.fail-on-rejected-records.

2. SparkConfig: @Configuration with
   @Bean(destroyMethod = "stop") public SparkSession sparkSession(ApplicationProperties properties)
   builds new SparkConf().setAppName(spark.appName()).setMaster(spark.master()), copies every entry of spark.config()
   (when not null) with conf.set(key, value), logs "Starting SparkSession (master={}, app={})" and returns
   SparkSession.builder().config(conf).getOrCreate().
````

✅ **Check:** `mvn -q compile`

---

## Phase 3: Database (meta tables, sidhu copy)

PostgreSQL ma `silver` database banavo, pachhi aa SQL chalavo. File naam: `sample-data/00_meta_tables.sql`. Aa badhi pipelines vachche common che. Re-run safe che.

```sql
CREATE SCHEMA IF NOT EXISTS meta;
CREATE SCHEMA IF NOT EXISTS etl_staging;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS curated;

CREATE OR REPLACE FUNCTION meta.set_updated_at() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at := CURRENT_TIMESTAMP;
    RETURN NEW;
END $$;

CREATE TABLE IF NOT EXISTS meta.meta_pipeline (
    pipeline_id           BIGINT        GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    pipeline_name         VARCHAR(30)   NOT NULL UNIQUE,
    pipeline_type         VARCHAR(30)   NOT NULL
                          CHECK (pipeline_type IN ('SOURCE_TO_RAW', 'RAW_TO_CURATED', 'CURATED_TO_AGGREGATED')),
    airflow_dag_id        VARCHAR(30)   UNIQUE,
    application_job_name  VARCHAR(50),
    pipeline_description  VARCHAR(100),
    last_run_time         TIMESTAMPTZ,                 -- NULL = never succeeded: next run reads everything
    active_flag           CHAR(1)       NOT NULL DEFAULT 'Y' CHECK (active_flag IN ('Y', 'N')),
    created_at            TIMESTAMPTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at            TIMESTAMPTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE OR REPLACE TRIGGER trg_meta_pipeline_updated_at BEFORE UPDATE ON meta.meta_pipeline
    FOR EACH ROW EXECUTE FUNCTION meta.set_updated_at();

CREATE TABLE IF NOT EXISTS meta.meta_pipeline_run (
    pipeline_run_id   BIGINT         GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    pipeline_id       BIGINT         NOT NULL REFERENCES meta.meta_pipeline (pipeline_id),
    start_time        TIMESTAMPTZ    NOT NULL,
    end_time          TIMESTAMPTZ,
    pipeline_status   VARCHAR(20)    NOT NULL
                      CHECK (pipeline_status IN ('RUNNING', 'SUCCESS', 'SUCCESS_WITH_FAILED', 'FAILED')),
    total_records     BIGINT         CHECK (total_records >= 0),
    success_records   BIGINT         CHECK (success_records >= 0),
    failed_records    BIGINT         CHECK (failed_records >= 0),
    last_run_time     TIMESTAMPTZ    NOT NULL,         -- window end of this run
    error_code        VARCHAR(100),
    error_message     VARCHAR(1000),
    created_at        TIMESTAMPTZ    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMPTZ    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ck_pipeline_run_end CHECK (end_time IS NULL OR end_time >= start_time)
);
CREATE INDEX IF NOT EXISTS idx_pipeline_run_pipeline ON meta.meta_pipeline_run (pipeline_id, start_time DESC);
CREATE INDEX IF NOT EXISTS idx_pipeline_run_status   ON meta.meta_pipeline_run (pipeline_status);
CREATE OR REPLACE TRIGGER trg_meta_pipeline_run_updated_at BEFORE UPDATE ON meta.meta_pipeline_run
    FOR EACH ROW EXECUTE FUNCTION meta.set_updated_at();

CREATE TABLE IF NOT EXISTS meta.meta_asset (
    asset_id           BIGINT        GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    asset_name         VARCHAR(100)  NOT NULL,
    asset_type         VARCHAR(30)   NOT NULL
                       CHECK (asset_type IN ('TABLE', 'VIEW', 'MATERIALIZED_VIEW', 'COLLECTION', 'API', 'DATASET', 'REPORT')),
    platform           VARCHAR(30)   NOT NULL CHECK (platform IN ('MONGODB', 'EDB_PAS', 'FILESYSTEM', 'API')),
    database_name      VARCHAR(64),
    schema_name        VARCHAR(64),
    layer_name         VARCHAR(20)   NOT NULL CHECK (layer_name IN ('SOURCE', 'RAW', 'CURATED', 'AGGREGATED')),
    asset_description  VARCHAR(100),
    active_flag        CHAR(1)       NOT NULL DEFAULT 'Y' CHECK (active_flag IN ('Y', 'N')),
    created_at         TIMESTAMPTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMPTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_meta_asset
    ON meta.meta_asset (platform, (COALESCE(database_name, '')), (COALESCE(schema_name, '')), asset_name);
CREATE OR REPLACE TRIGGER trg_meta_asset_updated_at BEFORE UPDATE ON meta.meta_asset
    FOR EACH ROW EXECUTE FUNCTION meta.set_updated_at();

CREATE TABLE IF NOT EXISTS meta.meta_lineage (
    lineage_id        BIGINT       GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    source_asset_id   BIGINT       NOT NULL REFERENCES meta.meta_asset (asset_id),
    target_asset_id   BIGINT       NOT NULL REFERENCES meta.meta_asset (asset_id),
    pipeline_id       BIGINT       REFERENCES meta.meta_pipeline (pipeline_id),
    active_flag       CHAR(1)      NOT NULL DEFAULT 'Y' CHECK (active_flag IN ('Y', 'N')),
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ck_lineage_not_self CHECK (source_asset_id <> target_asset_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_meta_lineage
    ON meta.meta_lineage (source_asset_id, target_asset_id, (COALESCE(pipeline_id, 0)));
CREATE OR REPLACE TRIGGER trg_meta_lineage_updated_at BEFORE UPDATE ON meta.meta_lineage
    FOR EACH ROW EXECUTE FUNCTION meta.set_updated_at();

CREATE TABLE IF NOT EXISTS meta.meta_column_lineage (
    column_lineage_id   BIGINT        GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    pipeline_id         BIGINT        REFERENCES meta.meta_pipeline (pipeline_id),
    source_asset_id     BIGINT        NOT NULL REFERENCES meta.meta_asset (asset_id),
    source_column_name  VARCHAR(100)  NOT NULL,
    target_asset_id     BIGINT        NOT NULL REFERENCES meta.meta_asset (asset_id),
    target_column_name  VARCHAR(100)  NOT NULL,
    mapping_type        VARCHAR(30)   NOT NULL DEFAULT 'DIRECT'
                        CHECK (mapping_type IN ('DIRECT', 'RENAMED', 'DERIVED', 'AGGREGATED', 'LOOKUP', 'CONSTANT',
                                                'CONDITIONAL', 'MASKED', 'HASHED')),
    active_flag         CHAR(1)       NOT NULL DEFAULT 'Y' CHECK (active_flag IN ('Y', 'N')),
    created_at          TIMESTAMPTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMPTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ck_column_lineage_not_self CHECK (source_asset_id <> target_asset_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_meta_column_lineage
    ON meta.meta_column_lineage (source_asset_id, source_column_name, target_asset_id, target_column_name,
                                 (COALESCE(pipeline_id, 0)));
CREATE OR REPLACE TRIGGER trg_meta_column_lineage_updated_at BEFORE UPDATE ON meta.meta_column_lineage
    FOR EACH ROW EXECUTE FUNCTION meta.set_updated_at();

CREATE TABLE IF NOT EXISTS meta.meta_error_master (
    error_id       SERIAL       PRIMARY KEY,
    error_type     VARCHAR(30)  NOT NULL CHECK (error_type IN ('CORRUPT', 'VALIDATION', 'ORPHAN', 'DUPLICATE')),
    field_name     VARCHAR(100),
    error_message  VARCHAR(500) NOT NULL,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT uq_error_master UNIQUE (error_type, error_message)
);

CREATE TABLE IF NOT EXISTS meta.meta_rejected_record (
    rejected_record_id  BIGSERIAL    PRIMARY KEY,
    pipeline_name       VARCHAR(100) NOT NULL,
    source_collection   VARCHAR(100) NOT NULL,
    source_id           VARCHAR(100) NOT NULL,
    business_key        VARCHAR(200),
    error_ids           INT[]        NOT NULL,
    error_messages      TEXT         NOT NULL,
    field_values        JSONB,
    raw_record          JSONB,
    attempt_count       INT          NOT NULL DEFAULT 1,
    first_job_run_id    BIGINT       NOT NULL,
    last_job_run_id     BIGINT       NOT NULL,
    first_rejected_at   TIMESTAMP    NOT NULL DEFAULT now(),
    last_rejected_at    TIMESTAMP    NOT NULL DEFAULT now(),
    CONSTRAINT uq_rejected_record UNIQUE (pipeline_name, source_collection, source_id)
);
CREATE INDEX IF NOT EXISTS idx_rejected_record_errors ON meta.meta_rejected_record USING GIN (error_ids);

-- shared date dimension (every pipeline uses it)
CREATE TABLE IF NOT EXISTS silver.dim_date (
    date_key      INT         PRIMARY KEY,           -- yyyyMMdd
    full_date     DATE        NOT NULL UNIQUE,
    day_of_month  INT         NOT NULL,
    month         INT         NOT NULL,
    month_name    VARCHAR(20) NOT NULL,
    quarter       INT         NOT NULL,
    year          INT         NOT NULL,
    day_of_week   INT         NOT NULL,              -- 1 = Sunday ... 7 = Saturday
    day_name      VARCHAR(20) NOT NULL,
    is_weekend    BOOLEAN     NOT NULL,
    job_run_id    BIGINT,
    loaded_at     TIMESTAMP
);
```

---

## Phase 4: Domain + validation (navi chat → Prompt 0 → P5 … P9)

### P5: domain records + 2 DTO

````text
Create these 6 records. All timestamps are java.time.OffsetDateTime unless I say otherwise.

Package com.tcs.dmj.dwh.domain:
1. record MetaAsset(String assetName, AssetType assetType, Platform platform, String databaseName, String schemaName,
                    LayerName layerName, String assetDescription)   (enums from com.tcs.dmj.dwh.enums)
   - static MetaAsset mongoCollection(String database, String collection, String description)
       -> (collection, COLLECTION, MONGODB, database, null, RAW, description)
   - static MetaAsset postgresTable(String database, String table): table is "schema.table" or "table" (schema "public")
       -> (name, TABLE, EDB_PAS, database, schema, CURATED, null)
   - String key(): platform + "|" + databaseName + "|" + schemaName + "|" + assetName
   - String displayName(): schemaName == null ? databaseName + "." + assetName : schemaName + "." + assetName
   - MetaAsset withDescription(String description): copy with a new description
2. record MetaPipeline(long pipelineId, String pipelineName, String pipelineType, String airflowDagId,
                       String applicationJobName, String pipelineDescription, OffsetDateTime lastRunTime,
                       String activeFlag, OffsetDateTime createdAt, OffsetDateTime updatedAt)
3. record MetaPipelineRun(long pipelineRunId, long pipelineId, String pipelineName, OffsetDateTime lastRunTime,
                          OffsetDateTime startTime, OffsetDateTime endTime, String pipelineStatus, Long totalRecords,
                          Long successRecords, Long failedRecords, String errorCode, String errorMessage,
                          OffsetDateTime createdAt, OffsetDateTime updatedAt)
4. record RejectedRecord(long rejectedRecordId, String pipelineName, String sourceCollection, String sourceId,
                         String businessKey, List<Integer> errorIds, String errorMessages, Map<String, String> fieldValues,
                         String rawRecord, int attemptCount, long firstJobRunId, long lastJobRunId,
                         java.time.LocalDateTime firstRejectedAt, java.time.LocalDateTime lastRejectedAt)

Package com.tcs.dmj.dwh.service.dto:
5. record RejectedRecordSummaryDTO(String pipelineName, String sourceCollection, int errorId, String errorType,
                                   String errorMessage, long recordCount)
6. record PipelineInfoDTO(long pipelineId, String name, String pipelineType, String description,
                          List<String> sourceCollections, List<SourceInfoDTO> sources, List<String> targetTables,
                          OffsetDateTime lastRunTime, boolean running)
   NOTE: SourceInfoDTO (same package) will be created LATER. Import nothing for it (same package).
````

> P5 pachhi compile error aavse (`SourceInfoDTO` nathi). E P10 ma bane che, etle aagal vadho.

### P6: validation basics

````text
Create these 4 files in package com.tcs.dmj.dwh.validation.

1. final class QualityColumns (private constructor). Technical column names, all start with "_":
   public static final String SOURCE_ID = "_id";              // MongoDB document id
   public static final String RAW_RECORD = "_raw_record";     // original document as JSON (before cleaning)
   static final String ERRORS = "_dq_errors";                 // package-private
   static final String ROW_NUMBER = "_dq_row_number";         // package-private
   static final String REFERENCE_PREFIX = "_dq_ref_";         // package-private
   Columns of the standard rejected dataset (ONE row per rejected record):
   public static final String REJECTED_SOURCE_ID = "source_id", REJECTED_BUSINESS_KEY = "business_key",
                              REJECTED_ERRORS = "errors", REJECTED_RAW_RECORD = "raw_record";
   Fields of ONE error struct:
   public static final String ERROR_TYPE = "error_type", FIELD_NAME = "field_name",
                              ERROR_MESSAGE = "error_message", FIELD_VALUE = "field_value";
   static final StructType ERROR_SCHEMA = new StructType().add(ERROR_TYPE, StringType).add(FIELD_NAME, StringType)
                                          .add(ERROR_MESSAGE, StringType).add(FIELD_VALUE, StringType);
   Methods:
   - public static boolean isTechnicalColumn(String column): column.startsWith("_")
   - static Column errorStruct(ErrorType type, String fieldName, String message, Column value):
       struct(lit(type.name()).as(ERROR_TYPE), lit(fieldName).cast(StringType).as(FIELD_NAME),
              lit(message).as(ERROR_MESSAGE), value.cast(StringType).as(FIELD_VALUE))
   - public static Column mainErrorType(): col(REJECTED_ERRORS).getItem(0).getField(ERROR_TYPE)
   - public static Dataset<Row> oneRowPerError(Dataset<Row> rejected): select source_id, business_key,
       explode(errors).as("e"), raw_record, then select source_id, business_key, e.error_type, e.field_name,
       e.error_message, e.field_value (aliased to the constant names), raw_record.

2. record ValidationRule(ErrorType errorType, Column failWhen, String fieldName, String message)
   - public Column errorWhenFailed(): value = fieldName == null ? lit(null) : col(fieldName);
     return when(failWhen, QualityColumns.errorStruct(errorType, fieldName, message, value));   // NULL when it passes

3. record ReferenceRule(String column, Dataset<Row> lookup, String lookupColumn, String lookupName)

4. record QualityResult(Dataset<Row> valid, Dataset<Row> rejected)
````

### P7: DataQualityRules

> Messages **exact** rakhvana che, kem ke e `meta_error_master` ni key che.

````text
Create com.tcs.dmj.dwh.validation.DataQualityRules: a final class, a fluent builder of validation rules.
It already has these classes in the same package: ValidationRule(errorType, failWhen, fieldName, message),
ReferenceRule(column, lookup, lookupColumn, lookupName). Use SparkTypes (com.tcs.dmj.dwh.service.constant) and
ErrorType (com.tcs.dmj.dwh.enums).

Fields: private final List<ValidationRule> rules; private final List<ReferenceRule> referenceRules;
private static final String EMAIL_REGEX = "^[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{2,}$";
private static final String INTEGER_REGEX = "^-?[0-9]+$";
private constructor; public static DataQualityRules create().
private DataQualityRules add(ErrorType type, Column failWhen, String fieldName, String message) adds a ValidationRule.
Every public method returns this. "v" below means col(column). failWhen is TRUE when the record is BAD.

CORRUPT rules:
- mustBeDecimal(String column):  v.isNotNull().and(v.cast(MONEY).isNull()),  message column + " is not a valid number"
- mustBeInteger(String column):  v.isNotNull().and(not(v.rlike(INTEGER_REGEX))), message column + " is not a whole number"
- mustBeDate(String column):     v.isNotNull().and(v.cast(DATE).isNull()),
                                 message column + " is not a valid date (expected yyyy-MM-dd)"
- mustBeTimestamp(String column): v.isNotNull().and(v.cast(TIMESTAMP).isNull()),
                                 message column + " is not a valid timestamp (expected yyyy-MM-ddTHH:mm:ss)"
VALIDATION rules:
- required(String... columns): for each: v.isNull(), message column + " is required"
- mustBePositive(column):      v.cast(MONEY).leq(0), message column + " must be greater than 0"
- mustNotBeNegative(column):   v.cast(MONEY).lt(0), message column + " cannot be negative"
- mustNotBeMoreThan(column, double max): v.cast(MONEY).gt(max), message column + " is more than " + max
- mustBeBetween(column, double min, double max): n = v.cast(RATE); n.lt(min).or(n.gt(max)),
                               message column + " must be between " + min + " and " + max
- mustNotBeInFuture(column):   v.cast(TIMESTAMP).gt(current_timestamp()), message column + " cannot be in the future"
- mustBeOneOf(column, String... allowed): v.isNotNull().and(not(v.isin((Object[]) allowed))),
                               message column + " must be one of " + Arrays.toString(allowed)
- mustMatch(column, String regex, String description): v.isNotNull().and(not(v.rlike(regex))),
                               message column + " " + description
- mustBeValidEmail(column):    mustMatch(column, EMAIL_REGEX, "is not a valid email address")
- custom(ErrorType errorType, String description, Column failWhen): add(errorType, failWhen, null, description)
ORPHAN rule:
- mustExistIn(String column, Dataset<Row> lookup, String lookupColumn, String lookupName): adds a ReferenceRule
Getters: List<ValidationRule> rules() and List<ReferenceRule> referenceRules(), both Collections.unmodifiableList.
````

### P8: DataSanitizer

````text
Create com.tcs.dmj.dwh.validation.DataSanitizer: a final class that cleans "messy but correct" values BEFORE validation.
It is a recipe: steps run in the order they were added. QualityColumns.isTechnicalColumn(name) already exists.

private final List<UnaryOperator<Dataset<Row>>> steps; private constructor; public static DataSanitizer create().
Helper: private DataSanitizer addStep(String[] columns, Function<String, Column> cleaner): one step that does
rows.withColumn(column, cleaner.apply(column)) for every column. Every public method returns this.

- trimAllAndBlankToNull(): for every column of the dataset that is NOT technical: trimmed = trim(col(c));
  withColumn(c, when(trimmed.equalTo(""), lit(null)).otherwise(trimmed))
- toUpperCase(String... columns): upper(col(c))
- toLowerCase(String... columns): lower(col(c))
- toProperCase(String... columns): initcap(col(c))
- removeThousandSeparators(String... columns): regexp_replace(col(c), ",", "")
- normalizeIndianPhone(String... columns): digits = regexp_replace(col(c), "[^0-9]", "");
    when(length(digits) == 12 and digits starts with "91", substring(digits, 3, 10))
    .when(length(digits) == 11 and digits starts with "0", substring(digits, 2, 10)).otherwise(digits)
- fromJson(String targetColumn, String jsonColumn, String jsonPath): one step
    rows.withColumn(targetColumn, get_json_object(col(jsonColumn), jsonPath))
    (nested Mongo documents arrive as JSON text, e.g. fromJson("city", "address", "$.city"))
- mapValues(String column, Map<String, String> mapping): result = col(column); for each entry:
    result = when(col(column).equalTo(key), lit(value)).otherwise(result)
- defaultIfNull(String column, String defaultValue): coalesce(col(column), lit(defaultValue))
- custom(String column, UnaryOperator<Column> cleaner): one step rows.withColumn(column, cleaner.apply(col(column)))
- public Dataset<Row> apply(Dataset<Row> rows): run every step in order.
````

### P9: DataQualityEngine

````text
Create com.tcs.dmj.dwh.validation.DataQualityEngine, a Spring @Component. Existing classes in the same package:
DataQualityRules (rules(), referenceRules()), ValidationRule (errorType(), errorWhenFailed()), ReferenceRule,
QualityResult(valid, rejected), QualityColumns (SOURCE_ID, RAW_RECORD, ERRORS, ROW_NUMBER, REFERENCE_PREFIX,
REJECTED_* constants, ERROR_SCHEMA, errorStruct(...)). ErrorType is in com.tcs.dmj.dwh.enums,
SparkTypes in com.tcs.dmj.dwh.service.constant.

public QualityResult check(Dataset<Row> rows, DataQualityRules qualityRules, String businessKey, String latestRecordColumn)
  rows always contain _id and _raw_record.
  1. rules = new ArrayList<>(qualityRules.rules()); checked = rows.
     For every ReferenceRule (index i from 0): foundColumn = REFERENCE_PREFIX + i;
       lookupKeys = reference.lookup().select(col(reference.lookupColumn()).as(foundColumn)).distinct();
       checked = checked.join(broadcast(lookupKeys), checked.col(reference.column()).equalTo(lookupKeys.col(foundColumn)), "left");
       add new ValidationRule(ErrorType.ORPHAN, col(reference.column()).isNotNull().and(col(foundColumn).isNull()),
           reference.column(), reference.column() + " not found in " + reference.lookupName())
  2. rules.sort(Comparator.comparing(ValidationRule::errorType))  (stable: errors[0] = most serious)
     checked = checked.withColumn(ERRORS, failedRules(rules)); then drop every REFERENCE_PREFIX + i column.
  3. failedRows = checked.filter(size(col(ERRORS)).gt(0))
  4. passedRows = checked.filter(size(col(ERRORS)).equalTo(0)).withColumn(ROW_NUMBER, row_number().over(
        Window.partitionBy(col(businessKey)).orderBy(col(latestRecordColumn).cast(SparkTypes.TIMESTAMP).desc_nulls_last(),
        col(SOURCE_ID).asc())))
     duplicateRows = passedRows.filter(ROW_NUMBER > 1).withColumn(ERRORS, array(QualityColumns.errorStruct(
        ErrorType.DUPLICATE, businessKey,
        "another record with the same " + businessKey + " was kept (latest " + latestRecordColumn + " wins)",
        col(businessKey))))
     validRows = passedRows.filter(ROW_NUMBER == 1).drop(ROW_NUMBER, ERRORS)
     rejectedRows = toRejectedFormat(failedRows, businessKey).unionByName(toRejectedFormat(duplicateRows, businessKey))
  return new QualityResult(validRows, rejectedRows)

private Column failedRules(List<ValidationRule> rules):
  empty -> array().cast(DataTypes.createArrayType(QualityColumns.ERROR_SCHEMA));
  else array_compact(array(every rule.errorWhenFailed()))   // a passed rule gives NULL, array_compact removes it
private Dataset<Row> toRejectedFormat(Dataset<Row> rows, String businessKey):
  select col(SOURCE_ID).as(REJECTED_SOURCE_ID), col(businessKey).cast(StringType).as(REJECTED_BUSINESS_KEY),
         col(ERRORS).as(REJECTED_ERRORS), col(RAW_RECORD).as(REJECTED_RAW_RECORD)
````

---

## Phase 5: Source + repository (navi chat → Prompt 0 → P10 … P13)

### P10: LoadWindow, SourceDefinition, SourceBatch

````text
Create 3 files in package com.tcs.dmj.dwh.connector.source, and 1 DTO (file 4).

1. record LoadWindow(java.time.Instant start, java.time.Instant end)
   Meaning: watermarkField > start AND watermarkField <= end. start == null = first run (no lower bound).
   - boolean isFirstRun(): start == null
   - String describe(String field): first run -> field + " <= " + end + " (first run)"
                                     else      -> field + " > " + start + " AND <= " + end

2. record SourceDefinition(String collection, List<String> fields, String businessKey, String latestRecordColumn,
                           Double maxRejectPercent, String sourceFilter, String softDeleteField, SourceMetadata metadata)
   The "data contract" of ONE MongoDB collection. ALL fields are read as String on purpose.
   - public static final String TEST_RECORD_FIELD = "isTest"; DEFAULT_SOFT_DELETE_FIELD = "isDeleted";
   - nested record SourceMetadata(String description, String owner, String sla)
   - compact constructor: throw IllegalArgumentException when businessKey or latestRecordColumn is not in fields,
     or maxRejectPercent != null and not between 0 and 100; then fields = List.copyOf(fields).
   - public StructType schema(): field QualityColumns.SOURCE_ID (StringType, NOT nullable) + every field (StringType,
     nullable) + softDeleteField when it is not null and not already in fields (StringType, nullable).
     (QualityColumns is com.tcs.dmj.dwh.validation.QualityColumns)
   - public String ownerHint(): metadata.owner() == null ? "" : " Source owner: " + owner + "."
   - public static Builder builder(String collection)
   - public static final class Builder with fluent methods (all return this):
       fields(String... fields), businessKey(String), latestRecordColumn(String), maxRejectPercent(double),
       sourceFilter(String mongoMatchJson), excludeTestRecords() = sourceFilter("{\"isTest\": {\"$ne\": true}}"),
       softDelete() = softDelete(DEFAULT_SOFT_DELETE_FIELD), softDelete(String flagField),
       description(String), owner(String), sla(String), and build().

3. record SourceBatch(SourceDefinition source, Dataset<Row> valid, Dataset<Row> rejected, Dataset<Row> deleted,
                      long sourceCount, long validCount, long rejectedCount, long duplicateCount, long deletedCount,
                      boolean derived)
   Result of reading + cleaning + validating ONE source in ONE run. deleted = business key + DELETED_AT column of
   documents soft-deleted at the source (null when the source has no soft delete).
   - public static final String DELETED_AT = "_deleted_at";
   - String collection(): source.collection()
   - long badRowCount(): rejectedCount - duplicateCount
   - long activeCount(): sourceCount - deletedCount

4. Package com.tcs.dmj.dwh.service.dto: record SourceInfoDTO(String collection, String description, String owner,
   String sla, String businessKey, List<String> fields, Double maxRejectPercent, String sourceFilter, String softDeleteField)
   with static SourceInfoDTO from(SourceDefinition source) (description / owner / sla from source.metadata()).
````

### P11: MetaPipelineRepository + MetaPipelineRunRepository

````text
Create 2 Spring @Repository classes in com.tcs.dmj.dwh.repository, each with a constructor (JdbcTemplate jdbc).
Use the SQL EXACTLY as written. Domain records exist: com.tcs.dmj.dwh.domain.MetaPipeline, MetaPipelineRun.
LoadWindow is com.tcs.dmj.dwh.connector.source.LoadWindow(Instant start, Instant end). PipelineStatus is an enum.
Timestamps are read with rs.getObject("col", OffsetDateTime.class) and written as OffsetDateTime.ofInstant(i, ZoneOffset.UTC).
Both classes have: private static String truncate(String text, int max) (null-safe, substring(0, max)).

1. MetaPipelineRepository (constants MAX_NAME_LENGTH 30, MAX_DESCRIPTION_LENGTH 100, MAX_JOB_NAME_LENGTH 50,
   MAX_DAG_ID_LENGTH 30). RowMapper for MetaPipeline from columns pipeline_id, pipeline_name, pipeline_type,
   airflow_dag_id, application_job_name, pipeline_description, last_run_time, active_flag, created_at, updated_at.
   - void register(String pipelineName, String pipelineType, String description, String applicationJobName, String airflowDagId)
     name longer than 30 -> IllegalStateException; name not matching "[A-Za-z0-9._-]+" -> IllegalStateException.
     Truncate job name / description / dag id. Then two updates:
       INSERT INTO meta.meta_pipeline (pipeline_name, pipeline_type, application_job_name, pipeline_description, airflow_dag_id)
       SELECT CAST(? AS VARCHAR), CAST(? AS VARCHAR), CAST(? AS VARCHAR), CAST(? AS VARCHAR), CAST(? AS VARCHAR)
        WHERE NOT EXISTS (SELECT 1 FROM meta.meta_pipeline WHERE pipeline_name = ?)
       ON CONFLICT DO NOTHING
     params: name, type, jobName, description, dagId, name
       UPDATE meta.meta_pipeline
          SET pipeline_type = ?, application_job_name = ?, pipeline_description = ?, airflow_dag_id = ?, active_flag = 'Y'
        WHERE pipeline_name = ?
          AND (pipeline_type IS DISTINCT FROM CAST(? AS VARCHAR) OR application_job_name IS DISTINCT FROM CAST(? AS VARCHAR)
               OR pipeline_description IS DISTINCT FROM CAST(? AS VARCHAR) OR airflow_dag_id IS DISTINCT FROM CAST(? AS VARCHAR)
               OR active_flag <> 'Y')
     params: type, jobName, description, dagId, name, type, jobName, description, dagId
     (last_run_time is NEVER touched here)
   - int deactivateAllExcept(Collection<String> pipelineNames):
       UPDATE meta.meta_pipeline SET active_flag = 'N' WHERE active_flag = 'Y' AND NOT (pipeline_name = ANY (?))
       param: (Object) pipelineNames.toArray(String[]::new)
   - List<MetaPipeline> findAll(): SELECT * FROM meta.meta_pipeline ORDER BY pipeline_name
   - Optional<MetaPipeline> findByName(String name): SELECT * FROM meta.meta_pipeline WHERE pipeline_name = ?
   - LoadWindow window(String pipelineName, Instant runEnd): queryForList("SELECT last_run_time FROM meta.meta_pipeline
       WHERE pipeline_name = ?", OffsetDateTime.class, name); start = empty or null ? null : toInstant(); new LoadWindow(start, runEnd)
   - void advanceLastRunTime(String pipelineName, Instant runEnd):
       UPDATE meta.meta_pipeline SET last_run_time = ? WHERE pipeline_name = ?   ; updated != 1 -> IllegalStateException

2. MetaPipelineRunRepository (MAX_ERROR_CODE_LENGTH 100, MAX_ERROR_MESSAGE_LENGTH 1000).
   SELECT constant: "SELECT r.*, p.pipeline_name FROM meta.meta_pipeline_run r JOIN meta.meta_pipeline p ON p.pipeline_id = r.pipeline_id"
   RowMapper for MetaPipelineRun from pipeline_run_id, pipeline_id, pipeline_name, last_run_time, start_time, end_time,
   pipeline_status, total_records / success_records / failed_records (rs.getObject(.., Long.class)), error_code,
   error_message, created_at, updated_at.
   - long createRun(String pipelineName, Instant windowEnd): queryForList(Long.class):
       INSERT INTO meta.meta_pipeline_run (pipeline_id, pipeline_status, last_run_time, start_time)
       SELECT pipeline_id, ?, ?, clock_timestamp() FROM meta.meta_pipeline WHERE pipeline_name = ?
       RETURNING pipeline_run_id
     params: "RUNNING", windowEnd as OffsetDateTime UTC, name. Empty -> IllegalStateException("Pipeline X is not registered...")
   - void finishRun(long pipelineRunId, PipelineStatus status, long totalRecords, long successRecords, long failedRecords,
                    String errorCode, String errorMessage):
       UPDATE meta.meta_pipeline_run SET pipeline_status = ?, end_time = clock_timestamp(), total_records = ?,
              success_records = ?, failed_records = ?, error_code = ?, error_message = ? WHERE pipeline_run_id = ?
     (truncate error code 100 / message 1000)
   - Optional<MetaPipelineRun> findById(long id): SELECT + " WHERE r.pipeline_run_id = ?"
   - List<MetaPipelineRun> findRecent(String pipelineName, String status, int limit): SELECT + " WHERE 1 = 1",
       optional " AND p.pipeline_name = ?", optional " AND r.pipeline_status = ?" (status.toUpperCase()),
       " ORDER BY r.pipeline_run_id DESC LIMIT ?"
   - List<String> pipelinesWithRunningRuns(): SELECT DISTINCT p.pipeline_name FROM meta.meta_pipeline_run r
       JOIN meta.meta_pipeline p ON p.pipeline_id = r.pipeline_id WHERE r.pipeline_status = ?   ("RUNNING")
   - boolean isRunning(String pipelineName): SELECT EXISTS (SELECT 1 FROM meta.meta_pipeline_run r JOIN meta.meta_pipeline p
       ON p.pipeline_id = r.pipeline_id WHERE p.pipeline_name = ? AND r.pipeline_status = ?)
   - int failInterruptedRuns(String pipelineName):
       UPDATE meta.meta_pipeline_run r SET pipeline_status = ?, end_time = clock_timestamp(), error_code = 'INTERRUPTED',
              error_message = 'The process stopped while the run was busy'
         FROM meta.meta_pipeline p
        WHERE p.pipeline_id = r.pipeline_id AND p.pipeline_name = ? AND r.pipeline_status = ?
     params: "FAILED", name, "RUNNING"
````

### P12: RejectedRecordRepository

````text
Create com.tcs.dmj.dwh.repository.RejectedRecordRepository (@Repository), constructor
(JdbcTemplate jdbc, org.springframework.transaction.support.TransactionTemplate transaction,
 com.fasterxml.jackson.databind.ObjectMapper objectMapper).
Existing: com.tcs.dmj.dwh.domain.RejectedRecord, com.tcs.dmj.dwh.service.dto.RejectedRecordSummaryDTO,
com.tcs.dmj.dwh.validation.QualityColumns (REJECTED_SOURCE_ID "source_id", REJECTED_BUSINESS_KEY "business_key",
REJECTED_RAW_RECORD "raw_record", ERROR_TYPE, FIELD_NAME, ERROR_MESSAGE, FIELD_VALUE).
meta_rejected_record has ONE row per rejected record with error_ids INT[] (ids of meta_error_master, sorted),
error_messages TEXT (same order, ", " separated), field_values JSONB {"field": "bad value"}.

Constants: MESSAGE_SEPARATOR = ", "; BATCH = 1000;
ON_CONFLICT_UPDATE =
 "ON CONFLICT (pipeline_name, source_collection, source_id) DO UPDATE
    SET business_key = EXCLUDED.business_key, error_ids = EXCLUDED.error_ids, error_messages = EXCLUDED.error_messages,
        field_values = EXCLUDED.field_values, raw_record = EXCLUDED.raw_record,
        attempt_count = meta.meta_rejected_record.attempt_count + 1,
        last_job_run_id = EXCLUDED.last_job_run_id, last_rejected_at = now()"

1. public void saveFromStaging(String stagingTable, String pipelineName, String collection, long pipelineRunId)
   The staging table has ONE row per record+error: source_id, business_key, error_type, field_name, error_message,
   field_value, raw_record. Inside transaction.executeWithoutResult run 2 statements (%s = stagingTable):
   a) INSERT INTO meta.meta_error_master (error_type, field_name, error_message)
      SELECT DISTINCT ON (s.error_type, s.error_message) s.error_type, s.field_name, s.error_message
        FROM %s s
       WHERE NOT EXISTS (SELECT 1 FROM meta.meta_error_master e WHERE e.error_type = s.error_type AND e.error_message = s.error_message)
      ON CONFLICT (error_type, error_message) DO NOTHING
   b) WITH errors AS (
          SELECT DISTINCT ON (s.source_id, e.error_id)
                 s.source_id, s.business_key, e.error_id, e.error_message, s.field_name, s.field_value, s.raw_record
            FROM %s s
            JOIN meta.meta_error_master e ON e.error_type = s.error_type AND e.error_message = s.error_message
           ORDER BY s.source_id, e.error_id
      ), records AS (
          SELECT source_id, MAX(business_key) AS business_key,
                 ARRAY_AGG(error_id ORDER BY error_id) AS error_ids,
                 STRING_AGG(error_message, ', ' ORDER BY error_id) AS error_messages,
                 JSONB_OBJECT_AGG(field_name, field_value) FILTER (WHERE field_name IS NOT NULL) AS field_values,
                 MAX(raw_record) AS raw_record
            FROM errors GROUP BY source_id
      )
      INSERT INTO meta.meta_rejected_record
             (pipeline_name, source_collection, source_id, business_key, error_ids, error_messages, field_values,
              raw_record, attempt_count, first_job_run_id, last_job_run_id, first_rejected_at, last_rejected_at)
      SELECT ?, ?, source_id, business_key, error_ids, error_messages, field_values, CAST(raw_record AS JSONB),
             1, ?, ?, now(), now()
        FROM records
      + ON_CONFLICT_UPDATE
      params: pipelineName, collection, pipelineRunId, pipelineRunId

2. public void saveDirect(Iterator<Row> errorRows, String pipelineName, String collection, long pipelineRunId)
   Same result WITHOUT a staging table (rows come from Spark, one row per record+error). Group in memory:
   errorKey = errorType + "|" + errorMessage; Map<String sourceId, LinkedHashMap<String errorKey, Row>> errorsBySource,
   LinkedHashMap<String errorKey, Row> distinctErrors. Empty -> return. In transaction.executeWithoutResult:
   a) jdbc.batchUpdate over distinctErrors.values(), BATCH:
      INSERT INTO meta.meta_error_master (error_type, field_name, error_message) SELECT ?, ?, ?
       WHERE NOT EXISTS (SELECT 1 FROM meta.meta_error_master WHERE error_type = ? AND error_message = ?)
      ON CONFLICT (error_type, error_message) DO NOTHING
      params: type, field, message, type, message
   b) jdbc.batchUpdate over the records (one Map<errorKey,Row> per record), BATCH:
      WITH errors AS (SELECT e.error_id, e.error_message FROM meta.meta_error_master e
                       WHERE e.error_type || '|' || e.error_message = ANY (?))
      INSERT INTO meta.meta_rejected_record (same 13 columns as above)
      SELECT ?, ?, ?, ?, (SELECT ARRAY_AGG(error_id ORDER BY error_id) FROM errors),
             (SELECT STRING_AGG(error_message, ', ' ORDER BY error_id) FROM errors),
             CAST(? AS JSONB), CAST(? AS JSONB), 1, ?, ?, now(), now()
      + ON_CONFLICT_UPDATE
      params: 1 = ps.getConnection().createArrayOf("text", errorKeys), 2 pipelineName, 3 collection, 4 source_id,
              5 business_key, 6 fieldValuesJson(errors), 7 raw_record, 8 runId, 9 runId
   fieldValuesJson: LinkedHashMap field_name -> field_value for errors whose field_name != null; empty -> null;
   else objectMapper.writeValueAsString (wrap JsonProcessingException in IllegalStateException).

3. public List<RejectedRecord> find(String pipelineName, String collection, String errorType, int limit):
   "SELECT r.* FROM meta.meta_rejected_record r WHERE 1 = 1" + optional " AND r.pipeline_name = ?",
   " AND r.source_collection = ?", " AND EXISTS (SELECT 1 FROM meta.meta_error_master e WHERE e.error_id = ANY (r.error_ids)
   AND e.error_type = ?)" (errorType.toUpperCase()), then
   " ORDER BY r.pipeline_name, r.source_collection, r.business_key, r.source_id LIMIT ?".
   RowMapper: error_ids via rs.getArray -> Arrays.asList((Integer[]) array.getArray()) (null -> List.of()),
   field_values JSON text -> Map<String,String> with objectMapper (null -> Map.of()),
   first/last_rejected_at as LocalDateTime.

4. public List<RejectedRecordSummaryDTO> summary():
   SELECT r.pipeline_name, r.source_collection, e.error_id, e.error_type, e.error_message, COUNT(*) AS record_count
     FROM meta.meta_rejected_record r
    CROSS JOIN LATERAL UNNEST(r.error_ids) AS u(error_id)
     JOIN meta.meta_error_master e ON e.error_id = u.error_id
    GROUP BY r.pipeline_name, r.source_collection, e.error_id, e.error_type, e.error_message
    ORDER BY r.pipeline_name, r.source_collection, e.error_type, e.error_id
````

### P13: lineage repositories

````text
Create 3 @Repository classes in com.tcs.dmj.dwh.repository, each with constructor (JdbcTemplate jdbc).
Pattern for every upsert: INSERT ... SELECT ... WHERE NOT EXISTS (same key) ON CONFLICT DO NOTHING
(so identity ids have no gaps), then UPDATE only when something changed, then SELECT the id.

1. MetaAssetRepository (MAX_NAME_LENGTH 100, MAX_DESCRIPTION_LENGTH 100). MetaAsset is com.tcs.dmj.dwh.domain.MetaAsset.
   SAME_ASSET = "platform = ? AND COALESCE(database_name, '') = COALESCE(CAST(? AS VARCHAR), '')
                 AND COALESCE(schema_name, '') = COALESCE(CAST(? AS VARCHAR), '') AND asset_name = ?"
   key params = {platform.name(), databaseName, schemaName, truncated name}
   public long upsert(MetaAsset asset):
   a) INSERT INTO meta.meta_asset (asset_name, asset_type, platform, database_name, schema_name, layer_name, asset_description)
      SELECT CAST(? AS VARCHAR) x7 WHERE NOT EXISTS (SELECT 1 FROM meta.meta_asset WHERE SAME_ASSET) ON CONFLICT DO NOTHING
      params: name, type, platform, database, schema, layer, description, then the 4 key params
   b) UPDATE meta.meta_asset SET asset_type = ?, layer_name = ?, asset_description = COALESCE(CAST(? AS VARCHAR), asset_description),
             active_flag = 'Y'
       WHERE SAME_ASSET AND (asset_type IS DISTINCT FROM CAST(? AS VARCHAR) OR layer_name IS DISTINCT FROM CAST(? AS VARCHAR)
             OR (CAST(? AS VARCHAR) IS NOT NULL AND asset_description IS DISTINCT FROM CAST(? AS VARCHAR)) OR active_flag <> 'Y')
      params: type, layer, description, 4 key params, type, layer, description, description
   c) SELECT asset_id FROM meta.meta_asset WHERE SAME_ASSET (4 key params)
   Helper: private static Object[] concat(Object[]... parts).

2. MetaLineageRepository:
   public long upsert(long sourceAssetId, long targetAssetId, long pipelineId):
     INSERT INTO meta.meta_lineage (source_asset_id, target_asset_id, pipeline_id)
     SELECT CAST(? AS BIGINT), CAST(? AS BIGINT), CAST(? AS BIGINT)
      WHERE NOT EXISTS (SELECT 1 FROM meta.meta_lineage WHERE source_asset_id = ? AND target_asset_id = ? AND pipeline_id = ?)
     ON CONFLICT DO NOTHING
     UPDATE meta.meta_lineage SET active_flag = 'Y' WHERE source_asset_id = ? AND target_asset_id = ? AND pipeline_id = ? AND active_flag <> 'Y'
     SELECT lineage_id FROM meta.meta_lineage WHERE source_asset_id = ? AND target_asset_id = ? AND pipeline_id = ?
   public int deactivateAllExcept(long pipelineId, Collection<Long> lineageIds):
     UPDATE meta.meta_lineage SET active_flag = 'N' WHERE pipeline_id = ? AND active_flag = 'Y' AND NOT (lineage_id = ANY (?))
     params: pipelineId, lineageIds.toArray(Long[]::new)

3. MetaColumnLineageRepository (MappingType from com.tcs.dmj.dwh.enums):
   SAME_MAPPING = "pipeline_id = ? AND source_asset_id = ? AND source_column_name = ? AND target_asset_id = ? AND target_column_name = ?"
   public long upsert(long pipelineId, long sourceAssetId, String sourceColumn, long targetAssetId, String targetColumn, MappingType mappingType):
     INSERT INTO meta.meta_column_lineage (pipeline_id, source_asset_id, source_column_name, target_asset_id, target_column_name, mapping_type)
     SELECT CAST(? AS BIGINT), CAST(? AS BIGINT), CAST(? AS VARCHAR), CAST(? AS BIGINT), CAST(? AS VARCHAR), CAST(? AS VARCHAR)
      WHERE NOT EXISTS (SELECT 1 FROM meta.meta_column_lineage WHERE SAME_MAPPING) ON CONFLICT DO NOTHING
     UPDATE meta.meta_column_lineage SET mapping_type = ?, active_flag = 'Y'
      WHERE SAME_MAPPING AND (mapping_type IS DISTINCT FROM CAST(? AS VARCHAR) OR active_flag <> 'Y')
     SELECT column_lineage_id FROM meta.meta_column_lineage WHERE SAME_MAPPING
   public int deactivateAllExcept(long pipelineId, Collection<Long> columnLineageIds): same as MetaLineageRepository
     on meta.meta_column_lineage / column_lineage_id.
````

✅ **Check:** `mvn -q compile`. Have koi error na hovi joie.

---

## Phase 6: Framework engine (navi chat → Prompt 0 → P14 … P19)

### P14: JobMetrics, PipelineContext, ColumnLineage

````text
Create 3 files in package com.tcs.dmj.dwh.core.

1. class JobMetrics: counters of one run, AtomicLong fields sourceCount, validCount, rejectedCount, duplicateCount,
   deletedCount, loadedCount. Methods: addSource(long), addValid(long), addRejected(long), addDuplicates(long),
   addDeleted(long), addLoaded(long), resetLoaded() (sets loaded to 0), and getters sourceCount(), validCount(),
   rejectedCount(), duplicateCount(), deletedCount(), loadedCount().

2. class PipelineContext: everything of ONE run, created new for every run.
   Constructor (long pipelineRunId, String pipelineName, Instant windowEnd, boolean directLoad).
   Fields: JobMetrics metrics = new JobMetrics(); List<SourceBatch> sourceBatches = new ArrayList<>();
   List<Dataset<Row>> cachedDatasets; AtomicInteger stagingTableCounter; Set<String> deletesApplied =
   ConcurrentHashMap.newKeySet(); Map<String, LineageEdge> lineage = new ConcurrentHashMap<>().
   (SourceBatch = com.tcs.dmj.dwh.connector.source.SourceBatch, MetaAsset = com.tcs.dmj.dwh.domain.MetaAsset)
   Methods: pipelineRunId(), pipelineName(), windowEnd(), directLoad(), metrics(),
   addSourceBatch(SourceBatch), sourceBatches() (unmodifiable),
   Dataset<Row> cache(Dataset<Row> d): persist(StorageLevel.MEMORY_AND_DISK()), remember it, return the cached one
     (Javadoc: Spark is lazy; without caching every action would read MongoDB again),
   releaseResources(): unpersist every cached dataset and clear the list,
   markDeletesApplied(String collection), boolean deletesApplied(String collection),
   int nextStagingTableNumber(): incrementAndGet,
   addLineage(MetaAsset source, MetaAsset target): putIfAbsent(source.key() + " -> " + target.key(), new LineageEdge(source, target)),
   List<LineageEdge> lineage(): List.copyOf(values),
   nested public record LineageEdge(MetaAsset source, MetaAsset target).

3. record ColumnLineage(String sourceCollection, String sourceField, String targetTable, String targetColumn,
                        MappingType mappingType) with static factory of(...same 5 params...).
````

### P15: MongoSourceReader + SourceProcessor

````text
Create 2 Spring @Component classes in com.tcs.dmj.dwh.connector.source. Existing classes:
LoadWindow, SourceDefinition (collection(), fields(), businessKey(), latestRecordColumn(), maxRejectPercent(),
sourceFilter(), softDeleteField(), schema(), ownerHint()), SourceBatch (record + DELETED_AT, badRowCount(), activeCount()),
ApplicationProperties (com.tcs.dmj.dwh.config; mongo().uri(), mongo().database(), quality().rejectGateMinRecords()),
PipelineContext (com.tcs.dmj.dwh.core; cache(), metrics(), addSourceBatch(), pipelineName(), windowEnd(), pipelineRunId()),
MetaPipelineRepository (com.tcs.dmj.dwh.repository; window(pipelineName, Instant)),
DataQualityEngine, DataSanitizer (apply), DataQualityRules, QualityResult, QualityColumns (com.tcs.dmj.dwh.validation),
DataQualityGateException (com.tcs.dmj.dwh.service), ErrorType, SparkTypes.

1. MongoSourceReader(SparkSession spark, ApplicationProperties properties). READ ONLY.
   public Dataset<Row> read(SourceDefinition source, LoadWindow window):
     pipeline = matchStages(...) each Document.toJson(), joined with ", " inside "[" "]".
     spark.read().format("mongodb").option("connection.uri", uri).option("database", db)
          .option("collection", source.collection()).option("aggregation.pipeline", pipeline)
          .schema(source.schema()).load()
   private List<org.bson.Document> matchStages(SourceDefinition source, LoadWindow window):
     range = new Document(); if not first run: range.append("$gt", Date.from(window.start()));
     range.append("$lte", Date.from(window.end()));
     stage 1: new Document("$match", new Document(source.latestRecordColumn(), range))
     stage 2 (when sourceFilter != null): new Document("$match", Document.parse(source.sourceFilter()))

2. SourceProcessor(MongoSourceReader reader, DataQualityEngine qualityEngine, MetaPipelineRepository metaPipelineRepository,
   ApplicationProperties properties) -> rejectGateMinRecords from properties. Constants DELETED_FLAG = "_sd_deleted",
   LATEST_IS_DELETED = "_sd_latest_deleted".
   public SourceBatch process(PipelineContext context, SourceDefinition source, DataSanitizer sanitizer, DataQualityRules rules):
     0. window = metaPipelineRepository.window(context.pipelineName(), context.windowEnd())
     1. raw = context.cache(reader.read(source, window).withColumn(QualityColumns.RAW_RECORD, to_json(struct(every field column))))
        log "[job {}] extract {} ({})" with window.describe(latestRecordColumn); sourceCount = raw.count()
     2. cleaned = sanitizer.apply(raw)
     3. split = splitSoftDeletes(context, cleaned, source)
     4. result = qualityEngine.check(split.active(), rules, source.businessKey(), source.latestRecordColumn())
        valid = context.cache(result.valid()); batch = toBatch(context, source, valid, result.rejected(), split.deleted(), sourceCount, false)
     5. metrics: addSource, addValid, addRejected, addDuplicates, addDeleted; context.addSourceBatch(batch); log the counts
     6. if source.maxRejectPercent() != null -> checkRejectGate(batch); return batch
   public SourceBatch processDerived(PipelineContext context, SourceDefinition definition, Dataset<Row> rows, DataQualityRules rules):
     rows built from another batch (e.g. order items[]), they contain _id and _raw_record already.
     result = check(rows, rules, businessKey, latestRecordColumn);
     batch = toBatch(context, definition, context.cache(result.valid()), result.rejected(), null, -1, true);
     only metrics addRejected + addDuplicates; addSourceBatch; log; return.
   private SourceBatch toBatch(context, source, valid, rejectedRows, deleted, long sourceCount, boolean derived):
     rejected = context.cache(rejectedRows); validCount = valid.count(); rejectedCount = rejected.count();
     duplicateCount = rejected.filter(QualityColumns.mainErrorType().equalTo("DUPLICATE")).count();
     deletedCount = deleted == null ? 0 : deleted.count(); count = sourceCount >= 0 ? sourceCount : valid + rejected.
   private record ActiveAndDeleted(Dataset<Row> active, Dataset<Row> deleted)
   private ActiveAndDeleted splitSoftDeletes(context, cleaned, source):
     softDeleteField == null -> (cleaned, null). Else key = businessKey;
     flag = lower(trim(col(softDeleteField))).isin("true", "1", "y", "yes");
     flagged = cleaned.withColumn(DELETED_FLAG, coalesce(flag, lit(false)).and(col(key).isNotNull()))
                      .withColumn(LATEST_IS_DELETED, first(col(DELETED_FLAG)).over(Window.partitionBy(col(key))
                        .orderBy(col(latestRecordColumn).cast(TIMESTAMP).desc_nulls_last(), col(SOURCE_ID).asc())));
     active = flagged.filter(not(col(LATEST_IS_DELETED))).drop(both flags);
     deleted = context.cache(flagged.filter(col(LATEST_IS_DELETED).and(col(DELETED_FLAG))).groupBy(col(key))
                 .agg(max(col(latestRecordColumn).cast(TIMESTAMP)).as(SourceBatch.DELETED_AT)))
   private void checkRejectGate(SourceBatch batch):
     activeCount < rejectGateMinRecords -> log "Reject gate skipped ..." and return.
     percent = 100.0 * badRowCount / activeCount; percent > max -> throw new DataQualityGateException(collection,
       String.format(Locale.ROOT, "Reject gate failed for %s: %d of %d rows rejected (%.1f%%), allowed at most %.1f%%. "
       + "Errors: %s. Example: %s. Nothing from this collection was loaded and the last_run_time did not move.%s",
       collection, bad, active, percent, max, errorSummary(badErrors), exampleError(badErrors), source.ownerHint()))
     where badErrors = QualityColumns.oneRowPerError(batch.rejected().filter(mainErrorType() != "DUPLICATE")).
   errorSummary: group by error_type count -> "ORPHAN=62, VALIDATION=3" (sorted).
   exampleError: first row -> "[" + error_type + "] " + error_message + " (value: " + field_value + ")", none -> "-".
````

### P16: target writers part 1

````text
Create 4 files in com.tcs.dmj.dwh.connector.target. Existing: ApplicationProperties (postgres(): url, username, password,
stagingSchema, batchSize), PipelineContext (pipelineRunId(), nextStagingTableNumber()), SparkTypes.

1. @Component SoftDeleteColumns(JdbcTemplate jdbc). Constants IS_DELETED = "is_deleted", DELETED_AT = "deleted_at".
   Map<String, Boolean> supportByTable = new ConcurrentHashMap<>().
   - boolean supports(String table): computeIfAbsent: split "schema.table";
       SELECT count(*) FROM information_schema.columns WHERE table_schema = ? AND table_name = ? AND column_name IN (?, ?)
     -> true when the count is 2.
   - Dataset<Row> markNotDeleted(Dataset<Row> rows, String table): not supported -> rows unchanged; else
     withColumn(IS_DELETED, lit(false)).withColumn(DELETED_AT, lit(null).cast("timestamp"))

2. @Component PostgresWriter(ApplicationProperties properties, JdbcTemplate jdbc, SoftDeleteColumns softDeleteColumns).
   public static final String JOB_RUN_ID_COLUMN = "job_run_id", LOADED_AT_COLUMN = "loaded_at".
   Javadoc: Spark JDBC can only insert/overwrite, so UPSERT = staging table + one INSERT ... ON CONFLICT SQL. Idempotent.
   - public long upsert(PipelineContext context, Dataset<Row> rows, String targetTable, String... businessKeyColumns):
       prepared = softDeleteColumns.markNotDeleted(rows, targetTable).dropDuplicates(businessKeyColumns)
                  .withColumn(JOB_RUN_ID_COLUMN, lit(context.pipelineRunId())).withColumn(LOADED_AT_COLUMN, current_timestamp());
       staging = writeToStaging(context, prepared, targetTable);
       try { sql = buildUpsertSql(targetTable, staging, prepared.columns(), businessKeyColumns); return jdbc.update(sql); }
       finally { dropStagingTable(staging); }
   - public String writeToStaging(PipelineContext context, Dataset<Row> rows, String baseName):
       name = "%s.stg_%s_%d_%d".formatted(stagingSchema, baseName.replace('.', '_').toLowerCase(), runId, context.nextStagingTableNumber());
       rows.write().mode(SaveMode.Overwrite).format("jdbc").option url/user/password/driver "org.postgresql.Driver"
           .option("dbtable", name).option("batchsize", batchSize).save(); return name.
   - public void dropStagingTable(String t): jdbc.execute("DROP TABLE IF EXISTS " + t)
   - static String buildUpsertSql(String targetTable, String stagingTable, String[] columns, String[] keyColumns):
       "INSERT INTO %s (%s) SELECT %s FROM %s ON CONFLICT (%s) %s" with the column list twice; action =
       "DO UPDATE SET c = EXCLUDED.c, ..." for every non-key column, or "DO NOTHING" when there is none.

3. @Component PostgresReader(SparkSession spark, ApplicationProperties properties).
   - Dataset<Row> readTable(String table, String... columns): spark.read().format("jdbc") url/user/password/driver,
     option("dbtable", table).load(); select the columns when given.
   - Dataset<Row> addSurrogateKey(Dataset<Row> facts, String dimensionTable, String surrogateKeyColumn, String... businessKeyColumns):
     dimension = readTable(dimensionTable, surrogateKey + business keys); join condition facts.col(k).equalTo(dimension.col(k))
     for every key (AND); left join; drop dimension.col(k) for every key; return.

4. final class DateDimension (private constructor). TABLE = "silver.dim_date", KEY_COLUMN = "date_key".
   - static Column dateKey(Column dateOrTimestamp): date_format(x, "yyyyMMdd").cast(SparkTypes.INT)
   - static Dataset<Row> fromColumn(Dataset<Row> rows, String dateColumn): select to_date(col(dateColumn)).as("full_date"),
     filter not null, distinct, then select dateKey(d).as(KEY_COLUMN), d (full_date), dayofmonth(d).as("day_of_month"),
     month(d).as("month"), date_format(d, "MMMM").as("month_name"), quarter(d).as("quarter"), year(d).as("year"),
     dayofweek(d).as("day_of_week"), date_format(d, "EEEE").as("day_name"), dayofweek(d).isin(1, 7).as("is_weekend").
````

### P17: Scd2Writer

````text
Create @Component com.tcs.dmj.dwh.connector.target.Scd2Writer(PostgresWriter postgresWriter, JdbcTemplate jdbc,
TransactionTemplate transaction). It loads a SLOWLY CHANGING DIMENSION TYPE 2 table (keeps history).
Javadoc example: customer C003 moved Mumbai -> Pune: old row closed (valid_to = change time, is_current 'N'),
new row inserted (valid_to 9999-12-31, is_current 'Y'). Facts keep the key of the version valid at event time.
Columns in scd2Columns create a NEW version; all other data columns are SCD1 (overwritten in every version).

public static final String OPEN_END = "TIMESTAMP '9999-12-31 00:00:00'";
public record Scd2Spec(String table, String businessKey, List<String> scd2Columns, String changedAtColumn, String firstValidFromColumn)
  changedAtColumn = TIMESTAMP column of the row (valid_from of a new version, valid_to of the closed one), not stored.
  firstValidFromColumn = valid_from of the FIRST version of a key (null = use changedAtColumn), not stored.

public long load(PipelineContext context, Dataset<Row> rows, Scd2Spec spec):
  unique = rows.dropDuplicates(spec.businessKey()); staging = postgresWriter.writeToStaging(context, unique, spec.table());
  try: technical = {businessKey, changedAtColumn, firstValidFromColumn if not null};
       scd1Columns = columns of unique that are not technical and not in scd2Columns;
       Long touched = transaction.execute(status -> {
           scd1 = scd1Columns.isEmpty() ? 0 : jdbc.update(scd1Sql(...)); closed = jdbc.update(closeSql(...));
           inserted = jdbc.update(insertSql(...)); log "[job {}] SCD2 {}: scd1Updated={}, closed={}, inserted={}"; return sum; });
       return touched == null ? 0 : touched;
  finally: postgresWriter.dropStagingTable(staging)

private static String distinct(String l, String r, List<String> cols): "(l.a, l.b) IS DISTINCT FROM (r.a, r.b)"
static String scd1Sql(Scd2Spec spec, String staging, List<String> scd1Columns, long runId):
  "UPDATE %s t SET a = s.a, b = s.b, job_run_id = %d, loaded_at = now() FROM %s s WHERE t.<key> = s.<key> AND "
  + distinct("t", "s", scd1Columns)
static String closeSql(Scd2Spec spec, String staging, long runId):
  "UPDATE %s t SET valid_to = s.<changedAt>, is_current = 'N', job_run_id = %d, loaded_at = now() FROM %s s "
  + "WHERE t.<key> = s.<key> AND t.is_current = 'Y' AND s.<changedAt> > t.valid_from AND " + distinct("t", "s", scd2Columns)
static String insertSql(Scd2Spec spec, String staging, List<String> scd1Columns, long runId):
  dataColumns = key + scd1Columns + scd2Columns; firstValidFrom = firstValidFromColumn == null ? "s.<changedAt>"
  : "COALESCE(s.<firstValidFrom>, s.<changedAt>)";
  "INSERT INTO T (<dataColumns>, valid_from, valid_to, is_current, job_run_id, loaded_at) "
  + "SELECT s.<each dataColumn>, CASE WHEN EXISTS (SELECT 1 FROM T d WHERE d.<key> = s.<key>) THEN s.<changedAt> ELSE "
  + firstValidFrom + " END, " + OPEN_END + ", 'Y', <runId>, now() FROM <staging> s "
  + "WHERE NOT EXISTS (SELECT 1 FROM T d WHERE d.<key> = s.<key> AND d.is_current = 'Y')"
````

### P18: SoftDeleteWriter + DirectPostgresWriter

````text
Create 2 @Component classes in com.tcs.dmj.dwh.connector.target. Existing: PostgresWriter (writeToStaging, dropStagingTable,
JOB_RUN_ID_COLUMN, LOADED_AT_COLUMN), SoftDeleteColumns (supports, markNotDeleted, DELETED_AT), SourceBatch
(deleted(), deletedCount(), source().businessKey(), collection(), DELETED_AT), PipelineContext (directLoad(), cache(), pipelineRunId()).

1. SoftDeleteWriter(PostgresWriter postgresWriter, SoftDeleteColumns softDeleteColumns, JdbcTemplate jdbc). KEY = "deleted_key".
   Soft delete = rows are NOT removed, they get is_deleted = TRUE, deleted_at = when deleted at the source.
   public long apply(PipelineContext context, SourceBatch batch, String targetTable, String targetKeyColumn, boolean scd2):
     deletedCount == 0 -> 0; table not supported -> IllegalStateException(targetTable + " has no is_deleted / deleted_at columns...");
     keys = batch.deleted().select(col(businessKey).as(KEY), col(SourceBatch.DELETED_AT).as("deleted_at"));
     marked = context.directLoad() ? applyDirect(...) : applyWithStaging(...); log; return marked.
   applyWithStaging: staging = writeToStaging(context, keys, "deleted_" + table); jdbc.update(stagingSql(...), runId); finally drop.
   applyDirect: collect keys.toLocalIterator() into a list; jdbc.batchUpdate(directSql(...), rows, 1000, setter):
     params: deleted_at (Timestamp), runId, [deleted_at again when scd2], key (String); return sum of counts > 0.
   static String stagingSql(String table, String keyColumn, boolean scd2, String staging):
     "UPDATE " + table + " t SET is_deleted = TRUE, deleted_at = s.deleted_at, job_run_id = ?, loaded_at = now()"
     + (scd2 ? ", is_current = 'N', valid_to = GREATEST(s.deleted_at, t.valid_from + INTERVAL '1 second')" : "")
     + " FROM " + staging + " s WHERE t." + keyColumn + " = s.deleted_key"
     + (scd2 ? " AND t.is_current = 'Y'" : " AND NOT t.is_deleted")
   static String directSql(String table, String keyColumn, boolean scd2):
     "UPDATE " + table + " t SET is_deleted = TRUE, deleted_at = ?, job_run_id = ?, loaded_at = now()"
     + (scd2 ? ", is_current = 'N', valid_to = GREATEST(CAST(? AS TIMESTAMP), t.valid_from + INTERVAL '1 second')" : "")
     + " WHERE t." + keyColumn + " = ?" + (scd2 ? " AND t.is_current = 'Y'" : " AND NOT t.is_deleted")

2. DirectPostgresWriter(JdbcTemplate jdbc, ApplicationProperties properties, SoftDeleteColumns softDeleteColumns).
   Writes rows DIRECTLY (no staging) inside the CURRENT transaction (pipelines with directLoad() = true).
   public record KeyLookup(String sourceColumn, String dimensionTable, String dimensionBusinessColumn, String keyColumn)
   public long upsert(PipelineContext context, Dataset<Row> rows, String targetTable, List<String> keyColumns, List<KeyLookup> lookups):
     if !TransactionSynchronizationManager.isActualTransactionActive() -> IllegalStateException("DirectPostgresWriter needs an
       active transaction: the pipeline must return true from directLoad()");
     prepared = context.cache(softDeleteColumns.markNotDeleted(rows, targetTable).dropDuplicates(keyColumns.toArray(String[]::new)));
     dataColumns = prepared columns that are not a lookup sourceColumn; sql = buildUpsertSql(...);
     parameterIndexes = schema().fieldIndex(...) of dataColumns, then of every lookup sourceColumn;
     iterate prepared.toLocalIterator(), batches of batchSize -> jdbc.batchUpdate(sql, batch, size, (ps, row) -> {
       ps.setObject(i, row.get(index)) for every index; last param ps.setLong(runId) }); return rows written.
   static String buildUpsertSql(String targetTable, List<String> dataColumns, List<String> keyColumns, List<KeyLookup> lookups):
     columns = dataColumns + each lookup.keyColumn + job_run_id + loaded_at;
     values = "?" per data column + "(SELECT keyColumn FROM dimensionTable WHERE dimensionBusinessColumn = ?)" per lookup + "?" + "now()";
     "INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO %s" with "UPDATE SET c = EXCLUDED.c, ..." for non-key columns
     or "NOTHING".
````

### P19: LineageCollector (advanced)

> Aa sauthi agharo class che. Compile na thay to **Prompt F** vaparo. Chhelle upaay tarike `record(...)` ne khaali (no-op) rakhi shakay. Bakinu badhu chalse, fakt lineage tables khaali raheshe.

````text
Create @Component com.tcs.dmj.dwh.core.LineageCollector(ApplicationProperties properties). It finds the TABLE-LEVEL
lineage of a load automatically from the Spark logical plan: which MongoDB collections and PostgreSQL tables a Dataset
was computed from. Existing: MetaAsset (mongoCollection, postgresTable, key(), displayName()),
PipelineContext (addLineage, lineage(), pipelineRunId()).
Fields: mongoDatabase = properties.mongo().database(); postgresDatabase = databaseOfJdbcUrl(properties.postgres().url()).

public void record(PipelineContext context, Dataset<?> rows, String targetTable, Collection<String> extraSourceTables):
  try { target = MetaAsset.postgresTable(postgresDatabase, targetTable); sources = sourcesOf(rows);
        every extra table -> putIfAbsent(asset.key(), asset);
        for every source whose key != target.key(): context.addLineage(source, target); }
  catch (RuntimeException e) { log.warn(...) }   // lineage must NEVER fail a load
public MetaAsset mongoCollection(String collection) -> MetaAsset.mongoCollection(mongoDatabase, collection, null)
public MetaAsset postgresTable(String table) -> MetaAsset.postgresTable(postgresDatabase, table)

Map<String, MetaAsset> sourcesOf(Dataset<?> rows):
  plan = rows.queryExecution().analyzed(); assets = new HashMap<>();
  provenance = provenance(plan, assets, new IdentityHashMap<>());
  for every Attribute column in plan.output(): add every asset key of provenance.get(column.exprId().id()) (LinkedHashMap).
  Only tables whose columns REACH the output are sources (a table used only for a filter / ORPHAN join is not).

private Map<Long, Set<String>> provenance(LogicalPlan node, Map<String, MetaAsset> assets, Map<LogicalPlan, Map<Long, Set<String>>> memo):
  memoized. children = list(node.children()).
  LEAF: asset = assetOf(node); keys = asset == null ? Set.of() : Set.of(asset.key()); register the asset;
        every output column id -> keys.
  ELSE: compute provenance of each child; fromChildren = all child maps merged.
        if node instanceof org.apache.spark.sql.catalyst.plans.logical.Union: output column i = union of the keys of
           column i of every child.
        else: definitions = for every expression in node.expressions() that is a NamedExpression and NOT an Attribute:
              exprId().id() -> expression.
              for every output column: id in fromChildren -> pass through; else use definition.references() (or
              node.references() when there is no definition), .toSeq(), and union the keys of every referenced attribute.
private static <T> List<T> list(scala.collection.Seq<T> seq): scala.collection.JavaConverters.seqAsJavaList(seq)
private MetaAsset assetOf(LogicalPlan leaf):
  leaf instanceof org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation r and r.options().get("collection") != null
    -> MetaAsset.mongoCollection(r.options().getOrDefault("database", mongoDatabase), collection, null)
  leaf instanceof org.apache.spark.sql.execution.datasources.LogicalRelation r and r.relation() instanceof
    org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation jdbc: table = jdbc.jdbcOptions().tableOrQuery();
    when it has no "(" -> MetaAsset.postgresTable(databaseOfJdbcUrl(jdbc.jdbcOptions().url()), table)
  else null
static String databaseOfJdbcUrl(String url): "jdbc:postgresql://localhost:5433/silver?x=y" -> "silver" (null when no "/").
static List<String> describe(PipelineContext context): "source.displayName() -> target.displayName()" per edge.
````

✅ **Check:** `mvn -q compile`

---

## Phase 7: Core + service (navi chat → Prompt 0 → P20 … P23)

### P20: EtlPipeline, PipelineToolkit, PipelineRegistry, AbstractEtlPipeline

````text
Create 4 files in com.tcs.dmj.dwh.core. Existing: PipelineContext, LineageCollector, ColumnLineage, SourceDefinition,
SourceBatch, SourceProcessor (process, processDerived), PostgresWriter (upsert), DirectPostgresWriter (upsert, KeyLookup),
PostgresReader (readTable, addSurrogateKey), Scd2Writer (load, Scd2Spec), SoftDeleteWriter (apply), DateDimension
(fromColumn, TABLE, KEY_COLUMN), DataSanitizer, DataQualityRules, PipelineType, PipelineNotFoundException.

1. interface EtlPipeline (Javadoc: to add a pipeline: extend AbstractEtlPipeline, add @Component, implement sources() and run()):
   String name();  (unique, max 30 chars, letters digits - _ . ; same name in REST URL, --run-pipeline, meta table, Airflow DAG id)
   String description();
   List<SourceDefinition> sources();
   default List<String> sourceCollections() { return sources().stream().map(SourceDefinition::collection).toList(); }
   List<String> targetTables();
   default PipelineType pipelineType() { return PipelineType.RAW_TO_CURATED; }
   default List<ColumnLineage> columnLineage() { return List.of(); }
   void run(PipelineContext context);
   default boolean directLoad() { return false; }  (true = whole run is ONE transaction, no staging)

2. @Component PipelineToolkit: constructor with (SourceProcessor sourceProcessor, PostgresWriter postgresWriter,
   DirectPostgresWriter directWriter, PostgresReader postgresReader, Scd2Writer scd2Writer, SoftDeleteWriter softDeleteWriter,
   LineageCollector lineageCollector) and a getter per field with the same name (sourceProcessor(), postgresWriter(), ...).
   Purpose: a pipeline constructor has ONE parameter.

3. @Component PipelineRegistry(List<EtlPipeline> pipelines): TreeMap name -> pipeline; same name twice ->
   IllegalStateException("Two pipelines use the same name: " + name); log "Registered ETL pipeline '{}' ({})".
   EtlPipeline getPipeline(String name): missing -> throw new PipelineNotFoundException(name, map.keySet()).
   Collection<EtlPipeline> getAllPipelines().

4. public abstract class AbstractEtlPipeline implements EtlPipeline, field protected final PipelineToolkit toolkit,
   protected constructor (PipelineToolkit toolkit). Protected methods (a pipeline reads like a recipe):
   - SourceBatch extractAndValidate(PipelineContext c, SourceDefinition source, DataSanitizer sanitizer, DataQualityRules rules)
       -> toolkit.sourceProcessor().process(c, source, sanitizer, rules)
   - SourceBatch validateDerived(PipelineContext c, SourceDefinition definition, Dataset<Row> rows, DataQualityRules rules)
       -> toolkit.sourceProcessor().processDerived(...)
   - long loadScd2(PipelineContext c, Dataset<Row> rows, Scd2Writer.Scd2Spec spec):
       toolkit.lineageCollector().record(c, rows, spec.table(), List.of()); return loaded(c, spec.table(), toolkit.scd2Writer().load(c, rows, spec))
   - long load(PipelineContext c, Dataset<Row> rows, String targetTable, String... businessKeyColumns):
       record lineage; return loaded(c, targetTable, toolkit.postgresWriter().upsert(c, rows, targetTable, businessKeyColumns))
   - long loadDateDimension(PipelineContext c, Dataset<Row> rows, String dateColumn):
       load(c, DateDimension.fromColumn(rows, dateColumn), DateDimension.TABLE, DateDimension.KEY_COLUMN)
   - long loadDirect(PipelineContext c, Dataset<Row> rows, String targetTable, List<String> keyColumns, DirectPostgresWriter.KeyLookup... lookups):
       record lineage with extra tables = the lookups' dimensionTable; loaded(..., toolkit.directWriter().upsert(c, rows, targetTable, keyColumns, List.of(lookups)))
   - long loadDateDimensionDirect(PipelineContext c, Dataset<Row> rows, String... dateColumns): union of
       DateDimension.fromColumn per column, distinct(), loadDirect(c, dates, TABLE, List.of(KEY_COLUMN))
   - long applyDeletes(PipelineContext c, SourceBatch batch, String targetTable, String targetKeyColumn) -> softDelete(..., false)
   - long applyDeletesScd2(PipelineContext c, SourceBatch batch, Scd2Writer.Scd2Spec spec) -> softDelete(c, batch, spec.table(), spec.businessKey(), true)
   - private long softDelete(...): c.markDeletesApplied(batch.collection()); deletedCount == 0 -> 0;
       else toolkit.softDeleteWriter().apply(c, batch, table, keyColumn, scd2)
   - private static long loaded(PipelineContext c, String table, long count): c.metrics().addLoaded(count);
       log.info("[job {}] loaded {}: {} rows", ...); return count
   - Dataset<Row> addSurrogateKey(Dataset<Row> facts, String dimensionTable, String surrogateKeyColumn, String... businessKeyColumns)
       -> toolkit.postgresReader().addSurrogateKey(...)
   - Dataset<Row> readTable(String table, String... columns) -> toolkit.postgresReader().readTable(...)
````

### P21: service interfaces + PipelineRunLock

````text
Create these files.

Package com.tcs.dmj.dwh.service (interfaces, Javadoc names the impl class in service.impl):
1. DwhPipelineService: List<PipelineInfoDTO> listPipelines();  MetaPipelineRun runPipeline(String pipelineName);
   (runPipeline runs in the calling thread and returns the finished run; throws PipelineNotFoundException,
    PipelineAlreadyRunningException)
2. DwhPipelineRunService: List<MetaPipelineRun> listPipelineRuns(String pipelineName, String status, int limit);
   MetaPipelineRun getPipelineRun(long pipelineRunId);
3. RejectedRecordService: List<RejectedRecord> listRejectedRecords(String pipelineName, String collection, String errorType, int limit);
   List<RejectedRecordSummaryDTO> summary();
4. SourceCommitService: void commit(PipelineContext context);   (save rejects + advance last_run_time)
5. LineageService: void record(EtlPipeline pipeline, PipelineContext context);

Package com.tcs.dmj.dwh.service.impl:
6. @Component PipelineRunLock(ApplicationProperties properties). The SAME pipeline never runs twice at the same time,
   across processes too. PostgreSQL session advisory lock on a DEDICATED connection (DriverManager.getConnection(url,
   user, password), not the pool), held for the whole run. KEY_PREFIX = "dmj-dwh:pipeline:".
   public Held tryAcquire(String pipelineName): open connection; "SELECT pg_try_advisory_lock(hashtext(?))" with
   KEY_PREFIX + name; true -> return new Held(connection, name); false -> close connection, return null.
   SQLException -> close quietly, throw IllegalStateException("Could not take the run lock of " + name + ": " + msg, e).
   public static final class Held implements AutoCloseable (private constructor (Connection, String)):
   close() closes the connection (that releases the lock); log.warn on SQLException.
````

### P22: 4 service implementations

````text
Create 4 @Service classes in com.tcs.dmj.dwh.service.impl.

1. DwhPipelineRunServiceImpl implements DwhPipelineRunService (MetaPipelineRunRepository repo), MAX_LIMIT 500:
   listPipelineRuns -> repo.findRecent(name, status, Math.min(Math.max(limit, 1), 500));
   getPipelineRun -> repo.findById(id).orElseThrow(() -> new PipelineRunNotFoundException(id)).

2. RejectedRecordServiceImpl implements RejectedRecordService (RejectedRecordRepository repo), MAX_LIMIT 1000:
   listRejectedRecords -> repo.find(name, collection, errorType, clamp(limit, 1, 1000)); summary -> repo.summary().

3. SourceCommitServiceImpl implements SourceCommitService (PostgresWriter postgresWriter,
   RejectedRecordRepository rejectedRecordRepository, MetaPipelineRepository metaPipelineRepository).
   Runs ONLY after the pipeline loaded all its tables. commit(context):
   a) for every SourceBatch: deletedCount > 0 and !context.deletesApplied(collection) -> throw IllegalStateException(
      count + " document(s) of " + collection + " were soft-deleted at the source, but the pipeline did not apply them
      (AbstractEtlPipeline.applyDeletes). last_run_time was not moved.")
   b) for every SourceBatch: saveRejectedRecords(context, batch)
   c) metaPipelineRepository.advanceLastRunTime(context.pipelineName(), context.windowEnd())   // LAST
   saveRejectedRecords: rejectedCount == 0 -> return;
     directLoad -> rejectedRecordRepository.saveDirect(QualityColumns.oneRowPerError(batch.rejected()).toLocalIterator(),
                   pipelineName, collection, runId); return;
     else staging = postgresWriter.writeToStaging(context, QualityColumns.oneRowPerError(batch.rejected()), "rejected_" + collection);
          try saveFromStaging(staging, pipelineName, collection, runId) finally dropStagingTable(staging).

4. LineageServiceImpl implements LineageService (MetaPipelineRepository, MetaAssetRepository assetRepository,
   MetaLineageRepository lineageRepository, MetaColumnLineageRepository columnLineageRepository,
   LineageCollector lineageCollector, TransactionTemplate transaction). Called after a SUCCESSFUL run.
   record(pipeline, context): try transaction.executeWithoutResult(s -> write(pipeline, context))
                              catch RuntimeException -> log.warn only (lineage never fails a run).
   write: pipelineId = metaPipelineRepository.findByName(name).orElseThrow(...).pipelineId();
     descriptions = collection -> source description, from context.sourceBatches(); assetIds cache Map<String key, Long id>.
     table lineage: for every context.lineage() edge: lineageIds.add(lineageRepository.upsert(assetId(source), assetId(target), pipelineId));
       then lineageRepository.deactivateAllExcept(pipelineId, lineageIds) (only when there were edges); log.
     column lineage: for every pipeline.columnLineage(): sourceId = assetId(lineageCollector.mongoCollection(sourceCollection)),
       targetId = assetId(lineageCollector.postgresTable(targetTable)); columnIds.add(columnLineageRepository.upsert(pipelineId,
       sourceId, sourceField, targetId, targetColumn, mappingType)); then columnLineageRepository.deactivateAllExcept(pipelineId, columnIds).
   assetId(asset, descriptions, cache): cache.computeIfAbsent(asset.key(), k -> assetRepository.upsert(
     MONGODB asset without description ? asset.withDescription(descriptions.get(asset.assetName())) : asset)).
````

### P23: DwhPipelineServiceImpl (heart of the run)

````text
Create @Service com.tcs.dmj.dwh.service.impl.DwhPipelineServiceImpl implements DwhPipelineService.
Constructor (PipelineRegistry pipelineRegistry, MetaPipelineRepository metaPipelineRepository,
MetaPipelineRunRepository pipelineRunRepository, SourceCommitService commitService, LineageService lineageService,
PipelineRunLock runLock, SparkSession spark, TransactionTemplate transaction, ApplicationProperties properties);
applicationJobName = properties.spark().appName().
Javadoc steps: 0 register pipelines at start-up, 1 lock, 2 window end = now + run row RUNNING, 3 pipeline.run,
4 commit (rejects + last_run_time), 5 finish run row, 6 lineage, 7 always free Spark cache + release lock.

listPipelines(): registered = metaPipelineRepository.findAll() as Map by name; for every pipelineRegistry pipeline:
  new PipelineInfoDTO(meta == null ? 0 : meta.pipelineId(), name, pipelineType().name(), description(), sourceCollections(),
  sources() mapped with SourceInfoDTO::from, targetTables(), meta == null ? null : meta.lastRunTime(),
  pipelineRunRepository.isRunning(name)).

runPipeline(String pipelineName):
  pipeline = pipelineRegistry.getPipeline(pipelineName);
  try (PipelineRunLock.Held lock = runLock.tryAcquire(pipeline.name())) {
     if (lock == null) throw new PipelineAlreadyRunningException(pipeline.name());
     interrupted = pipelineRunRepository.failInterruptedRuns(name); > 0 -> log.warn
     Instant windowEnd = Instant.now().truncatedTo(ChronoUnit.MILLIS);   // millis: MongoDB dates have ms precision
     long runId = pipelineRunRepository.createRun(name, windowEnd);
     execute(pipeline, runId, windowEnd);
     return pipelineRunRepository.findById(runId).orElseThrow(() -> new PipelineRunNotFoundException(runId));
  }

private void execute(EtlPipeline pipeline, long runId, Instant windowEnd):
  context = new PipelineContext(runId, pipeline.name(), windowEnd, pipeline.directLoad()); start millis;
  log "========== Job {} STARTED: pipeline={}, windowEnd={} ==========";
  spark.sparkContext().setJobGroup("etl-job-" + runId, name + " until " + windowEnd, true);
  try {
    if (context.directLoad()) { transaction.executeWithoutResult(s -> { pipeline.run(context); commitService.commit(context); }); }
    else { pipeline.run(context); commitService.commit(context); }
    metrics = context.metrics(); successRecords = validCount + deletedCount;
    status = PipelineStatus.of(successRecords, rejectedCount); allRejected = status == FAILED;
    pipelineRunRepository.finishRun(runId, status, sourceCount, successRecords, rejectedCount,
        allRejected ? "ALL_RECORDS_REJECTED" : null,
        allRejected ? "All " + rejected + " records were rejected (see meta.meta_rejected_record)" : null);
    log "========== Job {} {} in {} ms: read={}, valid={}, rejected={}, deleted={}, loaded={} ==========";
    if (!allRejected) lineageService.record(pipeline, context);
  } catch (Throwable error) {          // Throwable: even an Error never leaves a run RUNNING
    log.error("========== Job {} FAILED: {} ==========", runId, error.getMessage(), error);
    if (context.directLoad()) context.metrics().resetLoaded();
    saveFailure(runId, context, error);
    if (error instanceof Error fatal) throw fatal;
  } finally { context.releaseResources(); spark.sparkContext().clearJobGroup(); }

private void saveFailure(runId, context, error): try finishRun(runId, FAILED, sourceCount, valid + deleted, rejected,
  ErrorMessages.errorCode(error), ErrorMessages.shortMessage(error)) catch RuntimeException -> log.error only.

@EventListener(ApplicationStartedEvent.class) public void onStartup():
  try {
    every pipeline: metaPipelineRepository.register(name, pipelineType().name(), description(), applicationJobName, name);
    deactivated = metaPipelineRepository.deactivateAllExcept(all names); > 0 -> log.warn
    for every name in pipelineRunRepository.pipelinesWithRunningRuns():
      try (Held lock = runLock.tryAcquire(name)) { if (lock != null) closed += pipelineRunRepository.failInterruptedRuns(name); }
  } catch (BadSqlGrammarException e) {
    throw new IllegalStateException("Meta tables not found in PostgreSQL (" + e.getMostSpecificCause().getMessage()
      + "). Run the SQL scripts first, then start the application again.", e);
  }
  closed > 0 -> log.warn
````

✅ **Check:** `mvn -q compile`. Have koi error na hovi joie.

---

## Phase 8: REST + batch (navi chat → Prompt 0 → P24, P25)

### P24: REST controllers + errors

````text
Create these files.

Package com.tcs.dmj.dwh.web.rest (@RestController, constructor injection):
1. DwhPipelineResource, @RequestMapping("/api/admin/dwh/pipelines"), uses DwhPipelineService:
   @GetMapping List<PipelineInfoDTO> listPipelines()
   @PostMapping("/{pipelineName}/run") MetaPipelineRun runPipeline(@PathVariable String pipelineName)
   (synchronous; a FAILED job is still HTTP 200, the run row tells why)
2. DwhPipelineRunResource, @RequestMapping("/api/admin/dwh/pipeline-runs"), uses DwhPipelineRunService:
   @GetMapping listPipelineRuns(@RequestParam(required = false) String pipelineName,
       @RequestParam(required = false) String status, @RequestParam(defaultValue = "20") int limit)
   @GetMapping("/{pipelineRunId}") getPipelineRun(@PathVariable long pipelineRunId)
3. DwhRejectedRecordResource, @RequestMapping("/api/admin/dwh/rejected-records"), uses RejectedRecordService:
   @GetMapping listRejectedRecords(optional pipelineName, collection, errorType, @RequestParam(defaultValue = "100") int limit)
   @GetMapping("/summary") summary()

Package com.tcs.dmj.dwh.web.rest.errors:
4. record ApiError(java.time.LocalDateTime timestamp, int status, String error, String message, String path)
5. @RestControllerAdvice ExceptionTranslator, private build(HttpStatus, String message, HttpServletRequest) ->
   ResponseEntity with new ApiError(LocalDateTime.now(), status.value(), status.getReasonPhrase(), message, request.getRequestURI()).
   Handlers (jakarta.servlet.http.HttpServletRequest):
   - PipelineNotFoundException, PipelineRunNotFoundException -> 404
   - org.springframework.web.servlet.resource.NoResourceFoundException -> 404 "No endpoint " + method + " " + uri
   - PipelineAlreadyRunningException -> 409
   - IllegalArgumentException, MethodArgumentTypeMismatchException -> 400
   - EtlException -> 503 (log.error)
   - Exception -> 500 "Unexpected error: " + message (log.error)
````

### P25: PipelineBatchRunner + DmjDwhApp

````text
Create 2 files.

1. @Component com.tcs.dmj.dwh.batch.PipelineBatchRunner implements ApplicationRunner, ExitCodeGenerator.
   BATCH MODE for Airflow: java -jar dmj-dwh-1.0.0.jar --run-pipeline=sales-pipeline runs ONE pipeline and exits.
   public static final String RUN_PIPELINE_OPTION = "run-pipeline";
   Constructor (DwhPipelineService pipelineService, ApplicationProperties properties):
   failOnRejectedRecords = properties.batch().failOnRejectedRecords(); private int exitCode = 0;
   public static boolean isBatchMode(String[] args): any arg starts with "--run-pipeline="
   run(ApplicationArguments args): values = args.getOptionValues(RUN_PIPELINE_OPTION); null -> return (REST mode);
     not exactly one non-blank value -> IllegalArgumentException("Give exactly one pipeline: --run-pipeline=<name>");
     log "Batch mode: running {}" + Airflow info from env AIRFLOW_CTX_DAG_ID / AIRFLOW_CTX_DAG_RUN_ID when present;
     run = pipelineService.runPipeline(name); status = PipelineStatus.valueOf(run.pipelineStatus());
     ok = status == SUCCESS || (status == SUCCESS_WITH_FAILED && !failOnRejectedRecords); exitCode = ok ? 0 : 1;
     log.info("DWH_RESULT pipeline={} pipelineRunId={} status={} total={} success={} failed={} errorCode={} exitCode={}", ...)
   getExitCode() returns exitCode.

2. com.tcs.dmj.dwh.DmjDwhApp with @SpringBootApplication and @ConfigurationPropertiesScan.
   main(String[] args):
     SpringApplication application = new SpringApplication(DmjDwhApp.class);
     boolean batch = PipelineBatchRunner.isBatchMode(args);
     if (!batch) { application.run(args); return; }
     application.setWebApplicationType(WebApplicationType.NONE);
     int exitCode;
     try { ConfigurableApplicationContext ctx = application.run(args); exitCode = SpringApplication.exit(ctx); }
     catch (Throwable startupOrRunError) { exitCode = 1; }
     System.exit(exitCode);   // a leftover non-daemon thread must never keep an Airflow task hanging
````

✅ **Check:** `mvn -q compile`. Haju run na karo: ek pan pipeline vagar Spring start nahi thay (`List<EtlPipeline>` khaali).

---

## Phase 9: Pehli pipeline: sales-pipeline (navi chat → Prompt 0 → P26 … P29)

### Sales tables SQL (sidhu copy, `sample-data/sales-pipeline/03_postgres_tables.sql`)

```sql
CREATE SCHEMA IF NOT EXISTS curated;

CREATE TABLE IF NOT EXISTS curated.dim_product (
    product_key           BIGSERIAL     PRIMARY KEY,
    product_code          VARCHAR(10)   NOT NULL UNIQUE,
    product_name          VARCHAR(150)  NOT NULL,
    brand_name            VARCHAR(100),
    category_name         VARCHAR(100)  NOT NULL,
    parent_category_name  VARCHAR(100),
    current_price         NUMERIC(18,2) NOT NULL CHECK (current_price > 0),
    is_active             BOOLEAN,
    job_run_id            BIGINT,
    loaded_at             TIMESTAMP
);

CREATE TABLE IF NOT EXISTS curated.dim_customer (
    customer_key   BIGSERIAL    PRIMARY KEY,
    customer_code  VARCHAR(10)  NOT NULL,
    customer_name  VARCHAR(100) NOT NULL,
    email          VARCHAR(150),
    phone          VARCHAR(10),
    city           VARCHAR(60),
    state          VARCHAR(60),
    pincode        VARCHAR(6),
    tier           VARCHAR(10)  NOT NULL CHECK (tier IN ('Bronze', 'Silver', 'Gold', 'Platinum')),
    signup_date    DATE,
    valid_from     TIMESTAMP    NOT NULL,
    valid_to       TIMESTAMP    NOT NULL DEFAULT TIMESTAMP '9999-12-31 00:00:00',
    is_current     CHAR(1)      NOT NULL DEFAULT 'Y' CHECK (is_current IN ('Y', 'N')),
    job_run_id     BIGINT,
    loaded_at      TIMESTAMP,
    CONSTRAINT uq_dim_customer_version UNIQUE (customer_code, valid_from),
    CHECK (valid_to > valid_from)
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_customer_current ON curated.dim_customer (customer_code) WHERE is_current = 'Y';

CREATE TABLE IF NOT EXISTS curated.dim_order_status (
    order_status_key  BIGSERIAL   PRIMARY KEY,
    status_code       VARCHAR(15) NOT NULL UNIQUE,
    description       VARCHAR(50),
    is_final          CHAR(1)     NOT NULL CHECK (is_final IN ('Y', 'N')),
    job_run_id        BIGINT,
    loaded_at         TIMESTAMP
);
INSERT INTO curated.dim_order_status (status_code, description, is_final) VALUES
    ('PLACED', 'Order placed', 'N'), ('SHIPPED', 'Handed to courier', 'N'), ('DELIVERED', 'Delivered', 'Y'),
    ('CANCELLED', 'Cancelled', 'Y'), ('RETURNED', 'Returned', 'Y')
ON CONFLICT (status_code) DO NOTHING;

CREATE TABLE IF NOT EXISTS curated.dim_payment_method (
    payment_method_key  BIGSERIAL   PRIMARY KEY,
    method_code         VARCHAR(15) NOT NULL UNIQUE,
    method_name         VARCHAR(50),
    is_online           CHAR(1)     NOT NULL CHECK (is_online IN ('Y', 'N')),
    job_run_id          BIGINT,
    loaded_at           TIMESTAMP
);
INSERT INTO curated.dim_payment_method (method_code, method_name, is_online) VALUES
    ('UPI', 'Unified Payments Interface', 'Y'), ('CARD', 'Credit / Debit Card', 'Y'),
    ('NETBANKING', 'Net Banking', 'Y'), ('COD', 'Cash on Delivery', 'N')
ON CONFLICT (method_code) DO NOTHING;

CREATE TABLE IF NOT EXISTS curated.fact_sales (
    sales_key           BIGSERIAL     PRIMARY KEY,
    order_no            VARCHAR(10)   NOT NULL,
    line_no             INT           NOT NULL,
    order_date_key      INT           NOT NULL REFERENCES silver.dim_date (date_key),
    customer_key        BIGINT        NOT NULL REFERENCES curated.dim_customer (customer_key),
    product_key         BIGINT        NOT NULL REFERENCES curated.dim_product (product_key),
    payment_method_key  BIGINT        NOT NULL REFERENCES curated.dim_payment_method (payment_method_key),
    order_status_key    BIGINT        NOT NULL REFERENCES curated.dim_order_status (order_status_key),
    quantity            INT           NOT NULL CHECK (quantity > 0),
    unit_price          NUMERIC(18,2) NOT NULL CHECK (unit_price > 0),
    line_amount         NUMERIC(18,2) NOT NULL,
    job_run_id          BIGINT,
    loaded_at           TIMESTAMP,
    CONSTRAINT uq_fact_sales_line UNIQUE (order_no, line_no)
);

CREATE TABLE IF NOT EXISTS curated.fact_payment (
    payment_key         BIGSERIAL     PRIMARY KEY,
    payment_id          VARCHAR(10)   NOT NULL UNIQUE,
    order_no            VARCHAR(10)   NOT NULL,
    payment_date_key    INT           NOT NULL REFERENCES silver.dim_date (date_key),
    customer_key        BIGINT        NOT NULL REFERENCES curated.dim_customer (customer_key),
    payment_method_key  BIGINT        NOT NULL REFERENCES curated.dim_payment_method (payment_method_key),
    payment_status      VARCHAR(10)   NOT NULL CHECK (payment_status IN ('SUCCESS', 'FAILED', 'REFUNDED', 'PENDING')),
    amount              NUMERIC(18,2) NOT NULL CHECK (amount > 0),
    job_run_id          BIGINT,
    loaded_at           TIMESTAMP
);

-- soft delete columns
ALTER TABLE curated.dim_product  ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN NOT NULL DEFAULT FALSE, ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP;
ALTER TABLE curated.dim_customer ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN NOT NULL DEFAULT FALSE, ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP;
ALTER TABLE curated.fact_sales   ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN NOT NULL DEFAULT FALSE, ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP;
ALTER TABLE curated.fact_payment ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN NOT NULL DEFAULT FALSE, ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP;
```

### P26: SalesSources

````text
Create com.tcs.dmj.dwh.pipeline.sales.SalesSources: a package-private final class, private constructor.
It says WHAT we read from MongoDB and HOW we clean + validate it. Uses SourceDefinition.builder(...), DataSanitizer,
DataQualityRules, ErrorType, and SalesStarSchemaMapper.parseItems(Column) (created in the next step, same package).

Constants:
 static final String[] ORDER_STATUSES = {"PLACED", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED"};
 static final String[] PAYMENT_METHODS = {"UPI", "CARD", "NETBANKING", "COD"};
 static final String[] PAYMENT_STATUSES = {"SUCCESS", "FAILED", "REFUNDED", "PENDING"};
 static final String[] TIERS = {"Bronze", "Silver", "Gold", "Platinum"};
 private static final Map<String, String> STATE_CODES = GJ Gujarat, MH Maharashtra, RJ Rajasthan, DL Delhi, KA Karnataka,
   TN Tamil Nadu, TS Telangana, UP Uttar Pradesh, WB West Bengal, MP Madhya Pradesh, KL Kerala  (Map.ofEntries)
 private static final Map<String, String> PAYMENT_METHOD_ALIASES = "NET BANKING"->"NETBANKING", "INTERNET BANKING"->"NETBANKING",
   "CASH ON DELIVERY"->"COD", "CREDIT CARD"->"CARD", "DEBIT CARD"->"CARD"
 private static Column amountText(Column v): regexp_replace(v, "[₹,\\s]", "")     // "₹29,000" -> "29000"

Every source below: .businessKey(...).latestRecordColumn("updated_at").softDelete().maxRejectPercent(30) + metadata.

PRODUCTS = builder("products").fields("product_code","name","brand","category","price","is_active","updated_at")
  key product_code; description "Product catalogue (product master) of the shop", owner "catalog-team@demo.com", sla "on change"
  productSanitizer(): fromJson("brand_name","brand","$.name"), fromJson("category_name","category","$.name"),
    fromJson("parent_category_name","category","$.parent.name"), trimAllAndBlankToNull(), toUpperCase("product_code"),
    toLowerCase("is_active"), custom("price", SalesSources::amountText)
  productRules(): mustBeDecimal("price"), required("product_code","name","category_name","price"),
    mustMatch("product_code","^P[0-9]{3}$","must look like P001"), mustBePositive("price")

CUSTOMERS = fields("customer_code","name","email","phone","address","tier","signup_date","updated_at"), key customer_code,
  description "Registered customers (sign-ups and profile changes)", owner "customer-accounts-team@demo.com", sla "every hour"
  customerSanitizer(): fromJson city/state/pincode from "address" ("$.city", "$.state", "$.pincode"), trimAllAndBlankToNull(),
    toUpperCase("customer_code","state"), toLowerCase("email"), normalizeIndianPhone("phone"), toProperCase("city"),
    mapValues("state", STATE_CODES), toProperCase("state","tier"), defaultIfNull("tier","Bronze")
  customerRules(): mustBeDate("signup_date"), required("customer_code","name","email"),
    mustMatch("customer_code","^C[0-9]{3}$","must look like C001"), mustBeValidEmail("email"),
    mustMatch("phone","^[0-9]{10}$","must have 10 digits"), mustBeOneOf("tier", TIERS)

ORDERS = fields("order_no","customer_code","order_ts","status","payment_method","items","updated_at"), key order_no,
  description "Orders with their product lines (items[] array)", owner "checkout-team@demo.com", sla "every 15 minutes"
  orderSanitizer(): trimAllAndBlankToNull(), toUpperCase("order_no","customer_code","status","payment_method"),
    custom("payment_method", m -> regexp_replace(m, "\\s+", " ")), mapValues("payment_method", PAYMENT_METHOD_ALIASES)
  orderRules(Dataset<Row> customerCodes): mustBeTimestamp("order_ts"),
    required("order_no","customer_code","order_ts","status","payment_method"),
    custom(ErrorType.VALIDATION, "items must contain at least one line", size(SalesStarSchemaMapper.parseItems(col("items"))).leq(0)),
    mustBeOneOf("status", ORDER_STATUSES), mustBeOneOf("payment_method", PAYMENT_METHODS),
    mustExistIn("customer_code", customerCodes, "customer_code", "dim_customer")

ORDER_LINES = builder("orders.items").fields("line_id","order_no","line_no","product_code","qty","unit_price","updated_at")
  .businessKey("line_id").latestRecordColumn("updated_at") (NO softDelete, NO maxRejectPercent),
  description "Product lines of an order (items[] array, one row per line)", owner "checkout-team@demo.com"
  orderLineRules(Dataset<Row> productCodes): mustBeInteger("qty"), mustBeDecimal("unit_price"),
    required("product_code","qty","unit_price"), mustBePositive("qty"), mustBePositive("unit_price"),
    mustExistIn("product_code", productCodes, "product_code", "dim_product")

PAYMENTS = fields("payment_id","order_no","method","amount","status","paid_at","updated_at"), key payment_id,
  description "Payments from the payment gateway (UPI, card, net banking, COD)", owner "payments-team@demo.com", sla "every 15 minutes"
  paymentSanitizer(): trimAllAndBlankToNull(), toUpperCase("payment_id","order_no","status","method"),
    custom("method", m -> regexp_replace(m, "\\s+", " ")), mapValues("method", PAYMENT_METHOD_ALIASES),
    custom("amount", SalesSources::amountText)
  paymentRules(Dataset<Row> loadedOrders): mustBeDecimal("amount"), mustBeTimestamp("paid_at"),
    required("payment_id","order_no","amount","status","method"), mustBePositive("amount"),
    mustBeOneOf("status", PAYMENT_STATUSES), mustBeOneOf("method", PAYMENT_METHODS),
    mustExistIn("order_no", loadedOrders, "order_no", "fact_sales")

All sanitizer / rules methods are static and package-private.
````

### P27: SalesStarSchemaMapper

````text
Create com.tcs.dmj.dwh.pipeline.sales.SalesStarSchemaMapper: package-private final class, private constructor,
static package-private methods. TRANSFORM step: MongoDB documents (Strings) -> star-schema rows (real types).
Uses SparkTypes, DateDimension.dateKey(Column), QualityColumns.SOURCE_ID / RAW_RECORD.

private static final ArrayType ITEMS_TYPE = DataTypes.createArrayType(new StructType()
    .add("product_code", StringType).add("qty", StringType).add("unit_price", StringType));
private static Column blankToNull(Column v): nullif(trim(v), lit(""))

- static Column parseItems(Column itemsJson): from_json(itemsJson, ITEMS_TYPE)
- toDimProduct(validProducts): select product_code, name as product_name, brand_name, category_name, parent_category_name,
    price cast MONEY as current_price, is_active cast BooleanType as is_active
- toDimCustomer(validCustomers): customer_code, name as customer_name, email, phone, city, state, pincode, tier,
    signup_date cast DATE as signup_date, updated_at cast TIMESTAMP as changed_at,
    signup_date cast DATE then cast TIMESTAMP as first_valid_from
- toOrderLines(validOrders): ONE row per element of items[]:
    select col("_id").as("_doc_id"), order_no, customer_code, order_ts, status, payment_method, updated_at,
      posexplode(parseItems(col("items"))).as(new String[]{"_pos", "_item"})
    then line_no = _pos + 1; product_code = upper(blankToNull(_item.product_code)); qty = blankToNull(_item.qty);
    unit_price = regexp_replace(blankToNull(_item.unit_price), "[₹,\\s]", "");
    line_id = concat(order_no, lit("/line-"), line_no);  _id = concat(_doc_id, lit("#"), line_no);
    _raw_record = to_json(struct(order_no, line_no, _item.product_code as product_code, _item.qty as qty,
                  _item.unit_price as unit_price)); drop _doc_id, _pos, _item.
- toTypedOrderLines(validLines): order_no, line_no cast INT, customer_code, product_code, status as status_code,
    payment_method as method_code, order_ts cast TIMESTAMP, qty cast INT as quantity, unit_price cast MONEY.
- withCustomerKeyAt(Dataset<Row> rows, Dataset<Row> customerVersions, String eventTsColumn): SCD2 POINT-IN-TIME lookup.
    versions = customerVersions select customer_code as _v_code, customer_key as _v_key, valid_from as _v_from, valid_to as _v_to;
    current = customerVersions filter is_current = 'Y' select customer_code as _c_code, customer_key as _c_key;
    rows left join versions on customer_code = _v_code AND event >= _v_from AND event < _v_to,
    left join current on customer_code = _c_code; customer_key = coalesce(_v_key, _c_key); drop the 6 helper columns.
- toFactSales(linesWithKeys): order_no, line_no, DateDimension.dateKey(col("order_ts")).as("order_date_key"), customer_key,
    product_key, payment_method_key, order_status_key, quantity, unit_price,
    quantity.multiply(unit_price).cast(MONEY).as("line_amount")
- toTypedPayments(validPayments): payment_id, order_no, method as method_code, status as payment_status,
    amount cast MONEY, coalesce(paid_at, updated_at) cast TIMESTAMP as paid_at
- withOrderCustomerKey(payments, orderCustomers): orders = orderCustomers.dropDuplicates("order_no")
    .select(order_no as _o_no, customer_key); payments INNER join on order_no = _o_no; drop _o_no.
- toFactPayment(paymentsWithKeys): payment_id, order_no, dateKey(paid_at) as payment_date_key, customer_key,
    payment_method_key, payment_status, amount
````

### P28: SalesPipeline

````text
Create @Component com.tcs.dmj.dwh.pipeline.sales.SalesPipeline extends AbstractEtlPipeline (com.tcs.dmj.dwh.core).
Constructor (PipelineToolkit toolkit) -> super(toolkit). Static-import every method of SalesStarSchemaMapper (same package).
Class Javadoc: products -> dim_product (SCD1); customers -> dim_customer (SCD2: city/state/pincode/tier);
orders -> items[] explode -> fact_sales (one row per order line); payments -> fact_payment. Order matters.

Constants: DIM_PRODUCT = "curated.dim_product", DIM_CUSTOMER = "curated.dim_customer",
DIM_ORDER_STATUS = "curated.dim_order_status", DIM_PAYMENT_METHOD = "curated.dim_payment_method",
FACT_SALES = "curated.fact_sales", FACT_PAYMENT = "curated.fact_payment".
private static final Scd2Writer.Scd2Spec CUSTOMER_SCD2 = new Scd2Writer.Scd2Spec(DIM_CUSTOMER, "customer_code",
    List.of("city", "state", "pincode", "tier"), "changed_at", "first_valid_from");

name() "sales-pipeline"; description() "Sales star (J2_SALES): products, customers (SCD2), orders, payments -> fact_sales, fact_payment";
sources() List.of(SalesSources.PRODUCTS, CUSTOMERS, ORDERS, PAYMENTS);
targetTables() List.of(DIM_PRODUCT, DIM_CUSTOMER, DIM_ORDER_STATUS, DIM_PAYMENT_METHOD, "silver.dim_date", FACT_SALES, FACT_PAYMENT)

run(PipelineContext context):
 PART 0: products = extractAndValidate(context, PRODUCTS, productSanitizer(), productRules());
         load(context, toDimProduct(products.valid()), DIM_PRODUCT, "product_code");
 PART A: customers = extractAndValidate(context, CUSTOMERS, customerSanitizer(), customerRules());
         loadScd2(context, toDimCustomer(customers.valid()), CUSTOMER_SCD2);
 PART B: orders = extractAndValidate(context, ORDERS, orderSanitizer(), orderRules(readTable(DIM_CUSTOMER, "customer_code")));
         lines = validateDerived(context, ORDER_LINES, toOrderLines(orders.valid()), orderLineRules(readTable(DIM_PRODUCT, "product_code")));
         typedLines = toTypedOrderLines(lines.valid()); loadDateDimension(context, typedLines, "order_ts");
         linesWithKeys = withCustomerKeyAt(typedLines, readTable(DIM_CUSTOMER, "customer_key", "customer_code", "valid_from",
                         "valid_to", "is_current"), "order_ts");
         linesWithKeys = addSurrogateKey(linesWithKeys, DIM_PRODUCT, "product_key", "product_code");
         linesWithKeys = addSurrogateKey(linesWithKeys, DIM_ORDER_STATUS, "order_status_key", "status_code");
         linesWithKeys = addSurrogateKey(linesWithKeys, DIM_PAYMENT_METHOD, "payment_method_key", "method_code");
         load(context, toFactSales(linesWithKeys), FACT_SALES, "order_no", "line_no");
 PART C: payments = extractAndValidate(context, PAYMENTS, paymentSanitizer(), paymentRules(readTable(FACT_SALES, "order_no")));
         typedPayments = toTypedPayments(payments.valid()); loadDateDimension(context, typedPayments, "paid_at");
         paymentsWithKeys = withOrderCustomerKey(typedPayments, readTable(FACT_SALES, "order_no", "customer_key"));
         paymentsWithKeys = addSurrogateKey(paymentsWithKeys, DIM_PAYMENT_METHOD, "payment_method_key", "method_code");
         load(context, toFactPayment(paymentsWithKeys), FACT_PAYMENT, "payment_id");
 PART D (soft deletes, rows are marked, never removed):
         applyDeletes(context, products, DIM_PRODUCT, "product_code");
         applyDeletesScd2(context, customers, CUSTOMER_SCD2);
         applyDeletes(context, orders, FACT_SALES, "order_no");
         applyDeletes(context, payments, FACT_PAYMENT, "payment_id");
````

### P29: MongoDB test data

````text
Write a mongosh script sample-data/sales-pipeline/01_mongo_full_load.js for database "bronze_db".
Helper: const ist = (s) => new Date(s.replace(" ", "T") + ":00+05:30");  // "2026-10-01 09:00" IST -> BSON Date
Drop and fill 4 collections. updated_at is ALWAYS a BSON Date (ist(...)). Create indexes {<business key>: 1} and {updated_at: 1}
on every collection (business key index NOT unique: the source really has duplicates).

products (24 docs): { product_code, name, brand: {code, name}, category: {code, name, parent: {code, name}}, price, is_active: true, updated_at }
  P001..P020 valid (Indian shop: mobiles, audio, laptops, clothing, footwear, kitchen, fitness, cricket; prices 350..55000).
  P012 price is the string "₹1,200"; P013 name "  non-stick tawa " (spaces).
  P003 twice: older copy price 1900 updated 2026-10-05, newer copy price 2000 updated 2026-10-20.
  P021 category null; P022 price "abc"; P023 price -150.
customers (23 docs): { customer_code, name, email, phone, address: {city, state, pincode}, tier, signup_date "yyyy-MM-dd", updated_at }
  C001..C020 valid; some messy values: an email in upper case, phone "+91-98765 00004", city "lucknow ", state "RJ", tier missing.
  C005 twice (duplicate). C021 email "test@@mail". C022 name null.
orders (24 docs): { order_no, customer_code, order_ts (Date), status, payment_method, items: [{product_code, qty, unit_price}], updated_at }
  O1001..O1020 valid (one order per customer C001..C020, 1-2 lines each). Messy: status "delivered", payment "upi ",
  "Net Banking", "Cash on Delivery". O1008 line 2 has qty 0. O1012 line 2 has product P022 (it is rejected, so not in dim_product).
  O1005 twice (older copy status PLACED). O1021 customer C099 (does not exist). O1022 order_ts is the string "2026-13-45".
  O1023 items [].
payments (23 docs): { payment_id, order_no, method, amount, status, paid_at (Date), updated_at }
  PAY01..PAY20 one per order O1001..O1020. PAY01 amount "₹29,000"; PAY07 status "success"; PAY12 method "Net Banking".
  PAY05 twice (callback twice). PAY21 for O1021 (that order is rejected -> ORPHAN). PAY22 amount "abc".
Expected first run: products valid 20 / rejected 4, customers 20 / 3, orders 20 / 4, order lines 30 / 2, payments 20 / 3.
````

### ▶️ Pehlo run

```bash
psql -h localhost -p 5433 -U postgres -d silver -f sample-data/00_meta_tables.sql
psql -h localhost -p 5433 -U postgres -d silver -f sample-data/sales-pipeline/03_postgres_tables.sql
mongosh "mongodb://localhost:27017" sample-data/sales-pipeline/01_mongo_full_load.js
mvn -q clean package -DskipTests
java -jar target/dmj-dwh-1.0.0.jar --run-pipeline=sales-pipeline
```

**Aavu dekhavu joie:**

| Check | Expected |
|---|---|
| Log ma chhelli line | `DWH_RESULT pipeline=sales-pipeline ... status=SUCCESS_WITH_FAILED ... exitCode=0` |
| `SELECT count(*) FROM curated.dim_product` | 20 |
| `SELECT count(*) FROM curated.dim_customer` | 20 |
| `SELECT count(*) FROM curated.fact_sales` | 30 |
| `SELECT count(*) FROM curated.fact_payment` | 20 |
| `SELECT count(*) FROM meta.meta_rejected_record` | 16 |
| `SELECT last_run_time FROM meta.meta_pipeline WHERE pipeline_name='sales-pipeline'` | run no start time (NULL nahi) |
| Biji vaar run karo | read 0, status SUCCESS, koi duplicate nahi |

REST thi pan chalavi shakay: `java -jar target/dmj-dwh-1.0.0.jar` pachhi `curl -X POST http://localhost:8080/api/admin/dwh/pipelines/sales-pipeline/run`.

---

## Phase 10: Optional

### P30: unit test

````text
Create a plain JUnit 5 test (no Spring, no database) src/test/java/com/tcs/dmj/dwh/validation/DataQualityEngineTest.java.
@BeforeAll: SparkSession.builder().master("local[1]").appName("dq-test").config("spark.ui.enabled","false")
.config("spark.sql.ansi.enabled","false").config("spark.sql.shuffle.partitions","1").getOrCreate(); @AfterAll spark.stop().
Test: 6 rows with columns _id, _raw_record, orderId, customerId, amount, currency, updatedAt (all String):
 1 valid after cleaning (" 1,250.50 ", "inr"), 2 amount "12abc" (CORRUPT), 3 amount "-5" + currency "XYZ" (2 VALIDATION errors),
 4 customer "C9" not in lookup (ORPHAN), 5 + 6 same orderId ORD-5, row 6 newer -> row 5 DUPLICATE.
Lookup customers dataset with one row customer_id "C1".
Sanitizer: trimAllAndBlankToNull, toUpperCase("currency"), removeThousandSeparators("amount").
Rules: mustBeDecimal("amount"), required("orderId","amount"), mustBePositive("amount"), mustBeOneOf("currency","INR","USD"),
mustExistIn("customerId", customers, "customer_id", "customers").
Run new DataQualityEngine().check(cleaned, rules, "orderId", "updatedAt") and assert: valid = 2 rows (_id 1 and 6),
rejected = 4 rows; main error type per _id: 2 CORRUPT, 3 VALIDATION (2 errors), 4 ORPHAN, 5 DUPLICATE.
````

### P31: Airflow DAG

````text
Write airflow/dags/dmj_dwh_dags.py for Airflow 2.x. One DAG per pipeline, dag_id = pipeline name.
A list PIPELINES = [{"name": "sales-pipeline", "schedule": "0 * * * *", "depends_on": []}] (easy to extend).
Each DAG has one BashOperator task "run" with:
  java -jar {{ var.value.dmj_dwh_jar }} --spring.profiles.active=prod --run-pipeline=<name>
retries 2, retry_delay 5 minutes, catchup False, max_active_runs 1. Exit code 0 = success, 1 = failed.
For every name in depends_on add an ExternalTaskSensor (external_dag_id = that name, external_task_id = "run",
mode "reschedule") before the run task.
````

---

## Templates (game tyare vaparo)

### Prompt N: navi pipeline (sales jevi j)

> Sales pipeline bani jaay pachhi navi pipeline mate aa template vaparo. `#file:` thi `SalesPipeline.java`, `SalesSources.java`, `SalesStarSchemaMapper.java` attach karo (Copilot ne pattern dekhay).

````text
Using EXACTLY the same pattern as SalesPipeline / SalesSources / SalesStarSchemaMapper (attached), create a new pipeline:

package: com.tcs.dmj.dwh.pipeline.<folder>
pipeline name: <name-pipeline>    (max 30 chars, letters digits - _ .)
description: <one line, max 100 chars>

SOURCE collections (all fields are read as String):
- <collection 1>: fields <f1, f2, ...>, business key <key>, timestamp <updated_at>, soft delete yes/no, max reject 30%
  cleaning: <trim, upper case X, phone, JSON field a.b -> column c ...>
  rules: <required ..., decimal ..., one of ..., must exist in <table>.<column> ...>
- <collection 2>: ...

TARGET tables (PostgreSQL):
- <schema.dim_x> (SCD1 or SCD2) business key <col>, columns <...>       SCD2 columns: <...>
- <schema.fact_y> grain <one row per ...>, business key <cols>, keys: <dim lookups>

Steps of run(): <1. load dim_x, 2. load fact_y with surrogate keys from dim_x ..., 3. applyDeletes ...>

Give me 4 files, one at a time when I say "next":
1. sample-data/<name>/03_postgres_tables.sql (tables with job_run_id, loaded_at, is_deleted, deleted_at)
2. <Name>Sources.java   3. <Name>StarSchemaMapper.java   4. <Name>Pipeline.java
````

### Prompt F: compile / runtime error sudharva

````text
This file does not compile / fails at runtime. Fix ONLY what is needed, keep every name, signature and SQL the same.
Return the COMPLETE corrected file (no "...").

Error:
<paste the error lines from mvn or the log, max 40 lines>

File:
<paste the file, or attach it with #file:path>
````

### Prompt C: jawab kapaai jaay tyare

````text
continue. Start exactly where you stopped, inside the same file. Do not repeat what you already wrote.
````

> "continue" pachhi pan bagde to: `Give me again ONLY the file <path>, complete.`

---

## Basic model sathe vaparvani tips

1. **Darek prompt ma "COMPLETE file" lakhelu che.** Topan model `...` mukhe to e file reject karo ane Prompt C / F vaparo.
2. **Model naam badli naakhe** (dakhla tarike `extractAndValidate` ne `extract`): Prompt F ma lakho *"use the exact name extractAndValidate"*.
3. **Model navi class banave** (`Utils`, `Helper` jevi) to kaadhi naakho. Aa project ma fakt upar lakheli classes j che.
4. **Imports ni bhool sauthi vadhare thay che.** VS Code ma `Alt+Shift+O` (Organize imports) ane Quick Fix (`Ctrl+.`) ghanu sudhari aape.
5. **Agharo code** (LineageCollector, RejectedRecordRepository, Scd2Writer): compile thay pachhi **pehlo run** karine j check karo. Error log Prompt F ma aapo.
6. **Spark ane Java 17 errors** (`IllegalAccessError`, `cannot access class sun.nio.ch`): `pom.xml` na add-opens barabar copy thaya che e check karo. Code badalvano nathi.
7. **`Janino` / `CompileException` error:** `pom.xml` ma `<janino.version>3.1.9</janino.version>` che e check karo.
