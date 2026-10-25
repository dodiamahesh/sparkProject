# Tools અને Software Inventory – RFC અને Production Planning (ગુજરાતી)

> આ project (Medallion Data Pipeline) માં અત્યાર સુધી **download / install / configure** કરેલા બધા software, tools, plugins, jars અને Docker images નું list.
> હેતુ:
> 1. Organization માં દરેક tool માટે **RFC (Request for Change)** તૈયાર કરવો
> 2. **Production environment** ના planning માટે knowledge base
>
> Versions **2026-10-25** ના રોજ developer machine પર ચકાસેલા છે.
> License માહિતી સામાન્ય માર્ગદર્શન માટે છે. **અંતિમ નિર્ણય તમારી organization ની legal/compliance team નો રહેશે.**

---

## અનુક્રમણિકા
- [વિભાગ 1 – ફક્ત નામ (Master List)](#વિભાગ-1--ફક્ત-નામ-master-list)
- [વિભાગ 1A – Concept પ્રમાણે Tool Stack (CDC, ETL, Airflow, Lineage, Spark Cluster, Deployment, Monitoring, Security...)](#વિભાગ-1a--concept-પ્રમાણે-tool-stack)
- [વિભાગ 2 – વિગતવાર માહિતી](#વિભાગ-2--વિગતવાર-માહિતી)
  - [2.1 Developer Platform](#21-developer-platform)
  - [2.2 Processing (Spark)](#22-processing-spark)
  - [2.3 Databases](#23-databases)
  - [2.4 Streaming / CDC (Kafka)](#24-streaming--cdc-kafka)
  - [2.5 Application Libraries (Maven jars)](#25-application-libraries-maven-jars)
  - [2.6 Containers અને Data Governance](#26-containers-અને-data-governance)
  - [2.7 Orchestration – Apache Airflow](#27-orchestration--apache-airflow)
  - [2.8 EDB Postgres Advanced Server (EPAS) Stack](#28-edb-postgres-advanced-server-epas-stack)
- [વિભાગ 3 – Concept પ્રમાણે Mapping](#વિભાગ-3--concept-પ્રમાણે-mapping)
- [વિભાગ 4 – RFC માટે માર્ગદર્શન](#વિભાગ-4--rfc-માટે-માર્ગદર્શન)
- [વિભાગ 5 – Network Ports (Firewall RFC માટે)](#વિભાગ-5--network-ports-firewall-rfc-માટે)
- [વિભાગ 6 – Production માટે વધારાના Tools (હજુ install નથી)](#વિભાગ-6--production-માટે-વધારાના-tools-હજુ-install-નથી)
- [વિભાગ 7 – Production પહેલા ધ્યાન રાખવાના મુદ્દા](#વિભાગ-7--production-પહેલા-ધ્યાન-રાખવાના-મુદ્દા)

---

# વિભાગ 1 – ફક્ત નામ (Master List)

**Status:** ✅ Installed / વપરાયેલું · 🟡 Planned (આગળ setup કરવાનું) · 🔵 Future / Optional (વિભાગ 1A અને 6)

### A. Developer Platform ✅
1. Microsoft Build of OpenJDK 17
2. Apache Maven
3. Visual Studio Code
4. VS Code Extension Pack for Java (Red Hat Language Support for Java + Microsoft Java extensions)
5. Windows PowerShell 5.1 (Windows માં built-in)

### B. Processing ✅
6. Apache Spark 3.5.1 (pre-built for Hadoop 3)
7. Scala 2.12 runtime (Spark માં bundled)
8. Apache Hadoop client 3.3.4 (Spark માં bundled)
9. Hadoop Windows binaries: `winutils.exe` + `hadoop.dll`

### C. Databases ✅
10. MySQL Community Server 8.0
11. MySQL Installer – Community
12. MySQL Workbench (GUI, optional)
13. MongoDB Community Server
14. MongoDB Shell (mongosh)
15. MongoDB Database Tools (mongoimport વગેરે)
16. MongoDB Compass (GUI)

### D. Streaming / CDC ✅
17. Apache Kafka 4.3.1 (KRaft mode)
18. Kafka Connect (Kafka સાથે આવે છે)
19. Debezium MySQL Connector (Kafka Connect plugin)
20. Debezium MongoDB Connector (Kafka Connect plugin)
21. MongoDB Kafka Connector (Kafka Connect sink plugin)

### E. Application Libraries (Maven jars) ✅
22. Spring Boot 2.7.18 (starter, starter-jdbc, maven plugin)
23. Apache Spark SQL / Core jars (`spark-sql_2.12`)
24. MySQL Connector/J (JDBC driver)
25. HikariCP (connection pool)
26. Flyway Community (`flyway-core` + `flyway-mysql`)
27. MongoDB Spark Connector (`mongo-spark-connector_2.12`)
28. MongoDB Java Driver (`mongodb-driver-sync`, `-core`, `bson`, `bson-record-codec`)
29. OpenLineage Spark integration (`openlineage-spark_2.12`)
30. Jackson (JSON library, version pinned)
31. Janino (Spark code generation, version pinned)
32. Apache Log4j 2 API/Core (Spark logging, version pinned)
33. Logback + SLF4J (Spring Boot logging)
34. Maven Dependency Plugin

### F. Containers અને Data Governance ✅
35. Docker Desktop (WSL2 backend)
36. Windows Subsystem for Linux 2 (WSL2)
37. OpenMetadata Server (Docker image)
38. OpenMetadata PostgreSQL (Docker image)
39. Elasticsearch (Docker image)
40. OpenMetadata Ingestion (Docker image, `metadata` CLI)
41. Apache Spark Docker image (configured, run નથી કર્યું)
42. Eclipse Temurin 17 JRE Docker image (configured, run નથી કર્યું)

### G. Orchestration – Apache Airflow 🟡
43. Apache Airflow 3.x
44. Python 3 runtime (+ `pip` / `venv` અથવા `uv`)
45. PostgreSQL (Airflow metadata database)
46. Redis / Valkey (CeleryExecutor નો message broker; ફક્ત multi-worker setup માં)
47. Airflow Providers: `apache-airflow-providers-apache-spark`, `-mysql`, `-postgres`, `-mongo`, `-http`, `-openlineage`
48. Airflow runtime environment on Windows: WSL2 Ubuntu **અથવા** Docker (Airflow Windows પર native નથી ચાલતું)

### H. EDB Postgres Advanced Server (EPAS) Stack 🟡 (MySQL નો વિકલ્પ; બંને રાખ્યા છે)
49. EDB Postgres Advanced Server (EPAS) 16 / 17
50. PostgreSQL Community 16 / 17 (EPAS નો free વિકલ્પ)
51. pgAdmin 4 (GUI)
52. PostgreSQL JDBC Driver (`org.postgresql:postgresql`) / EDB JDBC Connector
53. Debezium PostgreSQL Connector (Kafka Connect plugin, `pgoutput`)
54. EDB Postgres Enterprise Manager (PEM): monitoring
55. EDB Failover Manager (EFM): high availability
56. Barman: backup અને recovery
57. EDB Migration Toolkit (MTK): MySQL/Oracle → EPAS migration
58. PgBouncer: connection pooling

---

# વિભાગ 1A – Concept પ્રમાણે Tool Stack

> દરેક concept માટે કયા tools જોઈએ. **Status:** ✅ Installed / વપરાયેલું · 🟡 Planned · 🔵 Future / Optional
> (નંબર = વિભાગ 1 નો નંબર; "–" = master list માં નથી, ભવિષ્ય માટે)

### 1A.1 Source Databases (Stage 1)
| Tool | Status | ભૂમિકા |
|---|---|---|
| MySQL Community Server 8.0 (#10) | ✅ | હાલનો SQL source (`de_source_mysql`) |
| EDB Postgres Advanced Server (#49) | 🟡 | Enterprise SQL source (MySQL નો વિકલ્પ, Oracle-compatible) |
| PostgreSQL Community (#50) | 🟡 | EPAS નો free વિકલ્પ |
| MongoDB Community Server (#13) | ✅ | Document source (`de_source_mongo`), replica set `rs0` |
| GUI: MySQL Workbench (#12) / pgAdmin 4 (#51) / Compass (#16) / DBeaver (–) | ✅ / 🟡 / ✅ / 🔵 | Data જોવો, SQL ચલાવવી |

### 1A.2 CDC (Change Data Capture): Stage 1 → Stage 2
| Tool | Status | ભૂમિકા |
|---|---|---|
| Apache Kafka 4.3.1, KRaft (#17) | ✅ | Change events store/deliver |
| Kafka Connect (#18) | ✅ | Connectors ચલાવવાનું framework (REST :8083) |
| **MySQL માટે:** Debezium MySQL Connector (#19) + MySQL binlog (ROW/FULL) + replication user | ✅ | MySQL changes → Kafka |
| **EPAS / PostgreSQL માટે:** Debezium PostgreSQL Connector (#53) + `wal_level=logical` + replication slot + publication | 🟡 | EPAS changes → Kafka |
| **MongoDB માટે:** Debezium MongoDB Connector (#20) + replica set (change streams) | ✅ | Mongo changes → Kafka |
| MongoDB Kafka Connector, source mode (#21 નો ભાગ) | 🔵 | Mongo CDC નો બીજો વિકલ્પ |
| JDBC Source Connector (Confluent) (–) | 🔵 | CDC ન ચાલે ત્યારે polling fallback |
| **Bronze sink:** MongoDB Kafka Connector (#21) + RegexRouter/InsertField SMTs + DLQ topic | ✅ | Kafka → `de_bronze` |
| Schema Registry (Confluent / Apicurio) + Avro (–) | 🔵 | Event schema versioning |
| Kafka UI (kafbat) / AKHQ (–) | 🔵 | Topics, messages, connectors GUI |
| `register-connectors.ps1`, Kafka CLI | ✅ | Connectors manage, live messages જોવા |

### 1A.3 Raw / Bronze Storage (Stage 2)
| Tool | Status | ભૂમિકા |
|---|---|---|
| MongoDB `de_bronze` (#13) | ✅ | As-is data + metadata |
| Object storage: S3 / ADLS / MinIO (–) | 🔵 | Lakehouse bronze (ઘણો data હોય ત્યારે) |
| Table formats: Delta Lake / Apache Iceberg / Hudi (–) | 🔵 | ACID tables on object storage, time travel |

### 1A.4 ETL / Processing (Stage 2 → 3 → 4)
| Tool | Status | ભૂમિકા |
|---|---|---|
| Apache Spark 3.5.1 (#6) + Scala 2.12 (#7) | ✅ | Batch transformations |
| MongoDB Spark Connector (#27) + MongoDB Java Driver (#28) | ✅ | Bronze વાંચવા (pushdown) |
| MySQL Connector/J (#24) | ✅ | Silver/Gold MySQL |
| PostgreSQL / EDB JDBC (#52) | 🟡 | Silver/Gold EPAS હોય ત્યારે (upsert: `ON CONFLICT`) |
| Spring Boot (#22) + HikariCP (#25) | ✅ | App framework, scheduling, connection pool |
| Flyway (#26) | ✅ | Silver/Gold tables (schema migration). EPAS/PostgreSQL પણ support કરે છે |
| Jackson / Janino / Log4j (#30–32) | ✅ | Spark ના required libraries (version pinned) |
| dbt (data build tool) (–) | 🔵 | SQL-based silver→gold transformations નો વિકલ્પ |

### 1A.5 Orchestration / Scheduling
| Tool | Status | ભૂમિકા |
|---|---|---|
| Spring Boot `@Scheduled` (#22) | ✅ | હાલનો scheduler (Job #1 → Job #2) |
| **Apache Airflow 3.x (#43)** | 🟡 | DAGs, retries, dependencies, UI, SLA, backfill |
| Python 3 (#44) | 🟡 | Airflow runtime |
| PostgreSQL, Airflow metadata DB (#45) | 🟡 | Airflow નું state (DAG runs, tasks) |
| Redis / Valkey (#46) | 🔵 | CeleryExecutor broker (ઘણા workers હોય ત્યારે) |
| Airflow providers (#47) | 🟡 | Spark submit, MySQL/Postgres/Mongo hooks, Kafka Connect REST (http), OpenLineage |
| WSL2 Ubuntu / Docker (#48) | 🟡 | Windows પર Airflow ચલાવવા |
| Airflow ↔ OpenMetadata ingestion scheduling | 🔵 | Metadata ingestion ને UI માંથી schedule કરવું |

### 1A.6 Data Lineage, Catalog અને Governance
| Tool | Status | ભૂમિકા |
|---|---|---|
| OpenMetadata Server (#37) | ✅ (setup) | Catalog, lineage UI, glossary, owners, tags |
| OpenMetadata PostgreSQL (#38) + Elasticsearch (#39) | ✅ | OpenMetadata નો DB + search |
| OpenSearch (–) | 🔵 | Elasticsearch નો Apache-licensed વિકલ્પ |
| OpenMetadata Ingestion (#40) | ✅ | DB schemas → catalog (MySQL, ભવિષ્યમાં Mongo/EPAS) |
| OpenLineage Spark (#29) | ✅ (dev profile) | Spark jobs નું lineage → OpenMetadata |
| Airflow OpenLineage provider (#47) | 🟡 | Airflow DAGs નું lineage |
| OpenMetadata Kafka / Kafka Connect connectors (–) | 🔵 | Topics અને connectors નું lineage |

### 1A.7 Data Quality
| Tool | Status | ભૂમિકા |
|---|---|---|
| Spark validation rules + `etl_rejected_record` | ✅ | Validation, duplicates, rejects (reason + raw data) |
| `station_data_quality` (gold) | ✅ | Missing values / completeness |
| OpenMetadata Data Quality tests (#37) | 🔵 | UI થી tests (not null, unique, range) |
| Great Expectations / Soda Core / AWS Deequ (–) | 🔵 | Declarative data quality framework |

### 1A.8 Spark Cluster Setup
| Tool | Status | ભૂમિકા |
|---|---|---|
| Spark distribution `C:\spark` (#6) | ✅ | Master + 2 workers (`spark-class`) |
| JDK 17 (#1), `JAVA_HOME`, `SPARK_HOME` | ✅ | Runtime + env variables |
| `winutils.exe` + `hadoop.dll`, `HADOOP_HOME` (#9) | ⚠️ `winutils.exe` ખૂટે છે | Windows workers માટે ફરજિયાત |
| Executor jars `target/executor-jars` + `spark.jars` (#34) | ✅ | Executors ને Mongo/MySQL drivers |
| Spark History Server (–) | 🔵 | પૂરા થયેલા jobs ની UI |
| Shared storage: HDFS / S3 / NFS (–) | 🔵 | ઘણી machines વાળો cluster |
| YARN / Kubernetes (Spark Operator) / Databricks / EMR (–) | 🔵 | Production compute platform |

### 1A.9 Build, Packaging અને Deployment
| Tool | Status | ભૂમિકા |
|---|---|---|
| Apache Maven (#2) + Maven Central | ✅ | Build, dependencies |
| `spring-boot-maven-plugin` (#22) | ✅ | Executable fat jar |
| `maven-dependency-plugin` (#34) | ✅ | Executor jars copy |
| Docker Desktop / Docker Engine (#35) + Dockerfiles (#41–42) | ✅ / 🟡 | Containers (OpenMetadata; app/Spark images configured) |
| Git + GitHub / GitLab / Azure Repos (–) | 🔵 | **Version control** (project હાલ git repository નથી) |
| Jenkins / GitHub Actions / Azure DevOps (–) | 🔵 | CI/CD pipeline |
| Nexus / JFrog Artifactory (–) | 🔵 | Internal Maven/Docker/PyPI repository |
| Kubernetes + Helm / OpenShift (–) | 🔵 | Production deployment |
| Ansible / Terraform (–) | 🔵 | Servers અને infrastructure automation |

### 1A.10 Monitoring અને Observability
| Tool | Status | ભૂમિકા |
|---|---|---|
| Spark UI :4040, Master UI :8080, Worker UI :8081/8082 | ✅ | Jobs, stages, executors |
| `etl_job_run`, `etl_watermark`, `etl_rejected_record` | ✅ | Pipeline audit |
| Kafka CLI: consumer-groups (LAG), console consumer | ✅ | Kafka/sink health |
| Prometheus + Grafana + Alertmanager (–) | 🔵 | Metrics, dashboards, alerts |
| JMX Exporter (Kafka/Connect/Spark), Kafka Exporter / Burrow (consumer lag) (–) | 🔵 | Kafka metrics |
| mysqld_exporter, postgres_exporter, mongodb_exporter (–) | 🔵 | DB metrics |
| EDB PEM (#54) | 🟡 | EPAS monitoring |
| ELK / OpenSearch / Loki / Splunk (–) | 🔵 | Central logging |

### 1A.11 Security
| Tool | Status | ભૂમિકા |
|---|---|---|
| DB users: `debezium` (replication), `om_reader` (read-only) | ✅ | Least privilege (શરૂઆત) |
| Secrets manager: HashiCorp Vault / Azure Key Vault / AWS Secrets Manager (–) | 🔵 | Passwords files માંથી દૂર |
| TLS certificates (internal CA) (–) | 🔵 | Kafka, MongoDB, MySQL/EPAS, OpenMetadata encryption |
| Kafka SASL (SCRAM) / mTLS + ACLs (–) | 🔵 | Kafka authentication/authorization |
| MongoDB SCRAM auth + RBAC, MySQL/EPAS roles (–) | 🔵 | DB authentication |
| SSO: Keycloak / Azure AD / Okta (–) | 🔵 | OpenMetadata, Airflow login |
| Snyk / OWASP Dependency-Check / Trivy / gitleaks (–) | 🔵 | Libraries, images, secrets scanning |
| EPAS: audit logging, data redaction (#49) | 🟡 | Enterprise DB security features |

### 1A.12 Backup અને Disaster Recovery
| Tool | Status | ભૂમિકા |
|---|---|---|
| MongoDB Database Tools `mongodump`/`mongorestore` (#15) | ✅ (install) | Mongo backup |
| `mysqldump` / Percona XtraBackup / MySQL Enterprise Backup (–) | 🔵 | MySQL backup |
| Barman (#56) / pgBackRest (–) | 🟡 / 🔵 | EPAS/PostgreSQL backup + PITR |
| EDB Failover Manager (#55) / Patroni (–) | 🟡 / 🔵 | EPAS/PostgreSQL HA |
| Kafka MirrorMaker 2 (Kafka સાથે આવે છે) | 🔵 | Kafka cluster replication / DR |

### 1A.13 Developer Productivity અને Testing
| Tool | Status | ભૂમિકા |
|---|---|---|
| VS Code + Java extensions (#3, #4), PowerShell (#5) | ✅ | Development |
| Postman / curl (–) | 🔵 | Kafka Connect / OpenMetadata REST APIs |
| JUnit 5 + Testcontainers (Kafka, MongoDB, MySQL, PostgreSQL) (–) | 🔵 | Automated integration tests |
| Jupyter / Zeppelin notebooks (–) | 🔵 | Data exploration |

---

# વિભાગ 2 – વિગતવાર માહિતી

**Columns નો અર્થ:**
- **પ્રકાર:** Install (installer) / Archive (zip/tgz extract) / Plugin / Maven dependency / Docker image / Built-in
- **RFC પ્રકાર:** organization માં કેવા પ્રકારની request લાગશે (વિભાગ 4)
- **Dev / Prod:** local development માં વપરાય છે કે production માં પણ જોઈશે

---

## 2.1 Developer Platform

### 1. Microsoft Build of OpenJDK 17
| Field | વિગત |
|---|---|
| Version | 17.0.19 (LTS) |
| પ્રકાર | Install (MSI) |
| Concept | Java runtime: Spark, Kafka, Kafka Connect, Spring Boot બધા JVM પર ચાલે છે |
| કેમ જરૂરી | Spark 3.5, Kafka 4.x broker અને Debezium 3.x ને **Java 17** જોઈએ છે |
| Source | https://learn.microsoft.com/java/openjdk/download |
| License | GPLv2 + Classpath Exception (free) |
| Dev / Prod | ✅ / ✅ (દરેક server પર) |
| RFC પ્રકાર | Software install |
| Production નોંધ | કોઈ પણ supported OpenJDK 17 distribution ચાલે (Temurin, Red Hat, Microsoft). બધા servers પર એક જ version રાખો |

### 2. Apache Maven
| Field | વિગત |
|---|---|
| Version | 3.9.11 |
| પ્રકાર | Archive (zip) + PATH |
| Concept | Build tool / dependency management |
| કેમ જરૂરી | `pom.xml` પ્રમાણે jars download કરે, project build કરે, executor jars copy કરે |
| Source | https://maven.apache.org/download.cgi |
| License | Apache 2.0 |
| Dev / Prod | ✅ / ❌ (ફક્ત build/CI server પર) |
| RFC પ્રકાર | Software install + **Maven Central access** (અથવા internal Nexus/Artifactory proxy) |
| Production નોંધ | CI/CD pipeline (Jenkins/GitHub Actions/Azure DevOps) માં build. Production servers પર ફક્ત બનેલી jar જાય |

### 3. Visual Studio Code
| Field | વિગત |
|---|---|
| Version | 1.143.1 |
| પ્રકાર | Install |
| Concept | IDE (code લખવો, debug, run) |
| કેમ જરૂરી | Java code edit/debug; `.vscode/launch.json` થી pipeline ચલાવવી |
| Source | https://code.visualstudio.com |
| License | Microsoft Software License (product free; source MIT) |
| Dev / Prod | ✅ / ❌ |
| RFC પ્રકાર | Software install (IntelliJ IDEA પણ વિકલ્પ) |

### 4. VS Code Extension Pack for Java
| Field | વિગત |
|---|---|
| Version | Java Pack 0.31.2; Red Hat Java 1.58.0; Debugger 0.60.0; Maven 0.46.0; Test 0.47.0; Dependency 0.27.7; Gradle 3.18.1 |
| પ્રકાર | VS Code extensions |
| Concept | Java language support, Maven project import, debugging |
| કેમ જરૂરી | VS Code માં Maven project load કરવા અને run/debug કરવા |
| Source | VS Code Marketplace |
| License | Red Hat Java: EPL-2.0; Microsoft Java extensions: MIT |
| Dev / Prod | ✅ / ❌ |
| RFC પ્રકાર | IDE extension (ઘણી orgs માં marketplace whitelist) |
| નોંધ | Red Hat extension પોતાનું JRE 25 સાથે લાવે છે. Project ચલાવવા JDK 17 ને `launch.json` માં pin કર્યું છે |

### 5. Windows PowerShell 5.1
| Field | વિગત |
|---|---|
| પ્રકાર | Windows માં built-in |
| Concept | Scripting / commands |
| કેમ જરૂરી | Setup commands, `register-connectors.ps1`, Kafka CLI |
| RFC પ્રકાર | જરૂર નથી. Script execution policy માટે IT ની મંજૂરી લાગી શકે |

---

## 2.2 Processing (Spark)

### 6. Apache Spark 3.5.1
| Field | વિગત |
|---|---|
| Version | 3.5.1 (`spark-3.5.1-bin-hadoop3`, built for Hadoop 3.3.4) |
| પ્રકાર | Archive (tgz) → `C:\spark` |
| Concept | Distributed data processing, **Batch ETL**, **Standalone cluster** (master + workers) |
| કેમ જરૂરી | Bronze → Silver → Gold transformations. `dev` profile માં Windows standalone cluster |
| Source | https://archive.apache.org/dist/spark/spark-3.5.1/ |
| License | Apache 2.0 |
| Dev / Prod | ✅ / ✅ |
| RFC પ્રકાર | Software (archive) install + ports (વિભાગ 5) |
| Production નોંધ | Linux cluster પર ચલાવવું (Standalone / YARN / Kubernetes) અથવા managed (Databricks, AWS EMR, Azure HDInsight/Synapse). **Cluster નું version = project નું version (3.5.1 / Scala 2.12)** |

### 7. Scala 2.12 runtime
| Field | વિગત |
|---|---|
| Version | 2.12.18 |
| પ્રકાર | Spark અને Maven jars સાથે bundled |
| Concept | Spark ની implementation language |
| કેમ જરૂરી | Jar નામોમાં `_2.12`. બધી Spark-related jars **એક જ Scala version** ની હોવી જોઈએ |
| RFC પ્રકાર | અલગ જરૂર નથી |

### 8. Apache Hadoop client 3.3.4
| Field | વિગત |
|---|---|
| પ્રકાર | Spark સાથે bundled |
| Concept | File system abstraction (local, HDFS, S3) |
| કેમ જરૂરી | Spark files વાંચવા/લખવા Hadoop libraries વાપરે છે |
| RFC પ્રકાર | અલગ જરૂર નથી |

### 9. Hadoop Windows binaries (`winutils.exe`, `hadoop.dll`)
| Field | વિગત |
|---|---|
| Version | hadoop-3.3.5 build |
| પ્રકાર | Download (binaries) → `C:\hadoop\bin` + `HADOOP_HOME` |
| Concept | Hadoop on Windows: file permissions/native IO |
| કેમ જરૂરી | Windows પર Spark workers/driver ને જોઈએ. **ફક્ત Windows માટે** |
| Source | https://github.com/cdarlint/winutils (**unofficial, community build**) |
| License | Hadoop code Apache 2.0. Binaries third-party compiled છે |
| Dev / Prod | ✅ / ❌ (Linux production માં જરૂર નથી) |
| RFC પ્રકાર | ⚠️ **Security review** લાગશે, કારણ કે આ unofficial executable છે |
| ⚠️ Status | Machine પર **`hadoop.dll` છે, પણ `winutils.exe` નથી** (ચકાસેલું) |

---

## 2.3 Databases

### 10. MySQL Community Server
| Field | વિગત |
|---|---|
| Version | 8.0.30 (Windows service `MySQL80`) |
| પ્રકાર | Install (MySQL Installer) |
| Concept | Relational DB: **Source** (`de_source_mysql`), **Silver** (`de_silver`), **Gold** (`de_gold`); **CDC** માટે **binlog** (ROW / FULL) |
| કેમ જરૂરી | Source data, curated + aggregate layers, pipeline control tables |
| Source | https://dev.mysql.com/downloads/installer/ |
| License | GPLv2 (Community Edition). Commercial: MySQL Enterprise |
| Dev / Prod | ✅ / ✅ |
| RFC પ્રકાર | Software install + DB users (`debezium` replication user) + port 3306 |
| Production નોંધ | ⚠️ **MySQL 8.0 નું support April 2026 માં પૂરું થયું (EOL).** Production માટે **MySQL 8.4 LTS** (Debezium 3.7 support કરે છે). HA: InnoDB Cluster / replica, અથવા managed (AWS RDS, Azure Database for MySQL) |

### 11. MySQL Installer – Community
| Field | વિગત |
|---|---|
| Version | 1.6.3.0 |
| પ્રકાર | Install |
| કેમ જરૂરી | Windows પર MySQL products install/upgrade કરવા |
| Dev / Prod | ✅ / ❌ |

### 12. MySQL Workbench (optional)
| Field | વિગત |
|---|---|
| પ્રકાર | Install (MySQL Installer માંથી) |
| Concept | DB GUI: SQL ચલાવવા, tables જોવા |
| License | GPLv2 |
| Dev / Prod | ✅ / ❌ |
| નોંધ | DBeaver જેવા GUI પણ ચાલે |

### 13. MongoDB Community Server
| Field | વિગત |
|---|---|
| Version | **8.3.13** (Windows service `MongoDB`), **single-node replica set `rs0`**, WiredTiger cache 0.5 GB |
| પ્રકાર | Install (MSI) |
| Concept | Document DB: **Source** (`de_source_mongo`), **Bronze / Raw layer** (`de_bronze`); **CDC** માટે **change streams** (replica set ફરજિયાત) |
| કેમ જરૂરી | Nested JSON as-is; raw layer |
| Source | https://www.mongodb.com/try/download/community |
| License | ⚠️ **SSPL v1** (Server Side Public License). OSI-approved open source નથી. Commercial: MongoDB Enterprise / Atlas |
| Dev / Prod | ✅ / ✅ |
| RFC પ્રકાર | Software install + **Legal review (SSPL)** + port 27017 |
| Production નોંધ | ⚠️ 8.3 એ **rapid release** છે. Production માટે **major release (8.0)** વાપરો. **Debezium 3.7 officially MongoDB 6.0 / 7.0 / 8.0 ને support કરે છે**; 8.3 હાલ ચાલે છે પણ officially listed નથી. Production: 3-node replica set + authentication + TLS, અથવા MongoDB Atlas |

### 14. MongoDB Shell (mongosh)
| Field | વિગત |
|---|---|
| Version | 2.14.0 |
| પ્રકાર | Install (MSI) |
| Concept | Mongo commands: `rs.initiate`, queries |
| Source | https://www.mongodb.com/try/download/shell |
| License | Apache 2.0 |
| Dev / Prod | ✅ / ✅ (admin માટે) |
| નોંધ | Machine પર PATH માં નથી. Compass નું built-in `>_ MONGOSH` પણ વાપરી શકાય |

### 15. MongoDB Database Tools
| Field | વિગત |
|---|---|
| Version | 100.19.1 |
| પ્રકાર | Install (MSI) |
| Concept | Data import/export/backup (`mongoimport`, `mongodump`, `mongorestore`) |
| કેમ જરૂરી | `data1.json` ને source collection માં import કરવા |
| Source | https://www.mongodb.com/try/download/database-tools |
| License | Apache 2.0 |
| Dev / Prod | ✅ / ✅ (backup/restore માટે) |

### 16. MongoDB Compass
| Field | વિગત |
|---|---|
| Version | 1.53.1 |
| પ્રકાર | Install |
| Concept | MongoDB GUI (data જોવો, import, shell) |
| Source | https://www.mongodb.com/try/download/compass |
| License | SSPL (legal ચકાસો) |
| Dev / Prod | ✅ / ❌ |

---

## 2.4 Streaming / CDC (Kafka)

### 17. Apache Kafka (KRaft mode)
| Field | વિગત |
|---|---|
| Version | **4.3.1** (`kafka_2.13-4.3.1`), `C:\kafka`, data `C:\kafka-data` |
| પ્રકાર | Archive (tgz) |
| Concept | **Event streaming / message broker**, **KRaft** (ZooKeeper વગર), topics, offsets, retention |
| કેમ જરૂરી | Source changes (CDC events) ને Stage 1 → Stage 2 વચ્ચે store અને deliver કરે છે |
| Source | https://archive.apache.org/dist/kafka/4.3.1/ |
| License | Apache 2.0 |
| Dev / Prod | ✅ / ✅ |
| RFC પ્રકાર | Software (archive) install + ports 9092, 9093 |
| Production નોંધ | ⚠️ **Kafka officially Windows support નથી કરતું.** Production: **Linux**, ઓછામાં ઓછા 3 brokers, replication factor 3, retention policy, monitoring. Managed: Confluent Cloud, AWS MSK, Azure Event Hubs (Kafka API) |

### 18. Kafka Connect
| Field | વિગત |
|---|---|
| Version | 4.3.1 (Kafka સાથે) |
| પ્રકાર | Kafka distribution નો ભાગ. Config: `C:\kafka\config\connect-distributed-de.properties` |
| Concept | **Data integration framework**: source/sink connectors, **SMT** (Single Message Transform), REST API, distributed mode |
| કેમ જરૂરી | Code વગર, ફક્ત JSON config થી DB ↔ Kafka જોડવા (`kafka-connect/*.json`) |
| License | Apache 2.0 |
| Dev / Prod | ✅ / ✅ |
| RFC પ્રકાર | Kafka સાથે આવે છે + port 8083 |
| Production નોંધ | ઓછામાં ઓછા 2 workers (distributed mode), plugins બધા workers પર સરખા. REST API ને secure કરો |

### 19. Debezium MySQL Connector
| Field | વિગત |
|---|---|
| Version | 3.7.0.Final |
| પ્રકાર | Kafka Connect plugin (zip) → `C:\kafka\plugins\debezium-connector-mysql` |
| Concept | **CDC (Change Data Capture)**: MySQL **binlog** વાંચે, **initial snapshot**, `ExtractNewRecordState` SMT |
| કેમ જરૂરી | MySQL source ના insert/update/delete ને Kafka topic માં મોકલે (connector: `source-mysql`) |
| Source | Maven Central: `io.debezium:debezium-connector-mysql:3.7.0.Final:plugin.zip` |
| License | Apache 2.0 |
| Dev / Prod | ✅ / ✅ |
| RFC પ્રકાર | Plugin download + MySQL replication privileges (`REPLICATION SLAVE/CLIENT`) |
| Production નોંધ | Supported MySQL: 8.0.x, 8.4.x, 9.7. Binlog retention connector downtime કરતાં લાંબી રાખો |

### 20. Debezium MongoDB Connector
| Field | વિગત |
|---|---|
| Version | 3.7.0.Final |
| પ્રકાર | Kafka Connect plugin (zip) → `C:\kafka\plugins\debezium-connector-mongodb` |
| Concept | **CDC**: MongoDB **change streams**, initial snapshot |
| કેમ જરૂરી | Mongo source ના changes ને Kafka topic માં મોકલે (connector: `source-mongo`) |
| Source | Maven Central: `io.debezium:debezium-connector-mongodb:3.7.0.Final:plugin.zip` |
| License | Apache 2.0 |
| Dev / Prod | ✅ / ✅ |
| નોંધ | Mixed-type arrays ને કારણે unwrap SMT વાપર્યો નથી (setup guide E2) |

### 21. MongoDB Kafka Connector (Sink)
| Field | વિગત |
|---|---|
| Version | 3.1.2 (`mongo-kafka-connect-3.1.2-all.jar`) |
| પ્રકાર | Kafka Connect plugin (jar) → `C:\kafka\plugins\mongo-kafka-connect` |
| Concept | **Sink connector**: Kafka → MongoDB, upsert, `RegexRouter`/`InsertField` SMTs, **DLQ** |
| કેમ જરૂરી | બધા `src.*` topics ને **Bronze** (`de_bronze`) માં લખે (connector: `sink-bronze`) |
| Source | Maven Central: `org.mongodb.kafka:mongo-kafka-connect:3.1.2` (`-all` classifier) |
| License | Apache 2.0 |
| Dev / Prod | ✅ / ✅ |

---

## 2.5 Application Libraries (Maven jars)

> Maven આ બધી jars **Maven Central** (https://repo1.maven.org/maven2) માંથી આપોઆપ download કરે છે.
> **RFC પ્રકાર (બધા માટે):** Maven Central access, અથવા internal **Nexus/Artifactory** proxy પર આ artifacts ની મંજૂરી. ઘણી organizations libraries નો security scan (SCA: Snyk, Dependency-Track વગેરે) કરે છે.

| # | Library (groupId:artifactId) | Version | Concept | કેમ જરૂરી | License | Prod |
|---|---|---|---|---|---|---|
| 22 | `org.springframework.boot:spring-boot-starter-parent`, `spring-boot-starter-jdbc`, `spring-boot-maven-plugin` | 2.7.18 | Application framework, **scheduling**, profiles, DI | Scheduler (Job #1 → Job #2), config, packaging (fat jar) | Apache 2.0 | ✅ |
| 23 | `org.apache.spark:spark-sql_2.12` (+ spark-core વગેરે) | 3.5.1 | Spark API (DataFrame, SQL) | App પોતે **Spark driver** છે | Apache 2.0 | ✅ |
| 24 | `com.mysql:mysql-connector-j` | 8.0.33 | JDBC driver | Silver/Gold MySQL read/write (driver + executors) | GPLv2 + Universal FOSS Exception | ✅ |
| 25 | `com.zaxxer:HikariCP` | Spring Boot managed | JDBC connection pool | Silver/Gold DataSources | Apache 2.0 | ✅ |
| 26 | `org.flywaydb:flyway-core`, `flyway-mysql` | 8.5.13 | **Schema migration** (DB versioning) | `de_silver` / `de_gold` tables SQL files થી બનાવે | Apache 2.0 (Community) | ✅ |
| 27 | `org.mongodb.spark:mongo-spark-connector_2.12` | 10.7.0 | Spark ↔ MongoDB, **filter pushdown** | Bronze વાંચવા | Apache 2.0 | ✅ |
| 28 | `org.mongodb:mongodb-driver-sync`, `mongodb-driver-core`, `bson`, `bson-record-codec` | 5.1.4 (pinned) | MongoDB Java driver | Mongo Spark connector ને જોઈએ | Apache 2.0 | ✅ |
| 29 | `io.openlineage:openlineage-spark_2.12` | 1.37.0 | **Data lineage** (Spark listener) | OpenMetadata માં lineage મોકલવા (`dev` profile) | Apache 2.0 | Optional |
| 30 | `com.fasterxml.jackson.*` | 2.15.2 (pinned) | JSON | Spark 3.5 ને 2.15 જોઈએ (Spring Boot 2.7 નું 2.13 ચાલતું નથી) | Apache 2.0 | ✅ |
| 31 | `org.codehaus.janino:janino`, `commons-compiler` | 3.1.9 (pinned) | Runtime code generation | Spark SQL codegen | BSD-3-Clause | ✅ |
| 32 | `org.apache.logging.log4j:*` | 2.20.0 (pinned) | Logging | Spark ની log4j-api/core | Apache 2.0 | ✅ |
| 33 | `ch.qos.logback:*`, `org.slf4j:*` | Spring Boot managed | Logging | App logging (Spark નો `log4j-slf4j2-impl` exclude કર્યો છે) | Logback: EPL 1.0 / LGPL 2.1; SLF4J: MIT | ✅ |
| 34 | `org.apache.maven.plugins:maven-dependency-plugin` | Maven managed | Build | Executor jars `target/executor-jars` માં copy (cluster mode) | Apache 2.0 | Build |

**Version pinning કેમ (`pom.xml` properties):** Spring Boot 2.7 પોતાના જૂના versions લાવે છે, જે Spark 3.5 / Mongo connector સાથે ચાલતા નથી. તેથી `jackson-bom.version`, `janino.version`, `log4j2.version` અને `mongodb.version` override કર્યા છે. Production માં પણ **આ જ versions** રાખો.

---

## 2.6 Containers અને Data Governance

### 35. Docker Desktop
| Field | વિગત |
|---|---|
| Version | 29.4.0 engine (વપરાયું હતું). ⚠️ હાલ `docker` command machine પર **મળતો નથી** (ચકાસેલું) |
| પ્રકાર | Install |
| Concept | **Containerization**: images, containers, networks, volumes |
| કેમ જરૂરી | OpenMetadata ચલાવવા. Kafka/Spark માટે **નથી** (તે native છે) |
| Source | https://www.docker.com/products/docker-desktop |
| License | ⚠️ **Docker Subscription Service Agreement**: મોટી organizations (250+ employees **અથવા** $10M+ revenue) માટે **paid subscription ફરજિયાત** |
| Dev / Prod | Optional / ❌ (production માં Docker Engine / Kubernetes) |
| RFC પ્રકાર | Software install + **License/procurement** + WSL2 enable |
| વિકલ્પ | Rancher Desktop, Podman Desktop (free) |

### 36. WSL2 (Windows Subsystem for Linux 2)
| Field | વિગત |
|---|---|
| પ્રકાર | Windows feature |
| Concept | Windows પર Linux kernel |
| કેમ જરૂરી | Docker Desktop backend. ભવિષ્યમાં Kafka ને Linux માં ચલાવવાનો વિકલ્પ |
| RFC પ્રકાર | Windows feature enable (admin rights, ઘણી વાર BIOS virtualization) |

### 37. OpenMetadata Server
| Field | વિગત |
|---|---|
| Version | 2.0.6 (`docker.getcollate.io/openmetadata/server:2.0.6`) |
| પ્રકાર | Docker image |
| Concept | **Data governance, data catalog, data lineage**, glossary, data quality |
| કેમ જરૂરી | Tables નું catalog અને Spark lineage (OpenLineage events) |
| Source | Docker registry `docker.getcollate.io` |
| License | Apache 2.0 |
| Dev / Prod | Optional / ✅ (જો governance જોઈએ તો) |
| RFC પ્રકાર | Docker registry access + ports 8585/8586 |
| Production નોંધ | Kubernetes (Helm chart) અથવા managed (Collate). Authentication (SSO) setup કરવું |

### 38. OpenMetadata PostgreSQL
| Field | વિગત |
|---|---|
| Version | 2.0.6 (`docker.getcollate.io/openmetadata/postgresql:2.0.6`) |
| પ્રકાર | Docker image |
| Concept | OpenMetadata નો પોતાનો metadata DB |
| License | PostgreSQL License |
| Production નોંધ | Managed PostgreSQL (RDS / Azure) + backups |

### 39. Elasticsearch
| Field | વિગત |
|---|---|
| Version | 9.3.0 (`docker.elastic.co/elasticsearch/elasticsearch:9.3.0`) |
| પ્રકાર | Docker image |
| Concept | Search index (OpenMetadata UI search) |
| License | ⚠️ AGPLv3 / SSPL / Elastic License 2.0 (ત્રણમાંથી એક પસંદ). **Apache નથી**, legal ચકાસો |
| Production નોંધ | વિકલ્પ: OpenSearch (Apache 2.0). OpenMetadata બંનેને support કરે છે |

### 40. OpenMetadata Ingestion
| Field | વિગત |
|---|---|
| Version | 2.0.6 (`docker.getcollate.io/openmetadata/ingestion:2.0.6`) |
| પ્રકાર | Docker image (`metadata ingest` CLI, Airflow વગર manually) |
| Concept | **Metadata ingestion**: DB schemas → catalog |
| કેમ જરૂરી | MySQL tables ને OpenMetadata માં લાવવા |
| License | Apache 2.0 |
| Production નોંધ | Scheduled ingestion માટે Airflow (અથવા બીજો orchestrator) |

### 41–42. Configured પણ run નથી કર્યા
| # | Image | ક્યાં | નોંધ |
|---|---|---|---|
| 41 | `spark:3.5.1-scala2.12-java17-ubuntu` | `docker/spark.Dockerfile` (જૂનો `prod` profile) | Docker માં Spark cluster માટે બનાવ્યું, ક્યારેય ચલાવ્યું નથી |
| 42 | `eclipse-temurin:17-jre` | `docker/app.Dockerfile` | App container. જૂના pipeline માટે; હાલના pipeline સાથે update કરવાનું બાકી |

---

## 2.7 Orchestration – Apache Airflow 🟡

> **Status:** હજુ install નથી. હાલ pipeline ને Spring Boot `@Scheduled` ચલાવે છે.
> Airflow production-grade orchestration આપે છે: DAG (job dependencies), retries, backfill, SLA/alerts, run history UI.

### 43. Apache Airflow
| Field | વિગત |
|---|---|
| Version | 3.x (install વખતે latest stable ચકાસો) |
| પ્રકાર | Python package (`pip install "apache-airflow==<version>" --constraint <official constraints file>`) અથવા official Docker image `apache/airflow` |
| Concept | **Workflow orchestration**: DAGs, tasks, scheduling, retries, backfill, dependencies |
| Components | API server/UI, Scheduler, DAG Processor, Triggerer, Executor (Local / Celery / Kubernetes) |
| કેમ જરૂરી | Job #1 → Job #2 ની dependency, failure પર retry/alert, Kafka Connect health check, OpenMetadata ingestion schedule — બધું એક જગ્યાએ |
| Source | https://airflow.apache.org |
| License | Apache 2.0 |
| Dev / Prod | 🟡 / ✅ |
| RFC પ્રકાર | WSL2/Docker + **PyPI access** (અથવા internal PyPI proxy) + port |
| ⚠️ Windows | Airflow **Windows પર native support નથી** → WSL2 (Ubuntu) માં અથવા Docker માં ચલાવો |
| ⚠️ Port | Default UI port **8080** = Spark Master UI નો port → Airflow ને **8090** પર ચલાવો |
| Project માં ફેરફાર (જ્યારે અપનાવીએ) | `@Scheduled` બંધ કરી app માં "run once and exit" mode ઉમેરવો; DAG: `BashOperator`/`SSHOperator` થી jar ચલાવે, અથવા jobs ને `spark-submit` + `SparkSubmitOperator` માં ખસેડવા; `HttpOperator` થી Kafka Connect status ચકાસવો |
| Production નોંધ | CeleryExecutor (Redis) અથવા KubernetesExecutor; HA scheduler; managed: Astronomer, AWS MWAA, Google Cloud Composer |

### 44. Python 3 runtime
| Field | વિગત |
|---|---|
| Version | પસંદ કરેલા Airflow version ના supported Python versions (official docs) પ્રમાણે |
| પ્રકાર | WSL2 Ubuntu માં `apt`/`uv`, અથવા Docker image માં built-in |
| Concept | Airflow અને DAG code નું runtime |
| કેમ જરૂરી | Airflow Python application છે; DAGs Python માં લખાય છે |
| License | PSF License |
| RFC પ્રકાર | Software install (WSL2 ની અંદર) + PyPI access |

### 45. PostgreSQL (Airflow metadata DB)
| Field | વિગત |
|---|---|
| Version | Airflow docs માં supported version (PostgreSQL 13+) |
| પ્રકાર | WSL2 માં install, Docker image, અથવા #50 નું જ PostgreSQL instance (અલગ database) |
| Concept | Airflow નું state: DAG runs, task instances, connections, variables |
| કેમ જરૂરી | **SQLite ફક્ત trial માટે**. Real use માં PostgreSQL (recommended) અથવા MySQL 8 |
| License | PostgreSQL License |
| Port | 5432 |

### 46. Redis / Valkey (optional)
| Field | વિગત |
|---|---|
| પ્રકાર | WSL2/Docker |
| Concept | Message broker: CeleryExecutor workers ને tasks આપે |
| કેમ જરૂરી | **ફક્ત** CeleryExecutor (ઘણા worker machines) વખતે. Local learning માં **LocalExecutor** પૂરતો છે → Redis જરૂરી નથી |
| License | Redis 8: RSALv2 / SSPLv1 / AGPLv3 (legal ચકાસો) · **Valkey: BSD-3 (free વિકલ્પ)** |
| Port | 6379 (Celery Flower UI: 5555) |

### 47. Airflow Providers
| Provider package | કેમ |
|---|---|
| `apache-airflow-providers-apache-spark` | `SparkSubmitOperator`: Spark cluster પર jobs submit |
| `apache-airflow-providers-mysql` | MySQL hook/operators (silver/gold checks) |
| `apache-airflow-providers-postgres` | EPAS/PostgreSQL hook/operators |
| `apache-airflow-providers-mongo` | MongoDB hook (bronze checks) |
| `apache-airflow-providers-http` | Kafka Connect REST API (connector status) |
| `apache-airflow-providers-openlineage` | Airflow DAGs નું lineage → OpenMetadata |
| `apache-airflow-providers-ssh` (optional) | Remote machine પર jar ચલાવવો |

License: Apache 2.0 · Source: PyPI · RFC: PyPI access

### 48. Airflow runtime on Windows (WSL2 Ubuntu / Docker)
| Field | વિગત |
|---|---|
| વિકલ્પ 1 | **WSL2 + Ubuntu**: Python venv માં Airflow (Docker Desktop license ની જરૂર નહીં) |
| વિકલ્પ 2 | **Docker**: official `apache/airflow` image (Docker Desktop license ધ્યાનમાં રાખો) |
| Networking | WSL2/Docker માંથી Windows ના MySQL/Mongo/Kafka/Spark સુધી પહોંચવા host IP / `host.docker.internal` |
| RAM | Airflow (scheduler + api-server + DB) ને આશરે 2–4 GB. 10 GB machine પર બાકીના stack સાથે ભારે પડશે |

---

## 2.8 EDB Postgres Advanced Server (EPAS) Stack 🟡

> **Status:** હજુ install નથી. હાલ pipeline **MySQL** વાપરે છે. EPAS ને **enterprise વિકલ્પ** તરીકે રાખ્યું છે, **બંને documented** છે.
> Organization નું standard EPAS હોય તો Source / Silver / Gold MySQL ની જગ્યાએ EPAS પર જઈ શકે.

### 49. EDB Postgres Advanced Server (EPAS)
| Field | વિગત |
|---|---|
| Version | 16 / 17 |
| પ્રકાર | Install (EDB installer / repository). Windows support EDB ના platform support matrix માં ચકાસો |
| Concept | Enterprise PostgreSQL: **Oracle compatibility** (PL/SQL packages, synonyms), security (audit, data redaction), EDB tooling |
| કેમ જરૂરી | Organization standard હોય તો Source / Silver / Gold DB તરીકે |
| Source | https://www.enterprisedb.com (download માટે **EDB account** જોઈએ) |
| License | ⚠️ **Commercial (EDB subscription)**. Community edition **નથી** |
| Port | **5444** (default), superuser `enterprisedb` |
| RFC પ્રકાર | Software install + **License/procurement** + port + DBA approval |
| CDC માટે | `wal_level = logical`, replication user, publication, `pg_hba.conf` entry → Debezium PostgreSQL connector (#53) |

### 50. PostgreSQL Community
| Field | વિગત |
|---|---|
| Version | 16 / 17 |
| પ્રકાર | Install (Windows installer EDB જ બનાવે છે: https://www.postgresql.org/download/windows/) |
| Concept | Open-source PostgreSQL. EPAS ના base જેવું, Oracle compatibility વગર |
| License | PostgreSQL License (free) |
| Port | 5432 |
| નોંધ | Debezium **officially** PostgreSQL ને support કરે છે. Learning અને compatibility માટે સૌથી સુરક્ષિત વિકલ્પ |

### 51. pgAdmin 4
| Field | વિગત |
|---|---|
| પ્રકાર | Install (EPAS/PostgreSQL installer સાથે આવે છે) |
| Concept | PostgreSQL/EPAS GUI |
| License | PostgreSQL License |
| Dev / Prod | ✅ / admin |

### 52. JDBC Driver (PostgreSQL / EDB)
| Field | વિગત |
|---|---|
| Artifact | `org.postgresql:postgresql` (EPAS સાથે પણ ચાલે છે) **અથવા** EDB JDBC Connector |
| પ્રકાર | Maven dependency (+ cluster mode માં executor jars) |
| Concept | Spark ↔ EPAS (Silver/Gold read/write) |
| License | PostgreSQL JDBC: BSD-2-Clause · EDB JDBC: EDB license |
| Project માં ફેરફાર (MySQL → EPAS) | JDBC URL `jdbc:postgresql://host:5444/db`; upsert `INSERT ... ON CONFLICT (key) DO UPDATE` (MySQL ના `ON DUPLICATE KEY UPDATE` ની જગ્યાએ); Flyway SQL માં types બદલવા (`DATETIME(3)` → `TIMESTAMP(3)`, `TINYINT(1)` → `SMALLINT`/`BOOLEAN`, `AUTO_INCREMENT` → `GENERATED ALWAYS AS IDENTITY`, `CREATE TABLE ... LIKE` → `(LIKE ... INCLUDING ALL)`) |

### 53. Debezium PostgreSQL Connector
| Field | વિગત |
|---|---|
| Version | 3.7.0.Final (હાલના Debezium plugins સાથે સરખું) |
| પ્રકાર | Kafka Connect plugin (zip) → `C:\kafka\plugins\debezium-connector-postgres` |
| Concept | **CDC**: WAL logical decoding (`plugin.name=pgoutput`), replication slot, publication, initial snapshot |
| કેમ જરૂરી | EPAS/PostgreSQL source ના changes → Kafka |
| Source | Maven Central: `io.debezium:debezium-connector-postgres:3.7.0.Final:plugin.zip` |
| License | Apache 2.0 |
| ⚠️ નોંધ | EPAS **officially listed નથી**. PostgreSQL ચોક્કસ ચાલે; EPAS સાથે પહેલા test કરો. Connector લાંબો બંધ રહે તો replication slot ને કારણે **WAL ભેગી થઈ disk ભરાઈ શકે** |

### 54–58. EDB / PostgreSQL ecosystem tools
| # | Tool | Concept | કેમ | License | Status |
|---|---|---|---|---|---|
| 54 | **EDB Postgres Enterprise Manager (PEM)** | Monitoring | EPAS performance, alerts, dashboards | Commercial (EDB) | 🟡 |
| 55 | **EDB Failover Manager (EFM)** | High availability | Primary fail થાય તો standby ને આપોઆપ primary બનાવે | Commercial (EDB) | 🟡 |
| 56 | **Barman** | Backup & recovery | Backups, point-in-time recovery (PITR) | GPLv3 (EDB maintained) | 🟡 |
| 57 | **EDB Migration Toolkit (MTK)** | Migration | MySQL/Oracle → EPAS schema અને data migration | EDB license | 🟡 |
| 58 | **PgBouncer** | Connection pooling | ઘણા clients (Spark executors) ના connections સંભાળે | ISC (free) | 🟡 |

**Open-source વિકલ્પો:** Patroni (HA, MIT), pgBackRest (backup, MIT), postgres_exporter + Grafana (monitoring).

### MySQL vs EPAS: ઝડપી સરખામણી
| મુદ્દો | MySQL (હાલ) | EPAS (વિકલ્પ) |
|---|---|---|
| License | GPLv2 (Community free) | Commercial (EDB subscription) |
| Debezium support | ✅ Official | ⚠️ PostgreSQL official; EPAS test કરવું |
| CDC mechanism | Binlog | WAL logical decoding |
| Upsert SQL | `ON DUPLICATE KEY UPDATE` | `ON CONFLICT ... DO UPDATE` |
| Oracle compatibility | ❌ | ✅ |
| Enterprise tools | MySQL Enterprise (paid) | PEM, EFM, Barman, MTK |
| Default port | 3306 | 5444 |

---

# વિભાગ 3 – Concept પ્રમાણે Mapping

| Concept | Tools |
|---|---|
| **Medallion Architecture** (Source → Bronze → Silver → Gold) | MongoDB, MySQL, Kafka, Kafka Connect, Spark |
| **CDC (Change Data Capture)** | Debezium MySQL/MongoDB connectors, MySQL binlog, MongoDB change streams (replica set) |
| **Event Streaming** | Apache Kafka (KRaft), topics, offsets, consumer groups |
| **Data Integration (no-code)** | Kafka Connect, SMTs, MongoDB Kafka Sink, DLQ |
| **Initial load of existing data** | Debezium `snapshot.mode=initial` |
| **Batch ETL / Distributed Processing** | Apache Spark (local + standalone cluster), Hadoop client, winutils |
| **Incremental Processing** | Watermark tables, MongoDB Spark Connector pushdown, upserts |
| **Data Validation / Quality / Deduplication** | Spark (validation rules, window functions), `etl_rejected_record`, `station_data_quality` |
| **Schema Migration** | Flyway |
| **Orchestration / Scheduling** | Spring Boot `@Scheduled` (હાલ), **Apache Airflow** + Python + PostgreSQL metadata DB + providers (planned) |
| **Enterprise SQL (MySQL નો વિકલ્પ)** | EDB Postgres Advanced Server / PostgreSQL, pgAdmin, PostgreSQL/EDB JDBC, Debezium PostgreSQL, PEM, EFM, Barman, MTK, PgBouncer |
| **Connection Management** | HikariCP, MySQL Connector/J, MongoDB Java Driver |
| **Data Governance / Catalog / Lineage** | OpenMetadata, OpenLineage (Spark + Airflow provider), Elasticsearch/OpenSearch, PostgreSQL |
| **Containerization** | Docker Desktop, WSL2 |
| **Build / Dependency Management** | Maven, Maven Central |
| **Development Environment** | JDK 17, VS Code + Java extensions, PowerShell |
| **Observability (basic)** | Spark UI (:4040), Spark Master UI (:8080), `etl_job_run` audit table |

---

# વિભાગ 4 – RFC માટે માર્ગદર્શન

### 4.1 RFC ના પ્રકાર
| RFC પ્રકાર | શામેલ tools | સામાન્ય રીતે શું માંગવું |
|---|---|---|
| **Software install (developer machine)** | JDK, Maven, VS Code, MySQL, MongoDB (+ mongosh, Tools, Compass), Spark, Kafka | Admin rights અથવા Software Center દ્વારા install, version, license |
| **Unofficial binary** | `winutils.exe`, `hadoop.dll` | Security review, hash verification, source repo |
| **Library repository access** | બધી Maven jars (2.5) અને Kafka Connect plugins (19–21) | Maven Central access, અથવા Nexus/Artifactory proxy whitelist + vulnerability scan |
| **Container registry access** | `docker.getcollate.io`, `docker.elastic.co`, Docker Hub | Registry whitelist / internal mirror |
| **Commercial license / procurement** | Docker Desktop (મોટી org) | Subscription, અથવા Rancher/Podman જેવો free વિકલ્પ |
| **Legal review (copyleft / source-available)** | MongoDB (SSPL), Compass (SSPL), Elasticsearch (AGPL/SSPL/ELv2), MySQL (GPLv2) | Internal use માટે મંજૂરી |
| **Network / firewall** | વિભાગ 5 ના ports | Local માટે સામાન્ય રીતે જરૂર નથી; servers વચ્ચે જોઈશે |
| **OS feature** | WSL2, Hyper-V/virtualization | Admin + BIOS |
| **DB access / users** | MySQL `debezium` (replication), `root@'%'`, `om_reader`; Mongo replica set; EPAS replication user + publication | DBA approval. Production માં `root` ન વાપરો, least-privilege users બનાવો |
| **Python package repository** | Airflow + providers (PyPI) | PyPI access અથવા internal PyPI proxy (Nexus/Artifactory) |
| **Linux environment on Windows** | Airflow (WSL2 Ubuntu અથવા Docker) | WSL2 enable + Ubuntu distribution install |
| **Enterprise DB subscription** | EDB EPAS, PEM, EFM, MTK | EDB subscription / procurement, EDB account (downloads) |

### 4.2 RFC માં દરેક tool માટે આપવાની માહિતી (template)
```
Tool name / Vendor   :
Version              :
Purpose (business)   :
Concept / Category   :
Download source URL  :
License              :
Install location     :
Ports used           :
Data handled         :  (PII? confidential?)
Dev / Test / Prod    :
Alternatives checked :
Owner / Support      :
```

### 4.3 License સારાંશ
| License | Tools | સામાન્ય અર્થ |
|---|---|---|
| Apache 2.0 / MIT / BSD / EPL / ISC / PostgreSQL | Spark, Kafka, Debezium, Mongo connectors/drivers, Spring Boot, Flyway Community, Maven, OpenMetadata, OpenLineage, mongosh, Database Tools, **Airflow + providers**, **PostgreSQL**, pgAdmin, PostgreSQL JDBC, PgBouncer, Valkey | Permissive. Commercial use OK (notice રાખવી) |
| Commercial (EDB) | **EPAS**, PEM, EFM, MTK, EDB JDBC | EDB subscription |
| GPLv3 | Barman | Internal use OK. Distribute કરો તો legal ચકાસો |
| RSALv2 / SSPL / AGPLv3 | Redis 8 | Legal ચકાસો. વિકલ્પ: Valkey (BSD-3) |
| GPLv2 (+ exceptions) | MySQL Server, Connector/J (FOSS exception), OpenJDK (Classpath exception) | Internal use સામાન્ય રીતે OK. Software distribute કરો તો legal ચકાસો |
| SSPL | MongoDB Server, Compass | Internal use OK, પણ MongoDB ને **service તરીકે બીજાને offer** કરો તો શરતો લાગુ |
| AGPL / SSPL / ELv2 | Elasticsearch | Legal ચકાસો. વિકલ્પ: OpenSearch |
| Commercial | Docker Desktop (મોટી org) | Subscription |
| Unofficial binary | winutils | Security approval |

---

# વિભાગ 5 – Network Ports (Firewall RFC માટે)

| Port | Tool | ઉપયોગ |
|---|---|---|
| 3306 | MySQL | Source / Silver / Gold DB |
| 27017 | MongoDB | Source / Bronze DB (replica set `rs0`) |
| 9092 | Kafka | Broker (clients) |
| 9093 | Kafka | KRaft controller |
| 8083 | Kafka Connect | REST API |
| 7077 | Spark Master | Cluster URL |
| 8080 | Spark Master | Web UI |
| 8081, 8082 | Spark Workers | Web UI |
| 4040 | Spark application | Web UI (driver) |
| Random high ports | Spark driver ↔ executors | Cluster communication (production માં fix કરવા: `spark.driver.port`, `spark.blockManager.port`) |
| 8585, 8586 | OpenMetadata | UI/API, admin |
| **8090** (default 8080) | Airflow UI / API server | Spark Master UI 8080 સાથે conflict ટાળવા 8090 |
| 5432 | PostgreSQL | Airflow metadata DB / community PostgreSQL |
| 6379 | Redis / Valkey | Airflow CeleryExecutor broker (optional) |
| 5555 | Celery Flower | Airflow Celery workers UI (optional) |
| 5444 | EDB EPAS | Source / Silver / Gold (MySQL ની જગ્યાએ) |
| 8443 | EDB PEM | Monitoring web UI |

---

# વિભાગ 6 – Production માટે વધારાના Tools (હજુ install નથી)

> આ tools **હાલ install નથી**. Production RFC/planning માટે ધ્યાનમાં રાખવા.

| Area | Tool (ઉદાહરણ) | કેમ |
|---|---|---|
| OS | **Linux** (RHEL / Ubuntu LTS) | Kafka/Spark production માટે Linux standard છે |
| Kafka cluster | 3+ brokers (અથવા Confluent Cloud / AWS MSK) | High availability, replication |
| Schema management | Confluent Schema Registry / Apicurio + Avro | Event schema versioning (હાલ schemaless JSON છે) |
| Kafka UI / ops | Kafka UI (kafbat), Cruise Control, AKHQ | Topics/connectors monitoring |
| Monitoring | Prometheus + Grafana, JMX exporter | Kafka/Connect/Spark/DB metrics, alerts |
| Logging | ELK/OpenSearch, Splunk, Loki | Central logs |
| Secrets | HashiCorp Vault / Azure Key Vault / AWS Secrets Manager | Passwords properties files માં ન રાખવા |
| Orchestration | **Apache Airflow** (વિગત: વિભાગ 2.7) / Dagster / Control-M | Job dependencies, retries, SLAs |
| Enterprise SQL | **EDB EPAS** + PEM + EFM + Barman (વિગત: વિભાગ 2.8) | Organization standard હોય તો MySQL ની જગ્યાએ |
| Data quality | Great Expectations / Soda Core / OpenMetadata DQ | Declarative checks + reports |
| Lakehouse | Delta Lake / Apache Iceberg + S3/ADLS/MinIO | મોટા data માટે bronze/silver on object storage |
| Version control | Git + GitHub/GitLab/Azure Repos | Code history, code review (project હાલ git repo નથી) |
| Testing | JUnit 5 + Testcontainers | Kafka/DBs સાથે automated integration tests |
| Spark platform | Kubernetes (Spark Operator) / YARN / Databricks / EMR | Scalable, managed compute |
| Shared storage | HDFS / S3 / ADLS / MinIO | Multi-node cluster માટે |
| CI/CD | Jenkins / GitHub Actions / Azure DevOps + Nexus/Artifactory | Build, test, deploy |
| Security scanning | Snyk / OWASP Dependency-Check / Trivy | Libraries અને images ના vulnerabilities |
| DB HA / backup | MySQL InnoDB Cluster / RDS; MongoDB 3-node replica set / Atlas; backup tools | Data safety |
| TLS / Auth | Certificates, Kafka SASL/SSL, MongoDB auth, MySQL TLS | Encrypted, authenticated connections |
| Container platform | Docker Engine / Kubernetes (OpenShift, AKS, EKS) | OpenMetadata અને apps માટે |

---

# વિભાગ 7 – Production પહેલા ધ્યાન રાખવાના મુદ્દા

| # | મુદ્દો | હાલ | Production માટે સલાહ |
|---|---|---|---|
| 1 | **Spring Boot 2.7.18** | Open-source support નવેમ્બર 2023 માં પૂરો | Spring Boot 3.x પર upgrade (Java 17, `javax` → `jakarta`). Spark સાથે compatibility test કરવી |
| 2 | **MySQL 8.0.30** | 8.0 નું support April 2026 માં પૂરું | MySQL 8.4 LTS |
| 3 | **MySQL Connector/J 8.0.33** | જૂનું | Server version સાથે મેળ ખાતું નવું version |
| 4 | **MongoDB 8.3.13** (rapid release) | Debezium 3.7 નું officially listed version નથી | MongoDB 8.0 (major release) |
| 5 | **Kafka on Windows** | Officially unsupported | Linux cluster |
| 6 | **Docker Desktop license** | Org size પ્રમાણે paid | Procurement અથવા free વિકલ્પ |
| 7 | **winutils** | Unofficial binary, `winutils.exe` ખૂટે છે | Linux માં જરૂર નથી |
| 8 | **Credentials** | `root` / passwords properties/JSON files માં | Least-privilege users + secrets manager |
| 9 | **Kafka retention** | `log.retention.ms=-1` (unlimited, Windows workaround) | Linux માં યોગ્ય retention (દા.ત. 7 દિવસ) |
| 10 | **Single node everything** | 1 Kafka, 1 Mongo, 1 MySQL | HA: replication + backups |
| 11 | **Version alignment** | Spark 3.5.1 / Scala 2.12 / Java 17 / Mongo driver 5.1.x | બધા environments માં એક સરખા versions |
| 12 | **Scheduler** | Spring Boot `@Scheduled` (એક JVM) | Airflow: retries, alerts, backfill, UI. App માં "run once" mode જોઈશે |
| 13 | **DB choice: MySQL vs EPAS** | MySQL | Org standard નક્કી કરો. EPAS માટે: Debezium compatibility test, upsert SQL (`ON CONFLICT`) અને Flyway DDL બદલવા |
| 14 | **Version control** | Project git repository નથી | Git repo + branch strategy + code review |
