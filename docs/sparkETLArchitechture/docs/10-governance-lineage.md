# 10 — Data Governance, Catalog and Lineage

> Builds on docs [01](01-architecture-and-structure.md) to [09](09-airflow-orchestration.md).
> Written in **simple language**: every section starts with an **"In simple words"** box, and every abbreviation is written out in full.
> Product features and API paths of OpenMetadata/OpenLineage change between versions; places to verify in the PoC (proof of concept) are marked ⚠.

---

## 0. Abbreviations Used in This Document

| Short form | Full form | Meaning in simple words |
|---|---|---|
| **OM** | OpenMetadata | The governance tool we use: a "library catalog" for all data |
| **OL** | OpenLineage | An open standard (a common "language") for describing which job read what and wrote what |
| **FQN** | Fully Qualified Name | The complete unique name of an object in OM, e.g. `epas_prod.dataplatform.gold_sales.fact_sales_line` |
| **PII** | Personally Identifiable Information | Name, email, phone, address, ID numbers |
| **DQ** | Data Quality | Checks that data is correct |
| **SLO** | Service Level Objective | A target like "99% of days the data is fresh by 8 AM" |
| **SLA** | Service Level Agreement | A promise to users based on SLOs |
| **SSO** | Single Sign-On | Log in once with the company account |
| **RBAC** | Role-Based Access Control | Permissions given by role |
| **API** | Application Programming Interface | A way for programs to call each other |
| **REST** | Representational State Transfer | The common web API style |
| **CDC** | Change Data Capture | Capturing every change from the source database |
| **CI/CD** | Continuous Integration / Continuous Delivery | Automatic build, test and deploy |
| **UUID** | Universally Unique Identifier | A random-looking unique ID |
| **BI** | Business Intelligence | Reports and dashboards |
| **GDPR** | General Data Protection Regulation | EU privacy law |
| **DPDP** | Digital Personal Data Protection (Act, 2023) | India's privacy law |
| **EPAS** | EDB Postgres Advanced Server (EDB = EnterpriseDB) | Our PostgreSQL-based database |
| **B2S / S2G** | Bronze to Silver / Silver to Gold | The two job families |
| **ETL** | Extract, Transform, Load | Our data pipelines |
| **YAML / JSON** | YAML Ain't Markup Language / JavaScript Object Notation | Config and data text formats |

---

## 1. What Governance Means Here

> **In simple words**
> Imagine a **big public library**:
> - The **catalog** tells you which books exist, what they are about, and on which shelf they are. → **Data catalog** (which tables exist, what they mean, where they are).
> - Each section has a **librarian** responsible for it. → **Ownership** (which team owns which table).
> - A **dictionary** explains the words used in the books. → **Glossary** ("Net Revenue means …").
> - Some books are in a **locked room** and need permission. → **PII tags and access control**.
> - A book's **bibliography** shows which other books it was based on. → **Lineage** (this Gold table was built from these Silver tables, which came from these Bronze collections, which came from these source collections).
> - A **quality stamp** on each book. → **Data quality results** shown next to each table.
> - An **agreement with publishers** about what they will deliver. → **Data contracts** with the microservice teams.

Why we need it, in our platform:

| Question people will ask | Answered by |
|---|---|
| "Where does the `net_amt` number in this dashboard come from?" | Column-level lineage |
| "If `svc-order` renames a field, which reports break?" | Lineage (impact analysis) + data contracts |
| "Who do I ask about `dim_customer`?" | Ownership |
| "Is this table safe to share with an external vendor?" | PII tags / classification |
| "Can I trust today's sales numbers?" | DQ results + job run status + freshness, shown on the table page |
| "What exactly is an 'active customer'?" | Glossary |

---

## 2. Final Choice: OpenMetadata

### 2.1 Comparison for exactly this stack

| Criteria | OpenMetadata | DataHub |
|---|---|---|
| EPAS | ⚠ No dedicated connector; the **Postgres connector** is used (standard tables/views fine; Oracle-compatible EPAS objects not guaranteed) | ⚠ Same situation: Postgres source |
| MongoDB (source + Bronze) | Connector with schema sampling | Connector with schema sampling |
| Kafka topics | Messaging connector | Kafka connector |
| Airflow | Pipeline connector | Airflow plugin |
| Spark lineage | Accepts **OpenLineage** events (⚠ connector availability per version) and a lineage REST API with **column lineage** | Accepts OpenLineage; own Spark agent |
| Built-in **data quality** (test cases + results via API, incidents) | ✅ Strong | Assertions; DQ usually external |
| Glossary, domains, data products, tiers, PII classification (+ auto-classification) | ✅ Built in | ✅ Built in |
| Operations footprint | Lighter (server + database + search engine; ingestion workers) | Heavier (GMS server, Kafka, search engine, database, frontend) |
| Fit for a team starting out, 20 → 100+ pipelines | ✅ Good | Better at very large metadata volumes |

### 2.2 Decision

**OpenMetadata** (confirms doc 01). The deciding points: built-in data quality (we want to publish our DQ results there), lighter operations, and good glossary/domain/PII features. Two design choices keep us **tool-neutral**, so switching to DataHub later is a configuration change, not a rewrite:
1. Runtime lineage is emitted as **OpenLineage** events (an open standard both tools understand).
2. All governance metadata (owners, domains, tags, glossary, contracts) lives **as code in Git** and is pushed by a small publisher (§10), not typed by hand into one tool's UI.

⚠ **PoC first** (from doc 01): connect the Postgres connector to one EPAS schema, check tables/columns/profiling, check that OpenLineage events show up as lineage between the right entities, and check the DQ results API.

---

## 3. How Spark Jobs Emit Lineage

### 3.1 Options compared

> **In simple words:** lineage is the **"who used what to make what"** record. There are two ways to get it:
> - **Automatic listening:** a "spy" inside Spark watches what Spark reads and writes.
> - **Telling it ourselves:** our framework already **knows** from the job YAML exactly what it reads and writes, column by column, so it can simply **announce** it.

| Option | How | Pros | Cons in **our** design |
|---|---|---|---|
| OpenLineage Spark listener (automatic) | Add the `openlineage-spark` library and `spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener` | No code; captures what Spark itself does | ➕ **Misses the most important parts of our jobs**: (1) we write to staging through **COPY inside `foreachPartition`**, which the listener cannot see as a table write; (2) the real merge into Silver/Gold happens **inside EPAS as SQL**, completely outside Spark; (3) MongoDB reads may not be recognized as datasets (⚠ connector support varies). The automatic graph would show "Bronze → (nothing)" or "→ staging table" |
| Native tool agent (e.g. a DataHub-specific Spark agent) | Tool-specific listener | — | Same blind spots + ties us to one tool |
| **Config-driven `LineageEmitter` in the framework** | The framework builds a lineage model from the **resolved job config** (sources, targets, column mappings, lookups, measures) and sends **OpenLineage events** at run start and end | Exact and complete (including column level); covers SQL-in-EPAS steps; tool-neutral; no extra Spark overhead | We must keep the model correct → it is generated from the same config the job runs with, so it can't drift |

**Decision:** the **config-driven `LineageEmitter`** is the primary source. The automatic listener is **optional** (off by default), useful only for debugging unusual plugin code.

### 3.2 One lineage model, two uses ➕

> **In simple words:** we build the lineage **once** from the config, and use it twice:
> 1. **At deploy time:** publish the "map" (which tables connect to which, column by column), even before a job has ever run. New users see the full picture immediately.
> 2. **At every run:** send "this run of the map happened, read 1,000 records, status PARTIAL", so the catalog also shows **when** and **how well** data flowed.

```
 resolved job config (doc 06/08)
          │
          ▼
 LineageModelBuilder  ──►  LineageModel { inputs, outputs, tableEdges, columnEdges, transformations }
          │                          │
          │ deploy (CI)              │ every run (Spark driver, at start + end)
          ▼                          ▼
 OpenMetadata REST:            OpenLineage events (START, COMPLETE/FAIL/ABORT)
 PUT lineage edges              → Kafka topic etl.lineage.events
 (table + column)               → OpenMetadata OpenLineage ingestion ⚠
                                (fallback: framework calls the OM lineage REST API directly)
```

---

## 4. Table-Level and Column-Level Lineage, End to End

### 4.1 The full flow

```
 SOURCE (MongoDB, microservice)           INGESTION                         BRONZE (MongoDB)
 ┌─────────────────────────────┐   Kafka Connect + cdc.source.events   ┌─────────────────────────────┐
 │ svc-order.order_db.orders   │ ──── pipeline: etl-ingestion ───────► │ bronze_svc_order.orders     │
 │ svc-customer.customer_db.   │ ──── pipeline: etl-ingestion ───────► │ bronze_svc_customer.        │
 │   customers                 │                                       │   customers                 │
 └─────────────────────────────┘                                       └──────────────┬──────────────┘
                                                                                      │ pipeline: b2s_order
                         ┌────────────────────────────────────────────────────────────┼─────────────────────┐
                         ▼                                ▼                           ▼                     ▼
 SILVER (EPAS)  silver_order.sales_order   silver_order.sales_order_item   ..._item_discount   ..._tag
                         ▲ lookup: silver_ref.order_status_map → status_code
                         │
                         │ pipeline: s2g_fact_sales_line (+ dims via SK lookups)
                         ▼
 GOLD (EPAS)    gold_sales.fact_sales_line  ◄── gold_core.dim_customer ◄── silver_customer.customer_hist ◄── bronze_svc_customer.customers
                         │                  ◄── gold_core.dim_product, dim_store, dim_date, dim_order_profile
                         │ pipeline: s2g_agg_sales_daily_store (SQL)
                         ▼
                gold_sales.agg_sales_daily_store  ──► BI dashboards (optional: BI tool connector in OM)
```

### 4.2 The shared Kafka topic: avoid "false lineage" ➕

> **In simple words:** all collections go through **one** Kafka topic (doc 02). If we drew the topic as a box in the middle, the picture would say "`orders` feeds `customers`" (because both go through the same box). That is wrong and confusing.

**Rule:** lineage edges go **directly** from each source collection to its own Bronze collection. The ingestion service is the **pipeline** on that edge, and the Kafka topic is shown as an **attribute** of the pipeline ("transport: `cdc.source.events`"), not as a node in the middle of the graph.

### 4.3 Where column-level lineage comes from

| Step | Source of column lineage | Example |
|---|---|---|
| Source → Bronze | 1:1 by field (Bronze `data.*` = source fields) + technical `_etl.*` fields | `orders.orderNo` → `bronze…orders.data.orderNo` |
| Bronze → Silver (split tables) | Target `columns:` in the job YAML (`source:` path → target column) + `transform:` list as the description | `data.orderNo` → `sales_order.order_no` (trim, upper) |
| Child tables | `from:` array path + child columns | `data.items[].sku` → `sales_order_item.sku` |
| Lookups / code mapping | `map: {refTable}` | `silver_ref.order_status_map.std_code` + `data.status` → `sales_order.status_code` |
| Silver → Gold dimensions | Dimension `columns:` + SCD config | `customer_hist.segment` → `dim_customer.segment`, `previous_segment` |
| Silver → Gold facts: measures | **Column references parsed from the measure expression** ➕ (Spark's own SQL expression parser lists the referenced columns) | `net_amt` ← `qty`, `unit_price`, `line_discount_total` |
| Facts: surrogate keys | The dimension lookup config | `dim_customer.customer_sk` → `fact_sales_line.customer_sk` (via `customer_id`) |
| Aggregates | Measures in the aggregate YAML | `fact_sales_line.net_amt` → `agg_sales_daily_store.net_amt` (SUM) |
| Plugins | Declared `requiredColumns` → `producedColumns` (doc 06 §10) | coarse but honest |

```java
// etl-core: lineage/MeasureColumnRefs.java — "net_amt = qty * unit_price - COALESCE(line_discount_total, 0)"
public static Set<String> referencedColumns(String sqlExpression) {
    Expression e = CatalystSqlParser.parseExpression(sqlExpression);         // Spark's built-in SQL parser
    Set<String> cols = new TreeSet<>();
    e.foreach(node -> {                                                         // walk the expression tree
        if (node instanceof UnresolvedAttribute a) cols.add(a.name());
        return null;
    });
    return cols;                                                                // [line_discount_total, qty, unit_price]
}
```

### 4.4 What is **not** in the business lineage

Staging tables (`stg_*`), `etl_ctl.*` control tables, quarantine and DLQ (Dead Letter Queue) are **technical**. They are excluded from the business lineage graph (filters in §5), so the picture stays readable. They are still documented in OM under a "Platform" domain for the data team.

### 4.5 An OpenLineage event (what the emitter sends)

```json
{
  "eventType": "COMPLETE",
  "eventTime": "2026-10-30T09:16:40.210Z",
  "producer": "https://github.com/company/data-platform/etl-core/1.14.2",
  "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/definitions/RunEvent",
  "run": {
    "runId": "4b0c8f8e-6d2e-5f4c-9a8b-1f7c2a3e9d10",
    "facets": {
      "etl_run": { "_producer": "etl-core", "_schemaURL": "https://company/schemas/etl_run.json",
                   "runId": "b2s_order-20261030T090000Z-a1", "runMode": "INCREMENTAL", "status": "PARTIAL",
                   "read": 1000, "success": 975, "failed": 10, "quarantined": 7, "skipped": 5,
                   "codeVersion": "1.14.2", "configHash": "9f2c…" }
    }
  },
  "job": { "namespace": "etl-prod", "name": "b2s_order",
           "facets": { "ownership": { "owners": [{ "name": "team-order-data", "type": "TEAM" }] } } },
  "inputs": [
    { "namespace": "mongodb://bronze-prod", "name": "bronze_svc_order.orders" },
    { "namespace": "postgres://epas-prod.db.internal:5444", "name": "dataplatform.silver_ref.order_status_map" }
  ],
  "outputs": [
    { "namespace": "postgres://epas-prod.db.internal:5444", "name": "dataplatform.silver_order.sales_order",
      "facets": {
        "columnLineage": { "fields": {
          "order_no":    { "inputFields": [{ "namespace": "mongodb://bronze-prod", "name": "bronze_svc_order.orders", "field": "data.orderNo" }],
                           "transformationDescription": "trim, upper" },
          "status_code": { "inputFields": [
                             { "namespace": "mongodb://bronze-prod", "name": "bronze_svc_order.orders", "field": "data.status" },
                             { "namespace": "postgres://epas-prod.db.internal:5444", "name": "dataplatform.silver_ref.order_status_map", "field": "std_code" }],
                           "transformationDescription": "trim, upper, map via order_status_map" } } },
        "outputStatistics": { "rowCount": 978 } } }
  ]
}
```

| Detail | Decision |
|---|---|
| OpenLineage needs a UUID run id | ➕ `UUID v5` derived from our `run_id` text (deterministic, so the same run always gets the same UUID); our readable id is kept in the `etl_run` facet |
| Namespaces/names must match the OM entities | ⚠ Configure the mapping in the OM OpenLineage ingestion (database/messaging service names) during the PoC; one mapping table in `governance/openmetadata/lineage/namespace-map.yml` |
| When events are sent | `START` after the lock, `COMPLETE` (SUCCESS/PARTIAL), `FAIL` (FAILED), `ABORT` (CANCELLED) |
| Dry-runs | Not sent |
| Dev environment | Lineage disabled (doc 08 `features.lineage.enabled: false`), so the prod catalog stays clean; qa/uat send to their own OM instance or not at all |
| ➕ Governance must never break ETL | Sending is **asynchronous and best-effort**: if Kafka or OM is down, the job logs a warning and continues. Events are buffered in Kafka (retention 7 days) |

---

## 5. Metadata Ingestion into OpenMetadata

> **In simple words:** OM needs to "scan the shelves" regularly to know which tables, columns and topics exist. These scans are called **ingestion workflows**. Each one connects to one system (EPAS, MongoDB, Kafka, Airflow), reads the structure (not the data), and updates the catalog.

### 5.1 Services and workflows

| OM service | Connects to | What it brings | Schedule |
|---|---|---|---|
| `epas_prod` (Postgres connector) | EPAS `dataplatform` DB, read-only user `om_reader` | Schemas `silver_*`, `silver_ref`, `gold_*`, `etl_ctl` (platform domain); **excluded:** `stg_*`, temp/partition children | Metadata daily; usage (query log, optional) daily; profiler weekly for Gold |
| `mongo_sources_prod` | Source microservice MongoDBs (read-only) | Source collections + sampled schemas | Daily |
| `mongo_bronze_prod` | Bronze MongoDB | Bronze collections | Daily |
| `kafka_prod` | Kafka cluster | Topics (`cdc.source.events`, DLQ, events, lineage topic) | Daily |
| `airflow_prod` | Airflow | DAGs and tasks as pipelines, run history | Hourly |
| `etl_jobs_prod` (custom pipeline service) | Created by **our publisher** (§10) | One pipeline per ETL job, with its steps, owner, schedule and run statuses | On deploy + every run |
| `openlineage_prod` ⚠ | Kafka topic `etl.lineage.events` | Runtime lineage + run facets | Continuous / every few minutes |

➕ **Where the ingestion workflows run:** OM's ingestion framework is a Python package with its own (sometimes conflicting) dependencies. Run it in **OM's own ingestion deployment** (the OM Helm chart's ingestion component, or K8s CronJobs with the official ingestion image), **not** inside our ETL Airflow. This refines doc 01 ("reuses Airflow"): we don't mix OM's Python packages into the ETL Airflow image.

### 5.2 Sample ingestion configs ⚠ (field names per OM version)

```yaml
# governance/openmetadata/ingestion/epas_prod.yaml
source:
  type: postgres
  serviceName: epas_prod
  serviceConnection:
    config:
      type: Postgres
      hostPort: epas-prod.db.internal:5444
      database: dataplatform
      username: om_reader
      authType: { password: "${OM_EPAS_PASSWORD}" }     # injected from Vault at runtime, never in Git
      sslMode: verify-full
  sourceConfig:
    config:
      type: DatabaseMetadata
      schemaFilterPattern:
        includes: ["^silver_.*", "^gold_.*", "^silver_ref$", "^etl_ctl$"]
        excludes: ["^stg_.*"]
      tableFilterPattern:
        excludes: [".*_\\d{4}_\\d{2}$", ".*_default$", ".*__rebuild$", ".*__old$"]   # partitions, shadow tables
      includeViews: true
      markDeletedTables: true
sink: { type: metadata-rest, config: {} }
workflowConfig:
  openMetadataServerConfig:
    hostPort: https://openmetadata.internal/api
    authProvider: openmetadata
    securityConfig: { jwtToken: "${OM_BOT_TOKEN}" }
```

```yaml
# governance/openmetadata/ingestion/epas_prod_profiler.yaml  (Gold only, weekly)
source:
  type: postgres
  serviceName: epas_prod
  sourceConfig:
    config:
      type: Profiler
      schemaFilterPattern: { includes: ["^gold_.*"] }
      profileSample: 10                  # percent
      generateSampleData: false          # ➕ NEVER store sample rows: they could contain PII
```

```yaml
# governance/openmetadata/ingestion/mongo_bronze_prod.yaml
source:
  type: mongodb
  serviceName: mongo_bronze_prod
  serviceConnection:
    config:
      type: MongoDB
      connectionDetails:
        connectionURI: "${OM_MONGO_BRONZE_URI}"     # read-only user, from Vault
  sourceConfig:
    config:
      type: DatabaseMetadata
      databaseFilterPattern: { includes: ["^bronze_.*"] }
```

```yaml
# governance/openmetadata/ingestion/kafka_prod.yaml
source:
  type: kafka
  serviceName: kafka_prod
  serviceConnection:
    config:
      type: Kafka
      bootstrapServers: kafka-prod-0.internal:9093,kafka-prod-1.internal:9093
      consumerConfig: { security.protocol: SASL_SSL, sasl.mechanism: SCRAM-SHA-512 }
  sourceConfig:
    config:
      type: MessagingMetadata
      topicFilterPattern: { includes: ["^cdc\\..*", "^dlq\\..*", "^etl\\..*"] }
      generateSampleData: false          # ➕ topic samples would contain PII
```

```yaml
# governance/openmetadata/ingestion/airflow_prod.yaml
source:
  type: airflow
  serviceName: airflow_prod
  serviceConnection:
    config:
      type: Airflow
      hostPort: https://airflow-prod.internal
      connection: { type: BackendConnection }     # ⚠ or a REST/DB connection depending on the OM version
  sourceConfig:
    config:
      type: PipelineMetadata
      pipelineFilterPattern: { includes: ["^etl__.*", "^etl_.*"] }
      includeLineage: false              # lineage comes from our emitter, not from Airflow
```

```yaml
# governance/openmetadata/ingestion/openlineage_prod.yaml  ⚠ verify availability/field names in your OM version
source:
  type: openlineage
  serviceName: openlineage_prod
  serviceConnection:
    config:
      type: OpenLineage
      brokersUrl: kafka-prod-0.internal:9093
      topicName: etl.lineage.events
      consumerGroupName: openmetadata-openlineage
      securityProtocol: SASL_SSL
  sourceConfig:
    config: { type: PipelineMetadata }
```

---

## 6. Catalog, Ownership, Glossary, Domains and PII

### 6.1 Governance as code ➕

> **In simple words:** who owns what, which domain a table belongs to, which columns are PII, what a business term means: all of this is written in **files in Git**, reviewed like code, and pushed to OM automatically. The OM UI is for **finding and understanding** data, and for conversations, not for typing in the official ownership or PII flags (they would be overwritten at the next deploy anyway).

| Field in OM | Managed by | Where |
|---|---|---|
| Domain, data product | Code | `governance/openmetadata/domains.yml` + job/table `domain` |
| Owner (team) | Code | Job YAML `owner` (for the tables it writes); data contract `owner` (for source collections) |
| Table/column description | Code (preferred), UI allowed for **new** descriptions ➕ | Contract field descriptions, target column `description:` |
| PII / sensitivity tags | Code | Data contracts (`pii: true`) + propagation rules (§6.5) |
| Glossary terms | Code | `governance/openmetadata/glossary/*.yml` + `kpis.yml` |
| Tier (criticality) | Code | Job YAML `orchestration.priority` → Tier |
| Custom properties (owning job, schedule, freshness SLA, last load) | Code + runtime | Publisher (§10) |
| Conversations, announcements, tasks | UI | OM UI |

### 6.2 Domains and data products

```yaml
# governance/openmetadata/domains.yml
domains:
  - name: Order
    description: "Orders, items, returns, fulfilment"
    owners: [team-order-data]
  - name: Customer
    description: "Customers, segments, addresses"
    owners: [team-customer-data]
  - name: Sales
    description: "Sales analytics marts"
    owners: [team-sales-analytics]
  - name: Finance
    owners: [team-finance-analytics]
  - name: Platform
    description: "Technical tables: control, audit, quarantine, staging"
    owners: [data-platform]

dataProducts:                                   # ➕ a "ready-to-use" bundle for consumers
  - name: SalesAnalytics
    domain: Sales
    description: "Star schema for sales reporting"
    assets: [gold_sales.fact_sales_line, gold_sales.agg_sales_daily_store, gold_core.dim_customer,
             gold_core.dim_product, gold_core.dim_store, gold_core.dim_date]
    owners: [team-sales-analytics]
```

| Our concept | OM concept |
|---|---|
| `domain:` in job YAML / contracts | Domain |
| Gold mart (star schema for one purpose) | Data product |
| `owner:` team | Team (synced from your identity provider via SSO groups) |
| `orchestration.priority` HIGH / NORMAL / LOW | Tier 1 / Tier 2 / Tier 3 |

### 6.3 Ownership rules

| Asset | Owner |
|---|---|
| Source collection (microservice DB) | The microservice team (from the data contract) |
| Bronze collection | Data platform team (technical copy) + the source team as "expert" |
| Silver / Gold table | The `owner` of the **one** job that writes it (`job_target`, doc 05) |
| Shared Gold dimensions (`gold_core`) | Their owner job's team |
| `etl_ctl`, staging | Data platform team |

➕ **Every table must have an owner.** A governance check (§9) fails when a table in OM has no owner, or when an owner team has no members.

### 6.4 Glossary

```yaml
# governance/openmetadata/glossary/business.yml
glossary: Business
terms:
  - name: Net Revenue
    description: "Revenue after discounts, excluding cancelled/deleted order lines."
    synonyms: [Net Sales]
    owners: [team-finance-analytics]
    relatedTerms: [Gross Revenue, Discount]
    linkedAssets:                                  # glossary term → column
      - gold_sales.fact_sales_line.net_amt
      - gold_sales.agg_sales_daily_store.net_amt
    kpi: net_revenue                               # → pipelines/silver-to-gold/sales/kpis.yml (doc 04 §10.3)
  - name: Active Customer
    description: "A customer with at least one non-cancelled order in the last 90 days."
    owners: [team-customer-data]
```

The publisher checks that every KPI in `kpis.yml` has a glossary term, and the other way round ➕, so definitions and numbers never drift apart.

### 6.5 PII tagging and classification

> **In simple words:** every column gets a **sensitivity label**, like the colour stickers on files in an office: green = public, yellow = internal, orange = confidential, red = restricted (personal data). The labels are set in the **data contracts** and then **automatically passed along** the lineage: if a Gold column is made from a PII column, it is also PII, **unless** it was masked or aggregated on the way.

**Tags used:**

| Classification | Tags | Meaning |
|---|---|---|
| `PII` (OM built-in) | `PII.Sensitive`, `PII.NonSensitive` | Personal data or not |
| `Sensitivity` (ours) | `Public`, `Internal`, `Confidential`, `Restricted` | How strictly access must be controlled |
| `PIIType` (ours) ➕ | `Email`, `Phone`, `Name`, `Address`, `NationalId`, `DateOfBirth`, `Financial` | What kind of personal data (drives the masking rule, doc 07 §6.4) |
| `Treatment` (ours) ➕ | `Masked`, `Hashed`, `Encrypted`, `Aggregated` | What was done to it on the way |

**Propagation rules (applied by the publisher along column lineage):**

| Upstream column | Transformation on the way | Downstream column gets |
|---|---|---|
| `PII.Sensitive` + `PIIType.Email` | none / trim / lower | same tags (still PII) |
| `PII.Sensitive` | `maskEmail` / `maskPhone` | `PII.Sensitive` + `Treatment.Masked` + `Sensitivity.Confidential` |
| `PII.Sensitive` | hash (deterministic) | `PII.Sensitive` + `Treatment.Hashed` (hashes of emails can still identify people) |
| `PII.Sensitive` | used only inside `COUNT(DISTINCT …)` | `PII.NonSensitive` + `Treatment.Aggregated` |
| `PII.Sensitive` | `NationalId` anywhere | ❌ **Not allowed** in Silver/Gold unless encrypted (CI check) ➕ |

**Auto-classification** (OM's PII detection by column names and sample patterns) runs as a **safety net**. If it finds a likely PII column that the contracts do **not** mark as PII, an alert goes to the owner ("is `notes` really free of personal data?"). Contracts stay the source of truth.

### 6.6 Access control in OM

| Rule | How |
|---|---|
| Log in via SSO | OM SSO with the company identity provider; teams from SSO groups |
| Seeing metadata (names, descriptions, lineage) | Everyone in the company (that is the point of a catalog) |
| Sample data / profiles of `Restricted` tables | Disabled (`generateSampleData: false`) or restricted to the owner team ➕ |
| Editing ownership, tags, domains in the UI | Only the platform bot (from Git) + OM admins |
| The governance tool **never** gives data access | Real data access is granted in EPAS/Mongo by roles (doc 05 §2.3); OM only documents it |

---

## 7. Publishing DQ Results and Run Status

### 7.1 Data quality results

> **In simple words:** our Spark DQ engine already checks the data at every run (doc 03 §5). We do **not** run the same checks a second time inside OM. We **send the results** to OM, so each table page shows a quality history: "rule SO-002 passed 3,000 times, failed twice last week".

| Option | Verdict |
|---|---|
| Let OM run its own tests on the tables | ❌ Double work, second set of rules to maintain, extra database load |
| **Push our `dq_results` into OM as test cases + test results** | ✅ One rule definition (our YAML), one execution (Spark), visible in OM |

**Rule type → OM test definition:**

| Our rule type (doc 03 §5.3) | OM test definition ⚠ (names per version) |
|---|---|
| `not_null` | `columnValuesToBeNotNull` |
| `range` | `columnValuesToBeBetween` |
| `allowed_values` | `columnValuesToBeInSet` |
| `regex` | `columnValuesToMatchRegex` |
| `length` | `columnValueLengthsToBeBetween` |
| `unique_in_batch` | `columnValuesToBeUnique` |
| `referential`, `expression`, `cross_field`, `freshness`, `custom` | `tableCustomSQLQuery` (described as "evaluated by etl-core") or a custom test definition `etlRule` ➕ |
| Reconciliation checks (doc 05 §6.12) | Custom test definition `etlReconciliation` (table level) |

**Test case naming:** `<rule_id>__<column>` on the target table, e.g. `SO-002__total_amount` on `gold`/`silver` tables. One test **case** per rule and table (created at deploy time from the YAML), one test **result** per run (pushed after each run).

### 7.2 Run status

| What | How it appears in OM |
|---|---|
| Each ETL job | A pipeline entity in the `etl_jobs_prod` service, with its owner, schedule, sources, targets and steps as tasks |
| Each run | A pipeline status: `run_etl` task = Successful (SUCCESS/PARTIAL) / Failed; `quality_gate` task = Successful / Failed (PARTIAL) ➕ (same two-box idea as Airflow, doc 09 §9) |
| Run details | Link to the ops UI run page; counts in the status description |
| Freshness | Custom property `lastSuccessfulLoad` (timestamp + run id) and `freshnessSlaMinutes` on each target table ➕ |

### 7.3 Never let governance slow down or break ETL ➕

> **In simple words:** the Spark job **doesn't talk to OM directly**. It writes its results into our own tables (`dq_results`, `reconciliation`, `job_run_audit`), which it does anyway. A separate **publisher** reads new rows from those tables and sends them to OM. If OM is down for an hour, the publisher simply catches up later. ETL never waits for OM.

```
 Spark job ──► etl_ctl.job_run_audit / dq_results / reconciliation   (already written, doc 05)
                       │
                       ▼  GovernancePublisher (inside etl-ops-api, every 2 minutes)
            reads rows newer than its bookmark  (etl_ctl.watermark: job_name = 'governance_publisher')
                       │
                       ├─► OM: test case results (DQ + reconciliation)
                       ├─► OM: pipeline status per run
                       ├─► OM: custom property lastSuccessfulLoad on target tables
                       └─► moves its bookmark only after OM accepted the batch (retries with backoff)
```

This reuses the bookmark table of doc 05. The OpenLineage events (§4.5) are the only thing sent from the job itself, and they go to Kafka (buffered), not to OM directly.

---

## 8. Data Contracts with the Microservice Teams

### 8.1 What a data contract is

> **In simple words:** a **written agreement** between a microservice team (the "producer" of the data) and the data team (the "consumer"), like a contract between a supplier and a shop:
> - **what** is delivered (fields, types, which are mandatory),
> - **what it means** (descriptions),
> - **how sensitive** it is (PII flags),
> - **how good** it will be (freshness, completeness),
> - **how changes are announced** (versioning, notice period),
> - **who to call** (owners, contacts).
>
> Without it, a developer renames `amount` to `totalAmount` on a Friday, and on Monday ten dashboards show zero sales.

### 8.2 Contract file (extends doc 02 §12)

```yaml
# contracts/svc-order/orders.contract.yml
contract:
  id: svc-order.orders
  version: 3.1.0                         # SemVer: MAJOR = breaking, MINOR = additive, PATCH = docs only
  status: ACTIVE                          # DRAFT | ACTIVE | DEPRECATED
  producer:
    service: svc-order
    team: team-order                      # microservice team (GitHub team for CODEOWNERS)
    contacts: ["#order-team", "order-team@company.example"]
  consumer:
    team: data-platform
  domain: Order
  source:
    database: order_db
    collection: orders
    delivery: { mode: CDC, connector: mongo-source-svc-order, topic: cdc.source.events }
  description: "One document per customer order, created at checkout and updated through fulfilment."

  schema:
    unknownFields: WARN                   # new fields are allowed (additive) and reported
    fields:
      _id:          { type: objectId, required: true, description: "Order id" }
      orderNo:      { type: string,   required: true, maxLength: 30, unique: true, description: "Human order number ORD-…" }
      orderDate:    { type: date,     required: true, description: "Checkout time (UTC)" }
      status:       { type: string,   required: true, allowed: [NEW, CONFIRMED, SHIPPED, DELIVERED, CANCELLED] }
      amount:       { type: decimal,  required: true, min: 0, description: "Order total incl. tax, in currency" }
      currency:     { type: string,   required: true, allowed: [INR, USD, EUR, GBP] }
      customer.id:  { type: objectId, required: true, references: svc-customer.customers._id }
      customer.email: { type: string, pii: { type: Email }, description: "Snapshot at checkout" }
      items:        { type: array,    required: true, minItems: 1 }
      items[].sku:  { type: string,   required: true }
      items[].qty:  { type: int,      required: true, min: 1 }

  quality:                                # ➕ promises (SLOs) from the producer
    freshness: { maxDelayMinutes: 15 }    # CDC lag at the source side
    completeness: { "customer.id": 99.9 } # % of documents where it is present
  changePolicy:
    breakingChangeNoticeDays: 30
    deprecationPeriodDays: 60
    compatibility: BACKWARD               # new versions must still be readable by the current ETL
```

### 8.3 Where contracts live and who approves

| Decision | Detail |
|---|---|
| Location | `data-platform` repo, `contracts/<service>/<collection>.contract.yml` (doc 01) |
| Approval | **CODEOWNERS** make the microservice team **and** the data platform team required reviewers of their contract files ➕ |
| Change in the microservice repo | ➕ A small **contract check** (a Maven plugin / CLI built from `etl-config`, e.g. `etl-contract-check`) runs in the microservice's CI. It validates the service's sample documents (or its entity classes) against the published contract. A breaking change fails **their** build before it reaches production |

### 8.4 What the contract drives automatically

| Consumer of the contract | What it does with it |
|---|---|
| `etl-ingestion` | Schema drift detection (doc 02 §12) |
| Spark B2S reader | The explicit Spark schema (doc 03 §2.2) |
| DQ rules ➕ | A **baseline rule set** is generated: `required` → `not_null`, `allowed` → `allowed_values`, `min/max` → `range`, `maxLength` → `length`. Job-specific rules are added on top |
| PII handling | Masking in quarantine snapshots (doc 07), Gold masking, OM PII tags (§6.5) |
| OpenMetadata | Descriptions, owners, tags and the contract version on the source collection entity (⚠ OM's own "data contract" feature can be used if your version has it; otherwise custom properties) |

### 8.5 Change process

```
 Microservice developer wants to change orders
        │
        ├─ ADDITIVE (new optional field)  → MINOR version
        │     contract PR (source team) → data team review → merge
        │     → ETL picks the field up via _unmapped / new column when needed (doc 03 §4.4)
        │
        └─ BREAKING (remove/rename field, type change, new required field, changed meaning) → MAJOR version
              1. contract PR with the new MAJOR version + migration note, ≥ 30 days notice
              2. OM impact analysis: lineage shows every Silver/Gold table and dashboard affected ➕
              3. ETL team prepares a compatible job version (reads old AND new shape), deploys it FIRST
              4. microservice deploys the change
              5. after the deprecation period, ETL removes the old-shape handling
```

➕ **Contract breach monitoring:** when the ingestion drift detection or the Silver DQ finds data that violates the contract (e.g. a "required" field missing in 5% of new documents), an alert goes to **both** teams with a link to the contract. The producer team fixes the source; the data team doesn't have to "clean up" silently forever.

---

## 9. Governance Checklist

### 9.1 For every new source collection

- [ ] Data contract written, version 1.0.0, both teams approved (CODEOWNERS)
- [ ] PII fields marked with `pii.type`; `NationalId`-type fields have an encryption/drop decision
- [ ] Domain and producer team set
- [ ] Contract check added to the microservice's CI
- [ ] Source collection appears in OM (`mongo_sources_*`) with owner and description
- [ ] Lineage edge source → Bronze visible

### 9.2 For every new job (in addition to doc 06 §16.3)

- [ ] `owner`, `domain`, `description` filled; column `description`s for all Gold columns
- [ ] Target tables appear in OM with the right domain, owner and tier
- [ ] Table-level **and** column-level lineage visible after deploy (design-time lineage)
- [ ] DQ test cases visible in OM; first run's results arrived
- [ ] PII tags propagated correctly; masked columns tagged `Treatment.Masked`
- [ ] Gold KPI columns linked to glossary terms; new KPIs have a glossary term
- [ ] Gold mart assets added to a data product

### 9.3 Every release (automated in CI)

- [ ] Every YAML `owner` exists as an OM team
- [ ] Every KPI ↔ glossary term pair complete
- [ ] No `NationalId` PII column in Silver/Gold without encryption
- [ ] No Gold table without a description
- [ ] Contract versions: breaking changes only with a MAJOR bump and an approved migration note

### 9.4 Periodic (owner: data platform team)

| Frequency | Check |
|---|---|
| Weekly | Tables in OM without owner; orphan tables (exist in the DB but no job writes them) ➕; failed publisher batches |
| Monthly | Auto-classification findings not in contracts (possible hidden PII); stale tables (not updated for > 30 days); lineage edges for disabled jobs |
| Quarterly | Access review: who can reveal PII (doc 07), who has `PLATFORM_ADMIN`, OM admin list; glossary review with business owners |
| Yearly | Retention policy review with legal/finance (doc 05 §8) |

---

## 10. Code and Folder Structure

### 10.1 Folders

```
data-platform/
├── contracts/                                   # data contracts (CODEOWNERS: source team + data platform)
│   └── <service>/<collection>.contract.yml
├── governance/
│   └── openmetadata/
│       ├── ingestion/                           # OM ingestion workflows per env (§5.2)
│       │   ├── {dev,qa,uat,prod}/epas.yaml, mongo_sources.yaml, mongo_bronze.yaml, kafka.yaml, airflow.yaml, openlineage.yaml
│       ├── domains.yml                          # domains + data products (§6.2)
│       ├── teams.yml                            # team ↔ SSO group mapping (if not synced automatically)
│       ├── glossary/business.yml                # glossary terms (§6.4)
│       ├── classifications.yml                  # Sensitivity, PIIType, Treatment tags (§6.5)
│       ├── pii-propagation.yml                  # propagation rules (§6.5)
│       ├── tiers.yml                            # priority → tier mapping
│       └── lineage/namespace-map.yml            # OpenLineage namespace → OM service names ⚠
├── etl-core/.../lineage/                        # LineageModelBuilder, OpenLineageEmitter (runtime)
├── etl-governance/                              # ➕ new module: OM REST client + publishers (no Spark, no Spring)
│   └── src/main/java/com/company/etl/governance/
│       ├── model/                               # LineageModel, TableEdge, ColumnEdge (shared with etl-core)
│       ├── om/OpenMetadataClient.java           # thin REST client (lineage, tables, tests, pipelines, tags)
│       ├── publish/DesignTimePublisher.java     # deploy time: lineage, owners, domains, tags, glossary, test cases
│       ├── publish/RuntimePublisher.java        # every 2 min: DQ results, pipeline status, freshness
│       └── contracts/ContractChecker.java       # used by CI and the microservices' contract check
└── tools/governance-cli/                        # CI entry point: `governance publish --env prod`
```

**Module dependencies:** `etl-governance` depends only on `etl-common` and `etl-config` (no Spark, no Spring, ArchUnit-checked). `etl-ops-api` wraps `RuntimePublisher` as a scheduled Spring bean. CI runs `DesignTimePublisher` through `tools/governance-cli` as an extra deploy step after the registry sync (doc 08 §9).

### 10.2 Java skeletons

```java
// etl-governance: model/LineageModel.java
public record LineageModel(String jobName, List<DatasetRef> inputs, List<DatasetRef> outputs,
                           List<TableEdge> tableEdges, List<ColumnEdge> columnEdges) {}

public record DatasetRef(Store store, String service, String fqn) {}        // e.g. EPAS, epas_prod, dataplatform.gold_sales.fact_sales_line
public record TableEdge(DatasetRef from, DatasetRef to, String pipeline) {}
public record ColumnEdge(DatasetRef fromTable, List<String> fromColumns,
                         DatasetRef toTable, String toColumn, String transformation) {}
```

```java
// etl-core: lineage/LineageModelBuilder.java — built from the SAME resolved config the job runs with
public final class LineageModelBuilder {

    public LineageModel build(JobConfig cfg, EnvConfig env) {
        return switch (cfg) {
            case B2SConfig b       -> forB2S(b, env);
            case DimensionConfig d -> forDimension(d, env);
            case FactConfig f      -> forFact(f, env);
            case SnapshotConfig s  -> forSnapshot(s, env);
            case AggregateConfig a -> forAggregate(a, env);
        };
    }

    private LineageModel forB2S(B2SConfig c, EnvConfig env) {
        DatasetRef bronze = DatasetRefs.mongo(env, c.source().database(), c.source().collection());
        List<ColumnEdge> cols = new ArrayList<>();
        List<TableEdge> tables = new ArrayList<>();
        for (TargetSpec t : c.targets()) {
            DatasetRef target = DatasetRefs.epas(env, t.table());
            tables.add(new TableEdge(bronze, target, c.job().name()));
            t.columns().forEach((col, spec) -> cols.add(new ColumnEdge(
                    bronze, List.of(t.qualify(spec.source())), target, col, String.join(", ", spec.transform()))));
            t.columns().values().stream().filter(s -> s.map() != null).forEach(s -> {
                DatasetRef ref = DatasetRefs.epas(env, s.map().refTable());
                tables.add(new TableEdge(ref, target, c.job().name()));
            });
        }
        return new LineageModel(c.job().name(), inputsOf(tables), outputsOf(tables), tables, cols);
    }
    // forFact(): measures → MeasureColumnRefs.referencedColumns(expr) (§4.3); dimension SK edges; date keys
}
```

```java
// etl-core: lineage/OpenLineageEmitter.java — best effort, never fails the job
public final class OpenLineageEmitter implements LineageEmitter {
    private final KafkaProducer<String, String> producer;     // topic etl.lineage.events
    private final LineageModel model;
    private final boolean enabled;

    @Override
    public void runStarted(RunContext ctx, List<String> in, List<String> out) {
        send(ctx, OpenLineageEvents.start(ctx, model));
    }

    @Override
    public void runCompleted(RunContext ctx, JobResult r) {
        send(ctx, switch (r.status()) {
            case SUCCESS, PARTIAL -> OpenLineageEvents.complete(ctx, model, r);   // with columnLineage + etl_run facets
            case CANCELLED        -> OpenLineageEvents.abort(ctx, model, r);
            default               -> OpenLineageEvents.fail(ctx, model, r);
        });
    }

    private void send(RunContext ctx, String json) {
        if (!enabled || ctx.dryRun()) return;
        try {
            producer.send(new ProducerRecord<>("etl.lineage.events", ctx.jobName(), json),
                          (meta, ex) -> { if (ex != null) LOG.warn("Lineage event not sent for {}", ctx.runId(), ex); });
        } catch (RuntimeException e) {
            LOG.warn("Lineage emitter failed for {} (ignored)", ctx.runId(), e);       // ➕ governance never breaks ETL
        }
    }
}
```

```java
// etl-governance: publish/RuntimePublisher.java — runs inside etl-ops-api every 2 minutes
public final class RuntimePublisher {
    private final ControlStore control;          // etl-control DAOs
    private final OpenMetadataClient om;

    public void publishOnce() {
        long from = control.watermarks().current("governance_publisher", "etl_ctl.dq_results").orElse(0L);
        Batch batch = control.governance().newSince(from, 500);        // dq_results + reconciliation + finished runs
        if (batch.isEmpty()) return;
        for (DqResultRow r : batch.dqResults())       om.addTestCaseResult(TestCases.fqn(r), TestCases.result(r));
        for (ReconRow r : batch.reconciliation())     om.addTestCaseResult(TestCases.reconFqn(r), TestCases.result(r));
        for (RunRow run : batch.finishedRuns()) {
            om.putPipelineStatus(Pipelines.fqn(run.jobName()), Pipelines.status(run));      // run_etl + quality_gate
            run.targets().forEach(t -> om.setCustomProperty(Tables.fqn(t), "lastSuccessfulLoad", run.endedAtAndId()));
        }
        control.watermarks().advanceStandalone("governance_publisher", "etl_ctl.dq_results", batch.maxSeq());  // after OM accepted
    }
}
```

---

## 11. ➕ Added by Architect (summary)

| # | Addition | Why |
|---|---|---|
| GV1 | Config-driven lineage emitter instead of the automatic Spark listener | The listener can't see COPY-in-`foreachPartition` or the SQL merges inside EPAS |
| GV2 | One `LineageModel` for design-time publishing and runtime events | Full map before the first run; no drift between the two |
| GV3 | Column lineage from measure expressions via Spark's SQL parser | Exact column lineage for Gold measures |
| GV4 | Shared Kafka topic shown as a pipeline attribute, not a graph node | No false "orders → customers" lineage |
| GV5 | Technical tables (`stg_*`, `etl_ctl`, quarantine) excluded from business lineage | A readable graph |
| GV6 | UUID v5 from our `run_id`; our id in a custom facet | Deterministic OpenLineage run ids |
| GV7 | Lineage via Kafka, async and best-effort; DQ/run status via an outbox-style publisher with a bookmark | Governance outages never slow down or fail ETL |
| GV8 | OM ingestion in its own deployment, not in the ETL Airflow | No Python dependency conflicts |
| GV9 | `generateSampleData: false` for profiler and Kafka | Sample rows would leak PII into the catalog |
| GV10 | Governance as code (owners, domains, tags, glossary, tiers) | Reviewed, versioned, reproducible in every environment |
| GV11 | PII taxonomy (`PIIType`, `Treatment`) + propagation rules along lineage + CI ban on unencrypted national IDs | PII is tracked end to end, including what was masked |
| GV12 | Auto-classification as a safety net with owner alerts | Finds PII the contracts missed |
| GV13 | DQ results pushed as OM test results (no duplicate execution); reconciliation as custom tests | One rule source, one execution, full visibility |
| GV14 | Pipeline status with the `run_etl` + `quality_gate` split; `lastSuccessfulLoad` per table | Same PARTIAL semantics everywhere; freshness visible on the table page |
| GV15 | Contract SLOs, change policy, CODEOWNERS co-approval, contract check in the microservices' CI | Breaking changes are caught in the producer's build |
| GV16 | Baseline DQ rules generated from contracts | Less hand-written rules, consistent checks |
| GV17 | Breaking-change process with OM impact analysis and "ETL deploys first" | No Monday-morning broken dashboards |
| GV18 | KPI ↔ glossary consistency check; data products for Gold marts | One meaning per number; ready-to-use bundles for consumers |
| GV19 | Orphan/stale table checks, quarterly access reviews | The catalog stays trustworthy |

---

## 12. Changes to Earlier Decisions

| Earlier | Now | Reason |
|---|---|---|
| Doc 01: "OpenMetadata reuses Airflow for ingestion" | OM ingestion runs in its own deployment (OM Helm chart / CronJobs) | Avoid dependency conflicts in the ETL Airflow |
| Doc 01: "Spark lineage via OpenLineage listener" | Lineage from the framework's config-driven `LineageEmitter` (OpenLineage format); the listener is optional | The listener misses our write path |
| Doc 01 module list | + `etl-governance` module and `tools/governance-cli` | Publishers and the contract checker |
| Doc 02 §12 contract example | Extended with version, producer/consumer, SLOs, change policy, PII types, references | Full data contract |
| Job/target YAML | Column `description:` fields recommended (required for Gold) | Catalog descriptions as code |

---

## 13. Key Decisions

1. **OpenMetadata** is the governance tool; tool-neutral through OpenLineage events and governance-as-code. PoC first for EPAS (Postgres connector), OpenLineage ingestion and the DQ API.
2. **Lineage source** = config-driven `LineageEmitter` in `etl-core` (OpenLineage format); one `LineageModel` built from the resolved job config, used both at deploy time (OM lineage API, table + column edges) and at every run (START/COMPLETE/FAIL/ABORT events to Kafka `etl.lineage.events`, best effort, not in dry-runs or dev).
3. **Column lineage** from target column mappings, lookups, SCD config, dimension SK lookups, measure expressions (parsed with Spark's SQL parser) and plugin column declarations. The shared CDC topic is a pipeline attribute, not a lineage node; technical tables are excluded.
4. **Metadata ingestion** (own OM ingestion deployment): EPAS (silver/gold/ref/etl_ctl, staging excluded), source and Bronze MongoDB, Kafka, Airflow, our `etl_jobs` pipeline service, OpenLineage; no sample data stored anywhere.
5. **Governance as code:** domains + data products, owners (job owner for targets, contract producer for sources), descriptions, glossary (linked to `kpis.yml`), tiers (from priority), classifications (`PII`, `Sensitivity`, `PIIType`, `Treatment`) with propagation along column lineage; OM UI for discovery and conversations.
6. **DQ and run status:** our `dq_results` and reconciliation pushed as OM test results; runs as pipeline statuses (`run_etl` + `quality_gate`); `lastSuccessfulLoad` per table; all through a bookmarked `RuntimePublisher` in `etl-ops-api`, so OM outages never affect ETL.
7. **Data contracts** per source collection in `contracts/`, SemVer, co-approved by the source team (CODEOWNERS), checked in the microservices' CI, driving ingestion drift checks, Spark schemas, baseline DQ rules, PII masking and OM metadata; breaking changes need a MAJOR version, notice, impact analysis and "ETL deploys first".
8. **Checklists** for new sources, new jobs, every release (automated) and periodic reviews (owners, orphans, hidden PII, access).
