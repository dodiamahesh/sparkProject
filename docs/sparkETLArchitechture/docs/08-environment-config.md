# 08 — Environment Configuration and Secrets (dev / QA / UAT / prod)

> Builds on docs [01](01-architecture-and-structure.md) to [07](07-job-lifecycle-run-modes.md).
> Written in **simple language**: every section starts with an **"In simple words"** box, and every abbreviation is written out in full.

---

## 0. Abbreviations Used in This Document

| Short form | Full form | Meaning in simple words |
|---|---|---|
| **env** | environment | One copy of the whole platform: dev, qa, uat or prod |
| **dev** | development | Where developers try things |
| **QA** | Quality Assurance | Where testers test |
| **UAT** | User Acceptance Testing | Where business users check before go-live (prod-like) |
| **prod** | production | The real system with real data |
| **CI/CD** | Continuous Integration / Continuous Delivery | Automatic build, test and deploy pipeline |
| **K8s** | Kubernetes | The container platform the services and Spark run on |
| **ConfigMap** | Kubernetes ConfigMap | A K8s object holding non-secret settings for pods |
| **Secret** (K8s) | Kubernetes Secret | A K8s object holding passwords/keys for pods |
| **Helm** | (name, not an abbreviation) | A tool that installs apps on K8s using templates + a values file per env |
| **Vault** | HashiCorp Vault | A dedicated, secure store for secrets |
| **ESO** | External Secrets Operator | A K8s add-on that copies secrets from Vault (or a cloud store) into K8s Secrets |
| **KMS** | Key Management Service | A service that keeps encryption keys safe |
| **SOPS** | Secrets OPerationS | A tool to keep encrypted secret files in Git |
| **TTL** | Time To Live | How long something stays valid before it expires |
| **TLS** | Transport Layer Security | Encryption of network traffic ("https") |
| **SASL** | Simple Authentication and Security Layer | How Kafka clients log in |
| **JDBC** | Java Database Connectivity | Standard Java API to talk to EPAS |
| **URI** | Uniform Resource Identifier | A connection address, e.g. `mongodb://host:27017/db` |
| **JVM** | Java Virtual Machine | The program that runs Java code |
| **DAG** | Directed Acyclic Graph | An Airflow workflow |
| **YAML** | YAML Ain't Markup Language | Our config file format |
| **JSON** | JavaScript Object Notation | Text format for structured data |
| **EPAS** | EDB Postgres Advanced Server (EDB = EnterpriseDB) | Our PostgreSQL-based database |
| **PII** | Personally Identifiable Information | Name, email, phone, ID numbers |
| **ETL** | Extract, Transform, Load | Our data pipelines |

---

## 1. The Core Idea

> **In simple words**
> Think of a **mobile app** that is the same on every phone, but each phone has **its own settings** (language, Wi-Fi password, account). We do the same:
> - The **code** (JAR, Docker images, DAG code) is built **once** and is **identical** in dev, qa, uat and prod.
> - Only the **settings** differ: which database to connect to, how big the Spark cluster is, how often jobs run.
> - **Passwords are never written in files or Git.** They are fetched at runtime from a safe (the secret manager), like a phone's password manager.

Three rules guide everything in this document:

| Rule | Meaning |
|---|---|
| **R1. Build once, configure per environment** | The same image tag goes dev → qa → uat → prod (doc 06 §11.3). Only config and secrets change |
| **R2. One source of truth for config = Git** | All non-secret settings for all environments live in **one folder tree** in the repo. No hand-edited settings on servers |
| **R3. Secrets only in the secret manager** | Git and images contain only **references** like `${secret:epas/etl_job#password}`, never the values |

---

## 2. What Changes per Environment

### 2.1 Overview

| Area | Typical differences between environments |
|---|---|
| **EPAS** (Silver, Gold, control) | Host, port, database name, TLS mode, connection pool size, statement timeouts, user names (passwords → secrets) |
| **MongoDB** (source read for snapshots, Bronze) | Replica-set URI, read preference, TLS, Bronze database names (`bronze_svc_order` vs `bronze_svc_order_dev` if shared) |
| **Spark** | Where it runs (local / K8s), executor count and size, dynamic allocation limits, shuffle partitions, event log location, image tag |
| **Kafka** | Bootstrap servers, security (TLS/SASL), topic prefix (only if environments share one cluster), partitions/retention (doc 02 `topics.yml`) |
| **Kafka Connect** | Connect REST URL, connector overlays (source DB, poll sizes, heartbeat), worker config provider paths |
| **Airflow** | Whether schedules are active, pool sizes, queues, retries, alert channels, the pinned image version, connections (via the secrets backend) |
| **Jobs** | Schedules, thresholds, batch sizes, `maxRecordsPerRun`, resources (doc 06 §3.4) |
| **Observability** | Log level, metrics push gateway, lineage target (OpenMetadata URL), alert routing |
| **Security / features** | Approvals required?, PII reveal allowed?, step-level audit on/off, dry-run default for risky modes |

### 2.2 Typical values side by side

| Setting | dev | qa | uat | prod |
|---|---|---|---|---|
| Spark runs on | K8s namespace `etl-dev` (or `local[*]` on a laptop) | K8s `etl-qa` | K8s `etl-uat` | K8s `etl-prod` |
| Executors (default job) | 1–2 | 2 | 2–4 | 4 (dynamic up to 20) |
| Executor memory | 2 GB | 2 GB | 4 GB | 4–8 GB |
| `spark.sql.shuffle.partitions` | 8 | 16 | 64 | 200 (AQE coalesces) |
| EPAS pool per job (writers) | 2 | 4 | 8 | 8–16 |
| Kafka partitions `cdc.source.events` | 3 | 6 | 12 | 24 |
| Kafka retention | 3 days | 7 days | 7 days | 14 days |
| Airflow schedules | **paused by default** ➕ (run on demand) | active, reduced frequency | same as prod | active |
| Airflow job retries | 0 | 1 | 3 | 3 |
| Thresholds | loose (find bugs, don't block) | as prod | as prod | strict |
| Approvals for FULL_LOAD / REPLAY | no | no | yes | yes |
| PII reveal | disabled (no real PII there) | disabled | allowed (masked copies) | allowed (audited) |
| Log level | DEBUG | INFO | INFO | INFO |
| Data | synthetic ➕ | synthetic | **masked copy** of prod ➕ | real |

➕ **Dev schedules paused by default:** 100+ jobs running hourly in dev cost money and fill logs, while developers usually run one job by hand. Developers un-pause the DAGs they work on.

➕ **No real PII outside prod:** dev and qa use **synthetic** (generated) data; uat uses a **masked** copy of prod (names, emails, phones replaced). Details in Prompt 11.

---

## 3. Config Layering and Precedence

### 3.1 Two kinds of config

> **In simple words:** there are two "families" of settings.
> 1. **Platform config:** *where things are* and *how big they are* (databases, Kafka, Spark cluster, Airflow). Same for all jobs.
> 2. **Job config:** *what a job does* (the recipe YAML from doc 06), plus small per-environment tweaks (schedule, thresholds, resources).

### 3.2 Precedence diagram

> **In simple words:** like stacking **transparent sheets**. A value on a higher sheet covers the same value on the sheets below. Anything not covered shows through from below.

```
  PLATFORM CONFIG (databases, Spark, Kafka, Airflow, observability, features)

        ┌───────────────────────────────────────────────────────────┐
   4 ▲  │ Runtime environment variables (injected by Helm/K8s)      │  only a few: ETL_ENV, POD_NAME, image tag
     │  ├───────────────────────────────────────────────────────────┤
   3 │  │ Secrets  ${secret:...}  resolved at runtime               │  passwords, keys (never in files)
     │  ├───────────────────────────────────────────────────────────┤
   2 │  │ config/environments/<env>/*.yml                           │  e.g. prod/datasources.yml
     │  ├───────────────────────────────────────────────────────────┤
   1 │  │ config/environments/base/*.yml                            │  shared defaults for all envs
        └───────────────────────────────────────────────────────────┘

  JOB CONFIG (per job; doc 06 §3.1)

        ┌───────────────────────────────────────────────────────────┐
   5 ▲  │ CLI --override (allow-listed keys only)                   │  emergency tuning of one run
     │  ├───────────────────────────────────────────────────────────┤
   4 │  │ config/environments/<env>/job-overrides.yml  (this job)   │  prod: schedule, resources, thresholds
     │  ├───────────────────────────────────────────────────────────┤
   3 │  │ pipelines/<layer>/<domain>/<job>.yml                      │  the job recipe
     │  ├───────────────────────────────────────────────────────────┤
   2 │  │ config/environments/<env>/job-defaults.yml  (optional)    │  ➕ env-wide defaults for ALL jobs (e.g. dev: loose thresholds)
     │  ├───────────────────────────────────────────────────────────┤
   1 │  │ pipelines/_defaults/job-defaults.yml                      │  company-wide defaults
        └───────────────────────────────────────────────────────────┘

  The job config can reference platform config by NAME only:  datasource: bronze_mongo,  sparkProfile: medium
  → the job YAML never contains a host name, so the same job YAML works in every environment.
```

**Merge rules:**

| Rule | Example |
|---|---|
| Maps are merged key by key | `resources: {executors: 8}` in prod keeps `executorMemory` from the job YAML |
| Lists are **replaced**, not merged | `dependsOn` in an override replaces the whole list (avoids surprises) |
| `null` removes a value | `schedule: null` in dev = no schedule (manual only) |
| Unknown keys fail validation | A typo like `thresholdz:` is caught in CI |
| An unresolved `${...}` fails fast at start | Never run with a half-filled config |

### 3.3 Options for Spring Boot apps (ingestion, ops API) compared

| Option | How | Pros | Cons | Verdict |
|---|---|---|---|---|
| **Spring profiles in the image + K8s env vars + secrets as files** | `application.yml` + `application-{env}.yml` packaged; Helm sets `SPRING_PROFILES_ACTIVE=prod` and a few env vars; secrets mounted and read with `spring.config.import=configtree:` | Simple, no extra server, versioned with the code, works offline | Changing a value needs a redeploy (which is fine: changes should be reviewed) | ✅ **Chosen** |
| Spring Cloud Config Server | A separate service serving config from Git | Central, can refresh without redeploy | One more critical service to run and secure; Spark jobs can't use it anyway (no Spring there) | ❌ unless your 20 microservices already use it ➕ (then reuse it for consistency) |
| K8s ConfigMaps hand-written per env | `kubectl apply` of ConfigMaps | Native to K8s | Config lives outside Git history if edited by hand; drift between envs | ❌ as a source; ✅ only as the **output** generated by Helm from Git |

➕ **Consistency with the existing microservices:** look at how your 20+ microservices are configured today. If they already use Spring Cloud Config or a specific Vault integration, the two ETL Spring services should do the **same**, so the operations team has one way of working. The Spark jobs follow §4 regardless.

---

## 4. How Spark Jobs Get Their Environment Config

### 4.1 Options compared

| Option | What it means | Verdict |
|---|---|---|
| Everything via `spark-submit --conf` / arguments | Pass hosts, sizes, flags as command-line values | ❌ Long, error-prone; secrets would be visible in the Spark UI and process list |
| A config database | The job reads its settings from a DB table at start | ❌ Chicken-and-egg (needs DB settings to read DB settings); not versioned with the code |
| **Files baked into the image + `--env` + Spark resources applied at submit time** | Config files are inside the Docker image (doc 06 §11.3); the runner gets `--env=prod` and loads `config/environments/prod/*`; Airflow builds the Spark resource settings from the job config when submitting | ✅ **Chosen** |

### 4.2 The important detail: resources must be known **before** the job starts ➕

> **In simple words:** the number and size of Spark executors is decided **when the job is launched**, not after. A job cannot read "I need 8 executors" from its own config file once it is already running with 2. So **Airflow** reads the job's resources from the config **before** launching, and passes them to Spark. Everything else (databases, thresholds, DQ rules) the job reads itself at start.

```
 Git commit X  ──build──►  image etl-spark-app:1.14.2  (contains /opt/etl/config/** and /opt/etl/pipelines/**)
                                   │
 Airflow (DAG factory, Prompt 09)  │ reads the SAME resolved config (published by CI as a JSON artifact for that tag)
   job b2s_order, env prod:        │
   resources = job YAML ⊕ prod/job-overrides.yml ⊕ prod/spark.yml profile
        │                          │
        ▼                          ▼
 SparkApplication (K8s) / spark-submit
   image: etl-spark-app:1.14.2
   driver/executor cores, memory, instances, dynamic allocation   ← from resources (submit time)
   spark.kubernetes.{driver,executor}.secrets.etl-secrets=/opt/etl/secrets   ← secret files mounted (§5)
   env: ETL_ENV=prod
   args: --job=b2s_order --env=prod --mode=INCREMENTAL --run-id=...
        │
        ▼
 EtlJobRunner (inside the driver pod)
   1. load /opt/etl/config/environments/{base,prod}/*.yml         ← platform config
   2. load the job config (defaults → job YAML → prod overrides → CLI)
   3. resolve ${secret:...} from /opt/etl/secrets/*                ← secrets
   4. guard: --env must equal ETL_ENV ➕
   5. compare config hash with job_registry.config_hash ➕ (drift warning)
   6. run
```

| Guard ➕ | Why |
|---|---|
| `--env` must equal the pod's `ETL_ENV` | Prevents "ran with `--env=prod` in the dev cluster" (or worse, the reverse) |
| Each environment's secrets only exist in that environment's namespace | Even a wrong argument cannot reach another environment's databases |
| Config hash compared with the registry | Warns when the image's config and the deployed registry differ (e.g. registry sync forgot to run) |

---

## 5. Secrets Management

### 5.1 What counts as a secret

| Secret | Used by |
|---|---|
| EPAS passwords (`etl_job`, `etl_ops`, `etl_monitor`, `etl_migrate`) | Spark jobs, ops API, Grafana, Flyway |
| MongoDB passwords (Bronze writer, CDC reader per source DB, Spark reader/write-back) | Ingestion, Kafka Connect, Spark |
| Kafka credentials (SASL users, TLS keystores) | Connect, ingestion, ops API (DLQ replay) |
| OpenMetadata token, Pushgateway credentials, Slack/Teams webhooks | Jobs, alerts |
| Airflow API credentials (ops API → Airflow REST) | Ops API |
| PII encryption keys (doc 07 §6.4) | Ingestion (optional), Silver column encryption, ops API "reveal" |

### 5.2 Options compared

| Option | Pros | Cons | Verdict |
|---|---|---|---|
| **HashiCorp Vault** (+ ESO or Vault Agent to deliver into pods) | Central, audited access, versioning, rotation, dynamic DB credentials possible | Must be run (or already exists) | ✅ **Preferred** (if your organization has Vault, or is willing to run it) |
| Cloud secret store (AWS Secrets Manager / Azure Key Vault / GCP Secret Manager) + ESO | Managed service, same delivery as Vault via ESO | Cloud-specific | ✅ Equal choice if you run in that cloud |
| Plain K8s Secrets (encrypted at rest with KMS) + SOPS / Sealed Secrets in Git | No extra system | Weaker audit and rotation; secrets (encrypted) in Git | ⚠️ Acceptable fallback for small setups |
| Airflow Connections stored in the Airflow DB | Built into Airflow | Only Airflow can use them; Spark/Spring can't | ❌ as a store; ✅ Airflow reads Vault through its **secrets backend** |
| Passwords in `application-prod.yml` / env files | — | Leaks via Git, images, logs | ❌ Never |

### 5.3 Secrets flow (Vault + External Secrets Operator)

> **In simple words**
> 1. The **safe** (Vault) holds the real passwords.
> 2. A small **courier** inside Kubernetes (ESO) copies the passwords that a namespace is allowed to have into a K8s Secret in that namespace.
> 3. The secret is **mounted as files** into the pods (like a USB stick plugged in), not as command-line arguments.
> 4. Our code reads `${secret:epas/etl_job#password}` → opens the file → uses the value. It never prints it.

```
 ┌──────────────────────────────── HashiCorp Vault ────────────────────────────────┐
 │  kv/etl/prod/epas/etl_job        {username, password}                           │
 │  kv/etl/prod/mongo/bronze_writer {username, password}                           │
 │  kv/etl/prod/kafka/etl_ingestion {sasl_username, sasl_password, truststore}     │
 │  kv/etl/prod/openmetadata        {token}                                        │
 │  policies: namespace etl-prod may read kv/etl/prod/* ONLY                       │
 └───────────────┬─────────────────────────────────────────────────────────────────┘
                 │ ESO (External Secrets Operator) syncs every 1h (+ on change)
                 ▼
 ┌─────────── K8s namespace etl-prod ────────────────────────────────────────────────┐
 │  Secret etl-secrets   (keys: epas-etl_job-password, mongo-bronze_writer-password…) │
 │        │ mounted read-only as files                                                │
 │        ├──► Spark driver + executor pods   /opt/etl/secrets/<key>                  │
 │        │      (spark.kubernetes.driver.secrets.etl-secrets=/opt/etl/secrets        │
 │        │       spark.kubernetes.executor.secrets.etl-secrets=/opt/etl/secrets)     │
 │        ├──► etl-ingestion pods          spring.config.import=configtree:/opt/etl/secrets/ │
 │        ├──► etl-ops-api pods            (same)                                     │
 │        └──► Kafka Connect workers       FileConfigProvider ${file:/opt/etl/secrets/...} │
 └────────────────────────────────────────────────────────────────────────────────────┘

 Airflow:  secrets backend = Vault (airflow.providers.hashicorp VaultBackend)
           reads connections/variables like conn/etl_prod_k8s directly from Vault
           ➕ Airflow does NOT get the database passwords of the jobs (least privilege):
              it only needs to launch Spark and call the ops API.
```

**Why executors need secrets too ➕:** the COPY into staging (doc 03 §8.1) and the Bronze write-back (doc 03 §9) run **on executors**, so the executor pods must also mount the secret. Mounting the same K8s Secret in driver and executors is the cleanest way.

### 5.4 Secret references in config

```yaml
# config/environments/prod/datasources.yml (excerpt)
epas:
  warehouse:
    url: "jdbc:postgresql://epas-prod.db.internal:5444/dataplatform?sslmode=verify-full&ApplicationName=etl"
    username: "${secret:epas-etl_job-username}"
    password: "${secret:epas-etl_job-password}"
```

```java
// etl-config: secrets/SecretResolver.java
public interface SecretResolver {
    String resolve(String key);                       // throws if missing: never fall back to an empty password
}

// etl-config: secrets/FileSecretResolver.java — reads the files mounted by K8s
public final class FileSecretResolver implements SecretResolver {
    private final Path dir;                           // /opt/etl/secrets
    public FileSecretResolver(Path dir) { this.dir = dir; }

    @Override
    public String resolve(String key) {
        if (!key.matches("[a-zA-Z0-9_.-]+")) throw new ConfigException(ErrorCode.CONFIG_INVALID, "Bad secret key " + key);
        Path f = dir.resolve(key);
        try {
            return Files.readString(f).strip();
        } catch (IOException e) {
            throw new ConfigException(ErrorCode.CONFIG_INVALID, "Secret not found: " + key, e);  // the key, never the value
        }
    }
}

// Local laptop runs: EnvVarSecretResolver (ETL_SECRET_<KEY>=...) from a .env.local file that is git-ignored.
```

**Rules for secrets in code ➕:**

| Rule | How |
|---|---|
| Never log a resolved value | Config objects holding secrets override `toString()`; a `Secret` wrapper type prints `****` |
| Never pass secrets as `--conf` or arguments | They show up in the Spark UI "Environment" tab and in `ps` output. Plus `spark.redaction.regex` as a second safety net |
| Never put secrets in Airflow DAG `conf` / XCom | Those are stored in the Airflow DB and shown in the UI |
| Never print a whole JDBC URL that contains a password | URLs never contain passwords (user/password are separate properties) |
| Scan for leaks | `gitleaks` (a secret-scanning tool) in a pre-commit hook **and** in CI; a found secret fails the build and must be **rotated**, not just deleted |

### 5.5 Rotation and dynamic credentials

| Item | Phase 1 | Phase 2 ➕ |
|---|---|---|
| DB passwords | Static per environment, rotated every 90 days (Vault versioned secret; ESO syncs; pods pick up the new file on the next start; long-running services reload the connection pool) | **Vault dynamic database credentials**: each Spark run gets its own short-lived DB user (TTL = a few hours). Nothing to rotate, and every query is traceable to one run |
| Kafka credentials | Static, rotated every 90 days | Same idea with the Kafka secrets engine, if available |
| Encryption keys | In KMS/Vault Transit, never exported | Key rotation with re-wrap |

For long-running Spring services, rotation works without downtime if both the old and the new password are valid for a short overlap window (Vault supports this pattern).

---

## 6. Folder Layout

> **In simple words:** **one place per environment** for all its settings. If someone asks "what is different in prod?", you open `config/environments/prod/` and you see everything (except secrets).

```
data-platform/
├── config/
│   └── environments/
│       ├── base/                              # shared defaults for ALL environments
│       │   ├── datasources.yml                # logical datasource names, pool defaults, timeouts
│       │   ├── spark.yml                      # Spark defaults + named sizing profiles (small/medium/large)
│       │   ├── kafka.yml                      # client defaults (acks, compression, security protocol)
│       │   ├── airflow.yml                    # retries, pools, queues defaults
│       │   ├── observability.yml              # log format, metrics, lineage defaults
│       │   └── features.yml                   # feature flags defaults
│       ├── dev/
│       │   ├── datasources.yml                # hosts, db names, pool sizes for dev
│       │   ├── spark.yml                      # cluster, namespace, profile sizes for dev
│       │   ├── kafka.yml                      # bootstrap servers, topic prefix (if shared)
│       │   ├── airflow.yml                    # schedules paused, small pools, alert channel
│       │   ├── observability.yml              # DEBUG, test OpenMetadata
│       │   ├── features.yml                   # approvals off, reveal off
│       │   ├── job-defaults.yml               # ➕ env-wide job defaults (loose thresholds)
│       │   └── job-overrides.yml              # per-job tweaks for dev
│       ├── qa/   (same files)
│       ├── uat/  (same files)
│       └── prod/ (same files)
│
├── kafka-connect/connectors/overlays/{dev,qa,uat,prod}/   # connector overlays (doc 02 §2.5)
├── kafka-connect/topics/topics.yml                        # per-env partitions/retention inline (doc 02 §3.1)
│
├── etl-ingestion/src/main/resources/
│   ├── application.yml                        # defaults
│   └── application-{dev,qa,uat,prod}.yml      # profile files (generated parts reference config/environments)
├── etl-ops-api/src/main/resources/
│   ├── application.yml
│   └── application-{dev,qa,uat,prod}.yml
│
├── deploy/
│   ├── helm/
│   │   ├── etl-ingestion/  values.yaml, values-{dev,qa,uat,prod}.yaml   # replicas, resources, ETL_ENV, image tag
│   │   ├── etl-ops-api/    values.yaml, values-{dev,qa,uat,prod}.yaml
│   │   └── etl-secrets/    externalsecret-{dev,qa,uat,prod}.yaml        # ESO mappings Vault → K8s Secret (NO values)
│   └── k8s/spark/sparkapplication-template.yaml                         # filled by the Airflow operator
│
├── orchestration/airflow/
│   └── dags/ ...                              # DAG code is env-agnostic; it reads config/environments/<env>/airflow.yml
│
├── tools/config-cli/                          # ➕ render + validate + diff configs per env (used by CI and developers)
└── .env.local.example                         # template for laptop runs (the real .env.local is git-ignored)
```

**Change from doc 01:** Airflow environment settings move from `orchestration/airflow/config/<env>.yml` to `config/environments/<env>/airflow.yml`, so **all** environment settings live under one folder.

---

## 7. Sample Configs: dev vs prod

### 7.1 `datasources.yml`

```yaml
# config/environments/base/datasources.yml
epas:
  defaults:
    driver: org.postgresql.Driver              # or the EDB JDBC driver
    sslmode: verify-full
    connectTimeoutSeconds: 10
    socketTimeoutSeconds: 1800
    statementTimeout: 30min                    # doc 07 §8.3
    lockTimeout: 30s
    idleInTransactionTimeout: 10min
    pool: { min: 1, max: 4 }
mongo:
  defaults:
    tls: true
    readPreference: secondaryPreferred
    retryWrites: true
    writeConcern: majority
```

```yaml
# config/environments/dev/datasources.yml
epas:
  warehouse:                                   # Silver + Gold + etl_ctl (one database, doc 05 §2)
    host: epas-dev.db.internal
    port: 5444
    database: dataplatform_dev
    sslmode: require                           # dev certificate is self-signed
    username: "${secret:epas-etl_job-username}"
    password: "${secret:epas-etl_job-password}"
    pool: { max: 2 }
mongo:
  bronze:
    uri: "mongodb://${secret:mongo-bronze-username}:${secret:mongo-bronze-password}@mongo-dev-0.db.internal:27017,mongo-dev-1.db.internal:27017/?replicaSet=rs-dev"
    databasePrefix: bronze_                    # bronze_svc_order ...
  sources:
    svc-order:   { uri: "${secret:mongo-src-svc-order-uri}" }      # read-only, for re-snapshots / Spark bulk copy
    svc-customer:{ uri: "${secret:mongo-src-svc-customer-uri}" }
```

```yaml
# config/environments/prod/datasources.yml
epas:
  warehouse:
    host: epas-prod.db.internal
    port: 5444
    database: dataplatform
    username: "${secret:epas-etl_job-username}"
    password: "${secret:epas-etl_job-password}"
    pool: { max: 8 }
  readReplica:                                 # ➕ optional: Gold BI reads / heavy Silver reads
    host: epas-prod-ro.db.internal
    port: 5444
    database: dataplatform
    username: "${secret:epas-etl_reader-username}"
    password: "${secret:epas-etl_reader-password}"
mongo:
  bronze:
    uri: "${secret:mongo-bronze-uri}"          # whole URI as a secret (hosts + credentials)
    databasePrefix: bronze_
  sources:
    svc-order:    { uri: "${secret:mongo-src-svc-order-uri}" }
    svc-customer: { uri: "${secret:mongo-src-svc-customer-uri}" }
```

### 7.2 `spark.yml`

> **In simple words:** instead of every job saying "4 executors with 4 GB", jobs pick a **T-shirt size** (`small`, `medium`, `large`). Each environment decides what a `medium` actually means. A `large` job in dev is still small, so dev stays cheap.

```yaml
# config/environments/base/spark.yml
spark:
  conf:                                        # applied to every job (doc 03 §11)
    spark.sql.adaptive.enabled: true
    spark.sql.adaptive.coalescePartitions.enabled: true
    spark.sql.adaptive.skewJoin.enabled: true
    spark.sql.autoBroadcastJoinThreshold: 52428800
    spark.serializer: org.apache.spark.serializer.KryoSerializer
    spark.sql.ansi.enabled: false
    spark.sql.session.timeZone: UTC
    spark.task.maxFailures: 4
    spark.redaction.regex: "(?i)secret|password|token|credential"     # ➕ hide secrets in the UI/logs
    spark.driver.extraJavaOptions: >-
      --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED
      --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED
    spark.executor.extraJavaOptions: >-
      --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED
      --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED
  profiles: {}                                 # defined per environment
```

```yaml
# config/environments/dev/spark.yml
spark:
  runtime:
    mode: KUBERNETES                           # KUBERNETES | LOCAL (laptop) | STANDALONE | YARN
    master: "k8s://https://k8s-dev.internal:6443"
    namespace: etl-dev
    serviceAccount: spark-etl
    eventLogDir: "s3a://etl-dev-spark-logs/events"     # for the Spark History Server
  conf:
    spark.sql.shuffle.partitions: 8
    spark.dynamicAllocation.enabled: false
  profiles:
    small:  { driverMemory: 1g, driverCores: 1, executors: 1, executorMemory: 2g, executorCores: 1 }
    medium: { driverMemory: 2g, driverCores: 1, executors: 2, executorMemory: 2g, executorCores: 2 }
    large:  { driverMemory: 2g, driverCores: 1, executors: 2, executorMemory: 4g, executorCores: 2 }
```

```yaml
# config/environments/prod/spark.yml
spark:
  runtime:
    mode: KUBERNETES
    master: "k8s://https://k8s-prod.internal:6443"
    namespace: etl-prod
    serviceAccount: spark-etl
    eventLogDir: "s3a://etl-prod-spark-logs/events"
    nodeSelector: { workload: spark }          # ➕ dedicated nodes: Spark never starves the microservices
  conf:
    spark.sql.shuffle.partitions: 200
    spark.dynamicAllocation.enabled: true
    spark.dynamicAllocation.shuffleTracking.enabled: true   # needed on K8s (no external shuffle service)
    spark.dynamicAllocation.executorIdleTimeout: 120s
  profiles:
    small:  { driverMemory: 2g, driverCores: 1, executors: 2, minExecutors: 1, maxExecutors: 4,  executorMemory: 4g, executorCores: 2 }
    medium: { driverMemory: 4g, driverCores: 2, executors: 4, minExecutors: 2, maxExecutors: 10, executorMemory: 8g, executorCores: 4 }
    large:  { driverMemory: 8g, driverCores: 2, executors: 8, minExecutors: 4, maxExecutors: 20, executorMemory: 8g, executorCores: 4 }
```

In the job YAML: `resources: { profile: medium }`, optionally with exact overrides (`resources: { profile: medium, executorMemory: 12g }`).

### 7.3 `kafka.yml`

```yaml
# config/environments/base/kafka.yml
kafka:
  client:
    securityProtocol: SASL_SSL
    saslMechanism: SCRAM-SHA-512
    acks: all
    compressionType: zstd
  topics:
    cdc: cdc.source.events
    snapshot: cdc.source.snapshot
    dlq: dlq.ingestion
    bronzeLoaded: etl.events.bronze-loaded
  topicPrefix: ""                              # ➕ only set if several envs share ONE Kafka cluster
```

```yaml
# config/environments/dev/kafka.yml
kafka:
  bootstrapServers: "kafka-dev-0.internal:9093,kafka-dev-1.internal:9093"
  connectUrl: "https://connect-dev.internal:8083"
  client:
    saslUsername: "${secret:kafka-etl-username}"
    saslPassword: "${secret:kafka-etl-password}"
  ingestion:
    concurrency: 1
    maxPollRecords: 200
```

```yaml
# config/environments/prod/kafka.yml
kafka:
  bootstrapServers: "kafka-prod-0.internal:9093,kafka-prod-1.internal:9093,kafka-prod-2.internal:9093"
  connectUrl: "https://connect-prod.internal:8083"
  client:
    saslUsername: "${secret:kafka-etl-username}"
    saslPassword: "${secret:kafka-etl-password}"
  ingestion:
    concurrency: 4                             # per pod; pods × concurrency ≤ 24 partitions
    maxPollRecords: 500
```

### 7.4 `airflow.yml`

```yaml
# config/environments/base/airflow.yml
airflow:
  defaultArgs:
    owner: data-platform
    retries: 3
    retryDelay: 5m
    retryExponentialBackoff: true
    maxRetryDelay: 45m
    retryOnExitCodes: [30]                     # only retryable failures (doc 06 §8.2)
  dag:
    catchup: false
    maxActiveRunsPerDag: 1
  pools:
    epas_writers: 6
    spark_cluster: 10
    mongo_writeback: 4
  queues: { default: default, heavy: heavy }
  imageVersionVariable: etl_spark_app_version
```

```yaml
# config/environments/dev/airflow.yml
airflow:
  defaultArgs: { retries: 0 }
  schedulesActive: false                       # ➕ DAGs created paused; developers un-pause what they need
  pools: { epas_writers: 2, spark_cluster: 3, mongo_writeback: 1 }
  alerts: { channel: "slack:#etl-dev-alerts", onSuccess: false }
  connections:
    k8s: etl_dev_k8s                           # names only; the values come from Vault (secrets backend)
    opsApi: etl_dev_ops_api
```

```yaml
# config/environments/prod/airflow.yml
airflow:
  schedulesActive: true
  pools: { epas_writers: 8, spark_cluster: 20, mongo_writeback: 6 }
  alerts:
    channel: "slack:#etl-prod-alerts"
    pager: "pagerduty:etl-oncall"              # FAILED of critical jobs + SLA breaches
    onSuccess: false
  connections:
    k8s: etl_prod_k8s
    opsApi: etl_prod_ops_api
  slaMissCallback: true
```

### 7.5 `features.yml` and `observability.yml`

```yaml
# config/environments/base/features.yml
features:
  approvals: { required: true, expiryHours: 24 }      # doc 07 §2.13
  piiReveal: { enabled: false }
  audit: { stepLevel: true, logWarnings: false }
  lineage: { enabled: true }
  autoReprocessQuarantine: { enabled: true, everyHours: 4 }

# config/environments/dev/features.yml
features:
  approvals: { required: false }
  piiReveal: { enabled: false }                       # no real PII in dev
  lineage: { enabled: false }                         # keep the prod catalog clean

# config/environments/prod/features.yml
features:
  piiReveal: { enabled: true }
```

```yaml
# config/environments/dev/observability.yml
observability:
  logLevel: DEBUG
  metrics: { pushgateway: "http://pushgateway.etl-dev.svc:9091" }
  lineage: { openMetadataUrl: "https://openmetadata-dev.internal/api", token: "${secret:openmetadata-token}" }

# config/environments/prod/observability.yml
observability:
  logLevel: INFO
  metrics: { pushgateway: "http://pushgateway.etl-prod.svc:9091" }
  lineage: { openMetadataUrl: "https://openmetadata.internal/api", token: "${secret:openmetadata-token}" }
```

### 7.6 `job-defaults.yml` and `job-overrides.yml`

```yaml
# config/environments/dev/job-defaults.yml   (➕ applies to ALL jobs in dev)
thresholds:
  maxFailedPct: 50            # dev: don't block on data quality, we want to SEE the errors
  onBreach: COMMIT_AND_FAIL
resources: { profile: small }
maxRecordsPerRun: 200000
```

```yaml
# config/environments/dev/job-overrides.yml
jobs:
  b2s_order:
    schedule: null            # manual only in dev
  s2g_fact_sales_line:
    schedule: null
```

```yaml
# config/environments/prod/job-overrides.yml
jobs:
  b2s_order:
    schedule: "*/30 * * * *"
    resources: { profile: large }
    write: { parallelism: 16 }
    writeBack: { parallelism: 8, batchSize: 2000 }
  s2g_fact_sales_line:
    schedule: "45 * * * *"
    resources: { profile: medium }
    thresholds: { maxFailedPct: 0.5, maxUnknownSkPct: 0.2 }
  s2g_agg_sales_daily_store:
    sla: { readyBy: "0 7 * * *" }
```

### 7.7 Spring Boot profile files

```yaml
# etl-ops-api/src/main/resources/application.yml
spring:
  application.name: etl-ops-api
  config:
    import:
      - "optional:configtree:/opt/etl/secrets/"          # each file name becomes a property (e.g. epas-etl_ops-password)
  datasource:
    url: "jdbc:postgresql://${EPAS_HOST}:${EPAS_PORT}/${EPAS_DB}?sslmode=verify-full&ApplicationName=etl-ops-api"
    username: ${epas-etl_ops-username}
    password: ${epas-etl_ops-password}
    hikari: { maximum-pool-size: 10 }
etl:
  env: ${ETL_ENV}
  airflow:
    baseUrl: ${AIRFLOW_URL}
    username: ${airflow-api-username}
    password: ${airflow-api-password}
management.endpoints.web.exposure.include: health,prometheus
```

```yaml
# etl-ops-api/src/main/resources/application-prod.yml
etl:
  features: { approvalsRequired: true, piiRevealEnabled: true }
logging.level.root: INFO
spring.datasource.hikari.maximum-pool-size: 20
```

```yaml
# deploy/helm/etl-ops-api/values-prod.yaml
image: { repository: registry.internal/etl-ops-api, tag: "1.14.2" }
replicas: 2
env:
  ETL_ENV: prod
  SPRING_PROFILES_ACTIVE: prod
  EPAS_HOST: epas-prod.db.internal
  EPAS_PORT: "5444"
  EPAS_DB: dataplatform
  AIRFLOW_URL: https://airflow-prod.internal
secretsVolume: { secretName: etl-secrets, mountPath: /opt/etl/secrets }
resources: { requests: { cpu: 500m, memory: 1Gi }, limits: { memory: 2Gi } }
```

---

## 8. How the Config Is Loaded (Java)

```java
// etl-config: env/EnvConfigLoader.java
public final class EnvConfigLoader {

    private static final List<String> FILES =
            List.of("datasources.yml", "spark.yml", "kafka.yml", "airflow.yml", "observability.yml", "features.yml");

    public EnvConfig load(Path configRoot, String env, SecretResolver secrets) {
        if (!Set.of("dev", "qa", "uat", "prod").contains(env))
            throw new ConfigException(ErrorCode.CONFIG_INVALID, "Unknown env " + env);
        String podEnv = System.getenv("ETL_ENV");
        if (podEnv != null && !podEnv.equals(env))                                       // ➕ env guard (§4.2)
            throw new ConfigException(ErrorCode.CONFIG_INVALID, "--env=" + env + " but ETL_ENV=" + podEnv);

        ObjectNode merged = JsonNodeFactory.instance.objectNode();
        for (String file : FILES) {
            YamlMerge.deepMerge(merged, Yaml.read(configRoot.resolve("environments/base/" + file)));
            YamlMerge.deepMerge(merged, Yaml.readIfExists(configRoot.resolve("environments/" + env + "/" + file)));
        }
        Placeholders.resolve(merged, key -> switch (key.prefix()) {                       // ${env:X}, ${secret:Y}
            case "env"    -> Optional.ofNullable(System.getenv(key.name()))
                                     .orElseThrow(() -> unresolved(key));
            case "secret" -> secrets.resolve(key.name());                                   // values never logged
            default       -> throw unresolved(key);
        });
        EnvConfigSchema.validate(merged);                                                    // JSON Schema for env files
        return Json.convert(merged, EnvConfig.class);                                        // typed records; secrets as Secret type
    }

    private static ConfigException unresolved(PlaceholderKey k) {
        return new ConfigException(ErrorCode.CONFIG_INVALID, "Unresolved placeholder " + k.text());
    }
}

// etl-common: model/Secret.java — a value that can't be printed by accident
public record Secret(String value) {
    @Override public String toString() { return "****"; }
}
```

---

## 9. How CI/CD Picks the Right Environment

> **In simple words:** the build is done **once**. Each deployment is the **same package** plus "you are now in environment X". The pipeline knows the environment because a person (or the pipeline stage) **explicitly** selects it, never by guessing from a server name.

```
 Pull request
   │  CI: build → unit tests → render + validate configs for ALL FOUR environments ➕
   │      (a prod-only typo is caught today, not on release day)
   │      → config diff report: "this PR changes prod: resources of b2s_order 4g → 8g"  ➕
   ▼
 Merge to main → image etl-spark-app:1.14.2 (+ ingestion + ops-api images) + resolved-config JSON artifacts per env
   │
   ▼  CD stage "deploy" with parameter ENV ∈ {dev, qa, uat, prod}
 ┌─────────────────────────────────────────────────────────────────────────────────────────┐
 │ 1. Flyway migrate        (db/**, connection from Vault for ENV, user etl_migrate)        │
 │ 2. Registry sync         (doc 05 §7) with config/environments/ENV                       │
 │ 3. Kafka topics          deploy-topics.sh ENV                                            │
 │ 4. Kafka connectors      deploy-connectors.sh ENV  (base + overlays/ENV)                 │
 │ 5. Spring services       helm upgrade etl-ingestion -f values-ENV.yaml (image tag)       │
 │                          helm upgrade etl-ops-api   -f values-ENV.yaml                   │
 │ 6. Secrets mapping       helm upgrade etl-secrets   -f externalsecret-ENV.yaml           │
 │ 7. Airflow               upload DAGs (same code) + set Variable etl_spark_app_version    │
 │ 8. Smoke test            one dry-run of a canary job in ENV ➕                           │
 └─────────────────────────────────────────────────────────────────────────────────────────┘
   │
   ▼  promotion: dev (automatic) → qa (automatic after tests) → uat (approval) → prod (approval + change ticket)
```

| Rule | Why |
|---|---|
| The environment is a **pipeline parameter**, never derived from a hostname | No accidental "deployed to prod" |
| Each environment has its own CI/CD credentials (only the prod stage can reach prod Vault/K8s) ➕ | A dev pipeline bug can't touch prod |
| Configs of all four environments are validated on every pull request | Errors appear early |
| A **config diff** per environment is shown in the pull request | Reviewers see exactly what changes in prod |
| The smoke test is a **dry-run** | Proves connectivity and config without writing data |
| ➕ Drift check (nightly): compare what is deployed (registry `config_hash`, Helm release values, connector configs from the Connect REST API) with Git | Finds manual changes made on servers |

---

## 10. Local Development (Laptop)

> **In simple words:** a developer can run the whole chain on their own computer with fake data and fake passwords.

| Item | How |
|---|---|
| Infrastructure | `docker-compose.local.yml` (doc 01): Kafka, Connect, MongoDB replica set, PostgreSQL, Airflow |
| Environment name | `dev` with `spark.runtime.mode: LOCAL` via an extra layer `config/environments/local/` (optional, git-tracked, no secrets) |
| Secrets | `.env.local` (git-ignored) read by `EnvVarSecretResolver`; `.env.local.example` shows the keys with dummy values |
| Data | Synthetic data generator scripts in `scripts/` |
| Run a job | `java -jar etl-spark-app.jar --job=b2s_order --env=dev --dry-run` with `SPARK_LOCAL=true` |

---

## 11. ➕ Added by Architect (summary)

| # | Addition | Why |
|---|---|---|
| E1 | Two config families (platform vs job); jobs reference platform settings **by name** only | The same job YAML works in every environment |
| E2 | Env-wide `job-defaults.yml` layer | One switch to make all dev jobs lenient or small |
| E3 | Spark T-shirt sizing profiles per environment | Jobs say "medium"; each env decides what that means |
| E4 | Spark resources resolved by Airflow at submit time | Executors can't be resized after start |
| E5 | `--env` vs `ETL_ENV` guard + secrets scoped per namespace | Wrong-environment runs are impossible |
| E6 | Config hash check against the registry + nightly drift check | Detects config mismatches and manual changes |
| E7 | Secrets mounted as files in driver **and** executor pods; never in `--conf`, XCom or logs; `spark.redaction.regex`; `Secret` wrapper type | No leaks through UIs, process lists or logs |
| E8 | Airflow gets no database passwords (least privilege) | A compromised scheduler can't read the data |
| E9 | `gitleaks` in pre-commit and CI; leaked secrets are rotated | Secrets never stay in Git history |
| E10 | Phase-2 Vault dynamic DB credentials per run | Nothing to rotate; queries traceable to one run |
| E11 | Dev schedules paused by default; synthetic data in dev/qa, masked data in uat | Lower cost; no real PII outside prod |
| E12 | All four environments validated + config diff on every pull request | Prod problems found before release day |
| E13 | Dry-run smoke test after each deployment | Proves the deployment works without writing data |
| E14 | Dedicated Spark node pool in prod | Spark never starves the 20+ microservices |
| E15 | Reuse the config/secret approach of the existing microservices if one exists | One way of working for the ops team |

---

## 12. Changes to Earlier Decisions

| Earlier | Now | Reason |
|---|---|---|
| Doc 01: Airflow env config in `orchestration/airflow/config/<env>.yml` | `config/environments/<env>/airflow.yml` | All env settings in one folder |
| Doc 06 §3.1: four job config layers | Five: + `config/environments/<env>/job-defaults.yml` | Env-wide job defaults |
| Doc 06 job YAML: `resources: {executors: 4, executorMemory: 4g}` | `resources: {profile: medium}` (+ optional exact overrides) | Size profiles per environment |
| Doc 02 connector URI via `${file:/opt/kafka/secrets/...}` | Same mechanism, secrets now delivered by ESO from Vault into the mounted K8s Secret | One delivery path for all secrets |

---

## 13. Key Decisions

1. **Build once, configure per environment:** the same images in all environments; non-secret config in Git; secrets only in the secret manager.
2. **Two config families:** platform config `config/environments/{base,<env>}/{datasources,spark,kafka,airflow,observability,features}.yml`; job config = `pipelines/_defaults` → `<env>/job-defaults.yml` → job YAML → `<env>/job-overrides.yml` → allow-listed CLI overrides. Jobs reference datasources and Spark profiles by name.
3. **Merge rules:** maps merge, lists replace, `null` removes, unknown keys fail, unresolved placeholders fail fast.
4. **Spring services:** Spring profiles inside the image + a few K8s env vars from Helm values + secrets as mounted files (`configtree:`). No Config Server unless the existing microservices already use one.
5. **Spark jobs:** config files baked into the image, `--env` argument (must equal `ETL_ENV`), Spark resources resolved from size profiles and applied by Airflow at submit time, config hash compared with the registry.
6. **Secrets:** Vault (or the cloud secret store) → External Secrets Operator → K8s Secret per namespace → mounted as files into Spark driver/executors, Spring pods and Connect workers; Airflow reads its own connections through the Vault secrets backend and gets no database passwords; references `${secret:...}`; never in arguments, XCom or logs; `gitleaks`; 90-day rotation, dynamic credentials in phase 2.
7. **Per-environment differences:** sizes, schedules (paused in dev), pools, retries, thresholds, approvals, PII reveal, lineage, log level; synthetic data in dev/qa, masked data in uat.
8. **CI/CD:** environment is an explicit pipeline parameter; all environments validated and diffed on every pull request; deploy order = Flyway → registry sync → topics → connectors → Helm → secrets mapping → Airflow variable → dry-run smoke test; promotion dev → qa → uat → prod with approvals; nightly drift check.
