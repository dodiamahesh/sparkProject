# ETL Framework: Features, Extras and Gaps

A checklist of what the framework does today, what it does beyond a basic ETL, and what is still missing for a production-grade pipeline design.
See [LOW_LEVEL_DESIGN.md](LOW_LEVEL_DESIGN.md) for the detailed design of each item.

---

## ✅ Features already in the framework

### Architecture
- Medallion flow: Bronze (MongoDB) → Silver (PostgreSQL)
- Plugin pipelines: a new `@Component` extending `AbstractEtlPipeline` is registered automatically
- Template method in `AbstractEtlPipeline` with one generic controller for all pipelines
- `SourceDefinition` holds the config only, with no Spark code in it
- Many collections per pipeline, with star and snowflake schemas

### Loading
- Load types: `FULL`, `INCREMENTAL`, `RETRY_REJECTED`
- Incremental load driven by the `processedFlag` field (N/Y/E/D)
- Staging table plus `INSERT … ON CONFLICT` upsert, so reruns are idempotent
- Spark cache freezes the snapshot
- Mongo flags are updated last, so a failed run is safe to rerun
- Surrogate keys and dimension lookups
- Shared `dim_date`, with role-playing date keys

### Data quality
- `DataSanitizer`: trim, blank-to-null, case changes, phone and number normalization
- Validation types: CORRUPT, VALIDATION, ORPHAN, DUPLICATE
- Dedup that keeps the latest `updatedAt` per business key
- Reject table with `attempt_count`; fixed records are removed from it automatically
- WARNING rules: the row still loads but is reported
- Reject gate (`maxRejectPercent`)
- Volume check (`expectedRows`)
- Schema drift detection, with an option to fail the run on drift
- PII masking (email, phone), also applied to the rejected `raw_record`
- Source filter (`excludeTestRecords`)
- Source metadata: owner, SLA, description

### Audit and operations
- Job run and step tables, with timing, row counts and errors
- Recovery of stuck jobs at startup
- In-memory lock per pipeline
- Sync or async (HTTP 202) runs
- REST APIs: list pipelines, run, job status, history, warnings, rejected records, summary
- Flyway migrations V1–V7
- Sample-data generator, ingestion API and dev reset
- Unit tests for the mappers and quality rules
- Actuator health endpoint
- README, API flow doc and LLD doc

---

## ➕ Extras (good to have, more than a basic ETL needs)
- Schema drift detection
- PII masking
- WARNING severity
- Owner, SLA and description metadata
- A rejected-records summary API
- Automatic removal of fixed records from the reject table
- Ingestion API and sample-data generator (dev tools only)
- Async run plus polling (ready for Airflow)

---

## ❌ Missing: must add (P0, before production)
- API authentication and authorization
- A distributed lock (Postgres advisory lock); the current lock is per JVM only
- HTTP status that reflects the result: a sync run that FAILED still returns 200
- Ingestion and dev APIs disabled in prod (`@Profile("dev")`)
- Atomic FULL load: a staging/shadow table and swap instead of truncate
- Per-pipeline processed flag; today the shared `processedFlag` breaks when 2 pipelines read the same collection
- Delete handling: soft delete or tombstones from bronze to silver
- A guaranteed reset of `processedFlag = 'N'` on update (Mongo Change Stream or a contract with the source app)

---

## ⚠️ Missing: should add (P1)
- Watermark-based incremental load (`updatedAt > lastWatermark`) instead of writing flags back into the source
- SCD Type 2 history for dimensions
- Pipeline dependencies/DAG, or an Airflow integration with documented contracts
- Retry with backoff on transient failures (DB or Mongo down)
- Run timeouts and cancellation
- Metrics (Micrometer/Prometheus) and alerts on failures, SLA breaches and reject %
- Structured logs with `jobRunId` in the MDC
- Data retention and purge for the warning, reject, job and step tables
- Secrets in a vault, not in `application.yml`
- Integration tests with Testcontainers (Mongo + Postgres + Spark)
- A real Spark cluster (YARN/K8s); today it runs in `local[*]`
- Partitioning and batch-size tuning for large volumes
- Backfill by date range
- Late-arriving dimensions (an "unknown member" row instead of rejecting as ORPHAN)

---

## 💡 Nice to have (P2)
- A Gold layer (aggregates and marts)
- Data lineage (OpenLineage) and a data catalog
- Data contracts stored as YAML/JSON outside the Java code
- Reconciliation: source count = target + rejected + duplicates, checked automatically
- A UI dashboard for jobs and rejects
- Notifications (Slack/email) to the source owner
- Rules for schema evolution (e.g. auto-add a column)
- Multi-tenant or per-environment config
- A CDC/streaming mode (Kafka or Mongo Change Streams)
- Quarantine replay from the UI
