# 11 — Production Readiness (Monitoring, Security, CI/CD, Recovery, Operations)

> Builds on docs [01](01-architecture-and-structure.md) to [10](10-governance-lineage.md).
> Written in **simple language**: every section starts with an **"In simple words"** box, and every abbreviation is written out in full.

---

## 0. Abbreviations Used in This Document

| Short form | Full form | Meaning in simple words |
|---|---|---|
| **RPO** | Recovery Point Objective | How much data you may **lose** in a disaster (e.g. "max 15 minutes") |
| **RTO** | Recovery Time Objective | How **long** recovery may take (e.g. "back within 4 hours") |
| **PITR** | Point-In-Time Recovery | Restoring a database to an exact moment (e.g. 10:42:00) |
| **WAL** | Write-Ahead Log | PostgreSQL's change log; archiving it enables PITR |
| **SSO** | Single Sign-On | One company login for all tools |
| **OIDC** | OpenID Connect | The standard protocol behind most SSO logins |
| **RBAC** | Role-Based Access Control | Permissions by role |
| **TLS / mTLS** | Transport Layer Security / mutual TLS | Encrypted connections; "mutual" = both sides show a certificate |
| **TDE** | Transparent Data Encryption | The database encrypts its files on disk |
| **KMS** | Key Management Service | Safe storage for encryption keys |
| **SBOM** | Software Bill Of Materials | A list of every library inside an image |
| **CVE** | Common Vulnerabilities and Exposures | Public IDs of known security holes |
| **SLA / SLO** | Service Level Agreement / Objective | A promise / a target for reliability or freshness |
| **p95** | 95th percentile | "95% of runs were faster than this" |
| **PromQL** | Prometheus Query Language | The query language of Prometheus |
| **DLQ** | Dead Letter Queue | Kafka "problem box" topic |
| **DQ** | Data Quality | Checks that data is correct |
| **PII** | Personally Identifiable Information | Name, email, phone, ID numbers |
| **GDPR / DPDP** | General Data Protection Regulation / Digital Personal Data Protection Act | EU / India privacy laws |
| **CI/CD** | Continuous Integration / Continuous Delivery | Automatic build, test and deploy |
| **K8s** | Kubernetes | The container platform |
| **HA** | High Availability | Keeps running when one part fails |
| **DR** | Disaster Recovery | Getting back after a big failure |
| **MM2** | MirrorMaker 2 | Kafka's tool to copy topics to another cluster |
| **UI / API** | User Interface / Application Programming Interface | Screens / program-to-program calls |
| **EPAS** | EDB Postgres Advanced Server (EDB = EnterpriseDB) | Our PostgreSQL-based database |
| **OM** | OpenMetadata | Our governance tool |

---

## 1. What "Production-Ready" Means

> **In simple words**
> Before a plane takes off, the pilots go through a **checklist**: fuel, engines, doors, radio, emergency exits. A data platform is "production-ready" when, in the same way, we can answer **yes** to these questions:
> 1. **Do we see what is happening?** (monitoring, dashboards)
> 2. **Are we told when something goes wrong?** (alerts)
> 3. **Can we find out why?** (logs, tracing one record)
> 4. **Is the data safe?** (security, PII, audit)
> 5. **Can we prove the numbers are right?** (reconciliation, DQ reports)
> 6. **Can we change it without breaking it?** (tests, CI/CD, rollback)
> 7. **Is it fast enough and not too expensive?** (performance, cost)
> 8. **Can we survive a disaster?** (backups, restore, replay)
> 9. **Can operators fix things without being developers?** (ops UI, runbooks)

The full go-live checklist is in §13.

---

## 2. Monitoring

### 2.1 The monitoring stack

> **In simple words:** **metrics** are numbers over time (like a car's speedometer and fuel gauge). **Prometheus** collects them, **Grafana** draws them as dashboards, and **Alertmanager** sends the alerts. **Logs** (§4) are the detailed "diary" lines, kept in **Loki** (or ELK).

```
 ┌───────────── sources ──────────────────────────────────────────────────────────────┐
 │ etl-ingestion, etl-ops-api (Micrometer /actuator/prometheus)            ── scrape ──┐
 │ Spark driver/executors (PrometheusServlet while running)                ── scrape ──┤
 │ Spark job results (MetricsPublisher → Pushgateway, at run end)          ── push ────┤
 │ Kafka brokers + Connect (JMX exporter), consumer lag (Kafka lag exporter)── scrape ──┤
 │ EPAS (postgres_exporter), MongoDB (mongodb_exporter)                    ── scrape ──┤──► Prometheus ──► Alertmanager ──► Slack / Teams / Email / PagerDuty
 │ Airflow (StatsD → statsd_exporter)                                      ── scrape ──┤        │
 │ K8s (kube-state-metrics, node exporter, cAdvisor)                       ── scrape ──┤        ▼
 │ etl_ctl tables (SQL exporter: run outcomes, freshness, backlogs) ➕     ── scrape ──┘     Grafana ◄── Postgres datasource (etl_monitor, read-only) ➕
 └─────────────────────────────────────────────────────────────────────────────────────┘        ▲
                                                                                                 └── Loki (logs), Spark History Server links
```

| Choice | Why |
|---|---|
| **Prometheus + Grafana + Alertmanager** | Standard, free, works on K8s; very likely what your microservices already use (➕ reuse the same stack and on-call routing) |
| **Pushgateway** for Spark job results | Batch jobs end before Prometheus can scrape them; the job pushes its final numbers once |
| ➕ **Grafana reads `etl_ctl` directly** (read-only `etl_monitor` role) for run history, DQ trends, quarantine tables | The control tables already have everything; no need to copy it into Prometheus |
| ➕ **SQL exporter** for a few alertable numbers from `etl_ctl` (freshness, backlog sizes, zombie runs) | Alerts need Prometheus metrics; a handful of queries every minute is cheap |

### 2.2 Metric catalog

| Metric | Type | Labels | Source | Used for |
|---|---|---|---|---|
| `etl_run_total` | counter | job, layer, domain, mode, status | Pushgateway | Runs per status |
| `etl_run_duration_seconds` | gauge (last) / histogram | job, layer, mode | Pushgateway | Duration, SLA |
| `etl_run_records_total` | counter | job, outcome (`success/partial/failed/quarantined/skipped`) | Pushgateway | Failure % |
| `etl_run_last_success_timestamp` | gauge | job | Pushgateway | Staleness |
| `etl_run_step_duration_seconds` | gauge | job, step | Pushgateway | Which step got slow |
| `etl_dq_rule_failed_ratio` | gauge | job, table, rule_id | Pushgateway | DQ trends/anomalies |
| `etl_table_freshness_seconds` | gauge | table | SQL exporter | Freshness SLA |
| `etl_quarantine_open_items` | gauge | job, error_code | SQL exporter | Backlog |
| `etl_quarantine_oldest_age_seconds` | gauge | job | SQL exporter | Forgotten items |
| `etl_runs_zombie` | gauge | — | SQL exporter | Stuck runs |
| `etl_ingest_events_total`, `etl_ingest_event_lag_seconds`, `etl_ingest_dlq_total`, `etl_ingest_last_event_timestamp` | as in doc 02 §13 | service, collection, outcome | Micrometer | Ingestion health |
| `kafka_consumergroup_lag` | gauge | group, topic, partition | Kafka lag exporter | Consumer lag |
| `kafka_connect_connector_status` | gauge | connector, state | Connect exporter | Connector failed |
| `pg_*` (connections, replication lag, deadlocks, database size, longest transaction) | gauge | instance, db | postgres_exporter | EPAS health |
| `mongodb_*` (oplog window, replication lag, connections, opcounters) | gauge | instance | mongodb_exporter | Mongo health |
| `airflow_*` (scheduler heartbeat, pool open slots, task failures) | gauge/counter | dag, pool | statsd_exporter | Orchestrator health |
| `kube_pod_status_phase{namespace="etl-prod"}` | gauge | pod | kube-state-metrics | Pending Spark pods = no capacity |
| `etl_governance_publish_lag_seconds` | gauge | — | ops API | OM publisher health |

➕ **Never put `run_id`, `source_id` or user ids as Prometheus labels.** Each unique value creates a new time series ("high cardinality"), and 100 jobs × 24 runs/day would overload Prometheus within weeks. Run ids belong in logs and in `etl_ctl`, and dashboards **link** to them.

### 2.3 Suggested Grafana dashboards

| # | Dashboard | Main panels | Audience |
|---|---|---|---|
| 1 | **Platform overview** ("traffic light") | Last status of every job as a coloured grid (green/amber/red/grey); failed and partial runs in the last 24 h; freshness breaches; open quarantine; DLQ inflow; Kafka lag | Everyone, on-call |
| 2 | **Job detail** (variable: job) | Runs over time by status; duration trend with p95 and SLA line; records by outcome; step durations (stacked); top error codes; link to the ops UI | Job owners |
| 3 | **Ingestion & Kafka** | Events/s per collection; event-time lag per collection; consumer lag per partition; DLQ by error code; connector states; oplog window per source | Platform team |
| 4 | **Data quality** | Failure ratio per rule (trend); top failing rules; reconciliation mismatches; contract breaches; unknown-SK share for Gold facts | Data stewards |
| 5 | **Freshness & SLA** | Freshness per Gold table vs SLA; ready-by status for today; SLA breaches over 30 days (SLO attainment %) | Business-facing owners |
| 6 | **Spark & cluster** | Running/pending Spark pods; executor memory/CPU usage vs requested (right-sizing); cost per job/domain (§8.5) | Platform team |
| 7 | **Databases** | EPAS: connections, longest transaction, locks, replication lag, table bloat, disk; Mongo: oplog, replication lag, slow ops | DBAs, platform |
| 8 | **Quarantine & DLQ backlog** | Open items by job/code/age; fixed vs reprocessed per day; DLQ new vs replayed/ignored | Data stewards |
| 9 | **Platform services** | Airflow scheduler/triggerer health, pool usage; ops API latency/errors; governance publisher lag | Platform team |

Dashboards are stored as JSON in `monitoring/grafana/dashboards/` and provisioned automatically (no hand-made dashboards in prod) ➕.

---

## 3. Alerting

### 3.1 Principles

> **In simple words:** a good alert is like a good **fire alarm**: it rings only when there is a real fire, it says **where**, and there is a sign next to it saying **what to do**. Too many false alarms, and people stop listening.

| Rule | Detail |
|---|---|
| Every alert has an **owner** and a **runbook link** | Alert annotations `owner_team` and `runbook_url` (§12) |
| ➕ **One source per kind of alert** | **Job outcomes** (FAILED, PARTIAL) are sent by **Airflow callbacks** (doc 09 §7.4), which have the full context. **Infrastructure, lag, freshness, backlogs** come from **Prometheus**. So the same problem is never alerted twice |
| ➕ **Inhibition** | If EPAS is down, the "EPAS down" alert **suppresses** the job-failure alerts that it causes |
| Grouping | Alerts of the same kind and owner within 5 minutes go into one message |
| Severity → channel | CRITICAL → pager + Slack/Teams; ERROR → Slack/Teams (owner team mentioned); WARNING → Slack/Teams channel, no mention; INFO → dashboard only |
| Weekly alert review ➕ | Which alerts fired, were they useful? Noisy alerts are fixed or removed |

### 3.2 Monitoring / alerting matrix

| # | Alert | Condition (simplified) | Severity | Channel | Source | Runbook |
|---|---|---|---|---|---|---|
| A1 | Job FAILED (final) | Final attempt failed | CRITICAL (priority HIGH) / ERROR | Pager / Slack | Airflow callback | R01–R03, R16 |
| A2 | Job PARTIAL | Run ended PARTIAL | WARNING (grouped per hour) | Slack | Airflow callback | R04 |
| A3 | Duration SLA | Running > 150% of `maxDurationMinutes` | WARNING | Slack | SLA monitor | R10 |
| A4 | Freshness SLA | `etl_table_freshness_seconds > freshnessMinutes × 60` | ERROR / CRITICAL for Tier 1 | Slack / Pager | SQL exporter | R10 |
| A5 | Ready-by missed | No complete load of yesterday by `readyBy` | CRITICAL | Pager + owner email | SLA monitor | R10 |
| A6 | Kafka consumer lag | `sum(kafka_consumergroup_lag{group="etl-ingestion"}) > 100000` for 10 min | ERROR | Slack | Prometheus | R05 |
| A7 | Event-time lag per collection | `etl_ingest_event_lag_seconds > 900` for 10 min | ERROR | Slack | Prometheus | R05 |
| A8 | Collection silent | `time() - etl_ingest_last_event_timestamp > expected gap` | WARNING | Slack | Prometheus | R06 |
| A9 | Connector failed | Connector/task state FAILED | CRITICAL | Pager | Prometheus | R06 |
| A10 | Oplog window low | Source oplog window < 48 h | ERROR | Slack + DBA | Prometheus | R06 |
| A11 | DLQ spike / unmapped collection | DLQ rate > X/min, or any `UNMAPPED_COLLECTION` | ERROR | Slack | Prometheus | R07 |
| A12 | DQ threshold breached | Run FAILED with `THRESHOLD_BREACHED` | ERROR | Slack (owner + steward) | Airflow callback | R02 |
| A13 | ➕ DQ anomaly | A rule's failure ratio > 3 × its 7-day average and > 0.5% | WARNING | Slack (steward) | Prometheus | R08 |
| A14 | Contract breach | Drift/required-field violations above the contract limits | ERROR | Slack (source team + data team) | Prometheus | R08 |
| A15 | Reconciliation mismatch | Any `MISMATCH_FAIL` | CRITICAL | Pager (platform) | Airflow callback | R03 |
| A16 | Zombie run / stuck lock | `etl_runs_zombie > 0`, or 3× `LOCK_NOT_ACQUIRED` in a row | ERROR | Slack | SQL exporter | R09 |
| A17 | Quarantine backlog | Open items growing for 24 h, or oldest > 7 days | WARNING | Slack (steward) | SQL exporter | R11 |
| A18 | Gold unknown-SK share / old inferred members | Above threshold / inferred > 7 days | WARNING | Slack | SQL exporter | R12 |
| A19 | EPAS health | Down; connections > 85%; replication lag > 5 min; disk > 80%; transaction open > 1 h | CRITICAL / ERROR | Pager / Slack + DBA | postgres_exporter | R13 |
| A20 | DEFAULT partition not empty | Rows in any `*_default` partition | WARNING | Slack | SQL exporter | R13 |
| A21 | MongoDB health | Down; replication lag > 1 min; disk > 80% | CRITICAL / ERROR | Pager / Slack | mongodb_exporter | R13 |
| A22 | Spark capacity | Spark pods Pending > 10 min | WARNING | Slack | kube-state-metrics | R16 |
| A23 | Airflow health | Scheduler/triggerer heartbeat missing > 2 min | CRITICAL | Pager | statsd_exporter | R19 |
| A24 | Governance publisher lag | > 30 min behind | WARNING | Slack (platform) | ops API | R17 |
| A25 | ➕ Backup failed / not run | Last successful EPAS or Mongo backup > 26 h ago | CRITICAL | Pager + DBA | backup tool metrics | R14 |
| A26 | ➕ Certificate / secret expiry | TLS certificate or secret rotation due in < 14 days | WARNING | Slack (platform) | exporter / Vault | — |
| A27 | ➕ Duration regression | p95 duration > 1.5 × its 14-day baseline | WARNING | Slack (owner) | Prometheus | R16 |

### 3.3 Sample Prometheus rules and Alertmanager routing

```yaml
# monitoring/prometheus/alerts/etl-ingestion.yml
groups:
  - name: etl-ingestion
    rules:
      - alert: EtlIngestionEventLagHigh
        expr: max by (service, collection) (etl_ingest_event_lag_seconds) > 900
        for: 10m
        labels: { severity: error, owner_team: data-platform, component: ingestion }
        annotations:
          summary: "CDC event lag {{ $value | humanizeDuration }} for {{ $labels.service }}/{{ $labels.collection }}"
          runbook_url: "https://git.internal/data-platform/docs/runbooks/R05-kafka-lag.md"
      - alert: EtlConnectorFailed
        expr: kafka_connect_connector_status{state="failed"} == 1
        for: 2m
        labels: { severity: critical, owner_team: data-platform, component: connect }
        annotations:
          summary: "Kafka Connect connector {{ $labels.connector }} FAILED"
          runbook_url: "https://git.internal/data-platform/docs/runbooks/R06-connector.md"

# monitoring/prometheus/alerts/etl-freshness.yml
  - name: etl-freshness
    rules:
      - alert: EtlTableStale
        expr: etl_table_freshness_seconds > on(table) group_left(owner_team, tier) etl_table_freshness_sla_seconds
        for: 5m
        labels: { severity: error }
        annotations:
          summary: "{{ $labels.table }} is stale ({{ $value | humanizeDuration }})"
          runbook_url: "https://git.internal/data-platform/docs/runbooks/R10-sla.md"
```

```yaml
# monitoring/alertmanager/routes.yml
route:
  receiver: slack-etl
  group_by: [alertname, owner_team]
  group_wait: 1m
  group_interval: 5m
  repeat_interval: 4h
  routes:
    - matchers: [severity="critical"]
      receiver: pagerduty-etl
      continue: true
    - matchers: [owner_team="team-sales-analytics"]
      receiver: slack-sales-analytics
inhibit_rules:                                     # ➕ the root cause silences its consequences
  - source_matchers: [alertname="EpasDown"]
    target_matchers: [component="etl-job"]
    equal: [env]
  - source_matchers: [alertname="KafkaClusterDown"]
    target_matchers: [component=~"ingestion|connect"]
    equal: [env]
```

---

## 4. Logging and Debugging

### 4.1 Central logs

| Choice | Decision |
|---|---|
| Store | **Loki** (cheap, label-based, built into Grafana) ➕ unless your microservices already ship logs to ELK (Elasticsearch, Logstash, Kibana); then use **the same** system |
| Collection | Fluent Bit (or Promtail) DaemonSet reads the JSON stdout logs of every pod (doc 06 §13) |
| Labels (low cardinality) | `env`, `namespace`, `app` (`etl-spark`, `etl-ingestion`, `etl-ops-api`, `airflow`), `etl_job`, `role` (`driver`/`executor`) |
| Fields (searchable, not labels) | `runId`, `step`, `sourceId`, `errorCode`, `ingestionBatchId` |
| Retention | 30 days hot (searchable), 90 days archived (object storage) |
| PII | Never logged (doc 06 §13.3); a log-scanning rule flags email/phone-looking patterns ➕ |

Example queries:

```
# everything of one run, from all pods
{env="prod", app="etl-spark", etl_job="b2s_order"} | json | runId="b2s_order-20261030T090000Z-a1"

# all errors of the ingestion service for one collection in the last hour
{env="prod", app="etl-ingestion"} | json | level="ERROR" | collection="orders"
```

### 4.2 Spark History Server

Every Spark job writes its event log to object storage (doc 08 §7.2). A **Spark History Server** shows the Spark UI of finished jobs: stages, tasks, skew, spills. Retention 30 days. The ops UI run page links directly to it (`spark_ui_url` in `job_run_audit`).

### 4.3 Debug one failed record end to end

> **In simple words:** a customer says "order **ORD-1001** is missing in the sales report". We follow the order like a **parcel tracking number** through every layer, until we find where it stopped.

➕ **Record trace API:** `GET /api/v1/trace?service=svc-order&collection=orders&id=65f1a2b3…` (or `&businessKey=ORD-1001`) returns the journey in one response, so operators don't need database access:

```
 TRACE svc-order / orders / 65f1a2b3c4d5e6f708091a2b   (ORD-1001)

 1. SOURCE     last change 2026-10-30 08:20:00Z (clusterTime 7432128847286337539)
 2. KAFKA      cdc.source.events partition 7 offset 1844221
 3. BRONZE     bronze_svc_order.orders   sourceVersion 7432128847286337539   isDeleted=false
               syncSilver: QUARANTINED   errorCode=DQ_NOT_NULL   runId=b2s_order-20261030T090000Z-a1
 4. QUARANTINE 3f2a… status QUARANTINED  rule SO-001 column order_no "order_no is null"
               (masked snapshot available, reveal needs PII_VIEWER)
 5. SILVER     silver_order.sales_order: ✗ not present
 6. GOLD       gold_sales.fact_sales_line: ✗ not present
 7. RUNS       b2s_order-20261030T090000Z-a1  PARTIAL  (17 bad of 1,000)  → logs | Spark UI | DQ results
 ➜ The record stopped in Bronze → Silver: mandatory field order_no empty at the source.
   Suggested action: SOURCE_FIX by team-order (contract field orderNo required).
```

The same steps by hand (for developers):

| Step | Where | Query / action |
|---|---|---|
| 1 | Bronze (Mongo) | `db.orders.findOne({"data.orderNo": "ORD-1001"}, {_etl: 1})` → status, error, runId, Kafka offset |
| 2 | Quarantine | `SELECT * FROM etl_ctl.quarantine_record WHERE source_key->>'_id' = '65f1…' ORDER BY created_at DESC` |
| 3 | Error history | `SELECT * FROM etl_ctl.record_error_log WHERE source_ref->>'id' = '65f1…' ORDER BY logged_at DESC` |
| 4 | Run | `SELECT * FROM etl_ctl.job_run_audit WHERE run_id = '…'` + `dq_results` of that run |
| 5 | Logs | Loki query with the `runId` (§4.1) |
| 6 | Kafka (rarely) | Read partition 7 at offset 1844221 with the ops API DLQ/inspect tool (masked) |
| 7 | Silver/Gold | `SELECT … FROM silver_order.sales_order WHERE order_id = '65f1…'` |

---

## 5. Security

### 5.1 Identity and RBAC

| Area | Decision |
|---|---|
| People | SSO (OIDC) for the ops UI, Airflow UI, Grafana, OpenMetadata and the Spark History Server. Roles come from SSO **groups** (doc 07 §2.13: VIEWER, OPERATOR, DATA_STEWARD, PII_VIEWER, PLATFORM_ADMIN) |
| Machines | One K8s ServiceAccount + one database role per component (doc 05 §2.3); no shared accounts |
| Admin access to prod databases | Break-glass only: temporary, approved, logged (EPAS audit log) ➕ |

**Least-privilege matrix** (who can touch what):

| Component | Source Mongo | Bronze Mongo | EPAS Silver/Gold | `etl_ctl` | Kafka | K8s |
|---|---|---|---|---|---|---|
| Kafka Connect (per source) | read (`changeStream`, `find`) | — | — | — | write `cdc.*` | own pods |
| `etl-ingestion` | — | read/write `bronze_*` | — | write DLQ index, replay audit | read `cdc.*`, write `dlq.*`, `etl.events.*` | own pods |
| Spark jobs (`etl_job`) | read (re-snapshot/bulk copy only) | read + update `_etl.syncSilver.*` | read/write (owned tables) | read/write | write `etl.lineage.events` | create pods in `etl-<env>` |
| `etl-ops-api` (`etl_ops`) | — | read (trace, reveal) | read | read/write (requests, quarantine, watermarks via API rules) | read DLQ, write replay | — |
| Airflow | — | — | — | — (uses the ops API) | — | create `SparkApplication` in `etl-<env>` |
| Grafana (`etl_monitor`) | — | — | — | read | — | — |
| OpenMetadata (`om_reader`) | read metadata | read metadata | read metadata | read metadata | describe topics | — |
| BI tools (`bi_reader`) | — | — | read Gold `v_*` views only | — | — | — |

### 5.2 Encryption

| Where | How |
|---|---|
| **In transit** | TLS on every connection: Kafka (SASL_SSL), Mongo (`tls=true`), EPAS (`sslmode=verify-full`), all HTTP APIs and UIs (HTTPS). ➕ Optional service mesh with mTLS inside K8s |
| **At rest: EPAS** | EPAS TDE (Transparent Data Encryption, in recent EPAS versions) or encrypted volumes; keys in KMS |
| **At rest: MongoDB** | Encryption at rest (edition-dependent) or encrypted volumes |
| **At rest: Kafka** | Encrypted broker volumes |
| **Object storage** (Spark logs, archives, backups) | Server-side encryption + bucket policies; versioning on |
| **Backups** | Encrypted with a separate backup key ➕ |
| **Very sensitive columns** | Column-level encryption (e.g. national IDs) with keys in KMS/Vault (doc 07 §6.4) |

### 5.3 PII protection (summary of docs 02–10, plus purge)

| Control | Doc |
|---|---|
| PII marked in data contracts (`pii.type`) | 10 §8 |
| Masked in Gold dimensions; BI sees only `v_*` views | 04 §11 |
| Quarantine stores masked snapshots; real values only via audited "reveal" | 07 §6 |
| No PII in logs, metrics, alerts, lineage, error messages, OM sample data | 06, 09, 10 |
| Synthetic data in dev/qa, masked copy in uat | 08 §2 |
| PII tags propagate along column lineage | 10 §6.5 |

**➕ Right-to-be-forgotten (GDPR/DPDP) purge workflow:**

```
 purge_request REQUESTED (ops API, by privacy officer)            ← doc 05 §6.15
   → APPROVED (second person)
   → IN_PROGRESS: purge job (one Spark/SQL job type, audited run) for (service, sourceId):
        Bronze:       hard delete the document + its __history entries
        Silver:       hard delete or anonymize rows (per table policy); SCD2 _hist rows too
        Gold:         anonymize dimension attributes (name/email → 'REDACTED'), KEEP the surrogate key
                      → facts stay valid, the person is no longer identifiable
        Quarantine:   delete snapshots / items for that record
        DLQ / Kafka:  can't delete single messages → covered by retention (≤ 30 days), documented ➕
        Backups:      expire with backup retention (≤ 35 days), documented in the privacy notice ➕
   → layers_done = {bronze, silver, gold, quarantine} → DONE (proof kept 5 years)
```

The source microservice must delete or anonymize first; otherwise a later CDC event would bring the data back. The purge job checks that the source document no longer exists before it starts ➕.

### 5.4 Network isolation

| Rule | How |
|---|---|
| Default deny inside the ETL namespaces | K8s NetworkPolicies: only explicitly allowed traffic (Spark → EPAS/Mongo/Kafka; Airflow → K8s API, ops API; ops API → EPAS/Mongo/Airflow/Kafka) |
| Databases never reachable from the internet | Private subnets; access only from the platform namespaces and admin bastions |
| UIs only internal | Ops UI, Airflow, Grafana, OM behind the internal ingress + SSO |
| Egress control ➕ | Spark pods can only reach the hosts they need (no random internet downloads at runtime) |
| Dedicated Spark node pool | Spark can't starve the microservices (doc 08 §7.2) |

### 5.5 Audit trail: who did what

| Action | Recorded in |
|---|---|
| Manual run request, approval, rejection | `etl_ctl.run_request` (doc 07 §10.2) |
| Every run (who/what/why/which config/which code) | `etl_ctl.job_run_audit` |
| Quarantine fix/approve/reject | `etl_ctl.quarantine_status_history` |
| Bookmark reset | `etl_ctl.watermark_history` |
| DLQ replay/ignore | `etl_ctl.ingestion_dlq_index`, `ingestion_replay_audit` |
| PII reveal | ➕ `etl_ctl.pii_access_log` (below) |
| Privacy purge | `etl_ctl.purge_request` |
| Config/code changes | Git history + pull request approvals |
| Direct database access by people | EPAS audit logging (`edb_audit`) ➕, Mongo audit log |
| K8s actions | K8s audit log |

```sql
-- db/control/V21__pii_access_log.sql
CREATE TABLE etl_ctl.pii_access_log (
    access_id       uuid          PRIMARY KEY DEFAULT gen_random_uuid(),
    accessed_at     timestamptz   NOT NULL DEFAULT now(),
    user_id         varchar(100)  NOT NULL,
    user_roles      text[]        NOT NULL,
    action          varchar(20)   NOT NULL CHECK (action IN ('REVEAL','EXPORT','TRACE_FULL')),
    object_type     varchar(20)   NOT NULL,              -- QUARANTINE_ITEM, BRONZE_DOC, SILVER_ROW
    object_ref      jsonb         NOT NULL,              -- ids only, never the values
    fields          text[]        NOT NULL,              -- which fields were revealed
    reason          varchar(500)  NOT NULL,              -- mandatory justification
    ticket          varchar(50),
    client_ip       inet
);
CREATE INDEX ix_pii_access_user ON etl_ctl.pii_access_log (user_id, accessed_at DESC);
-- retention: 2 years (doc 05 §8); reviewed quarterly (doc 10 §9.4)
```

### 5.6 Software supply chain ➕

| Control | Tool (examples) |
|---|---|
| Dependency vulnerability scan | OWASP Dependency-Check / Snyk in CI; build fails on critical CVEs |
| Image scan | Trivy (or Grype) on every image; base images updated monthly |
| SBOM | Generated for every image (CycloneDX) and stored with the release |
| Signed images | Cosign signatures; K8s admission policy accepts only signed images from our registry |
| Secret scanning | `gitleaks` (doc 08 §5.4) |
| Static analysis | SpotBugs / SonarQube quality gate |

---

## 6. Reconciliation and DQ Reports Between Layers

> **In simple words:** each job already checks its own counts (doc 03 §12). But we also need a **daily stock-take across the whole warehouse**: "does the number of orders in the source match Bronze, Silver and Gold?". It catches problems that no single job can see, such as a connector that silently missed events.

### 6.1 Per-run vs cross-layer checks

| Level | When | What |
|---|---|---|
| Per run (exists) | Every run | `read = success + partial + failed + quarantined + skipped`; merge counts; write-back; sums (doc 03 §12, doc 04 §16) |
| ➕ **Cross-layer reconciliation job** `xlayer_recon` | Daily (02:30 UTC) + on demand | Counts and sums per collection/table and per business day across Source → Bronze → Silver → Gold |

### 6.2 Cross-layer checks

| Check | Expected | Tolerance |
|---|---|---|
| Source vs Bronze (per collection) | `countDocuments(source)` = Bronze non-deleted docs | Small difference allowed for in-flight CDC (e.g. ≤ events of the last 15 min) |
| Bronze vs Silver (per B2S job) | Bronze non-deleted docs with `syncSilver = SUCCESS/PARTIAL` = Silver parent rows not deleted | 0 |
| Bronze not yet synced | Docs `PENDING/FAILED` older than 24 h | Alert if > 0 (stuck records) |
| Silver vs Gold (per fact) | Silver driving rows (not deleted) = fact rows (not deleted) | 0 (minus documented filters) |
| Money per day | `Σ amount` Source = Bronze = Silver = Gold fact = Gold aggregate | ≤ 0.01 per day |
| Gold aggregate vs fact | per touched date | 0 (doc 04 §16) |

```sql
-- excerpt: Silver vs Gold, per order date (yesterday)
SELECT 'silver' AS layer, count(*) AS rows, sum(i.qty * i.unit_price) AS gross
FROM silver_order.sales_order_item i JOIN silver_order.sales_order o USING (order_id)
WHERE NOT i._is_deleted AND NOT o._is_deleted AND o.order_ts::date = current_date - 1
UNION ALL
SELECT 'gold', count(*), sum(gross_amt)
FROM gold_sales.fact_sales_line
WHERE NOT is_deleted AND order_date_key = to_char(current_date - 1, 'YYYYMMDD')::int;
```

Results go to `etl_ctl.reconciliation` (job `xlayer_recon`) and to OM as test results (doc 10 §7).

### 6.3 DQ reports

| Report | Audience | Channel |
|---|---|---|
| ➕ **Daily DQ digest** per domain: failed/partial runs, top failing rules, new quarantine items, contract breaches, cross-layer mismatches | Data stewards, job owners | Email/Slack at 08:00 |
| DQ dashboard (Grafana #4) | Data team | Grafana |
| Table quality history (test results) | Everyone | OpenMetadata table page |
| ➕ **DQ scorecard** per Gold table: completeness (% non-null mandatory), validity (% passing rules), uniqueness, timeliness (% runs within freshness SLA), over 30 days | Business owners | Monthly, OM + Grafana |

---

## 7. Testing

### 7.1 Test pyramid

> **In simple words:** many **small, fast** tests at the bottom (check one function), fewer **bigger** tests in the middle (check parts working together), and a few **full end-to-end** tests at the top (check the whole chain). Fast tests run on every code change; slow ones run nightly.

```
                 ▲  few, slow
                 │   ┌──────────────────────────────────────────────┐
                 │   │ E2E: canary jobs in qa on synthetic data;      │ nightly / per release
                 │   │ regression diff vs previous version; perf test │
                 │   ├──────────────────────────────────────────────┤
                 │   │ Integration: Testcontainers Kafka + Connect +  │ every merge to main
                 │   │ Mongo replica set + Postgres/EPAS              │
                 │   ├──────────────────────────────────────────────┤
                 │   │ Golden dataset tests (any job YAML + fixtures) │ every pull request
                 │   │ Contract tests, config validation, DAG tests   │
                 │   ├──────────────────────────────────────────────┤
                 │   │ Unit: transforms, DQ rules, SQL generators,    │ every pull request
                 │   │ classifiers, run-mode policies, parsers        │
                 ▼   └──────────────────────────────────────────────┘
                     many, fast
```

### 7.2 What each level covers

| Level | Covers | Tools |
|---|---|---|
| Unit | Every column transform, every DQ rule type, `try_cast` errors, splitter, dedupe strategies, SCD change detection, status classifier, run-mode policies, error classifier, Mongo/SQL filter translators | JUnit 5, local `SparkSession` from `etl-testkit` |
| SQL generators | Generated merge/SCD/fact SQL **executed** against a real Postgres container, every row of the merge-guard tables (docs 03/04) | Testcontainers PostgreSQL |
| Golden dataset | Any job YAML + `input/*.json` → `expected/*.csv` (doc 06 §12.4) | Generic JUnit runner |
| Integration | Kafka + Connect + Mongo replica set → ingestion → Bronze → B2S → Silver → S2G → Gold, with deletes, duplicates, out-of-order events, restarts | Testcontainers (Kafka, MongoDB, PostgreSQL) |
| ➕ EPAS-specific | Nightly run of the integration suite against the **real EPAS** in dev (or an EDB container image if licensed), because plain PostgreSQL may hide EPAS differences | Nightly pipeline |
| Contract | Contract files valid; microservice sample documents pass their contract; connector configs validate via the Connect API; YAML JSON Schema | `etl-contract-check`, CI |
| DAG | Integrity, factory output, manifests, result mapping (doc 09 §11) | pytest |
| ➕ Regression diff | Run the **new** and the **previous** version on the same input snapshot (uat, masked data) and compare outputs table by table; differences must be explained in the release notes | "shadow run" job + diff report |
| ➕ Performance | Synthetic volume (e.g. 10 M orders): runtime, memory, DB load per job type; trend tracked per release | Nightly in qa |
| Recovery / chaos | Kill pods mid-run, stop EPAS/Mongo briefly, replay a range: data must end identical (doc 02 §17, doc 07 §9.2) | Monthly in qa |

### 7.3 Test data

| Data | Where |
|---|---|
| Small fixtures (hand-written) | `src/test/resources/**` next to the tests |
| ➕ Synthetic generators (realistic Indian names/addresses/phones with a fake-data library such as Datafaker) | `etl-testkit/generators`, used for dev/qa and performance tests |
| Masked prod copy | uat only, refreshed monthly by a masking job (same masking rules as doc 07 §6.4) |
| Edge-case library ➕ | One shared set of "nasty" documents: nulls, empty arrays, huge arrays, unicode, `$`-prefixed fields, future dates, decimal precision, deleted-then-recreated ids |

**Coverage goals:** framework modules ≥ 80% line coverage; **100%** of rule types, merge paths, run-mode policies and status-decision rows tested.

---

## 8. CI/CD

### 8.1 Pipeline diagram

```
 ┌─────────────────────────── PULL REQUEST (every change) ───────────────────────────┐
 │ build all modules → unit tests → static analysis → dependency + secret scan       │
 │ → config validation for ALL envs + config diff report (doc 08 §9)                  │
 │ → contract tests → golden dataset tests → DAG tests → SQL generator tests          │
 │ → integration tests (Testcontainers)                    → required reviewers (CODEOWNERS)
 └───────────────────────────────────────┬───────────────────────────────────────────┘
                                         │ merge to main
 ┌───────────────────────────────────────▼───────────────────────────────────────────┐
 │ RELEASE BUILD  version 1.14.2                                                      │
 │ images: etl-spark-app, etl-ingestion, etl-ops-api (+ SBOM, scan, sign)             │
 │ DAG bundle (dags + etl_airflow + generated/jobs.<env>.json for every env)          │
 │ migrations (db/**), connector configs, governance files → one versioned release    │
 └───────────────────────────────────────┬───────────────────────────────────────────┘
                                         ▼
 ┌──────────── DEPLOY (same steps per env, doc 08 §9) ────────────┐
 │ 1 Flyway migrate (expand-only ➕)   2 registry sync             │
 │ 3 governance publish (doc 10)       4 Kafka topics              │
 │ 5 Kafka connectors                  6 Helm: ingestion, ops API  │
 │ 7 secrets mapping (ESO)             8 Airflow DAG bundle        │
 │ 9 smoke test: dry-run of canary jobs                            │
 └───────┬─────────────────────┬──────────────────┬───────────────┘
         ▼                     ▼                  ▼                       ▼
       dev (auto)  ──►  qa (auto + nightly   ──► uat (approval +     ──► prod (approval + change ticket
                        E2E canaries,             regression diff on      + deploy window; not during
                        perf, recovery)           masked data)            month-end freeze ➕)
                                                                              │
                                                                              ▼
                                                     post-deploy verification (15 min): smoke dry-run OK,
                                                     no new CRITICAL alerts, first scheduled runs green
                                                     → else ROLLBACK (§8.3)
```

### 8.2 Versioning rules

| Item | Rule |
|---|---|
| One **release** | Images + DAG bundle + configs + migrations + connector configs + governance files, all with the same version and Git commit |
| Version format | SemVer (doc 06 §11.2); `code_version` in every `job_run_audit` row |
| ➕ **Database migrations: expand/contract** | A release only **adds** (new tables/columns, nullable or with defaults). Removing or renaming happens in a **later** release, after no code uses the old thing. So rolling back the code never requires rolling back the database. Flyway has no "down" migrations in prod |
| Connector changes | Changes to the `pipeline`, key or topic mapping of a connector are **change-managed** (they affect partitioning and ordering, doc 02), reviewed by the platform team |
| ➕ Feature flags | Risky behaviour changes ship behind a flag in `features.yml` (doc 08), switched on per environment after the deploy |

### 8.3 Rollback

| What went wrong | Rollback |
|---|---|
| Spark code | Deploy the previous release (DAG bundle with the previous `imageVersion`), or set `etl_spark_app_version_override` (doc 09 §6.1) for an instant switch |
| Ingestion / ops API | `helm rollback` to the previous revision |
| DAG bundle | Redeploy the previous bundle |
| Configs | Part of the release → go back with the release |
| Database | Not rolled back (expand/contract); the old code works with the expanded schema |
| Connectors | Re-`PUT` the previous config (the deploy script is idempotent) |
| **Data written by a bad release** | Find the affected runs (`job_run_audit.code_version = '1.14.2'`), fix/rollback the code, then `BACKFILL`/`REPLAY` the affected range with cascade (doc 07) ➕ |

### 8.4 Tools

Use the CI/CD system your microservices already use (GitHub Actions, GitLab CI or Jenkins). Optional: Argo CD for GitOps-style Helm deployments. The pipeline steps above are tool-independent shell/Maven/Python commands.

---

## 9. Performance and Cost

### 9.1 Spark

| Practice | Detail |
|---|---|
| Dynamic allocation | On in uat/prod with shuffle tracking (doc 08); min/max executors per profile |
| ➕ Right-sizing from metrics | Grafana #6 compares used vs requested memory/CPU per job; profiles are reviewed monthly; over-sized jobs move down a profile |
| AQE, broadcast joins, no UDFs, explicit schemas, pushdown | docs 03 §11, 06 §12.2 |
| Caching discipline | Only via `PipelineState.persist()`, always unpersisted |
| Small, frequent jobs | Empty-run short-circuit; job grouping (`--jobs=`) in one Spark app (doc 06) |
| ➕ Spot / preemptible nodes | For LOW-priority and non-urgent jobs (retries are safe thanks to idempotency); never for HIGH-priority Gold facts |
| ➕ K8s quotas | `ResourceQuota` + `LimitRange` per `etl-<env>` namespace: Spark can never take more than its share of the cluster |

### 9.2 "Small files"

> **In simple words:** a classic data-lake problem is millions of tiny files, which make reading slow. Our main targets are **databases**, so this problem mostly doesn't exist for us. It appears only in two places:

| Place | Handling |
|---|---|
| Archive Parquet files (doc 05 §8) | One archive job writes **one file per table per month**, targeting 128–512 MB; coalesce before writing |
| Spark event logs | Rolling event logs (`spark.eventLog.rolling.enabled=true`) + 30-day retention |
| (database equivalent) "small batches" | Sensible schedules + empty-run short-circuit + job grouping |

### 9.3 Databases

| EPAS | MongoDB |
|---|---|
| Partitioning for large facts (doc 04 §12) | Partial index for the sync pick-up (doc 02 §6.4) |
| ➕ `fillfactor` 80–90 on update-heavy Silver tables → more in-place ("HOT") updates, less bloat | Write concern majority only where needed (Bronze writes, write-back) |
| ➕ Autovacuum tuned per heavy table (merge targets, `gold_record_status`) | Oplog sized ≥ 72 h (doc 02 §1) |
| ➕ PgBouncer (connection pooler) in front of EPAS for the ops API and BI tools | Monitor slow ops (> 100 ms) |
| Read replica for BI and heavy reads (doc 08 §7.1) | Secondary reads for Spark (doc 03 §2.2) |
| `ANALYZE` staging before merges (doc 03) | — |

### 9.4 Kafka and Airflow

| Kafka | Airflow |
|---|---|
| zstd compression | Deferrable operators (waiting costs no worker) |
| Partitions sized once, generously (doc 02 §3.2) | Pools with weighted slots (doc 09 §8) |
| Retention sized for replay; tiered storage for longer windows | Scheduler parsing interval tuned for 150 DAGs; JSON-based factory (fast parse) |
| — | `catchup=False` (no surprise runs) |

### 9.5 Cost visibility ➕

| Practice | Detail |
|---|---|
| Labels on every Spark pod | `etl/job`, `etl/domain`, `etl/team` (doc 09 §3.3) |
| Cost per team/domain | OpenCost/Kubecost (or the cloud's cost tool) by those labels; monthly report per domain |
| Storage growth | Monthly table/collection size report (EPAS, Mongo, Kafka, object storage) with the top growers |
| Budget alerts | When a domain's monthly cost exceeds its budget by 20% |

---

## 10. Disaster Recovery

### 10.1 Targets per component

> **In simple words:** **RPO** = "how much recent data may we lose" (like how often you save a document). **RTO** = "how fast must we be working again". Different parts of the platform need different promises.

| Component | RPO | RTO | How |
|---|---|---|---|
| EPAS (Silver, Gold, `etl_ctl`) | ≤ 5 min | ≤ 4 h | Continuous WAL archiving + daily full backups → PITR; optional streaming standby in a second site |
| Bronze MongoDB | ≤ 15 min (plus Kafka replay) | ≤ 4 h | Backups with point-in-time (e.g. Percona Backup for MongoDB / Ops Manager / snapshots) + Kafka replay window (14 days) |
| Kafka | Replication factor 3 (no data loss on one broker failure) | ≤ 2 h | Not a backup system; for site loss: MirrorMaker 2 to a DR cluster (optional) or re-snapshot |
| Code, configs, DAGs, governance | 0 | ≤ 1 h | Git + artifact registry |
| Secrets | ≤ 1 h | ≤ 1 h | Vault backups/snapshots |
| Airflow metadata DB | ≤ 24 h | ≤ 2 h | Daily backup. ➕ Losing it is not critical: DAGs come from the bundle, the real run history is in `job_run_audit` |
| OpenMetadata | ≤ 24 h | ≤ 1 day | Its DB backup; most metadata can be re-ingested, governance-as-code is in Git |
| Object storage (archives, event logs) | — | — | Versioning + cross-region replication for archives |

### 10.2 Backups

| System | Tool (examples) | Schedule | Retention | Restore test |
|---|---|---|---|---|
| EPAS | Barman or pgBackRest (EDB-supported tools) with WAL archiving | Daily full/incremental + continuous WAL | 35 days (+ monthly kept 12 months, if required) | ➕ **Monthly** restore into a test instance, with a reconciliation run against it |
| Bronze MongoDB | Point-in-time backup tool or filesystem snapshots | Daily + oplog | 35 days | Monthly |
| Vault | Raft snapshots | Hourly | 30 days | Quarterly |
| Airflow / OM databases | `pg_dump` | Daily | 14 days | Quarterly |

All backups encrypted, stored **off-site** (another region/data centre), and monitored (alert A25).

### 10.3 Recovery scenarios

| # | Scenario | Recovery steps |
|---|---|---|
| D1 | **EPAS lost/corrupted** | 1. PITR restore to time **T** (last good moment). 2. Silver, Gold, bookmarks and statuses are **consistent with each other**, because they live in one database and were written in one transaction (doc 05 §2.2) ➕. 3. ➕ **Bronze is ahead of Silver now:** Bronze still says `SUCCESS` for documents synced after T. Reset them: `updateMany({"_etl.syncSilver.updatedAt": {$gt: T}}, {$set: {"_etl.syncSilver.status": "PENDING"}})` (an ops API "resync after restore" action, audited). 4. Run incremental B2S → Gold follows via change numbers. 5. `xlayer_recon` to confirm |
| D2 | **Bronze MongoDB lost** | 1. Restore the latest backup (time T). 2. Kafka **replay** from T (within 14 days) with the replay job (doc 02 §15). 3. If T is older than Kafka retention: **re-snapshot** + reconciliation (doc 02 §4.3). 4. Silver is idempotent: unchanged data stays "unchanged" |
| D3 | **Kafka cluster lost** | Connector offsets and consumer offsets are lost with it. 1. Rebuild/restore the cluster (or fail over to the DR cluster via MM2). 2. Start connectors with a **re-snapshot** (option B, doc 02 §4.1) + reconciliation for deletes. 3. Everything downstream is idempotent |
| D4 | **Spark / K8s cluster lost** | Stateless: redeploy, locks expire (10 min lease), zombie runs are closed, Airflow reruns. No data loss |
| D5 | **Airflow lost** | Redeploy, restore its DB (or start empty), DAGs come from the bundle. Incremental jobs catch up automatically on the first run (`catchup=False`, status/bookmark-based) |
| D6 | **Bad release corrupted data** | Rollback the code (§8.3); find affected runs by `code_version`; `BACKFILL`/`FULL_LOAD` + cascade for the affected jobs/range; `xlayer_recon` |
| D7 | **Accidental delete/truncate of a Gold table** | Option A: `FULL_LOAD` of its job (dimensions: only via merge, never truncate). Option B: PITR into a **side** instance, copy the table back, then incremental runs |
| D8 | **Whole site/region down** (optional, depends on the business RTO) | Warm standby: EPAS standby + Mongo replica member + Kafka MM2 in the DR site; K8s + images ready; switch DNS; resume from the last replicated state; reconciliation |

➕ **DR drill twice a year:** practice D1 and D2 in qa/uat with a stopwatch; update the runbooks with what was learned.

---

## 11. Operational API and UI

> **In simple words:** the **ops UI** is the **control panel** for people who run the platform. They can see everything, start any kind of run (with the right permission), fix quarantined records and trace a single record, **without** writing SQL or touching Kubernetes.

The API design is in doc 07 §10. Here is the complete list in one place.

### 11.1 Endpoint list

| Area | Method & path | Purpose | Minimum role (prod) |
|---|---|---|---|
| **Runs** | `GET /api/v1/runs` | List/filter runs | VIEWER |
| | `GET /api/v1/runs/{runId}` | Run detail: counts, steps, error, links | VIEWER |
| | `GET /api/v1/runs/{runId}/chain` | Rerun/cascade tree (root → parents → children) | VIEWER |
| | `GET /api/v1/runs/{runId}/steps` | Step timeline (`job_step_audit`) | VIEWER |
| | `GET /api/v1/runs/{runId}/dq` | DQ results of the run | VIEWER |
| | `GET /api/v1/runs/{runId}/reconciliation` | Reconciliation checks | VIEWER |
| | `POST /api/v1/runs/preview` | Size estimate for a request | OPERATOR |
| | `POST /api/v1/runs` | Request a run (all modes, dry-run, cascade) | per mode (§11.2) |
| | `POST /api/v1/runs/{runId}/rerun` | `RERUN_JOB` / `RERUN_FAILED_RECORDS` shortcut | OPERATOR |
| | `POST /api/v1/runs/{runId}/cancel` | Cancel | OPERATOR |
| **Approvals** | `GET /api/v1/run-requests?status=PENDING_APPROVAL` | Approval inbox | OPERATOR |
| | `POST /api/v1/run-requests/{id}/approve` · `/reject` | Four-eyes decision | OPERATOR (≠ requester) |
| **Jobs** | `GET /api/v1/jobs` · `/jobs/{job}` | Registry, last run, SLA state, config viewer | VIEWER |
| | `GET /api/v1/jobs/{job}/runs` | History of one job | VIEWER |
| | `GET /api/v1/jobs/{job}/downstream` | Cascade preview | VIEWER |
| | `GET /api/v1/jobs/{job}/watermarks` | Bookmarks + history | VIEWER |
| | `POST /api/v1/jobs/{job}/watermarks/reset` | Move a bookmark (reason + ticket) | PLATFORM_ADMIN (+ approval) |
| | `POST /api/v1/jobs/{job}/lock/release` | Emergency lock release | PLATFORM_ADMIN |
| **Quarantine** | `GET /api/v1/quarantine` | List / group by job, code, rule | OPERATOR |
| | `GET /api/v1/quarantine/{id}` | Item detail (masked) + history | OPERATOR |
| | `POST /api/v1/quarantine/{id}/reveal` | Real values once (audited in `pii_access_log`) | PII_VIEWER |
| | `POST /api/v1/quarantine/{id}/fix` | → FIXED (fix type, patch, note) | OPERATOR / DATA_STEWARD |
| | `POST /api/v1/quarantine/{id}/approve` · `/reject` | → READY_TO_REPROCESS / REJECTED | DATA_STEWARD (≠ fixer) |
| | `POST /api/v1/quarantine/bulk` | Bulk fix/approve/reject by filter | DATA_STEWARD |
| | `POST /api/v1/quarantine/reprocess` | `REPROCESS_QUARANTINE` request | OPERATOR / DATA_STEWARD |
| **DQ** | `GET /api/v1/dq/results?job=&table=&rule=&from=&to=` | DQ history | VIEWER |
| | `GET /api/v1/dq/scorecards` | Table scorecards | VIEWER |
| | `GET /api/v1/reconciliation/xlayer?date=` | Cross-layer results | VIEWER |
| **Ingestion** | `GET /api/v1/dlq` | DLQ index (grouped) | OPERATOR |
| | `POST /api/v1/dlq/replay` · `/ignore` | Replay / ignore with ticket | OPERATOR (prod: approval for > 10k) |
| | `POST /api/v1/ingestion/replay` | Kafka range replay / re-snapshot | PLATFORM_ADMIN (+ approval) |
| | `POST /api/v1/ingestion/resync-after-restore` | D1 step 3 (Bronze status reset after a time T) | PLATFORM_ADMIN (+ approval) |
| **Trace** | `GET /api/v1/trace?service=&collection=&id=` or `&businessKey=` | Record journey across layers (§4.3) | OPERATOR (masked) |
| **Privacy** | `POST /api/v1/purge-requests` · `GET` · `/approve` | GDPR/DPDP purge workflow | PRIVACY_OFFICER ➕ (+ approval) |
| **Audit** | `GET /api/v1/audit?user=&action=&from=` | Who did what (requests, approvals, reveals, resets) | PLATFORM_ADMIN |
| **Platform** | `GET /actuator/health`, `/actuator/prometheus` | Health + metrics | system |

➕ New role `PRIVACY_OFFICER` (named people from the privacy/legal team) for purge requests.

### 11.2 Who can trigger which run mode (summary of doc 07 §2.13)

| Run mode | dev / qa | uat / prod |
|---|---|---|
| `INCREMENTAL`, `RERUN_JOB`, `RERUN_FAILED_RECORDS`, `ID_REPROCESS` | OPERATOR | OPERATOR |
| any mode + **dry-run** | OPERATOR | OPERATOR |
| `REPROCESS_QUARANTINE` | OPERATOR / DATA_STEWARD | OPERATOR / DATA_STEWARD |
| `BACKFILL` ≤ 31 days | OPERATOR | OPERATOR |
| `BACKFILL` > 31 days, `FULL_LOAD` | OPERATOR | OPERATOR + **approval** |
| `REPLAY`, watermark reset, ingestion replay, resync-after-restore | PLATFORM_ADMIN | PLATFORM_ADMIN + **approval** + ticket |

### 11.3 UI screens

| Screen | Content |
|---|---|
| **Home** | Traffic-light grid of all jobs; today's failures/partials; freshness breaches; open quarantine; approvals waiting for me |
| **Jobs** | Searchable list (layer, domain, owner, status, schedule, next run) |
| **Job detail** | Run history (colour by status), duration trend vs SLA, DQ trend, config viewer (resolved YAML, secrets hidden), dependencies (upstream/downstream graph), "Request run" button |
| **Run detail** | Counts with the equation, step timeline, DQ results, reconciliation, error code + catalog explanation, links (logs, Spark UI, Airflow, OM), **rerun chain tree** (`root_run_id` / `parent_run_id`) |
| **Request run** | Mode-specific form (dates for BACKFILL, ids for ID_REPROCESS …), dry-run toggle, size preview, cascade preview, reason + ticket |
| **Approvals** | Inbox with request details, preview, approve/reject |
| **Quarantine** | Grouped view (job + code + rule), item detail (masked), reveal (audited), fix/approve/reject, bulk actions |
| **DLQ** | Grouped by error code/collection, replay/ignore |
| **Record trace** | Search by source id or business key → the journey (§4.3) |
| **Watermarks & locks** | Current bookmarks + history, lock holders (admin actions) |
| **Privacy** | Purge requests and their progress per layer |
| **Audit** | Who did what |

**Technology:** Spring Boot (doc 06 §9) + Spring Security (OIDC SSO); the frontend in whatever your organization already uses for internal tools (React/Angular). OpenAPI (Swagger) spec published from the code. Two replicas behind the internal ingress (stateless) ➕. Every write action is audited.

---

## 12. Runbooks

> **In simple words:** a runbook is a **recipe card for problems**: "if you see this, check these things, do these steps, and confirm with this". The on-call person at 3 AM should not need to be the person who wrote the code.

Each runbook is a file `docs/runbooks/RNN-<name>.md` with the same sections: **Symptom → Impact → Diagnose → Fix → Verify → Prevent**. Alerts link to them (`runbook_url`).

| # | Scenario | Diagnose | Fix | Verify |
|---|---|---|---|---|
| **R01** | Job FAILED `INFRA_DB_UNAVAILABLE` / `INFRA_MONGO_UNAVAILABLE` | DB alerts (A19/A21)? Network? Credentials rotated? | Fix the DB/network; Airflow retries automatically; if retries exhausted → `RERUN_JOB` (nothing was committed) | Next run SUCCESS; no data gap (`xlayer_recon`) |
| **R02** | `THRESHOLD_BREACHED` (circuit breaker) | `dq_results` of the run: which rule exploded? Compare with the source: schema change? Recent deploy (`code_version`)? | Source issue → contact the source team (contract breach); our bug → rollback/fix, then `RERUN_JOB`; rule too strict → change the rule (PR) | Rerun SUCCESS/PARTIAL with a normal failure rate |
| **R03** | `RECONCILIATION_MISMATCH` / `SCD_INTEGRITY_VIOLATION` | This means a **framework bug**. Look at the `reconciliation` details and the run's steps; reproduce with the staging data (kept until the next run) | Pause the job (disable in the registry via PR or the ops API emergency switch); fix; `FULL_LOAD`/`BACKFILL` of the affected range | Equations hold; integrity checks pass |
| **R04** | PARTIAL with many `REF_PARENT_MISSING` | Is the parent job (e.g. `b2s_customer`) late or failing? | Fix/run the parent job; the child records retry automatically (backoff); urgent → `RERUN_FAILED_RECORDS` | Failed count drops to 0 within the retry window |
| **R05** | Kafka consumer lag / event-time lag high | Ingestion pods healthy? Mongo Bronze slow (write latency)? One hot collection (per-collection lag)? Bulk snapshot running on the main topic? | Scale ingestion pods (≤ partitions); fix Bronze performance; move a hot collection to its own topic (config, doc 02 §3.4) | Lag back below threshold |
| **R06** | Connector FAILED / collection silent / oplog low | Connector status and logs via the Connect REST API; resume token error? | Restart the connector; if the resume token is lost (oplog rolled over) → re-snapshot + reconciliation (doc 02 §4.3); grow the oplog | Events flowing; `xlayer_recon` source vs Bronze OK |
| **R07** | DLQ spike / `UNMAPPED_COLLECTION` | Group the DLQ by error code and fingerprint | Add the missing route / fix the handler; deploy; **DLQ replay** with a ticket | DLQ items `REPLAYED`; Bronze has the documents |
| **R08** | Schema drift / contract breach / DQ anomaly | Which field changed? Contract version? | Talk to the source team; additive → update the contract (MINOR) and use the field; breaking → the source team rolls back or follows the MAJOR process (doc 10 §8.5) | Drift metric back to 0; DQ ratio normal |
| **R09** | Zombie run / stuck lock | Is the Spark driver pod still alive? Last heartbeat? | Dead → the lock expires by itself (10 min); force with `lock/release` only if the holder is confirmed dead; the zombie check marks the run FAILED; `RERUN_JOB` | Next run acquires the lock |
| **R10** | SLA breach (duration / freshness / ready-by) | Upstream late? Job slower than usual (step timeline)? Pool full (pending tasks)? | Fix the upstream; raise priority or pool slots temporarily; right-size the profile; tell the business owner (template message) | Freshness back within SLA |
| **R11** | Quarantine backlog growing | Grouped view: one rule / one source behind most items? | Bulk action: source fix request, rule change or code fix → bulk `FIXED` → approve → reprocess | Open items decreasing |
| **R12** | Gold unknown-SK share high / old inferred members | Dimension job failing? Source dimension data missing? | Fix the dimension job; inferred members fill in automatically; for missing source data → source team | Unknown share below threshold |
| **R13** | EPAS disk/connections/locks, DEFAULT partition rows | Biggest tables, bloat, long transactions, missing future partitions | Run partition maintenance; move DEFAULT rows into a new partition; kill runaway sessions; add disk; tune autovacuum | Alerts clear |
| **R14** | Backup failed | Backup tool logs, disk space, credentials | Fix and run a manual backup immediately | Successful backup + restore test scheduled |
| **R15** | Suspected PII leak (e.g. PII found in logs or a wrong grant) | What, where, since when, who had access (`pii_access_log`, grants, log queries) | Contain (remove the grant / purge the logs), rotate exposed secrets, inform the privacy officer (legal reporting timelines), fix the root cause | Incident report; follow-up checks added |
| **R16** | Spark OOM / executors lost / duration regression | Spark History Server: skew? spill? which stage? data volume jump? | Bigger profile temporarily; fix skew (salting/AQE), reduce `maxRecordsPerRun`, check for a missing filter | Duration back to baseline |
| **R17** | Governance publisher lagging | OM reachable? API errors? | Fix OM/network; the publisher catches up from its bookmark | Lag < 5 min |
| **R18** | GDPR/DPDP purge request | Request valid? Source already purged? | Follow §5.3: approval → purge job → layers_done → DONE | All layers checked; proof stored |
| **R19** | Airflow scheduler/triggerer down | Pod status, metadata DB | Restart / restore; jobs catch up automatically (`catchup=False`, status-based) | DAGs running again |

---

## 13. Production Readiness Checklist (Go-Live)

> **In simple words:** tick every box before the first production job goes live. Items marked **(gate)** must be done; the rest may follow within the first month, with an owner and a date.

### 13.1 Platform and infrastructure
- [ ] (gate) Separate namespaces per environment; dedicated Spark node pool in prod; ResourceQuota/LimitRange set
- [ ] (gate) EPAS, Mongo (replica sets), Kafka (RF 3) sized and load-tested; oplog ≥ 72 h
- [ ] (gate) Spark Operator, Airflow (scheduler + triggerer HA), ops API (2 replicas), Spark History Server deployed
- [ ] Partitions created ahead; DEFAULT partition alert active

### 13.2 Security
- [ ] (gate) SSO for all UIs; roles mapped from SSO groups; break-glass procedure documented
- [ ] (gate) TLS everywhere; encryption at rest for EPAS, Mongo, Kafka, object storage, backups
- [ ] (gate) Secrets only in Vault/ESO; `gitleaks` in CI; no secrets in images, args, XCom or logs
- [ ] (gate) Least-privilege DB roles and K8s ServiceAccounts as in §5.1; NetworkPolicies default-deny
- [ ] (gate) PII: contracts mark PII; Gold masking; quarantine masking; reveal audited; no PII in logs/metrics/lineage
- [ ] Image signing + admission policy; SBOM per release; CVE scanning gates
- [ ] Purge (GDPR/DPDP) workflow tested end to end

### 13.3 Data correctness
- [ ] (gate) Per-run reconciliation strict for all jobs; CHECK constraint on run counts
- [ ] (gate) Cross-layer reconciliation job running daily
- [ ] (gate) DQ rule sets for every job (baseline from contracts + business rules)
- [ ] (gate) Data contracts v1.0.0 for every source collection, approved by both teams
- [ ] DQ digest and scorecards published

### 13.4 Observability
- [ ] (gate) Dashboards #1–#7 provisioned from Git
- [ ] (gate) Alerts A1–A12, A15, A19, A21, A23, A25 active with owners and runbook links; routing + inhibition tested
- [ ] (gate) Central logs with `runId` search; Spark History Server linked from the ops UI
- [ ] Record trace API working for B2S and S2G
- [ ] Weekly alert review scheduled

### 13.5 Operations
- [ ] (gate) Ops UI: runs, chain view, request run (all modes), approvals, quarantine, DLQ, trace
- [ ] (gate) Runbooks R01–R10 written and reviewed by on-call engineers
- [ ] (gate) On-call rotation and escalation defined; pager tested
- [ ] Runbooks R11–R19 written

### 13.6 Delivery
- [ ] (gate) CI pipeline with all PR checks; release = one versioned bundle
- [ ] (gate) Expand/contract migration rule agreed; rollback tested once in uat
- [ ] (gate) Deploy-time smoke dry-run; post-deploy verification
- [ ] Regression diff and performance tests running for releases
- [ ] Release calendar with freeze windows (e.g. month-end close)

### 13.7 Recovery
- [ ] (gate) EPAS and Mongo backups running, off-site, encrypted, monitored
- [ ] (gate) One successful restore test of each (with reconciliation)
- [ ] (gate) Kafka retention = replay window confirmed (14 days prod); replay job tested
- [ ] DR drill scheduled (twice a year)
- [ ] RPO/RTO agreed with the business

### 13.8 Governance
- [ ] (gate) OpenMetadata PoC passed (EPAS connector, lineage, DQ API)
- [ ] (gate) Every table has an owner and a domain; Gold columns have descriptions
- [ ] Lineage (table + column) visible for all go-live jobs; DQ results visible in OM
- [ ] Glossary terms for every KPI

### 13.9 Go-live approach ➕
- [ ] Start with a **pilot**: 2–3 sources, their B2S jobs, 1–2 Gold facts
- [ ] Run the pilot in **shadow mode** first (Gold not yet used by business users) for 2 weeks; compare with existing reports
- [ ] Sign-off by the business owner of the first Gold mart
- [ ] Only then scale out domain by domain (the roadmap is in the final consolidation document)

---

## 14. ➕ Added by Architect (summary)

| # | Addition | Why |
|---|---|---|
| P1 | Grafana reads `etl_ctl` directly + a small SQL exporter for alertable numbers | No duplicated run data; alerts still possible |
| P2 | No run/record ids as Prometheus labels | Prevents metric cardinality explosions |
| P3 | Clear split: job-outcome alerts from Airflow, infra/lag/freshness from Prometheus; inhibition rules | No duplicate alerts; root causes silence their consequences |
| P4 | DQ anomaly alert (vs 7-day baseline), duration-regression alert, backup and certificate alerts | Catches slow degradation and silent operational risks |
| P5 | Record trace API | One-click end-to-end debugging without DB access |
| P6 | Least-privilege matrix; break-glass DB access; EPAS audit logging | Clear, minimal permissions and full traceability |
| P7 | `pii_access_log` + `PRIVACY_OFFICER` role + full purge workflow (incl. Kafka/backup handling) | Privacy compliance that can be proven |
| P8 | Supply-chain security: scanning, SBOM, signed images, admission policy | Protects against vulnerable or tampered images |
| P9 | Cross-layer reconciliation job + DQ digest + scorecards | Proves numbers across the whole chain |
| P10 | Nightly integration tests against real EPAS; regression diff; performance trend; edge-case library | Catches EPAS differences and behaviour changes before prod |
| P11 | Expand/contract migrations; feature flags; freeze windows; post-deploy verification | Safe deploys and safe rollbacks |
| P12 | Right-sizing from metrics; spot nodes for low-priority jobs; quotas; cost per team | Lower and visible cost |
| P13 | `fillfactor`/autovacuum tuning, PgBouncer | Healthy EPAS under constant merges |
| P14 | "Resync after restore" for Bronze statuses after an EPAS PITR | Prevents silent data gaps after a restore |
| P15 | Monthly restore tests, DR drills | Backups that are proven to work |
| P16 | Pilot + shadow mode go-live | Trust before scale |

---

## 15. Changes to Earlier Decisions

| Earlier | Now | Reason |
|---|---|---|
| Doc 05 table list | + `etl_ctl.pii_access_log` (`V21__pii_access_log.sql`) | Audited PII reveals |
| Doc 07 RBAC roles | + `PRIVACY_OFFICER` | Purge workflow |
| Doc 07 §10 endpoints | + trace, DQ scorecards, cross-layer reconciliation, resync-after-restore, purge, audit | Complete ops API |
| Doc 05 §8 | Retention of `pii_access_log` = 2 years | — |

---

## 16. Key Decisions

1. **Monitoring:** Prometheus + Grafana + Alertmanager (reuse the microservices' stack), Pushgateway for job results, exporters for Kafka/Connect/EPAS/Mongo/Airflow/K8s, Grafana reading `etl_ctl` directly, a SQL exporter for alertable control-table numbers; nine provisioned dashboards; no high-cardinality labels.
2. **Alerting:** the 27-row matrix with owners and runbooks; job outcomes from Airflow callbacks, everything else from Prometheus; grouping, inhibition, severity-based routing; weekly alert review.
3. **Logging/debugging:** JSON logs to Loki (or the existing ELK), `runId` as a searchable field; Spark History Server; record trace API for end-to-end debugging.
4. **Security:** SSO + RBAC from groups; least-privilege roles per component; TLS everywhere and encryption at rest incl. backups; PII controls across all layers + a full purge workflow; NetworkPolicies; complete audit trail incl. `pii_access_log`; supply-chain controls.
5. **Correctness:** per-run reconciliation + a daily cross-layer reconciliation job; DQ digest and scorecards; results also in OpenMetadata.
6. **Testing:** pyramid of unit, SQL-generator, golden, contract, DAG, integration (Testcontainers), nightly real-EPAS, regression diff, performance and recovery tests; synthetic and masked data; coverage goals.
7. **CI/CD:** PR checks → one versioned release bundle → same deploy steps per env → dev/qa automatic, uat/prod with approvals, freeze windows, post-deploy verification; expand/contract migrations; documented rollback for code, configs, connectors and data.
8. **Performance/cost:** dynamic allocation, right-sizing, spot nodes for low-priority jobs, quotas, DB tuning (partitions, fillfactor, autovacuum, PgBouncer, replicas), archive file sizing, cost per team via pod labels.
9. **Disaster recovery:** RPO/RTO per component; PITR backups for EPAS and Mongo with monthly restore tests; Kafka replay/re-snapshot; scenario procedures D1–D8 incl. Bronze resync after an EPAS restore; DR drills twice a year.
10. **Ops API/UI:** complete endpoint list with roles, screens incl. rerun chain, quarantine, DLQ, trace, approvals, privacy and audit.
11. **Runbooks R01–R19** in `docs/runbooks/`, linked from alerts; go-live via pilot + shadow mode and the gated checklist.
