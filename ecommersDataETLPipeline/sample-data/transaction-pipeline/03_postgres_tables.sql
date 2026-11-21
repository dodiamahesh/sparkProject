-- =====================================================================
--  transaction-pipeline  |  03_postgres_tables.sql  |  ALL PostgreSQL tables of this job in ONE file
--  MongoDB customers + accounts + merchants + transactions -> star schema (silver)
--
--  Contents:
--    PART 1  META tables (etl_audit.* incl. etl_watermark + etl_staging + silver.dim_date), shared by every job
--    PART 2  the target tables of this job
--
--  Run (database must exist:  CREATE DATABASE shop_dw;):
--      psql -h localhost -p 5433 -U postgres -d shop_dw -f sample-data/transaction-pipeline/03_postgres_tables.sql
--
--  Re-run safe: every statement uses IF NOT EXISTS / ON CONFLICT, so you can run it again, and you can
--  run the files of several jobs on the same database (the meta part is identical in all of them).
--
--  The application does NOT create tables: run this script once before the first run of the job.
--  It is the ONLY place where these tables are defined (the meta part is the same in every job file).
-- =====================================================================

-- =====================================================================
-- PART 1: META tables (shared by EVERY job; safe to run again: IF NOT EXISTS)
--   etl_audit.etl_job_run          one row per job run (status, counters, error)
--   etl_audit.etl_job_step         one row per step of a run (where did it fail, how long)
--   etl_audit.etl_rejected_record  bad records (CORRUPT / VALIDATION / ORPHAN / DUPLICATE), one per document
--   etl_audit.etl_data_warning     loaded records that broke a warning rule + schema drift + volume warnings
--   etl_audit.etl_watermark        incremental watermark per pipeline + collection (timestamp based)
--   etl_staging                    temporary tables used by the UPSERT (created and dropped by the job)
--   silver.dim_date                shared (conformed) date dimension, filled by the jobs
-- =====================================================================
CREATE SCHEMA IF NOT EXISTS etl_audit;
CREATE SCHEMA IF NOT EXISTS etl_staging;
CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS etl_audit.etl_job_run (
    job_run_id       BIGSERIAL    PRIMARY KEY,
    pipeline_name    VARCHAR(100) NOT NULL,
    window_end       TIMESTAMPTZ  NOT NULL,              -- this run read data with updatedAt <= window_end
    status           VARCHAR(30)  NOT NULL,              -- RUNNING, SUCCESS, SUCCESS_WITH_REJECTS, FAILED
    triggered_by     VARCHAR(100),
    start_time       TIMESTAMP    NOT NULL DEFAULT now(),
    end_time         TIMESTAMP,
    duration_ms      BIGINT,
    source_count     BIGINT       NOT NULL DEFAULT 0,    -- records read from MongoDB
    valid_count      BIGINT       NOT NULL DEFAULT 0,    -- records that passed all checks
    rejected_count   BIGINT       NOT NULL DEFAULT 0,    -- records that failed (includes duplicates)
    duplicate_count  BIGINT       NOT NULL DEFAULT 0,    -- part of rejected_count
    loaded_count     BIGINT       NOT NULL DEFAULT 0,    -- rows inserted/updated in target tables
    resolved_count   BIGINT       NOT NULL DEFAULT 0,    -- old rejected records that are valid now
    warning_count    BIGINT       NOT NULL DEFAULT 0,    -- rule + schema-drift + volume warnings
    error_message    TEXT,                               -- short failure reason
    error_detail     TEXT                                -- stack trace (truncated)
);
CREATE INDEX IF NOT EXISTS idx_job_run_pipeline ON etl_audit.etl_job_run (pipeline_name, start_time DESC);
CREATE INDEX IF NOT EXISTS idx_job_run_status   ON etl_audit.etl_job_run (status);

CREATE TABLE IF NOT EXISTS etl_audit.etl_job_step (
    job_step_id    BIGSERIAL    PRIMARY KEY,
    job_run_id     BIGINT       NOT NULL REFERENCES etl_audit.etl_job_run (job_run_id),
    step_order     INT          NOT NULL,
    step_name      VARCHAR(200) NOT NULL,
    status         VARCHAR(30)  NOT NULL,                -- RUNNING, SUCCESS, FAILED
    record_count   BIGINT,
    start_time     TIMESTAMP    NOT NULL DEFAULT now(),
    end_time       TIMESTAMP,
    duration_ms    BIGINT,
    error_message  TEXT
);
CREATE INDEX IF NOT EXISTS idx_job_step_run ON etl_audit.etl_job_step (job_run_id, step_order);

CREATE TABLE IF NOT EXISTS etl_audit.etl_rejected_record (
    rejected_record_id  BIGSERIAL    PRIMARY KEY,
    pipeline_name       VARCHAR(100) NOT NULL,
    source_collection   VARCHAR(100) NOT NULL,
    source_id           VARCHAR(100) NOT NULL,           -- MongoDB _id (order lines: <_id>#<line_no>)
    business_key        VARCHAR(200),                    -- e.g. PAY-0007, O1008/line-2
    error_type          VARCHAR(30)  NOT NULL,           -- CORRUPT, VALIDATION, ORPHAN, DUPLICATE
    error_reason        TEXT         NOT NULL,           -- all failed rules, separated by " | "
    raw_record          JSONB,                           -- original document (before cleaning)
    attempt_count       INT          NOT NULL DEFAULT 1, -- how many runs rejected this record
    first_job_run_id    BIGINT       NOT NULL,
    last_job_run_id     BIGINT       NOT NULL,
    first_rejected_at   TIMESTAMP    NOT NULL DEFAULT now(),
    last_rejected_at    TIMESTAMP    NOT NULL DEFAULT now(),
    CONSTRAINT uq_rejected_record UNIQUE (pipeline_name, source_collection, source_id)
);
CREATE INDEX IF NOT EXISTS idx_rejected_pipeline ON etl_audit.etl_rejected_record (pipeline_name, source_collection, error_type);

CREATE TABLE IF NOT EXISTS etl_audit.etl_data_warning (
    data_warning_id    BIGSERIAL    PRIMARY KEY,
    job_run_id         BIGINT       NOT NULL REFERENCES etl_audit.etl_job_run (job_run_id),
    pipeline_name      VARCHAR(100) NOT NULL,
    source_collection  VARCHAR(100) NOT NULL,
    warning_type       VARCHAR(30)  NOT NULL,            -- RULE, SCHEMA_DRIFT, VOLUME
    source_id          VARCHAR(100),                     -- MongoDB _id (RULE only)
    business_key       VARCHAR(200),                     -- (RULE only)
    warning_message    TEXT         NOT NULL,
    created_at         TIMESTAMP    NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_data_warning_run    ON etl_audit.etl_data_warning (job_run_id);
CREATE INDEX IF NOT EXISTS idx_data_warning_source ON etl_audit.etl_data_warning (pipeline_name, source_collection, warning_type);

-- incremental watermark: up to which timestamp did pipeline X read collection Y (moved only after a successful run)
CREATE TABLE IF NOT EXISTS etl_audit.etl_watermark (
    pipeline_name      VARCHAR(100) NOT NULL,
    source_collection  VARCHAR(100) NOT NULL,
    watermark_field    VARCHAR(100) NOT NULL,            -- e.g. updatedAt (BSON Date in MongoDB)
    last_watermark     TIMESTAMPTZ  NOT NULL,            -- next run reads watermark_field > last_watermark
    last_job_run_id    BIGINT       NOT NULL,
    updated_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline_name, source_collection)       -- no row = first run: read everything
);

CREATE TABLE IF NOT EXISTS silver.dim_date (
    date_key      INT         PRIMARY KEY,               -- yyyyMMdd, e.g. 20261015
    full_date     DATE        NOT NULL UNIQUE,
    day_of_month  INT         NOT NULL,
    month         INT         NOT NULL,
    month_name    VARCHAR(20) NOT NULL,
    quarter       INT         NOT NULL,
    year          INT         NOT NULL,
    day_of_week   INT         NOT NULL,                  -- 1 = Sunday ... 7 = Saturday
    day_name      VARCHAR(20) NOT NULL,
    is_weekend    BOOLEAN     NOT NULL,
    job_run_id    BIGINT,
    loaded_at     TIMESTAMP
);


-- =====================================================================
-- PART 2: transaction-pipeline tables (STAR schema, schema "silver")
--   MongoDB customers, accounts, merchants, transactions
--   -> dim_customer, dim_account, dim_merchant, dim_date -> fact_transaction
-- =====================================================================
CREATE TABLE IF NOT EXISTS silver.dim_customer (
    customer_key   BIGSERIAL    PRIMARY KEY,
    customer_id    VARCHAR(20)  NOT NULL UNIQUE,
    first_name     VARCHAR(100) NOT NULL,
    last_name      VARCHAR(100) NOT NULL,
    full_name      VARCHAR(200) NOT NULL,
    email          VARCHAR(150) NOT NULL,            -- masked (PII)
    phone          VARCHAR(15)  NOT NULL,            -- masked (PII)
    date_of_birth  DATE,
    gender         VARCHAR(1),
    city           VARCHAR(100),
    state          VARCHAR(100),
    country        VARCHAR(100),
    kyc_status     VARCHAR(20),
    job_run_id     BIGINT,
    loaded_at      TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.dim_account (
    account_key      BIGSERIAL     PRIMARY KEY,
    account_id       VARCHAR(20)   NOT NULL UNIQUE,
    customer_id      VARCHAR(20)   NOT NULL,
    account_type     VARCHAR(20)   NOT NULL,
    branch_code      VARCHAR(20),
    ifsc_code        VARCHAR(11),
    currency         VARCHAR(3)    NOT NULL,
    account_status   VARCHAR(20)   NOT NULL,
    current_balance  NUMERIC(18,2),
    opened_date      DATE,
    job_run_id       BIGINT,
    loaded_at        TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.dim_merchant (
    merchant_key     BIGSERIAL    PRIMARY KEY,
    merchant_id      VARCHAR(20)  NOT NULL UNIQUE,
    merchant_name    VARCHAR(150) NOT NULL,
    category         VARCHAR(30)  NOT NULL,
    mcc_code         VARCHAR(4),
    city             VARCHAR(100),
    country          VARCHAR(100),
    merchant_status  VARCHAR(20),
    onboarded_date   DATE,
    job_run_id       BIGINT,
    loaded_at        TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.fact_transaction (
    transaction_key     BIGSERIAL     PRIMARY KEY,
    transaction_id      VARCHAR(20)   NOT NULL UNIQUE,
    customer_key        BIGINT        NOT NULL REFERENCES silver.dim_customer (customer_key),
    account_key         BIGINT        NOT NULL REFERENCES silver.dim_account (account_key),
    merchant_key        BIGINT        NOT NULL REFERENCES silver.dim_merchant (merchant_key),
    date_key            INT           NOT NULL REFERENCES silver.dim_date (date_key),
    transaction_type    VARCHAR(10)   NOT NULL,
    channel             VARCHAR(20)   NOT NULL,
    amount              NUMERIC(18,2) NOT NULL,
    currency            VARCHAR(3)    NOT NULL,
    transaction_status  VARCHAR(20)   NOT NULL,
    description         VARCHAR(255),
    transaction_ts      TIMESTAMP     NOT NULL,
    job_run_id          BIGINT,
    loaded_at           TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_fact_txn_date     ON silver.fact_transaction (date_key);
CREATE INDEX IF NOT EXISTS idx_fact_txn_customer ON silver.fact_transaction (customer_key);


-- =====================================================================
-- Verify: list the tables of this job + the meta tables
-- =====================================================================
SELECT table_schema, table_name
  FROM information_schema.tables
 WHERE table_schema IN ('etl_audit', 'silver')
 ORDER BY table_schema, table_name;
