-- =====================================================================
--  payment-pipeline  |  03_postgres_tables.sql  |  ALL PostgreSQL tables of this job in ONE file
--  MongoDB payments -> star schema (silver.dim_payer, dim_payment_method, dim_biller, fact_payment)
--
--  Contents:
--    PART 1  META tables (meta.* + etl_staging + silver.dim_date), shared by every job
--    PART 2  the target tables of this job
--    PART 3  meta.meta_error_master seed: every data-quality error of this job
--
--  Run (database must exist:  CREATE DATABASE shop_dw;):
--      psql -h localhost -p 5433 -U postgres -d shop_dw -f sample-data/payment-pipeline/03_postgres_tables.sql
--
--  Re-run safe: every statement uses IF NOT EXISTS / ON CONFLICT, so you can run it again, and you can
--  run the files of several jobs on the same database (the meta part is identical in all of them).
--
--  The application does NOT create tables: run this script once before the first run of the job.
--  It is the ONLY place where these tables are defined (the meta part is the same in every job file).
-- =====================================================================

-- =====================================================================
-- PART 1: META tables (shared by EVERY job; safe to run again: IF NOT EXISTS)
--   meta.meta_pipeline_run     one row per pipeline run (status, counters, error)
--   meta.meta_pipeline_step    one row per step of a run (where did it fail, how long)
--   meta.meta_error_master     every possible data-quality error, once (error_type + message)
--   meta.meta_rejected_record  bad records: one row per record + failed rule, error_id -> meta_error_master
--   meta.meta_watermark        incremental watermark per pipeline + collection (timestamp based)
--   etl_staging                temporary tables used by the UPSERT (created and dropped by the job)
--   silver.dim_date            shared (conformed) date dimension, filled by the jobs
-- =====================================================================
CREATE SCHEMA IF NOT EXISTS meta;
CREATE SCHEMA IF NOT EXISTS etl_staging;
CREATE SCHEMA IF NOT EXISTS silver;

-- status: SUCCESS             = every record read was loaded (nothing rejected)
--         SUCCESS_WITH_FAILED = some records loaded, some rejected
--         FAILED              = all records rejected, OR exception / reject gate, OR application stopped (interrupted)
--         NULL                = the run is still busy
CREATE TABLE IF NOT EXISTS meta.meta_pipeline_run (
    job_run_id       BIGSERIAL    PRIMARY KEY,
    pipeline_name    VARCHAR(100) NOT NULL,
    window_end       TIMESTAMPTZ  NOT NULL,              -- this run read data with updatedAt <= window_end
    status           VARCHAR(30)  CHECK (status IN ('SUCCESS', 'SUCCESS_WITH_FAILED', 'FAILED')),
    triggered_by     VARCHAR(100),
    start_time       TIMESTAMP    NOT NULL DEFAULT now(),
    end_time         TIMESTAMP,
    duration_ms      BIGINT,
    source_count     BIGINT       NOT NULL DEFAULT 0,    -- records read from MongoDB
    valid_count      BIGINT       NOT NULL DEFAULT 0,    -- records that passed all checks
    rejected_count   BIGINT       NOT NULL DEFAULT 0,    -- records that failed (includes duplicates)
    duplicate_count  BIGINT       NOT NULL DEFAULT 0,    -- part of rejected_count
    loaded_count     BIGINT       NOT NULL DEFAULT 0,    -- rows inserted/updated in target tables
    error_message    TEXT,                               -- short failure reason
    error_detail     TEXT                                -- stack trace (truncated)
);
CREATE INDEX IF NOT EXISTS idx_pipeline_run_pipeline ON meta.meta_pipeline_run (pipeline_name, start_time DESC);
CREATE INDEX IF NOT EXISTS idx_pipeline_run_status   ON meta.meta_pipeline_run (status);

CREATE TABLE IF NOT EXISTS meta.meta_pipeline_step (
    job_step_id    BIGSERIAL    PRIMARY KEY,
    job_run_id     BIGINT       NOT NULL REFERENCES meta.meta_pipeline_run (job_run_id),
    step_order     INT          NOT NULL,
    step_name      VARCHAR(200) NOT NULL,
    status         VARCHAR(30)  CHECK (status IN ('SUCCESS', 'FAILED')),   -- NULL = step still busy
    record_count   BIGINT,
    start_time     TIMESTAMP    NOT NULL DEFAULT now(),
    end_time       TIMESTAMP,
    duration_ms    BIGINT,
    error_message  TEXT
);
CREATE INDEX IF NOT EXISTS idx_pipeline_step_run ON meta.meta_pipeline_step (job_run_id, step_order);

-- One row per possible error. Seeded at the end of this file with every error of this job; a rule that is
-- added to the code later gets its row automatically on the first run that rejects a record with it.
CREATE TABLE IF NOT EXISTS meta.meta_error_master (
    error_id       SERIAL       PRIMARY KEY,
    error_type     VARCHAR(30)  NOT NULL CHECK (error_type IN ('CORRUPT', 'VALIDATION', 'ORPHAN', 'DUPLICATE')),
    field_name     VARCHAR(100),                         -- checked field (NULL = rule on several fields)
    error_message  VARCHAR(500) NOT NULL,                -- e.g. 'amount must be greater than 0'
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT uq_error_master UNIQUE (error_type, error_message)
);

-- A record that broke 2 rules has 2 rows (same source_id, different error_id).
CREATE TABLE IF NOT EXISTS meta.meta_rejected_record (
    rejected_record_id  BIGSERIAL    PRIMARY KEY,
    pipeline_name       VARCHAR(100) NOT NULL,
    source_collection   VARCHAR(100) NOT NULL,
    source_id           VARCHAR(100) NOT NULL,           -- MongoDB _id (order lines: <_id>#<line_no>)
    business_key        VARCHAR(200),                    -- e.g. PAY-0007, O1008/line-2
    error_id            INT          NOT NULL REFERENCES meta.meta_error_master (error_id),
    field_value         TEXT,                            -- the bad value of error_master.field_name
    raw_record          JSONB,                           -- original document (before cleaning)
    attempt_count       INT          NOT NULL DEFAULT 1, -- how many runs rejected this record with this error
    first_job_run_id    BIGINT       NOT NULL,
    last_job_run_id     BIGINT       NOT NULL,
    first_rejected_at   TIMESTAMP    NOT NULL DEFAULT now(),
    last_rejected_at    TIMESTAMP    NOT NULL DEFAULT now(),
    CONSTRAINT uq_rejected_record UNIQUE (pipeline_name, source_collection, source_id, error_id)
);
CREATE INDEX IF NOT EXISTS idx_rejected_record_error ON meta.meta_rejected_record (error_id);

-- incremental watermark: up to which timestamp did pipeline X read collection Y (moved only after a successful run)
CREATE TABLE IF NOT EXISTS meta.meta_watermark (
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
-- PART 2: payment-pipeline tables (STAR schema, schema "silver")
--   MongoDB payments  ->  dim_payer, dim_payment_method, dim_biller, dim_date  ->  fact_payment
-- =====================================================================
CREATE TABLE IF NOT EXISTS silver.dim_payer (
    payer_key    BIGSERIAL    PRIMARY KEY,           -- surrogate key
    payer_id     VARCHAR(20)  NOT NULL UNIQUE,       -- business key from source
    payer_name   VARCHAR(150) NOT NULL,
    payer_email  VARCHAR(150),
    payer_phone  VARCHAR(15), 
    payer_city   VARCHAR(100),
    job_run_id   BIGINT,
    loaded_at    TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.dim_payment_method (
    payment_method_key  BIGSERIAL   PRIMARY KEY,
    payment_method      VARCHAR(30) NOT NULL,        -- UPI, CARD, NETBANKING, WALLET
    payment_provider    VARCHAR(50) NOT NULL,        -- GPAY, VISA, HDFC ...
    job_run_id          BIGINT,
    loaded_at           TIMESTAMP,
    CONSTRAINT uq_payment_method UNIQUE (payment_method, payment_provider)
);

CREATE TABLE IF NOT EXISTS silver.dim_biller (
    biller_key       BIGSERIAL    PRIMARY KEY,
    biller_name      VARCHAR(150) NOT NULL UNIQUE,
    biller_category  VARCHAR(50),
    job_run_id       BIGINT,
    loaded_at        TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.fact_payment (
    payment_key         BIGSERIAL     PRIMARY KEY,
    payment_id          VARCHAR(20)   NOT NULL UNIQUE,   -- business key (used for UPSERT)
    payer_key           BIGINT        NOT NULL REFERENCES silver.dim_payer (payer_key),
    payment_method_key  BIGINT        NOT NULL REFERENCES silver.dim_payment_method (payment_method_key),
    biller_key          BIGINT        NOT NULL REFERENCES silver.dim_biller (biller_key),
    date_key            INT           NOT NULL REFERENCES silver.dim_date (date_key),
    amount              NUMERIC(18,2) NOT NULL,
    currency            VARCHAR(3)    NOT NULL,
    payment_status      VARCHAR(20)   NOT NULL,
    reference_number    VARCHAR(50),
    payment_ts          TIMESTAMP     NOT NULL,
    job_run_id          BIGINT,
    loaded_at           TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_fact_payment_date ON silver.fact_payment (date_key);


-- =====================================================================
-- PART 3: meta.meta_error_master seed = every data-quality error the payment-pipeline can produce
--   (generated from the rules in the code; shared errors such as "amount must be greater than 0" exist once).
--   The job looks an error up by error_type + error_message. Re-run safe: existing errors are skipped.
--   field_name = the checked field; its bad value is stored per record in meta_rejected_record.field_value.
-- =====================================================================
INSERT INTO meta.meta_error_master (error_type, field_name, error_message)
SELECT v.error_type, v.field_name, v.error_message
  FROM (VALUES
-- payments
    ('CORRUPT',    'amount',         'amount is not a valid number'),
    ('CORRUPT',    'paymentDate',    'paymentDate is not a valid timestamp (expected yyyy-MM-ddTHH:mm:ss)'),
    ('VALIDATION', 'paymentId',      'paymentId is required'),
    ('VALIDATION', 'payerId',        'payerId is required'),
    ('VALIDATION', 'payerName',      'payerName is required'),
    ('VALIDATION', 'paymentMethod',  'paymentMethod is required'),
    ('VALIDATION', 'paymentProvider', 'paymentProvider is required'),
    ('VALIDATION', 'billerName',     'billerName is required'),
    ('VALIDATION', 'amount',         'amount is required'),
    ('VALIDATION', 'currency',       'currency is required'),
    ('VALIDATION', 'paymentDate',    'paymentDate is required'),
    ('VALIDATION', 'status',         'status is required'),
    ('VALIDATION', 'paymentId',      'paymentId must look like PAY-0001'),
    ('VALIDATION', 'amount',         'amount must be greater than 0'),
    ('VALIDATION', 'paymentDate',    'paymentDate cannot be in the future'),
    ('VALIDATION', 'paymentMethod',  'paymentMethod must be one of [UPI, CARD, NETBANKING, WALLET]'),
    ('VALIDATION', 'currency',       'currency must be one of [INR, USD]'),
    ('VALIDATION', 'status',         'status must be one of [SUCCESS, FAILED, PENDING]'),
    ('VALIDATION', 'payerEmail',     'payerEmail is not a valid email address'),
    ('VALIDATION', 'payerPhone',     'payerPhone must be a 10 digit Indian mobile number'),
    ('DUPLICATE',  'paymentId',      'another record with the same paymentId was kept (latest updatedAt wins)')
       ) AS v (error_type, field_name, error_message)
 WHERE NOT EXISTS (SELECT 1 FROM meta.meta_error_master e                  -- already there: skip (keeps error_id gap-free)
                    WHERE e.error_type = v.error_type AND e.error_message = v.error_message)
ON CONFLICT (error_type, error_message) DO NOTHING;


-- =====================================================================
-- Verify: list the tables of this job + the meta tables
-- =====================================================================
SELECT table_schema, table_name
  FROM information_schema.tables
 WHERE table_schema IN ('meta', 'silver')
 ORDER BY table_schema, table_name;
