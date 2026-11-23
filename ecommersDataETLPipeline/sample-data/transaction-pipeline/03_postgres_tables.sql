-- =====================================================================
--  transaction-pipeline  |  03_postgres_tables.sql  |  ALL PostgreSQL tables of this job in ONE file
--  MongoDB customers + accounts + merchants + transactions -> star schema (silver)
--
--  Contents:
--    PART 1  META tables (meta.* + etl_staging + silver.dim_date), shared by every job
--    PART 2  the target tables of this job
--    PART 3  meta.meta_error_master seed: every data-quality error of this job
--
--  Run (database silver on localhost:5433 already exists):
--      psql -h localhost -p 5433 -U postgres -d silver -f sample-data/transaction-pipeline/03_postgres_tables.sql
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
    deleted_count    BIGINT       NOT NULL DEFAULT 0,    -- documents soft-deleted at the source (applied, not validated)
    loaded_count     BIGINT       NOT NULL DEFAULT 0,    -- rows inserted/updated in target tables
    error_message    TEXT,                               -- short failure reason
    error_detail     TEXT                                -- stack trace (truncated)
);
CREATE INDEX IF NOT EXISTS idx_pipeline_run_pipeline ON meta.meta_pipeline_run (pipeline_name, start_time DESC);
CREATE INDEX IF NOT EXISTS idx_pipeline_run_status   ON meta.meta_pipeline_run (status);
-- upgrade of a database created before soft delete existed (re-run safe)
ALTER TABLE meta.meta_pipeline_run ADD COLUMN IF NOT EXISTS deleted_count BIGINT NOT NULL DEFAULT 0;

-- the step table is no longer used: removed from a database created before (re-run safe)
DROP TABLE IF EXISTS meta.meta_pipeline_step;

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
    email          VARCHAR(150) NOT NULL,
    phone          VARCHAR(15)  NOT NULL,
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


-- ---------------------------------------------------------------------
-- SOFT DELETE columns: a document deleted at the source (isDeleted = true) is NOT removed here; the row is
-- marked is_deleted = TRUE, deleted_at = when it was deleted. Reports filter WHERE NOT is_deleted.
-- A document restored at the source (isDeleted = false again) sets is_deleted back to FALSE.
-- ALTER ... ADD COLUMN IF NOT EXISTS also upgrades a database created with an older version (re-run safe).
-- ---------------------------------------------------------------------
ALTER TABLE silver.dim_customer ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN NOT NULL DEFAULT FALSE, ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP;
ALTER TABLE silver.dim_account ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN NOT NULL DEFAULT FALSE, ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP;
ALTER TABLE silver.dim_merchant ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN NOT NULL DEFAULT FALSE, ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP;
ALTER TABLE silver.fact_transaction ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN NOT NULL DEFAULT FALSE, ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP;


-- =====================================================================
-- PART 3: meta.meta_error_master seed = every data-quality error the transaction-pipeline can produce
--   (generated from the rules in the code; shared errors such as "amount must be greater than 0" exist once).
--   The job looks an error up by error_type + error_message. Re-run safe: existing errors are skipped.
--   field_name = the checked field; its bad value is stored per record in meta_rejected_record.field_value.
-- =====================================================================
INSERT INTO meta.meta_error_master (error_type, field_name, error_message)
SELECT v.error_type, v.field_name, v.error_message
  FROM (VALUES
-- customers
    ('CORRUPT',    'dateOfBirth',    'dateOfBirth is not a valid date (expected yyyy-MM-dd)'),
    ('VALIDATION', 'customerId',     'customerId is required'),
    ('VALIDATION', 'firstName',      'firstName is required'),
    ('VALIDATION', 'lastName',       'lastName is required'),
    ('VALIDATION', 'email',          'email is required'),
    ('VALIDATION', 'phone',          'phone is required'),
    ('VALIDATION', 'customerId',     'customerId must look like CUST-0001'),
    ('VALIDATION', 'email',          'email is not a valid email address'),
    ('VALIDATION', 'phone',          'phone must be a 10 digit Indian mobile number'),
    ('VALIDATION', 'dateOfBirth',    'dateOfBirth cannot be in the future'),
    ('VALIDATION', 'gender',         'gender must be one of [M, F, O]'),
    ('VALIDATION', 'kycStatus',      'kycStatus must be one of [VERIFIED, PENDING, REJECTED]'),
    ('DUPLICATE',  'customerId',     'another record with the same customerId was kept (latest updatedAt wins)'),
-- accounts
    ('CORRUPT',    'balance',        'balance is not a valid number'),
    ('CORRUPT',    'openedDate',     'openedDate is not a valid date (expected yyyy-MM-dd)'),
    ('VALIDATION', 'accountId',      'accountId is required'),
    ('VALIDATION', 'accountType',    'accountType is required'),
    ('VALIDATION', 'currency',       'currency is required'),
    ('VALIDATION', 'status',         'status is required'),
    ('VALIDATION', 'accountId',      'accountId must look like ACC-0001'),
    ('VALIDATION', 'balance',        'balance cannot be negative'),
    ('VALIDATION', 'openedDate',     'openedDate cannot be in the future'),
    ('VALIDATION', 'accountType',    'accountType must be one of [SAVINGS, CURRENT, SALARY, NRI]'),
    ('VALIDATION', 'currency',       'currency must be one of [INR, USD, EUR, GBP]'),
    ('VALIDATION', 'status',         'status must be one of [ACTIVE, DORMANT, CLOSED]'),
    ('VALIDATION', 'ifscCode',       'ifscCode is not a valid IFSC code'),
    ('ORPHAN',     'customerId',     'customerId not found in dim_customer'),
    ('DUPLICATE',  'accountId',      'another record with the same accountId was kept (latest updatedAt wins)'),
-- merchants
    ('CORRUPT',    'onboardedDate',  'onboardedDate is not a valid date (expected yyyy-MM-dd)'),
    ('VALIDATION', 'merchantId',     'merchantId is required'),
    ('VALIDATION', 'merchantName',   'merchantName is required'),
    ('VALIDATION', 'category',       'category is required'),
    ('VALIDATION', 'merchantId',     'merchantId must look like MER-0001'),
    ('VALIDATION', 'category',       'category must be one of [GROCERY, ELECTRONICS, FUEL, TRAVEL, DINING, HEALTHCARE, ENTERTAINMENT, UTILITIES]'),
    ('VALIDATION', 'mccCode',        'mccCode must be a 4 digit MCC code'),
    ('VALIDATION', 'onboardedDate',  'onboardedDate cannot be in the future'),
    ('VALIDATION', 'status',         'status must be one of [ACTIVE, INACTIVE]'),
    ('DUPLICATE',  'merchantId',     'another record with the same merchantId was kept (latest updatedAt wins)'),
-- transactions
    ('CORRUPT',    'amount',         'amount is not a valid number'),
    ('CORRUPT',    'transactionDate', 'transactionDate is not a valid timestamp (expected yyyy-MM-ddTHH:mm:ss)'),
    ('VALIDATION', 'transactionId',  'transactionId is required'),
    ('VALIDATION', 'transactionType', 'transactionType is required'),
    ('VALIDATION', 'channel',        'channel is required'),
    ('VALIDATION', 'amount',         'amount is required'),
    ('VALIDATION', 'transactionDate', 'transactionDate is required'),
    ('VALIDATION', 'transactionId',  'transactionId must look like TXN-000001'),
    ('VALIDATION', 'amount',         'amount must be greater than 0'),
    ('VALIDATION', 'transactionDate', 'transactionDate cannot be in the future'),
    ('VALIDATION', 'transactionType', 'transactionType must be one of [DEBIT, CREDIT]'),
    ('VALIDATION', 'channel',        'channel must be one of [POS, ONLINE, ATM, UPI, NETBANKING]'),
    ('VALIDATION', 'status',         'status must be one of [SUCCESS, FAILED, PENDING, REVERSED]'),
    ('ORPHAN',     'accountId',      'accountId not found in dim_account'),
    ('ORPHAN',     'merchantId',     'merchantId not found in dim_merchant'),
    ('DUPLICATE',  'transactionId',  'another record with the same transactionId was kept (latest updatedAt wins)')
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
