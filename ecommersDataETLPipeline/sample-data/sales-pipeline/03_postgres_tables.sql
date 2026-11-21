-- =====================================================================
--  sales-pipeline  |  03_postgres_tables.sql  |  ALL PostgreSQL tables of this job in ONE file
--  Job-2 (J2_SALES): products + customers + orders + payments -> star schema (curated)
--
--  Contents:
--    PART 1  META tables (meta.* + etl_staging + silver.dim_date), shared by every job
--    PART 2  the target tables of this job
--    PART 3  meta.meta_error_master seed: every data-quality error of this job
--
--  Run (database must exist:  CREATE DATABASE shop_dw;):
--      psql -h localhost -p 5433 -U postgres -d shop_dw -f sample-data/sales-pipeline/03_postgres_tables.sql
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
    field_value         TEXT,                            -- the bad value of error_master.field_name (PII masked)
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
-- PART 2: STAR schema for Job-2 (J2_SALES) -> sales-pipeline
--     Source: FOUR MongoDB collections  products, customers, orders (items[] array), payments
--     Target: schema "curated"  (silver.dim_customer / silver.dim_product already belong to other examples)
--
--                         silver.dim_date
--                               |
--        dim_customer ----- fact_sales ----- dim_product
--             |               |    \------- dim_order_status
--             |               \------------ dim_payment_method
--             |                                   |
--             \------------ fact_payment ---------/
--                               |
--                         silver.dim_date
--
--     The job is self-contained: dim_product is built by this same pipeline from the "products"
--     collection (no other job has to run first).
-- =====================================================================
CREATE SCHEMA IF NOT EXISTS curated;

-- ---------------------------------------------------------------------
-- dim_product (SCD1): one row per product_code, overwritten when the product changes
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS curated.dim_product (
    product_key           BIGSERIAL     PRIMARY KEY,
    product_code          VARCHAR(10)   NOT NULL UNIQUE,      -- P001
    product_name          VARCHAR(150)  NOT NULL,
    brand_name            VARCHAR(100),
    category_name         VARCHAR(100)  NOT NULL,             -- Mobiles
    parent_category_name  VARCHAR(100),                       -- Electronics
    current_price         NUMERIC(18,2) NOT NULL CHECK (current_price > 0),
    is_active             BOOLEAN,
    job_run_id            BIGINT,
    loaded_at             TIMESTAMP
);

-- ---------------------------------------------------------------------
-- dim_customer (SCD2): city / state / tier change -> new version; name / email / phone -> overwritten
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS curated.dim_customer (
    customer_key   BIGSERIAL    PRIMARY KEY,
    customer_code  VARCHAR(10)  NOT NULL,                     -- C001 (one customer can have many versions)
    customer_name  VARCHAR(100) NOT NULL,
    email          VARCHAR(150),
    phone          VARCHAR(10),
    city           VARCHAR(60),
    state          VARCHAR(60),
    pincode        VARCHAR(6),
    tier           VARCHAR(10)  NOT NULL CHECK (tier IN ('Bronze', 'Silver', 'Gold', 'Platinum')),
    signup_date    DATE,
    valid_from     TIMESTAMP    NOT NULL,
    valid_to       TIMESTAMP    NOT NULL DEFAULT TIMESTAMP '9999-12-31 00:00:00',   -- 9999-12-31 = still current
    is_current     CHAR(1)      NOT NULL DEFAULT 'Y' CHECK (is_current IN ('Y', 'N')),
    job_run_id     BIGINT,
    loaded_at      TIMESTAMP,
    CONSTRAINT uq_dim_customer_version UNIQUE (customer_code, valid_from),
    CHECK (valid_to > valid_from)
);
-- The most important SCD2 rule: ONE current version per customer
CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_customer_current ON curated.dim_customer (customer_code) WHERE is_current = 'Y';

-- ---------------------------------------------------------------------
-- Small reference dimensions: fixed values, seeded here (not from MongoDB)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS curated.dim_order_status (
    order_status_key  BIGSERIAL   PRIMARY KEY,
    status_code       VARCHAR(15) NOT NULL UNIQUE,
    description       VARCHAR(50),
    is_final          CHAR(1)     NOT NULL CHECK (is_final IN ('Y', 'N')),   -- Y = will not change any more
    job_run_id        BIGINT,
    loaded_at         TIMESTAMP
);
INSERT INTO curated.dim_order_status (status_code, description, is_final) VALUES
    ('PLACED',    'Order mukayo',      'N'),
    ('SHIPPED',   'Courier ne apayo',  'N'),
    ('DELIVERED', 'Customer ne malyo', 'Y'),
    ('CANCELLED', 'Radd thayo',        'Y'),
    ('RETURNED',  'Pachho aavyo',      'Y')
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
    ('UPI',        'Unified Payments Interface', 'Y'),
    ('CARD',       'Credit / Debit Card',        'Y'),
    ('NETBANKING', 'Net Banking',                'Y'),
    ('COD',        'Cash on Delivery',           'N')
ON CONFLICT (method_code) DO NOTHING;

-- ---------------------------------------------------------------------
-- fact_sales: GRAIN = one product line of one order. Upsert key (order_no, line_no)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS curated.fact_sales (
    sales_key           BIGSERIAL     PRIMARY KEY,
    order_no            VARCHAR(10)   NOT NULL,               -- degenerate dimension
    line_no             INT           NOT NULL,
    order_date_key      INT           NOT NULL REFERENCES silver.dim_date (date_key),
    customer_key        BIGINT        NOT NULL REFERENCES curated.dim_customer (customer_key),  -- version valid at order time
    product_key         BIGINT        NOT NULL REFERENCES curated.dim_product (product_key),
    payment_method_key  BIGINT        NOT NULL REFERENCES curated.dim_payment_method (payment_method_key),
    order_status_key    BIGINT        NOT NULL REFERENCES curated.dim_order_status (order_status_key),
    quantity            INT           NOT NULL CHECK (quantity > 0),
    unit_price          NUMERIC(18,2) NOT NULL CHECK (unit_price > 0),
    line_amount         NUMERIC(18,2) NOT NULL,               -- quantity x unit_price
    job_run_id          BIGINT,
    loaded_at           TIMESTAMP,
    CONSTRAINT uq_fact_sales_line UNIQUE (order_no, line_no)
);
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON curated.fact_sales (customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_date     ON curated.fact_sales (order_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product  ON curated.fact_sales (product_key);

-- ---------------------------------------------------------------------
-- fact_payment: GRAIN = one payment. Upsert key payment_id
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS curated.fact_payment (
    payment_key         BIGSERIAL     PRIMARY KEY,
    payment_id          VARCHAR(10)   NOT NULL UNIQUE,
    order_no            VARCHAR(10)   NOT NULL,
    payment_date_key    INT           NOT NULL REFERENCES silver.dim_date (date_key),
    customer_key        BIGINT        NOT NULL REFERENCES curated.dim_customer (customer_key),  -- taken from the order
    payment_method_key  BIGINT        NOT NULL REFERENCES curated.dim_payment_method (payment_method_key),
    payment_status      VARCHAR(10)   NOT NULL CHECK (payment_status IN ('SUCCESS', 'FAILED', 'REFUNDED', 'PENDING')),
    amount              NUMERIC(18,2) NOT NULL CHECK (amount > 0),
    job_run_id          BIGINT,
    loaded_at           TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_fact_payment_order    ON curated.fact_payment (order_no);
CREATE INDEX IF NOT EXISTS idx_fact_payment_customer ON curated.fact_payment (customer_key);


-- =====================================================================
-- PART 3: meta.meta_error_master seed = every data-quality error the sales-pipeline can produce
--   (generated from the rules in the code; shared errors such as "amount must be greater than 0" exist once).
--   The job looks an error up by error_type + error_message. Re-run safe: existing errors are skipped.
--   field_name = the checked field; its bad value is stored per record in meta_rejected_record.field_value.
-- =====================================================================
INSERT INTO meta.meta_error_master (error_type, field_name, error_message)
SELECT v.error_type, v.field_name, v.error_message
  FROM (VALUES
-- products
    ('CORRUPT',    'price',          'price is not a valid number'),
    ('VALIDATION', 'product_code',   'product_code is required'),
    ('VALIDATION', 'name',           'name is required'),
    ('VALIDATION', 'category_name',  'category_name is required'),
    ('VALIDATION', 'price',          'price is required'),
    ('VALIDATION', 'product_code',   'product_code must look like P001'),
    ('VALIDATION', 'price',          'price must be greater than 0'),
    ('DUPLICATE',  'product_code',   'another record with the same product_code was kept (latest updated_at wins)'),
-- customers
    ('CORRUPT',    'signup_date',    'signup_date is not a valid date (expected yyyy-MM-dd)'),
    ('VALIDATION', 'customer_code',  'customer_code is required'),
    ('VALIDATION', 'email',          'email is required'),
    ('VALIDATION', 'customer_code',  'customer_code must look like C001'),
    ('VALIDATION', 'email',          'email is not a valid email address'),
    ('VALIDATION', 'phone',          'phone must have 10 digits'),
    ('VALIDATION', 'tier',           'tier must be one of [Bronze, Silver, Gold, Platinum]'),
    ('DUPLICATE',  'customer_code',  'another record with the same customer_code was kept (latest updated_at wins)'),
-- orders
    ('CORRUPT',    'order_ts',       'order_ts is not a valid timestamp (expected yyyy-MM-ddTHH:mm:ss)'),
    ('VALIDATION', 'order_no',       'order_no is required'),
    ('VALIDATION', 'order_ts',       'order_ts is required'),
    ('VALIDATION', 'status',         'status is required'),
    ('VALIDATION', 'payment_method', 'payment_method is required'),
    ('VALIDATION', NULL,             'items must contain at least one line'),
    ('VALIDATION', 'status',         'status must be one of [PLACED, SHIPPED, DELIVERED, CANCELLED, RETURNED]'),
    ('VALIDATION', 'payment_method', 'payment_method must be one of [UPI, CARD, NETBANKING, COD]'),
    ('ORPHAN',     'customer_code',  'customer_code not found in dim_customer'),
    ('DUPLICATE',  'order_no',       'another record with the same order_no was kept (latest updated_at wins)'),
-- orders.items
    ('CORRUPT',    'qty',            'qty is not a whole number'),
    ('CORRUPT',    'unit_price',     'unit_price is not a valid number'),
    ('VALIDATION', 'qty',            'qty is required'),
    ('VALIDATION', 'unit_price',     'unit_price is required'),
    ('VALIDATION', 'qty',            'qty must be greater than 0'),
    ('VALIDATION', 'unit_price',     'unit_price must be greater than 0'),
    ('ORPHAN',     'product_code',   'product_code not found in dim_product'),
    ('DUPLICATE',  'line_id',        'another record with the same line_id was kept (latest updated_at wins)'),
-- payments
    ('CORRUPT',    'amount',         'amount is not a valid number'),
    ('CORRUPT',    'paid_at',        'paid_at is not a valid timestamp (expected yyyy-MM-ddTHH:mm:ss)'),
    ('VALIDATION', 'payment_id',     'payment_id is required'),
    ('VALIDATION', 'amount',         'amount is required'),
    ('VALIDATION', 'method',         'method is required'),
    ('VALIDATION', 'amount',         'amount must be greater than 0'),
    ('VALIDATION', 'status',         'status must be one of [SUCCESS, FAILED, REFUNDED, PENDING]'),
    ('VALIDATION', 'method',         'method must be one of [UPI, CARD, NETBANKING, COD]'),
    ('ORPHAN',     'order_no',       'order_no not found in fact_sales'),
    ('DUPLICATE',  'payment_id',     'another record with the same payment_id was kept (latest updated_at wins)')
       ) AS v (error_type, field_name, error_message)
 WHERE NOT EXISTS (SELECT 1 FROM meta.meta_error_master e                  -- already there: skip (keeps error_id gap-free)
                    WHERE e.error_type = v.error_type AND e.error_message = v.error_message)
ON CONFLICT (error_type, error_message) DO NOTHING;


-- =====================================================================
-- Verify: list the tables of this job + the meta tables
-- =====================================================================
SELECT table_schema, table_name
  FROM information_schema.tables
 WHERE table_schema IN ('meta', 'silver', 'curated')
 ORDER BY table_schema, table_name;
