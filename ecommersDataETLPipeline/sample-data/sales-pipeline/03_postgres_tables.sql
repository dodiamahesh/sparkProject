-- =====================================================================
--  sales-pipeline  |  03_postgres_tables.sql  |  ALL PostgreSQL tables of this job in ONE file
--  Job-2 (J2_SALES): products + customers + orders + payments -> star schema (curated)
--
--  Contents:
--    PART 1  META tables (etl_audit.* + etl_staging + silver.dim_date), shared by every job
--    PART 2  the target tables of this job
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
--   etl_audit.etl_job_run          one row per job run (status, counters, error)
--   etl_audit.etl_job_step         one row per step of a run (where did it fail, how long)
--   etl_audit.etl_rejected_record  bad records (CORRUPT / VALIDATION / ORPHAN / DUPLICATE), one per document
--   etl_audit.etl_data_warning     loaded records that broke a warning rule + schema drift + volume warnings
--   etl_staging                    temporary tables used by the UPSERT (created and dropped by the job)
--   silver.dim_date                shared (conformed) date dimension, filled by the jobs
-- =====================================================================
CREATE SCHEMA IF NOT EXISTS etl_audit;
CREATE SCHEMA IF NOT EXISTS etl_staging;
CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS etl_audit.etl_job_run (
    job_run_id       BIGSERIAL    PRIMARY KEY,
    pipeline_name    VARCHAR(100) NOT NULL,
    load_type        VARCHAR(30)  NOT NULL,              -- FULL, INCREMENTAL, RETRY_REJECTED
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
-- Verify: list the tables of this job + the meta tables
-- =====================================================================
SELECT table_schema, table_name
  FROM information_schema.tables
 WHERE table_schema IN ('etl_audit', 'silver', 'curated')
 ORDER BY table_schema, table_name;
