-- =====================================================================
--  ecommerce-order-pipeline  |  03_postgres_tables.sql  |  ALL PostgreSQL tables of this job in ONE file
--  MongoDB ecom_categories/products/customers/orders/order_items -> snowflake schema (silver)
--
--  Contents:
--    PART 1  META tables (meta.* + etl_staging + silver.dim_date), shared by every job
--    PART 2  the target tables of this job
--    PART 3  meta.meta_error_master seed: every data-quality error of this job
--
--  Run (database must exist:  CREATE DATABASE shop_dw;):
--      psql -h localhost -p 5433 -U postgres -d shop_dw -f sample-data/ecommerce-order-pipeline/03_postgres_tables.sql
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
    resolved_count   BIGINT       NOT NULL DEFAULT 0,    -- old rejected records that are valid now
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
-- PART 2: ecommerce-order-pipeline tables (SNOWFLAKE schema, schema "silver")
--   ecom_categories -> dim_category <- dim_product (category_key = snowflake link)
--   ecom_customers  -> dim_ecom_customer
--   ecom_orders     -> fact_order       (grain: one order)
--   ecom_order_items-> fact_order_item  (grain: one order line)
-- =====================================================================
CREATE TABLE IF NOT EXISTS silver.dim_category (
    category_key   BIGSERIAL    PRIMARY KEY,
    category_id    VARCHAR(20)  NOT NULL UNIQUE,
    category_name  VARCHAR(100) NOT NULL,
    department     VARCHAR(30)  NOT NULL,
    job_run_id     BIGINT,
    loaded_at      TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.dim_product (
    product_key     BIGSERIAL     PRIMARY KEY,
    product_id      VARCHAR(20)   NOT NULL UNIQUE,
    sku             VARCHAR(40),
    product_name    VARCHAR(200)  NOT NULL,
    brand           VARCHAR(100),
    category_key    BIGINT        NOT NULL REFERENCES silver.dim_category (category_key),   -- SNOWFLAKE link
    list_price      NUMERIC(18,2) NOT NULL,
    cost_price      NUMERIC(18,2),
    product_status  VARCHAR(20)   NOT NULL,
    launch_date     DATE,
    job_run_id      BIGINT,
    loaded_at       TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_dim_product_category ON silver.dim_product (category_key);

-- Named dim_ecom_customer because silver.dim_customer already belongs to the banking example
CREATE TABLE IF NOT EXISTS silver.dim_ecom_customer (
    customer_key      BIGSERIAL    PRIMARY KEY,
    customer_id       VARCHAR(20)  NOT NULL UNIQUE,
    first_name        VARCHAR(100) NOT NULL,
    last_name         VARCHAR(100) NOT NULL,
    full_name         VARCHAR(200) NOT NULL,
    email             VARCHAR(150) NOT NULL,
    phone             VARCHAR(15),
    city              VARCHAR(100),
    state             VARCHAR(100),
    pincode           VARCHAR(6),
    customer_segment  VARCHAR(20)  NOT NULL,
    signup_date       DATE,
    job_run_id        BIGINT,
    loaded_at         TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.fact_order (
    order_key       BIGSERIAL     PRIMARY KEY,
    order_id        VARCHAR(20)   NOT NULL UNIQUE,
    customer_key    BIGINT        NOT NULL REFERENCES silver.dim_ecom_customer (customer_key),
    date_key        INT           NOT NULL REFERENCES silver.dim_date (date_key),
    order_status    VARCHAR(20)   NOT NULL,
    payment_mode    VARCHAR(20)   NOT NULL,
    channel         VARCHAR(20)   NOT NULL,
    coupon_code     VARCHAR(30),
    shipping_fee    NUMERIC(18,2) NOT NULL,
    shipping_city   VARCHAR(100),
    shipping_state  VARCHAR(100),
    order_ts        TIMESTAMP     NOT NULL,
    job_run_id      BIGINT,
    loaded_at       TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_fact_order_date     ON silver.fact_order (date_key);
CREATE INDEX IF NOT EXISTS idx_fact_order_customer ON silver.fact_order (customer_key);

CREATE TABLE IF NOT EXISTS silver.fact_order_item (
    order_item_key   BIGSERIAL     PRIMARY KEY,
    order_item_id    VARCHAR(20)   NOT NULL UNIQUE,
    order_id         VARCHAR(20)   NOT NULL,                                        -- degenerate dimension
    product_key      BIGINT        NOT NULL REFERENCES silver.dim_product (product_key),
    customer_key     BIGINT        NOT NULL REFERENCES silver.dim_ecom_customer (customer_key),
    date_key         INT           NOT NULL REFERENCES silver.dim_date (date_key),
    quantity         INT           NOT NULL,
    unit_price       NUMERIC(18,2) NOT NULL,
    discount_amount  NUMERIC(18,2) NOT NULL,
    gross_amount     NUMERIC(18,2) NOT NULL,   -- quantity * unit_price
    net_amount       NUMERIC(18,2) NOT NULL,   -- gross_amount - discount_amount
    job_run_id       BIGINT,
    loaded_at        TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_fact_order_item_order   ON silver.fact_order_item (order_id);
CREATE INDEX IF NOT EXISTS idx_fact_order_item_product ON silver.fact_order_item (product_key);
CREATE INDEX IF NOT EXISTS idx_fact_order_item_date    ON silver.fact_order_item (date_key);


-- =====================================================================
-- PART 3: meta.meta_error_master seed = every data-quality error the ecommerce-order-pipeline can produce
--   (generated from the rules in the code; shared errors such as "amount must be greater than 0" exist once).
--   The job looks an error up by error_type + error_message. Re-run safe: existing errors are skipped.
--   field_name = the checked field; its bad value is stored per record in meta_rejected_record.field_value.
-- =====================================================================
INSERT INTO meta.meta_error_master (error_type, field_name, error_message)
SELECT v.error_type, v.field_name, v.error_message
  FROM (VALUES
-- ecom_categories
    ('VALIDATION', 'categoryId',     'categoryId is required'),
    ('VALIDATION', 'categoryName',   'categoryName is required'),
    ('VALIDATION', 'department',     'department is required'),
    ('VALIDATION', 'categoryId',     'categoryId must look like CAT-01'),
    ('VALIDATION', 'department',     'department must be one of [ELECTRONICS, FASHION, HOME, BEAUTY, GROCERY, SPORTS, BOOKS]'),
    ('DUPLICATE',  'categoryId',     'another record with the same categoryId was kept (latest updatedAt wins)'),
-- ecom_products
    ('CORRUPT',    'listPrice',      'listPrice is not a valid number'),
    ('CORRUPT',    'costPrice',      'costPrice is not a valid number'),
    ('CORRUPT',    'launchDate',     'launchDate is not a valid date (expected yyyy-MM-dd)'),
    ('VALIDATION', 'productId',      'productId is required'),
    ('VALIDATION', 'productName',    'productName is required'),
    ('VALIDATION', 'listPrice',      'listPrice is required'),
    ('VALIDATION', 'status',         'status is required'),
    ('VALIDATION', 'productId',      'productId must look like PRD-0001'),
    ('VALIDATION', 'listPrice',      'listPrice must be greater than 0'),
    ('VALIDATION', 'costPrice',      'costPrice cannot be negative'),
    ('VALIDATION', NULL,             'costPrice cannot be higher than listPrice'),
    ('VALIDATION', 'status',         'status must be one of [ACTIVE, OUT_OF_STOCK, DISCONTINUED]'),
    ('VALIDATION', 'launchDate',     'launchDate cannot be in the future'),
    ('ORPHAN',     'categoryId',     'categoryId not found in dim_category'),
    ('DUPLICATE',  'productId',      'another record with the same productId was kept (latest updatedAt wins)'),
-- ecom_customers
    ('CORRUPT',    'signupDate',     'signupDate is not a valid date (expected yyyy-MM-dd)'),
    ('VALIDATION', 'customerId',     'customerId is required'),
    ('VALIDATION', 'firstName',      'firstName is required'),
    ('VALIDATION', 'lastName',       'lastName is required'),
    ('VALIDATION', 'email',          'email is required'),
    ('VALIDATION', 'segment',        'segment is required'),
    ('VALIDATION', 'customerId',     'customerId must look like ECU-0001'),
    ('VALIDATION', 'email',          'email is not a valid email address'),
    ('VALIDATION', 'phone',          'phone must be a 10 digit Indian mobile number'),
    ('VALIDATION', 'pincode',        'pincode must be a 6 digit Indian pincode'),
    ('VALIDATION', 'segment',        'segment must be one of [REGULAR, PRIME, VIP]'),
    ('VALIDATION', 'signupDate',     'signupDate cannot be in the future'),
    ('DUPLICATE',  'customerId',     'another record with the same customerId was kept (latest updatedAt wins)'),
-- ecom_orders
    ('CORRUPT',    'orderDate',      'orderDate is not a valid timestamp (expected yyyy-MM-ddTHH:mm:ss)'),
    ('CORRUPT',    'shippingFee',    'shippingFee is not a valid number'),
    ('VALIDATION', 'orderId',        'orderId is required'),
    ('VALIDATION', 'orderDate',      'orderDate is required'),
    ('VALIDATION', 'orderStatus',    'orderStatus is required'),
    ('VALIDATION', 'paymentMode',    'paymentMode is required'),
    ('VALIDATION', 'channel',        'channel is required'),
    ('VALIDATION', 'shippingFee',    'shippingFee is required'),
    ('VALIDATION', 'orderId',        'orderId must look like ORD-00001'),
    ('VALIDATION', 'orderDate',      'orderDate cannot be in the future'),
    ('VALIDATION', 'shippingFee',    'shippingFee cannot be negative'),
    ('VALIDATION', 'orderStatus',    'orderStatus must be one of [PLACED, CONFIRMED, SHIPPED, DELIVERED, CANCELLED, RETURNED]'),
    ('VALIDATION', 'paymentMode',    'paymentMode must be one of [UPI, CARD, COD, NETBANKING, WALLET]'),
    ('VALIDATION', 'channel',        'channel must be one of [WEB, ANDROID, IOS]'),
    ('ORPHAN',     'customerId',     'customerId not found in dim_ecom_customer'),
    ('DUPLICATE',  'orderId',        'another record with the same orderId was kept (latest updatedAt wins)'),
-- ecom_order_items
    ('CORRUPT',    'quantity',       'quantity is not a whole number'),
    ('CORRUPT',    'unitPrice',      'unitPrice is not a valid number'),
    ('CORRUPT',    'discountAmount', 'discountAmount is not a valid number'),
    ('VALIDATION', 'orderItemId',    'orderItemId is required'),
    ('VALIDATION', 'quantity',       'quantity is required'),
    ('VALIDATION', 'unitPrice',      'unitPrice is required'),
    ('VALIDATION', 'orderItemId',    'orderItemId must look like OIT-000001'),
    ('VALIDATION', 'quantity',       'quantity must be greater than 0'),
    ('VALIDATION', 'unitPrice',      'unitPrice must be greater than 0'),
    ('VALIDATION', 'discountAmount', 'discountAmount cannot be negative'),
    ('VALIDATION', NULL,             'discountAmount cannot be higher than quantity * unitPrice'),
    ('ORPHAN',     'orderId',        'orderId not found in fact_order'),
    ('ORPHAN',     'productId',      'productId not found in dim_product'),
    ('DUPLICATE',  'orderItemId',    'another record with the same orderItemId was kept (latest updatedAt wins)')
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
