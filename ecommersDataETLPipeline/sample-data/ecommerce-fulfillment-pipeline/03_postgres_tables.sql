-- =====================================================================
--  ecommerce-fulfillment-pipeline  |  03_postgres_tables.sql  |  ALL PostgreSQL tables of this job in ONE file
--  MongoDB ecom_warehouses/carriers/shipments -> star schema (silver); needs the order tables
--
--  Contents:
--    PART 1  META tables (meta.* + etl_staging + silver.dim_date), shared by every job
--    PART 2  the target tables of this job
--    PART 3  meta.meta_error_master seed: every data-quality error of this job
--
--  Run (database must exist:  CREATE DATABASE shop_dw;):
--      psql -h localhost -p 5433 -U postgres -d shop_dw -f sample-data/ecommerce-fulfillment-pipeline/03_postgres_tables.sql
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
    deleted_count    BIGINT       NOT NULL DEFAULT 0,    -- documents soft-deleted at the source (applied, not validated)
    loaded_count     BIGINT       NOT NULL DEFAULT 0,    -- rows inserted/updated in target tables
    error_message    TEXT,                               -- short failure reason
    error_detail     TEXT                                -- stack trace (truncated)
);
CREATE INDEX IF NOT EXISTS idx_pipeline_run_pipeline ON meta.meta_pipeline_run (pipeline_name, start_time DESC);
CREATE INDEX IF NOT EXISTS idx_pipeline_run_status   ON meta.meta_pipeline_run (status);
-- upgrade of a database created before soft delete existed (re-run safe)
ALTER TABLE meta.meta_pipeline_run ADD COLUMN IF NOT EXISTS deleted_count BIGINT NOT NULL DEFAULT 0;

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
-- PART 2: ecommerce-fulfillment-pipeline tables (STAR schema, schema "silver")
--   ecom_warehouses -> dim_warehouse, ecom_carriers -> dim_carrier, ecom_shipments -> fact_shipment
--   DEPENDS ON ecommerce-order-pipeline: fact_shipment uses silver.fact_order + silver.dim_ecom_customer,
--   so the order tables are created here too (IF NOT EXISTS, same definition as in that job's file).
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

CREATE TABLE IF NOT EXISTS silver.dim_warehouse (
    warehouse_key     BIGSERIAL    PRIMARY KEY,
    warehouse_id      VARCHAR(20)  NOT NULL UNIQUE,
    warehouse_name    VARCHAR(150) NOT NULL,
    city              VARCHAR(100),
    state             VARCHAR(100),
    pincode           VARCHAR(6),
    capacity_units    INT,
    warehouse_status  VARCHAR(20)  NOT NULL,
    job_run_id        BIGINT,
    loaded_at         TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.dim_carrier (
    carrier_key     BIGSERIAL    PRIMARY KEY,
    carrier_id      VARCHAR(20)  NOT NULL UNIQUE,
    carrier_name    VARCHAR(150) NOT NULL,
    service_type    VARCHAR(20)  NOT NULL,
    contact_email   VARCHAR(150),
    carrier_status  VARCHAR(20)  NOT NULL,
    job_run_id      BIGINT,
    loaded_at       TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.fact_shipment (
    shipment_key        BIGSERIAL     PRIMARY KEY,
    shipment_id         VARCHAR(20)   NOT NULL UNIQUE,
    order_id            VARCHAR(20)   NOT NULL,                                          -- degenerate dimension
    customer_key        BIGINT        NOT NULL REFERENCES silver.dim_ecom_customer (customer_key),
    warehouse_key       BIGINT        NOT NULL REFERENCES silver.dim_warehouse (warehouse_key),
    carrier_key         BIGINT        NOT NULL REFERENCES silver.dim_carrier (carrier_key),
    shipped_date_key    INT           NOT NULL REFERENCES silver.dim_date (date_key),
    delivered_date_key  INT                    REFERENCES silver.dim_date (date_key),       -- NULL = not delivered yet
    tracking_number     VARCHAR(40),
    shipment_status     VARCHAR(30)   NOT NULL,
    shipping_cost       NUMERIC(18,2) NOT NULL,
    weight_kg           NUMERIC(9,3),
    promised_date       DATE          NOT NULL,
    shipped_ts          TIMESTAMP     NOT NULL,
    delivered_ts        TIMESTAMP,
    delivery_days       INT,          -- delivered date - shipped date
    is_on_time          BOOLEAN,      -- delivered on or before promised_date (NULL = not delivered)
    job_run_id          BIGINT,
    loaded_at           TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_fact_shipment_order     ON silver.fact_shipment (order_id);
CREATE INDEX IF NOT EXISTS idx_fact_shipment_shipped   ON silver.fact_shipment (shipped_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_shipment_carrier   ON silver.fact_shipment (carrier_key);
CREATE INDEX IF NOT EXISTS idx_fact_shipment_warehouse ON silver.fact_shipment (warehouse_key);


-- ---------------------------------------------------------------------
-- SOFT DELETE columns: a document deleted at the source (isDeleted = true) is NOT removed here; the row is
-- marked is_deleted = TRUE, deleted_at = when it was deleted. Reports filter WHERE NOT is_deleted.
-- A document restored at the source (isDeleted = false again) sets is_deleted back to FALSE.
-- ALTER ... ADD COLUMN IF NOT EXISTS also upgrades a database created with an older version (re-run safe).
-- ---------------------------------------------------------------------
ALTER TABLE silver.dim_category ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN NOT NULL DEFAULT FALSE, ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP;
ALTER TABLE silver.dim_product ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN NOT NULL DEFAULT FALSE, ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP;
ALTER TABLE silver.dim_ecom_customer ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN NOT NULL DEFAULT FALSE, ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP;
ALTER TABLE silver.fact_order ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN NOT NULL DEFAULT FALSE, ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP;
ALTER TABLE silver.fact_order_item ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN NOT NULL DEFAULT FALSE, ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP;
ALTER TABLE silver.dim_warehouse ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN NOT NULL DEFAULT FALSE, ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP;
ALTER TABLE silver.dim_carrier ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN NOT NULL DEFAULT FALSE, ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP;
ALTER TABLE silver.fact_shipment ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN NOT NULL DEFAULT FALSE, ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP;


-- =====================================================================
-- PART 3: meta.meta_error_master seed = every data-quality error the ecommerce-fulfillment-pipeline can produce
--   (generated from the rules in the code; shared errors such as "amount must be greater than 0" exist once).
--   The job looks an error up by error_type + error_message. Re-run safe: existing errors are skipped.
--   field_name = the checked field; its bad value is stored per record in meta_rejected_record.field_value.
-- =====================================================================
INSERT INTO meta.meta_error_master (error_type, field_name, error_message)
SELECT v.error_type, v.field_name, v.error_message
  FROM (VALUES
-- ecom_warehouses
    ('CORRUPT',    'capacityUnits',  'capacityUnits is not a whole number'),
    ('VALIDATION', 'warehouseId',    'warehouseId is required'),
    ('VALIDATION', 'warehouseName',  'warehouseName is required'),
    ('VALIDATION', 'city',           'city is required'),
    ('VALIDATION', 'status',         'status is required'),
    ('VALIDATION', 'warehouseId',    'warehouseId must look like WH-01'),
    ('VALIDATION', 'pincode',        'pincode must be a 6 digit Indian pincode'),
    ('VALIDATION', 'capacityUnits',  'capacityUnits must be greater than 0'),
    ('VALIDATION', 'status',         'status must be one of [ACTIVE, INACTIVE]'),
    ('DUPLICATE',  'warehouseId',    'another record with the same warehouseId was kept (latest updatedAt wins)'),
-- ecom_carriers
    ('VALIDATION', 'carrierId',      'carrierId is required'),
    ('VALIDATION', 'carrierName',    'carrierName is required'),
    ('VALIDATION', 'serviceType',    'serviceType is required'),
    ('VALIDATION', 'carrierId',      'carrierId must look like CRR-01'),
    ('VALIDATION', 'serviceType',    'serviceType must be one of [STANDARD, EXPRESS, SAME_DAY]'),
    ('VALIDATION', 'contactEmail',   'contactEmail is not a valid email address'),
    ('DUPLICATE',  'carrierId',      'another record with the same carrierId was kept (latest updatedAt wins)'),
-- ecom_shipments
    ('CORRUPT',    'shippedDate',    'shippedDate is not a valid timestamp (expected yyyy-MM-ddTHH:mm:ss)'),
    ('CORRUPT',    'deliveredDate',  'deliveredDate is not a valid timestamp (expected yyyy-MM-ddTHH:mm:ss)'),
    ('CORRUPT',    'promisedDate',   'promisedDate is not a valid date (expected yyyy-MM-dd)'),
    ('CORRUPT',    'shippingCost',   'shippingCost is not a valid number'),
    ('CORRUPT',    'weightKg',       'weightKg is not a valid number'),
    ('VALIDATION', 'shipmentId',     'shipmentId is required'),
    ('VALIDATION', 'orderId',        'orderId is required'),
    ('VALIDATION', 'shippedDate',    'shippedDate is required'),
    ('VALIDATION', 'promisedDate',   'promisedDate is required'),
    ('VALIDATION', 'shipmentStatus', 'shipmentStatus is required'),
    ('VALIDATION', 'shippingCost',   'shippingCost is required'),
    ('VALIDATION', 'shipmentId',     'shipmentId must look like SHP-00001'),
    ('VALIDATION', 'shippedDate',    'shippedDate cannot be in the future'),
    ('VALIDATION', 'deliveredDate',  'deliveredDate cannot be in the future'),
    ('VALIDATION', 'shippingCost',   'shippingCost cannot be negative'),
    ('VALIDATION', 'weightKg',       'weightKg must be greater than 0'),
    ('VALIDATION', 'shipmentStatus', 'shipmentStatus must be one of [IN_TRANSIT, DELIVERED, RETURNED_TO_ORIGIN, LOST]'),
    ('VALIDATION', NULL,             'deliveredDate is required when shipmentStatus is DELIVERED'),
    ('VALIDATION', NULL,             'deliveredDate cannot be before shippedDate'),
    ('ORPHAN',     'orderId',        'orderId not found in fact_order'),
    ('ORPHAN',     'warehouseId',    'warehouseId not found in dim_warehouse'),
    ('ORPHAN',     'carrierId',      'carrierId not found in dim_carrier'),
    ('DUPLICATE',  'shipmentId',     'another record with the same shipmentId was kept (latest updatedAt wins)')
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
