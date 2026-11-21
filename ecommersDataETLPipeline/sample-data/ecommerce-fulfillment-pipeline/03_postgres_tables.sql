-- =====================================================================
--  ecommerce-fulfillment-pipeline  |  03_postgres_tables.sql  |  ALL PostgreSQL tables of this job in ONE file
--  MongoDB ecom_warehouses/carriers/shipments -> star schema (silver); needs the order tables
--
--  Contents:
--    PART 1  META tables (etl_audit.* + etl_staging + silver.dim_date), shared by every job
--    PART 2  the target tables of this job
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


-- =====================================================================
-- Verify: list the tables of this job + the meta tables
-- =====================================================================
SELECT table_schema, table_name
  FROM information_schema.tables
 WHERE table_schema IN ('etl_audit', 'silver')
 ORDER BY table_schema, table_name;
