-- =============================================================================
--  03_postgres_create_tables.sql
--  PostgreSQL / EPAS: badha 5 jobs na target tables (job wise) + meta tables
--  Schemas: curated (= SILVER), gold (= GOLD), meta
--  (ETL_Jobs_Data_Design_Guide.md)
--
--  Kevi rite chalavvu:
--      psql -h localhost -U postgres -d <your_db> -f 03_postgres_create_tables.sql
--
--  Kram:
--      0. Schemas: curated, meta
--      1. Common: dim_date (2020–2035 generate), etl_updated_at trigger function
--      2. Job-1  J1_PRODUCT_INVENTORY   (Snowflake)
--      3. Job-2  J2_SALES               (Star, SCD2)
--      4. Job-3  J3_SUPPORT_TICKETS     (Single table)
--      5. Job-4  J4_FULFILMENT_REVIEWS  (Star + shared dims)
--      6. Job-5  J5_CUSTOMER_360        (Summary table)
--      7. Meta:  etl_watermark, meta_rejected, etl_run_audit
--
--  Niyam je badha curated tables ma che:
--    - Surrogate key: IDENTITY (Postgres/EPAS pote number aape)
--    - Business key par UNIQUE constraint  → ON CONFLICT upsert mate FARAJIYAT
--    - etl_run_id, etl_inserted_at, etl_updated_at columns
--      etl_updated_at trigger thi auto update thay → Job-5 ne khabar pade ke kai row badlai
--    - Business timestamps (order_ts, created_at …) IST ma, TIMESTAMP (tz vagar)
--    - ETL / watermark timestamps TIMESTAMPTZ
--
--  ⚠️ IDENTITY ane ON CONFLICT: upsert ma row UPDATE thay to pan sequence no ek number
--     vapray jay che. Etle keys ma gap aavi shake (21 ne badle 23). Aa normal che.
--     Test ma surrogate key nahi, business key (product_code, order_no …) thi compare karo.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- (Optional) Farithi shuru karvu hoy to j aa 2 lines chalavo. ⚠️ BADHO DATA JASE.
-- DROP SCHEMA IF EXISTS curated CASCADE;
-- DROP SCHEMA IF EXISTS gold CASCADE;
-- DROP SCHEMA IF EXISTS meta CASCADE;
-- -----------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS curated;   -- SILVER: dim + fact (Job 1–4)
CREATE SCHEMA IF NOT EXISTS gold;      -- GOLD: summary / reporting (Job-5)
CREATE SCHEMA IF NOT EXISTS meta;

-- =============================================================================
-- 1) COMMON
-- =============================================================================

-- etl_updated_at auto update (UPDATE vakhte now() set thay)
CREATE OR REPLACE FUNCTION meta.set_etl_updated_at()
RETURNS trigger AS $$
BEGIN
    NEW.etl_updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- -----------------------------------------------------------------------------
-- curated.dim_date  (conformed dimension, Mongo mathi nathi. Ek vaar generate)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS curated.dim_date (
    date_key        INTEGER      PRIMARY KEY,               -- 20261021
    full_date       DATE         NOT NULL UNIQUE,
    day_name        VARCHAR(10)  NOT NULL,                  -- Wednesday
    day_of_month    SMALLINT     NOT NULL,
    month_no        SMALLINT     NOT NULL,
    month_name      VARCHAR(10)  NOT NULL,                  -- October
    quarter_no      SMALLINT     NOT NULL,
    year_no         SMALLINT     NOT NULL,
    is_weekend      CHAR(1)      NOT NULL CHECK (is_weekend IN ('Y','N')),
    is_holiday      CHAR(1)      NOT NULL CHECK (is_holiday IN ('Y','N')),
    holiday_name    VARCHAR(50)
);

INSERT INTO curated.dim_date
SELECT  to_char(d, 'YYYYMMDD')::INT                           AS date_key,
        d::DATE                                               AS full_date,
        to_char(d, 'FMDay')                                   AS day_name,
        EXTRACT(DAY     FROM d)::SMALLINT                     AS day_of_month,
        EXTRACT(MONTH   FROM d)::SMALLINT                     AS month_no,
        to_char(d, 'FMMonth')                                 AS month_name,
        EXTRACT(QUARTER FROM d)::SMALLINT                     AS quarter_no,
        EXTRACT(YEAR    FROM d)::SMALLINT                     AS year_no,
        CASE WHEN EXTRACT(ISODOW FROM d) IN (6,7) THEN 'Y' ELSE 'N' END AS is_weekend,
        CASE WHEN to_char(d,'MM-DD') IN ('01-26','08-15','10-02') THEN 'Y' ELSE 'N' END AS is_holiday,
        CASE to_char(d,'MM-DD')
             WHEN '01-26' THEN 'Republic Day'
             WHEN '08-15' THEN 'Independence Day'
             WHEN '10-02' THEN 'Gandhi Jayanti'
        END                                                   AS holiday_name
FROM generate_series(DATE '2020-01-01', DATE '2035-12-31', INTERVAL '1 day') AS g(d)
ON CONFLICT (date_key) DO NOTHING;


-- =============================================================================
-- 2) JOB-1  J1_PRODUCT_INVENTORY   source: products   (Snowflake)
--    dim_brand, dim_category (self parent), dim_supplier, dim_warehouse,
--    dim_product (SCD1), fact_inventory_snapshot
-- =============================================================================

CREATE TABLE IF NOT EXISTS curated.dim_brand (
    brand_key        INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    brand_code       VARCHAR(10)  NOT NULL UNIQUE,          -- B01
    brand_name       VARCHAR(100) NOT NULL,                 -- Volt
    etl_run_id       VARCHAR(40),
    etl_inserted_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    etl_updated_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS curated.dim_category (
    category_key         INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    category_code        VARCHAR(20)  NOT NULL UNIQUE,      -- C-ELE-MOB
    category_name        VARCHAR(100) NOT NULL,             -- Mobiles
    level                SMALLINT     NOT NULL CHECK (level IN (1,2)),   -- 1 = parent, 2 = sub
    parent_category_key  INTEGER      REFERENCES curated.dim_category(category_key),  -- snowflake: self reference
    etl_run_id           VARCHAR(40),
    etl_inserted_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    etl_updated_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CHECK ((level = 1 AND parent_category_key IS NULL) OR (level = 2 AND parent_category_key IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS curated.dim_supplier (
    supplier_key     INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    supplier_code    VARCHAR(10)  NOT NULL UNIQUE,          -- SUP-01
    supplier_name    VARCHAR(150) NOT NULL,
    city             VARCHAR(60),
    etl_run_id       VARCHAR(40),
    etl_inserted_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    etl_updated_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS curated.dim_warehouse (
    warehouse_key    INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    warehouse_code   VARCHAR(10)  NOT NULL UNIQUE,          -- WH-AHM
    city             VARCHAR(60)  NOT NULL,
    state            VARCHAR(60)  NOT NULL,
    etl_run_id       VARCHAR(40),
    etl_inserted_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    etl_updated_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS curated.dim_product (                -- SCD Type 1 (overwrite)
    product_key      INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    product_code     VARCHAR(10)   NOT NULL UNIQUE,         -- P001
    product_name     VARCHAR(150)  NOT NULL,
    brand_key        INTEGER       NOT NULL REFERENCES curated.dim_brand(brand_key),
    category_key     INTEGER       NOT NULL REFERENCES curated.dim_category(category_key),
    supplier_key     INTEGER       NOT NULL REFERENCES curated.dim_supplier(supplier_key),
    current_price    NUMERIC(12,2) NOT NULL CHECK (current_price > 0),
    is_active        CHAR(1)       NOT NULL DEFAULT 'Y' CHECK (is_active IN ('Y','N')),
    etl_run_id       VARCHAR(40),
    etl_inserted_at  TIMESTAMPTZ   NOT NULL DEFAULT now(),
    etl_updated_at   TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CHECK (product_code ~ '^P[0-9]{3}$')
);

CREATE TABLE IF NOT EXISTS curated.fact_inventory_snapshot (    -- Periodic snapshot fact
    inventory_key      BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    snapshot_date_key  INTEGER       NOT NULL REFERENCES curated.dim_date(date_key),
    product_key        INTEGER       NOT NULL REFERENCES curated.dim_product(product_key),
    warehouse_key      INTEGER       NOT NULL REFERENCES curated.dim_warehouse(warehouse_key),
    stock_qty          INTEGER       NOT NULL CHECK (stock_qty >= 0),
    unit_price         NUMERIC(12,2) NOT NULL,
    stock_value        NUMERIC(14,2) NOT NULL,              -- stock_qty × unit_price
    etl_run_id         VARCHAR(40),
    etl_inserted_at    TIMESTAMPTZ   NOT NULL DEFAULT now(),
    etl_updated_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT uq_inventory_grain UNIQUE (snapshot_date_key, product_key, warehouse_key)   -- upsert key
);


-- =============================================================================
-- 3) JOB-2  J2_SALES   source: customers, orders, payments   (Star)
--    dim_customer (SCD2), dim_order_status, dim_payment_method, fact_sales, fact_payment
-- =============================================================================

CREATE TABLE IF NOT EXISTS curated.dim_customer (               -- SCD2: city/state/tier, SCD1: email/phone
    customer_key     INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    customer_code    VARCHAR(10)  NOT NULL,                 -- C001 (ek customer ni ghani versions hoi sake)
    customer_name    VARCHAR(100) NOT NULL,
    email            VARCHAR(150),
    phone            VARCHAR(10),
    city             VARCHAR(60),
    state            VARCHAR(60),
    tier             VARCHAR(10)  NOT NULL CHECK (tier IN ('Bronze','Silver','Gold','Platinum')),
    signup_date      DATE,
    valid_from       TIMESTAMP    NOT NULL,
    valid_to         TIMESTAMP    NOT NULL DEFAULT TIMESTAMP '9999-12-31 00:00:00',
    is_current       CHAR(1)      NOT NULL DEFAULT 'Y' CHECK (is_current IN ('Y','N')),
    etl_run_id       VARCHAR(40),
    etl_inserted_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    etl_updated_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT uq_customer_version UNIQUE (customer_code, valid_from),
    CHECK (valid_to > valid_from)
);
-- Ek customer ni FAKT EK current row hovi joie (SCD2 no sauthi important niyam)
CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_customer_current
    ON curated.dim_customer (customer_code) WHERE is_current = 'Y';
-- Job-3 email / phone thi customer shodhe che
CREATE INDEX IF NOT EXISTS ix_dim_customer_email ON curated.dim_customer (email) WHERE is_current = 'Y';
CREATE INDEX IF NOT EXISTS ix_dim_customer_phone ON curated.dim_customer (phone) WHERE is_current = 'Y';

CREATE TABLE IF NOT EXISTS curated.dim_order_status (
    order_status_key  SMALLINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    status_code       VARCHAR(15)  NOT NULL UNIQUE,         -- PLACED, SHIPPED, DELIVERED, CANCELLED, RETURNED
    description       VARCHAR(50),
    is_final          CHAR(1)      NOT NULL DEFAULT 'N' CHECK (is_final IN ('Y','N')),
    etl_run_id        VARCHAR(40),
    etl_inserted_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    etl_updated_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS curated.dim_payment_method (
    payment_method_key  SMALLINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    method_code         VARCHAR(15)  NOT NULL UNIQUE,       -- UPI, CARD, NETBANKING, COD
    method_name         VARCHAR(50),
    is_online           CHAR(1)      NOT NULL DEFAULT 'Y' CHECK (is_online IN ('Y','N')),
    etl_run_id          VARCHAR(40),
    etl_inserted_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    etl_updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS curated.fact_sales (                 -- Transaction fact, grain = 1 order line
    sales_key           BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    order_no            VARCHAR(10)   NOT NULL,             -- degenerate dimension
    line_no             SMALLINT      NOT NULL,
    order_date_key      INTEGER       NOT NULL REFERENCES curated.dim_date(date_key),
    customer_key        INTEGER       NOT NULL REFERENCES curated.dim_customer(customer_key),   -- order vakhte ni version
    product_key         INTEGER       NOT NULL REFERENCES curated.dim_product(product_key),
    payment_method_key  SMALLINT      NOT NULL REFERENCES curated.dim_payment_method(payment_method_key),
    order_status_key    SMALLINT      NOT NULL REFERENCES curated.dim_order_status(order_status_key),
    quantity            INTEGER       NOT NULL CHECK (quantity > 0),
    unit_price          NUMERIC(12,2) NOT NULL CHECK (unit_price > 0),
    line_amount         NUMERIC(14,2) NOT NULL,             -- quantity × unit_price
    etl_run_id          VARCHAR(40),
    etl_inserted_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),
    etl_updated_at      TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT uq_fact_sales_line UNIQUE (order_no, line_no)            -- upsert key
);
CREATE INDEX IF NOT EXISTS ix_fact_sales_customer ON curated.fact_sales (customer_key);
CREATE INDEX IF NOT EXISTS ix_fact_sales_date     ON curated.fact_sales (order_date_key);

CREATE TABLE IF NOT EXISTS curated.fact_payment (               -- Transaction fact, grain = 1 payment
    payment_key         BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    payment_id          VARCHAR(10)   NOT NULL UNIQUE,      -- upsert key
    order_no            VARCHAR(10)   NOT NULL,
    payment_date_key    INTEGER       NOT NULL REFERENCES curated.dim_date(date_key),
    customer_key        INTEGER       NOT NULL REFERENCES curated.dim_customer(customer_key),
    payment_method_key  SMALLINT      NOT NULL REFERENCES curated.dim_payment_method(payment_method_key),
    payment_status      VARCHAR(10)   NOT NULL CHECK (payment_status IN ('SUCCESS','FAILED','REFUNDED','PENDING')),
    amount              NUMERIC(14,2) NOT NULL CHECK (amount > 0),
    etl_run_id          VARCHAR(40),
    etl_inserted_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),
    etl_updated_at      TIMESTAMPTZ   NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_fact_payment_order    ON curated.fact_payment (order_no);
CREATE INDEX IF NOT EXISTS ix_fact_payment_customer ON curated.fact_payment (customer_key);


-- =============================================================================
-- 4) JOB-3  J3_SUPPORT_TICKETS   source: email_tickets, chat_tickets, call_logs
--    support_ticket (UNION merge → 1 table)
-- =============================================================================

CREATE TABLE IF NOT EXISTS curated.support_ticket (
    ticket_key         BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    source_channel     VARCHAR(10)  NOT NULL CHECK (source_channel IN ('EMAIL','CHAT','CALL')),
    source_ticket_id   VARCHAR(20)  NOT NULL,               -- EM-001 / CH-001 / CL-001
    customer_code      VARCHAR(10)  NOT NULL DEFAULT 'UNKNOWN',   -- soft rule: FK nathi, UNKNOWN allowed
    order_no           VARCHAR(10),
    issue_text         VARCHAR(500) NOT NULL,
    issue_category     VARCHAR(20)  NOT NULL CHECK (issue_category IN
                         ('PAYMENT','RETURN','PRODUCT_QUALITY','ACCOUNT','DELIVERY','OTHER')),
    status             VARCHAR(15)  NOT NULL CHECK (status IN ('OPEN','IN_PROGRESS','RESOLVED','ESCALATED')),
    priority           VARCHAR(10)  NOT NULL CHECK (priority IN ('LOW','MEDIUM','HIGH')),
    created_at         TIMESTAMP    NOT NULL,               -- IST (chat UTC mathi convert)
    handled_by         VARCHAR(50),
    call_duration_sec  INTEGER      CHECK (call_duration_sec IS NULL OR call_duration_sec >= 0),
    etl_run_id         VARCHAR(40),
    etl_inserted_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    etl_updated_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT uq_support_ticket_source UNIQUE (source_channel, source_ticket_id)   -- upsert key
);
CREATE INDEX IF NOT EXISTS ix_support_ticket_customer ON curated.support_ticket (customer_code);
CREATE INDEX IF NOT EXISTS ix_support_ticket_status   ON curated.support_ticket (status);


-- =============================================================================
-- 5) JOB-4  J4_FULFILMENT_REVIEWS   source: shipments, reviews (+ orders lookup)
--    dim_courier, fact_shipment (role-playing dates), fact_product_review
--    dim_customer (Job-2) ane dim_product (Job-1) REUSE thay che
-- =============================================================================

CREATE TABLE IF NOT EXISTS curated.dim_courier (
    courier_key      INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    courier_name     VARCHAR(50)  NOT NULL UNIQUE,          -- SpeedEx, QuickMove, EcoDeliver
    etl_run_id       VARCHAR(40),
    etl_inserted_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    etl_updated_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS curated.fact_shipment (              -- Accumulating-ish fact, grain = 1 package
    shipment_key        BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    shipment_id         VARCHAR(10)  NOT NULL UNIQUE,       -- upsert key
    order_no            VARCHAR(10)  NOT NULL,
    customer_key        INTEGER      NOT NULL REFERENCES curated.dim_customer(customer_key),
    courier_key         INTEGER      NOT NULL REFERENCES curated.dim_courier(courier_key),
    -- dim_date 4 roles ma (role-playing dimension)
    order_date_key      INTEGER      NOT NULL REFERENCES curated.dim_date(date_key),
    dispatch_date_key   INTEGER      NOT NULL REFERENCES curated.dim_date(date_key),
    promised_date_key   INTEGER      NOT NULL REFERENCES curated.dim_date(date_key),
    delivered_date_key  INTEGER      REFERENCES curated.dim_date(date_key),     -- IN_TRANSIT hoy to NULL
    shipment_status     VARCHAR(15)  NOT NULL CHECK (shipment_status IN ('IN_TRANSIT','DELIVERED','RETURNED')),
    days_to_deliver     INTEGER,
    delay_days          INTEGER      CHECK (delay_days IS NULL OR delay_days >= 0),
    is_on_time          CHAR(1)      CHECK (is_on_time IS NULL OR is_on_time IN ('Y','N')),
    etl_run_id          VARCHAR(40),
    etl_inserted_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    etl_updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CHECK (delivered_date_key IS NULL OR delivered_date_key >= dispatch_date_key),
    CHECK (shipment_status = 'IN_TRANSIT' OR delivered_date_key IS NOT NULL)
);
CREATE INDEX IF NOT EXISTS ix_fact_shipment_customer ON curated.fact_shipment (customer_key);

CREATE TABLE IF NOT EXISTS curated.fact_product_review (        -- Transaction fact, grain = 1 review
    review_key        BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    review_id         VARCHAR(10)  NOT NULL UNIQUE,         -- upsert key
    product_key       INTEGER      NOT NULL REFERENCES curated.dim_product(product_key),
    customer_key      INTEGER      NOT NULL REFERENCES curated.dim_customer(customer_key),
    order_no          VARCHAR(10)  NOT NULL,
    review_date_key   INTEGER      NOT NULL REFERENCES curated.dim_date(date_key),
    rating            SMALLINT     NOT NULL CHECK (rating BETWEEN 1 AND 5),
    sentiment         VARCHAR(10)  NOT NULL CHECK (sentiment IN ('Positive','Neutral','Negative')),
    review_comment    VARCHAR(500),
    etl_run_id        VARCHAR(40),
    etl_inserted_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    etl_updated_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_fact_review_product  ON curated.fact_product_review (product_key);
CREATE INDEX IF NOT EXISTS ix_fact_review_customer ON curated.fact_product_review (customer_key);


-- =============================================================================
-- 6) JOB-5  J5_CUSTOMER_360   source: SILVER (curated) tables (Mongo nahi)  → GOLD schema
--    gold.customer_360_summary (1 row = 1 customer_code)
-- =============================================================================

CREATE TABLE IF NOT EXISTS gold.customer_360_summary (
    customer_code       VARCHAR(10)   PRIMARY KEY,          -- upsert key (customer_key nahi! SCD2 ni badhi versions ek code ma)
    customer_name       VARCHAR(100)  NOT NULL,
    city                VARCHAR(60),                        -- current version
    tier                VARCHAR(10),                        -- current version
    total_orders        INTEGER       NOT NULL DEFAULT 0,
    net_spend           NUMERIC(14,2) NOT NULL DEFAULT 0,   -- CANCELLED, RETURNED sivay
    last_order_date     DATE,
    paid_amount         NUMERIC(14,2) NOT NULL DEFAULT 0,   -- SUCCESS
    refund_amount       NUMERIC(14,2) NOT NULL DEFAULT 0,   -- REFUNDED
    total_tickets       INTEGER       NOT NULL DEFAULT 0,
    open_tickets        INTEGER       NOT NULL DEFAULT 0,   -- RESOLVED sivay
    avg_rating          NUMERIC(3,2),                       -- review na hoy to NULL (0 nahi)
    late_deliveries     INTEGER       NOT NULL DEFAULT 0,
    customer_segment    VARCHAR(15)   NOT NULL CHECK (customer_segment IN
                          ('NO_ORDER','AT_RISK','VIP','ZERO_SPEND','REGULAR')),
    spend_rank          INTEGER,
    last_refreshed_run  VARCHAR(40),
    etl_inserted_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),
    etl_updated_at      TIMESTAMPTZ   NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_c360_segment ON gold.customer_360_summary (customer_segment);


-- =============================================================================
-- etl_updated_at trigger: badha curated tables par (dim_date sivay)
-- =============================================================================
DO $$
DECLARE t TEXT;
BEGIN
    FOREACH t IN ARRAY ARRAY[
        'dim_brand','dim_category','dim_supplier','dim_warehouse','dim_product','fact_inventory_snapshot',
        'dim_customer','dim_order_status','dim_payment_method','fact_sales','fact_payment',
        'support_ticket',
        'dim_courier','fact_shipment','fact_product_review']
    LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS trg_%1$s_etl_updated_at ON curated.%1$I', t);
        EXECUTE format('CREATE TRIGGER trg_%1$s_etl_updated_at BEFORE UPDATE ON curated.%1$I
                        FOR EACH ROW EXECUTE FUNCTION meta.set_etl_updated_at()', t);
    END LOOP;
END $$;
-- Gold table no trigger (alag schema)
DROP TRIGGER IF EXISTS trg_customer_360_summary_etl_updated_at ON gold.customer_360_summary;
CREATE TRIGGER trg_customer_360_summary_etl_updated_at BEFORE UPDATE ON gold.customer_360_summary
    FOR EACH ROW EXECUTE FUNCTION meta.set_etl_updated_at();
-- Note: PostgreSQL 10 ke juna EPAS ma "EXECUTE FUNCTION" ne badle "EXECUTE PROCEDURE" lakhvu.

-- Job-5 mate: "kai row badlai?" e shodhva etl_updated_at par index
CREATE INDEX IF NOT EXISTS ix_dim_customer_etl   ON curated.dim_customer        (etl_updated_at);
CREATE INDEX IF NOT EXISTS ix_fact_sales_etl     ON curated.fact_sales          (etl_updated_at);
CREATE INDEX IF NOT EXISTS ix_fact_payment_etl   ON curated.fact_payment        (etl_updated_at);
CREATE INDEX IF NOT EXISTS ix_support_ticket_etl ON curated.support_ticket      (etl_updated_at);
CREATE INDEX IF NOT EXISTS ix_fact_shipment_etl  ON curated.fact_shipment       (etl_updated_at);
CREATE INDEX IF NOT EXISTS ix_fact_review_etl    ON curated.fact_product_review (etl_updated_at);


-- =============================================================================
-- 7) META tables (badha jobs mate common)
-- =============================================================================

-- Darek job + darek source ni ek row. last_watermark NULL = FULL LOAD
CREATE TABLE IF NOT EXISTS meta.etl_watermark (
    job_name           VARCHAR(40)  NOT NULL,
    source_collection  VARCHAR(40)  NOT NULL,
    watermark_column   VARCHAR(40)  NOT NULL,
    last_watermark     TIMESTAMPTZ,                         -- NULL → full load
    last_run_id        VARCHAR(40),
    last_status        VARCHAR(10)  CHECK (last_status IS NULL OR last_status IN ('SUCCESS','FAILED','RUNNING')),
    updated_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (job_name, source_collection)
);

-- Badha jobs na rejects ek j jagyae
CREATE TABLE IF NOT EXISTS meta.meta_rejected (
    reject_id          BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    job_name           VARCHAR(40)  NOT NULL,
    run_id             VARCHAR(40)  NOT NULL,               -- J1_20261021_0000
    source_collection  VARCHAR(40)  NOT NULL,
    source_key         VARCHAR(60),                         -- P022, O1008/line-2
    reject_level       VARCHAR(10)  NOT NULL DEFAULT 'DOCUMENT' CHECK (reject_level IN ('DOCUMENT','LINE')),
    reject_type        VARCHAR(20)  NOT NULL CHECK (reject_type IN
                         ('DUPLICATE','CORRUPT','VALIDATION','FK_MISSING','BUSINESS_RULE')),
    rule_code          VARCHAR(20)  NOT NULL,               -- J1-C01
    reason             VARCHAR(300) NOT NULL,
    bad_value          TEXT,
    raw_document       JSONB,                               -- aakho source doc (debug / reprocess mate)
    rejected_at        TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_meta_rejected_run  ON meta.meta_rejected (run_id);
CREATE INDEX IF NOT EXISTS ix_meta_rejected_type ON meta.meta_rejected (job_name, reject_type);

-- Run audit: darek run ni darek source ni ek row + ek 'JOB' summary row
CREATE TABLE IF NOT EXISTS meta.etl_run_audit (
    audit_id           BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    run_id             VARCHAR(40)  NOT NULL,
    job_name           VARCHAR(40)  NOT NULL,
    source_name        VARCHAR(40)  NOT NULL,               -- customers / orders / … / JOB
    load_type          VARCHAR(12)  NOT NULL CHECK (load_type IN ('FULL','INCREMENTAL')),
    triggered_by       VARCHAR(20),                         -- API / AIRFLOW / SCHEDULER / CLI
    status             VARCHAR(10)  NOT NULL CHECK (status IN ('RUNNING','SUCCESS','FAILED')),
    start_time         TIMESTAMPTZ  NOT NULL,
    end_time           TIMESTAMPTZ,
    total_record       INTEGER      NOT NULL DEFAULT 0,     -- window ma vanchela source documents
    processed_record   INTEGER      NOT NULL DEFAULT 0,     -- load thaya (partial pan aama)
    rejected_record    INTEGER      NOT NULL DEFAULT 0,     -- document level reject
    partial_record     INTEGER      NOT NULL DEFAULT 0,     -- load thaya, pan ek line reject thai
    inserted_record    INTEGER      NOT NULL DEFAULT 0,     -- target ma navi rows
    updated_record     INTEGER      NOT NULL DEFAULT 0,     -- target ma update thayeli rows
    error_message      TEXT,
    CONSTRAINT uq_run_source UNIQUE (run_id, source_name),
    CHECK (status <> 'SUCCESS' OR total_record = processed_record + rejected_record)
);
-- Juni version ni script pehla chalavi hoy to aa column umero (navi script ma upar j che)
ALTER TABLE meta.etl_run_audit ADD COLUMN IF NOT EXISTS triggered_by VARCHAR(20);

-- Watermark rows: badhi NULL → pehlo run FULL LOAD thashe
INSERT INTO meta.etl_watermark (job_name, source_collection, watermark_column, last_watermark) VALUES
    ('J1_PRODUCT_INVENTORY',  'products',       'updated_at',     NULL),
    ('J2_SALES',              'customers',      'updated_at',     NULL),
    ('J2_SALES',              'orders',         'updated_at',     NULL),
    ('J2_SALES',              'payments',       'updated_at',     NULL),
    ('J3_SUPPORT_TICKETS',    'email_tickets',  'updated_at',     NULL),
    ('J3_SUPPORT_TICKETS',    'chat_tickets',   'updatedAt',      NULL),    -- UTC string!
    ('J3_SUPPORT_TICKETS',    'call_logs',      'logged_at',      NULL),
    ('J4_FULFILMENT_REVIEWS', 'shipments',      'updated_at',     NULL),
    ('J4_FULFILMENT_REVIEWS', 'reviews',        'updated_at',     NULL),
    -- J4 mate 'orders' ni row NATHI: Job-4 ma orders key lookup che
    ('J5_CUSTOMER_360',       'curated',        'etl_updated_at', NULL)
ON CONFLICT (job_name, source_collection) DO NOTHING;


-- =============================================================================
-- Verify
-- =============================================================================
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema IN ('curated','gold','meta')
ORDER BY table_schema, table_name;
-- Expected: curated 16 tables, gold 1 table, meta 3 tables

SELECT count(*) AS dim_date_rows, min(date_key), max(date_key) FROM curated.dim_date;
-- Expected: 5844 rows, 20200101 → 20351231

SELECT * FROM curated.dim_date WHERE date_key BETWEEN 20261001 AND 20261003;
-- 20261001 Thursday N N | 20261002 Friday N Y (Gandhi Jayanti) | 20261003 Saturday Y N
