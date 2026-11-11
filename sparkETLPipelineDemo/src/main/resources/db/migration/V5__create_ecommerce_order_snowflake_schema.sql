-- =====================================================================
-- V5: SNOWFLAKE schema for EXAMPLE 3 -> ecommerce-order-pipeline
--     Source: FIVE MongoDB collections
--             ecom_categories, ecom_products, ecom_customers, ecom_orders, ecom_order_items
--     Target: 3 dimensions + 2 facts (+ shared dim_date)
--
--     Why "snowflake"? dim_product does NOT repeat the category columns. It points to a
--     separate dim_category table (dimension -> dimension). In a STAR schema the category
--     columns would be copied into dim_product instead.
--
--        dim_category
--             |  (category_key)
--        dim_product        dim_ecom_customer      dim_date
--             \                /        \             /
--              fact_order_item  ------  fact_order ---
--              (one row per order LINE) (one row per ORDER)
--
--     Two facts with a different GRAIN:
--       fact_order       = order header (status, payment mode, shipping fee)
--       fact_order_item  = order line   (product, quantity, price, discount)
--     order_id is kept in both as a "degenerate dimension" (an id with no table of its own).
-- =====================================================================
CREATE TABLE silver.dim_category (
    category_key   BIGSERIAL    PRIMARY KEY,
    category_id    VARCHAR(20)  NOT NULL UNIQUE,
    category_name  VARCHAR(100) NOT NULL,
    department     VARCHAR(30)  NOT NULL,
    job_run_id     BIGINT,
    loaded_at      TIMESTAMP
);

CREATE TABLE silver.dim_product (
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
CREATE INDEX idx_dim_product_category ON silver.dim_product (category_key);

-- Named dim_ecom_customer because silver.dim_customer already belongs to the banking example
CREATE TABLE silver.dim_ecom_customer (
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

CREATE TABLE silver.fact_order (
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
CREATE INDEX idx_fact_order_date     ON silver.fact_order (date_key);
CREATE INDEX idx_fact_order_customer ON silver.fact_order (customer_key);

CREATE TABLE silver.fact_order_item (
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
CREATE INDEX idx_fact_order_item_order   ON silver.fact_order_item (order_id);
CREATE INDEX idx_fact_order_item_product ON silver.fact_order_item (product_key);
CREATE INDEX idx_fact_order_item_date    ON silver.fact_order_item (date_key);
