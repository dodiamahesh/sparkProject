-- =====================================================================
-- V6: STAR schema for EXAMPLE 4 -> ecommerce-fulfillment-pipeline
--     Source: THREE MongoDB collections (ecom_warehouses, ecom_carriers, ecom_shipments)
--     Target: 2 new dimensions + 1 fact, and it REUSES two existing dimensions:
--       - silver.dim_ecom_customer (from ecommerce-order-pipeline)   -> "conformed" dimension
--       - silver.dim_date          (shared by every pipeline)
--
--          dim_warehouse     dim_carrier
--                  \             /
--                   fact_shipment ---- dim_ecom_customer
--                    /         \
--     dim_date (shipped)     dim_date (delivered)      <- "role-playing" dimension:
--                                                          the SAME table used for two meanings
--
--     DEPENDENCY: every shipment must belong to an order in silver.fact_order, so run
--     ecommerce-order-pipeline FIRST. Shipments of unknown orders are rejected as ORPHAN and
--     can be re-processed later with loadType=RETRY_REJECTED.
-- =====================================================================
CREATE TABLE silver.dim_warehouse (
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

CREATE TABLE silver.dim_carrier (
    carrier_key     BIGSERIAL    PRIMARY KEY,
    carrier_id      VARCHAR(20)  NOT NULL UNIQUE,
    carrier_name    VARCHAR(150) NOT NULL,
    service_type    VARCHAR(20)  NOT NULL,
    contact_email   VARCHAR(150),
    carrier_status  VARCHAR(20)  NOT NULL,
    job_run_id      BIGINT,
    loaded_at       TIMESTAMP
);

CREATE TABLE silver.fact_shipment (
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
CREATE INDEX idx_fact_shipment_order     ON silver.fact_shipment (order_id);
CREATE INDEX idx_fact_shipment_shipped   ON silver.fact_shipment (shipped_date_key);
CREATE INDEX idx_fact_shipment_carrier   ON silver.fact_shipment (carrier_key);
CREATE INDEX idx_fact_shipment_warehouse ON silver.fact_shipment (warehouse_key);
