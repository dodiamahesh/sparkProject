-- =============================================================================
--  00_cdc_duplicate_rule.sql  |  duplicate check of the CDC subscriber: rule table + duplicate record table
--
--  Run (once; running it again is safe). Same database as 00_cdc_collection_mapping.sql:
--      & "C:\Program Files\PostgreSQL\18\bin\psql.exe" -h localhost -p 5433 -U postgres -d silver -f sample-data\00_cdc_duplicate_rule.sql
--
--  Design: docs\DUPLICATE_CHECK_DESIGN_EN.md (Gujarati: docs\DUPLICATE_CHECK_DESIGN_GU.md)
--
--  meta.cdc_duplicate_rule
--      A NEW record (a write that creates a new document in bronze_db) is a duplicate when a LIVE bronze document
--      (_deleted: false) with another _id has the same values for ALL fields of the rule. First one wins.
--      fields: comma separated, nested with dot notation ("address.pincode"). Not allowed: _id, _op, _deleted, ...
--      No active rule = no duplicate check for that source. Read for every Kafka batch (no restart needed).
--      The same fields must be in the unique index of the collection: sample-data\<pipeline>\03_bronze_unique_indexes.js
--
--  meta.meta_duplicate_record
--      The rejected records. One row per source record (unique db_type + data_store_name + record_id):
--      a Kafka replay or a later update of the same rejected record updates object_payload, no new row.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS meta;

-- ---------------------------------------------------------------- rule table
CREATE TABLE IF NOT EXISTS meta.cdc_duplicate_rule (
    source       VARCHAR(255) NOT NULL,                     -- source collection (NOSQL) / table (SQL), = ns.coll
    db_type      VARCHAR(10)  NOT NULL
                 CHECK (db_type IN ('NOSQL', 'SQL')),       -- type of the SOURCE (same as the mapping table)
    fields       TEXT         NOT NULL,                     -- 'name,email,phone': ALL equal = duplicate
    active       BOOLEAN      NOT NULL DEFAULT TRUE,        -- false = no duplicate check
    PRIMARY KEY (source, db_type)
);

COMMENT ON TABLE meta.cdc_duplicate_rule IS
    'CDC subscriber: fields that identify a duplicate record per source. All fields equal = duplicate.';

-- ---------------------------------------------------------------- duplicate record table
CREATE TABLE IF NOT EXISTS meta.meta_duplicate_record (
    id_pk            BIGSERIAL    PRIMARY KEY,
    record_id        TEXT         NOT NULL,                 -- source _id: a string as it is, else JSON ({"$oid": "..."})
    data_store_name  VARCHAR(255) NOT NULL,                 -- source collection / table
    object_payload   JSONB        NOT NULL,                 -- the rejected record (latest copy, relaxed Extended JSON)
    db_type          VARCHAR(10)  NOT NULL
                     CHECK (db_type IN ('NOSQL', 'SQL')),
    CONSTRAINT uq_meta_duplicate_record UNIQUE (db_type, data_store_name, record_id)
);

COMMENT ON TABLE meta.meta_duplicate_record IS
    'CDC subscriber: records not written to bronze because they are duplicates (meta.cdc_duplicate_rule).';

-- ---------------------------------------------------------------- rules (MongoDB collections of the sample data)
-- The business key of every sample collection (the field the ETL also uses to find duplicates).
-- More fields = a stricter rule, for example customers: 'customer_code,email' (both equal = duplicate).
-- Only collections with an active mapping AND synced by the connector are really checked (now: customers, products).
INSERT INTO meta.cdc_duplicate_rule (source, db_type, fields, active) VALUES
    -- sales-pipeline
    ('products',          'NOSQL', 'product_code',   TRUE),
    ('customers',         'NOSQL', 'customer_code',  TRUE),
    ('orders',            'NOSQL', 'order_no',       TRUE),
    ('payments',          'NOSQL', 'payment_id',     TRUE),
    -- payment-pipeline
    ('bank_payments',     'NOSQL', 'paymentId',      TRUE),
    -- transaction-pipeline
    ('bank_customers',    'NOSQL', 'customerId',     TRUE),
    ('bank_accounts',     'NOSQL', 'accountId',      TRUE),
    ('bank_merchants',    'NOSQL', 'merchantId',     TRUE),
    ('bank_transactions', 'NOSQL', 'transactionId',  TRUE),
    -- ecommerce-order-pipeline
    ('ecom_categories',   'NOSQL', 'categoryId',     TRUE),
    ('ecom_products',     'NOSQL', 'productId',      TRUE),
    ('ecom_customers',    'NOSQL', 'customerId',     TRUE),
    ('ecom_orders',       'NOSQL', 'orderId',        TRUE),
    ('ecom_order_items',  'NOSQL', 'orderItemId',    TRUE),
    -- ecommerce-fulfillment-pipeline
    ('ecom_warehouses',   'NOSQL', 'warehouseId',    TRUE),
    ('ecom_carriers',     'NOSQL', 'carrierId',      TRUE),
    ('ecom_shipments',    'NOSQL', 'shipmentId',     TRUE),
    -- vat-return-pipeline
    ('vat_return_coll',   'NOSQL', 'ackNo',          TRUE)
ON CONFLICT (source, db_type) DO UPDATE
    SET fields = EXCLUDED.fields,
        active = EXCLUDED.active;

-- ---------------------------------------------------------------- examples (run by hand when needed)
-- Several fields (ALL equal = duplicate). Change the unique index too (03_bronze_unique_indexes.js):
--   UPDATE meta.cdc_duplicate_rule SET fields = 'customer_code,email' WHERE source = 'customers' AND db_type = 'NOSQL';
-- Turn the check off for one source:
--   UPDATE meta.cdc_duplicate_rule SET active = FALSE WHERE source = 'orders' AND db_type = 'NOSQL';
-- A rule for a SQL table:
--   INSERT INTO meta.cdc_duplicate_rule (source, db_type, fields) VALUES ('station', 'SQL', 'station_id')
--   ON CONFLICT (source, db_type) DO UPDATE SET fields = EXCLUDED.fields, active = TRUE;

-- ---------------------------------------------------------------- queries
-- The rules:
SELECT source, db_type, fields, active FROM meta.cdc_duplicate_rule ORDER BY db_type, source;
-- Rejected records (run after a test):
--   SELECT data_store_name, count(*) FROM meta.meta_duplicate_record GROUP BY 1 ORDER BY 1;
--   SELECT id_pk, data_store_name, record_id, object_payload->>'product_code' AS product_code
--     FROM meta.meta_duplicate_record WHERE data_store_name = 'products';
