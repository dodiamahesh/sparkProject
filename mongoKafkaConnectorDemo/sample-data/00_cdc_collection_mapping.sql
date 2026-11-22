-- =============================================================================
--  00_cdc_collection_mapping.sql  |  PostgreSQL meta table for the CDC subscriber (mongo-cdc-subscriber)
--
--  Run (once; running it again is safe):
--      & "C:\Program Files\PostgreSQL\18\bin\psql.exe" -h localhost -p 5433 -U postgres -d silver -f sample-data\00_cdc_collection_mapping.sql
--  (database silver already exists; the script creates schema meta if it is missing)
--
--  Sources can be MongoDB COLLECTIONS and SQL TABLES (same message format: ns.coll = collection / table name).
--  db_type = type of the SOURCE:  NOSQL = MongoDB collection,  SQL = SQL table.
--  destination = always a collection in bronze_db.
--
--  Each subscriber reads one source system (one topic) and uses only the rows of its own type
--  (subscriber.properties: source.db.type=NOSQL or SQL). Before writing an event it looks up ns.coll here:
--      active row of its db_type found -> the event is written to bronze_db.<destination>
--      no row / active = false         -> MappingException, the message goes to the DLQ topic (cdc-dlq)
--      destination collection does not exist in bronze_db -> MappingException, DLQ
--                                 (the subscriber NEVER creates a collection: sample-data\00_create_bronze_collections.js)
--  The table is read again for every Kafka batch, so changes here work without restarting the subscriber.
--
--  The same name can exist as a collection AND as a table (key = source + db_type), for example
--  ('products', 'products', 'NOSQL') and ('products', 'sql_products', 'SQL'): give them different destinations.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS meta;

CREATE TABLE IF NOT EXISTS meta.cdc_collection_mapping (
    source       VARCHAR(255) NOT NULL,                     -- source collection (NOSQL) / table (SQL), = ns.coll
    destination  VARCHAR(255) NOT NULL,                     -- collection in bronze_db to write to
    db_type      VARCHAR(10)  NOT NULL
                 CHECK (db_type IN ('NOSQL', 'SQL')),       -- type of the SOURCE
    active       BOOLEAN      NOT NULL DEFAULT TRUE,        -- false = events of this source are rejected (DLQ)
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (source, db_type)                           -- one destination per source and source type
);

COMMENT ON TABLE meta.cdc_collection_mapping IS
    'CDC subscriber: source collection (NOSQL) / table (SQL) -> bronze_db collection. Only active rows are used.';
COMMENT ON COLUMN meta.cdc_collection_mapping.db_type IS
    'Type of the source: NOSQL = MongoDB collection, SQL = SQL table';

-- ---------------------------------------------------------------- mappings
INSERT INTO meta.cdc_collection_mapping (source, destination, db_type, active) VALUES
    -- MongoDB collections: the 2 the connector syncs now (kafka-connect\mongo-cdc-db-source.json: customers, products).
    -- Same name in bronze_db, so the ETL (ecommersDataETLPipeline) and 99_verify_bronze.js keep working.
    ('customers', 'customers',      'NOSQL', TRUE),
    ('products',  'products',       'NOSQL', TRUE),
    -- not active yet: if the connector sends "orders" events, they go to the DLQ (example of active = false)
    ('orders',    'orders',         'NOSQL', FALSE),
    -- SQL tables (for a SQL source topic + a subscriber with source.db.type=SQL). Not active until that source exists.
    ('station',       'sql_station',       'SQL', FALSE),
    ('station_owner', 'sql_station_owner', 'SQL', FALSE)
ON CONFLICT (source, db_type) DO UPDATE
    SET destination = EXCLUDED.destination,
        active      = EXCLUDED.active,
        updated_at  = now();

-- ---------------------------------------------------------------- examples (run by hand when needed)
-- Other destination name (collection "products" -> bronze_db.raw_products). Create the collection first!
--   UPDATE meta.cdc_collection_mapping SET destination = 'raw_products', updated_at = now()
--    WHERE source = 'products' AND db_type = 'NOSQL';
-- Stop one source (its events go to the DLQ from the next batch on):
--   UPDATE meta.cdc_collection_mapping SET active = FALSE, updated_at = now() WHERE source = 'customers' AND db_type = 'NOSQL';
-- Turn on a SQL table (needs its topic + a subscriber with source.db.type=SQL, and bronze_db.sql_station):
--   UPDATE meta.cdc_collection_mapping SET active = TRUE, updated_at = now() WHERE source = 'station' AND db_type = 'SQL';
-- A new source:
--   INSERT INTO meta.cdc_collection_mapping (source, destination, db_type) VALUES ('orders', 'orders', 'NOSQL')
--   ON CONFLICT (source, db_type) DO UPDATE SET active = TRUE, updated_at = now();

SELECT source, destination, db_type, active FROM meta.cdc_collection_mapping ORDER BY db_type, source;
