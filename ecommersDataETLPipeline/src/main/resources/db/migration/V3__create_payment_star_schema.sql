-- =====================================================================
-- V3: Star schema for EXAMPLE 1 -> payment-pipeline
--     Source: ONE MongoDB collection (payments)
--     Target: 3 dimensions + 1 fact (+ shared dim_date)
--
--            dim_payer      dim_payment_method
--                  \           /
--                  fact_payment ---- dim_date
--                       |
--                   dim_biller
-- =====================================================================
CREATE TABLE silver.dim_payer (
    payer_key    BIGSERIAL    PRIMARY KEY,           -- surrogate key
    payer_id     VARCHAR(20)  NOT NULL UNIQUE,       -- business key from source
    payer_name   VARCHAR(150) NOT NULL,
    payer_email  VARCHAR(150),
    payer_phone  VARCHAR(15),
    payer_city   VARCHAR(100),
    job_run_id   BIGINT,
    loaded_at    TIMESTAMP
);

CREATE TABLE silver.dim_payment_method (
    payment_method_key  BIGSERIAL   PRIMARY KEY,
    payment_method      VARCHAR(30) NOT NULL,        -- UPI, CARD, NETBANKING, WALLET
    payment_provider    VARCHAR(50) NOT NULL,        -- GPAY, VISA, HDFC ...
    job_run_id          BIGINT,
    loaded_at           TIMESTAMP,
    CONSTRAINT uq_payment_method UNIQUE (payment_method, payment_provider)
);

CREATE TABLE silver.dim_biller (
    biller_key       BIGSERIAL    PRIMARY KEY,
    biller_name      VARCHAR(150) NOT NULL UNIQUE,
    biller_category  VARCHAR(50),
    job_run_id       BIGINT,
    loaded_at        TIMESTAMP
);

CREATE TABLE silver.fact_payment (
    payment_key         BIGSERIAL     PRIMARY KEY,
    payment_id          VARCHAR(20)   NOT NULL UNIQUE,   -- business key (used for UPSERT)
    payer_key           BIGINT        NOT NULL REFERENCES silver.dim_payer (payer_key),
    payment_method_key  BIGINT        NOT NULL REFERENCES silver.dim_payment_method (payment_method_key),
    biller_key          BIGINT        NOT NULL REFERENCES silver.dim_biller (biller_key),
    date_key            INT           NOT NULL REFERENCES silver.dim_date (date_key),
    amount              NUMERIC(18,2) NOT NULL,
    currency            VARCHAR(3)    NOT NULL,
    payment_status      VARCHAR(20)   NOT NULL,
    reference_number    VARCHAR(50),
    payment_ts          TIMESTAMP     NOT NULL,
    job_run_id          BIGINT,
    loaded_at           TIMESTAMP
);
CREATE INDEX idx_fact_payment_date ON silver.fact_payment (date_key);
