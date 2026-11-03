-- =====================================================================
-- V4: Star schema for EXAMPLE 2 -> transaction-pipeline
--     Source: FOUR MongoDB collections (customers, accounts, merchants, transactions)
--     Target: 3 dimensions + 1 fact (+ shared dim_date)
--
--            dim_customer     dim_account
--                    \          /
--                 fact_transaction ---- dim_date
--                         |
--                    dim_merchant
-- =====================================================================
CREATE TABLE silver.dim_customer (
    customer_key   BIGSERIAL    PRIMARY KEY,
    customer_id    VARCHAR(20)  NOT NULL UNIQUE,
    first_name     VARCHAR(100) NOT NULL,
    last_name      VARCHAR(100) NOT NULL,
    full_name      VARCHAR(200) NOT NULL,
    email          VARCHAR(150) NOT NULL,
    phone          VARCHAR(15)  NOT NULL,
    date_of_birth  DATE,
    gender         VARCHAR(1),
    city           VARCHAR(100),
    state          VARCHAR(100),
    country        VARCHAR(100),
    kyc_status     VARCHAR(20),
    job_run_id     BIGINT,
    loaded_at      TIMESTAMP
);

CREATE TABLE silver.dim_account (
    account_key      BIGSERIAL     PRIMARY KEY,
    account_id       VARCHAR(20)   NOT NULL UNIQUE,
    customer_id      VARCHAR(20)   NOT NULL,
    account_type     VARCHAR(20)   NOT NULL,
    branch_code      VARCHAR(20),
    ifsc_code        VARCHAR(11),
    currency         VARCHAR(3)    NOT NULL,
    account_status   VARCHAR(20)   NOT NULL,
    current_balance  NUMERIC(18,2),
    opened_date      DATE,
    job_run_id       BIGINT,
    loaded_at        TIMESTAMP
);

CREATE TABLE silver.dim_merchant (
    merchant_key     BIGSERIAL    PRIMARY KEY,
    merchant_id      VARCHAR(20)  NOT NULL UNIQUE,
    merchant_name    VARCHAR(150) NOT NULL,
    category         VARCHAR(30)  NOT NULL,
    mcc_code         VARCHAR(4),
    city             VARCHAR(100),
    country          VARCHAR(100),
    merchant_status  VARCHAR(20),
    onboarded_date   DATE,
    job_run_id       BIGINT,
    loaded_at        TIMESTAMP
);

CREATE TABLE silver.fact_transaction (
    transaction_key     BIGSERIAL     PRIMARY KEY,
    transaction_id      VARCHAR(20)   NOT NULL UNIQUE,
    customer_key        BIGINT        NOT NULL REFERENCES silver.dim_customer (customer_key),
    account_key         BIGINT        NOT NULL REFERENCES silver.dim_account (account_key),
    merchant_key        BIGINT        NOT NULL REFERENCES silver.dim_merchant (merchant_key),
    date_key            INT           NOT NULL REFERENCES silver.dim_date (date_key),
    transaction_type    VARCHAR(10)   NOT NULL,
    channel             VARCHAR(20)   NOT NULL,
    amount              NUMERIC(18,2) NOT NULL,
    currency            VARCHAR(3)    NOT NULL,
    transaction_status  VARCHAR(20)   NOT NULL,
    description         VARCHAR(255),
    transaction_ts      TIMESTAMP     NOT NULL,
    job_run_id          BIGINT,
    loaded_at           TIMESTAMP
);
CREATE INDEX idx_fact_txn_date     ON silver.fact_transaction (date_key);
CREATE INDEX idx_fact_txn_customer ON silver.fact_transaction (customer_key);
