-- =====================================================================
--  vat-return-pipeline  |  03_postgres_tables.sql  |  ALL PostgreSQL tables of this job in ONE file
--  MongoDB vat_return_coll (one document per filed monthly VAT return) -> STAR schema in "silver"
--
--  Contents:
--    PART 1  META tables (meta.* + etl_staging + silver.dim_date), shared by every job
--    PART 2  the target tables of this job (+ seed of the 4 reference dimensions)
--    PART 3  meta.meta_error_master seed: every data-quality error of this job
--
--  Run (database silver on localhost:5433 already exists):
--      psql -h localhost -p 5433 -U postgres -d silver -f sample-data/vat-return-pipeline/03_postgres_tables.sql
--
--  Re-run safe: every statement uses IF NOT EXISTS / ON CONFLICT, so you can run it again, and you can
--  run the files of several jobs on the same database (the meta part is identical in all of them).
--
--  This job writes DIRECTLY into the silver tables (no etl_staging tables, see vat-return-pipeline.md);
--  the etl_staging schema in PART 1 is only there because the meta part is the same in every job file.
-- =====================================================================

-- =====================================================================
-- PART 1: META tables (shared by EVERY job; safe to run again: IF NOT EXISTS)
--   meta.meta_pipeline_run     one row per pipeline run (status, counters, error)
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

-- the step table is no longer used: removed from a database created before (re-run safe)
DROP TABLE IF EXISTS meta.meta_pipeline_step;

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
-- PART 2: STAR schema for vat-return-pipeline (schema "silver")
--
--                    dim_tax_period      dim_taxpayer
--                              \           /
--     dim_tax_form ------------ fact_vat_return ------------ dim_filing_channel
--                              /     |      \
--          dim_vat_return_type       |       dim_vat_return_status
--                                    |
--                           silver.dim_date  (3 roles: filed / due / acknowledgement date)
--
--   Grain of the fact: ONE row per filed return (ack_no). An amended return is a new filing with its own
--   ack_no, so the original and the amendment are both kept.
--   Every dimension joins the fact directly (no dimension-to-dimension join) -> STAR, not snowflake.
--   The tax type (101 = VAT) is stored inside dim_tax_form instead of a separate dim_tax_type (denormalized).
-- =====================================================================

-- ---------------------------------------------------------------------
-- dim_taxpayer (SCD1): one row per TIN, overwritten with the latest name / category / jurisdiction
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.dim_taxpayer (
    taxpayer_key        BIGSERIAL    PRIMARY KEY,
    tin                 VARCHAR(20)  NOT NULL UNIQUE,     -- business key, e.g. P0000000620
    taxpayer_id         VARCHAR(20),                      -- registration id, e.g. 110330002299
    taxpayer_name       VARCHAR(200) NOT NULL,
    taxpayer_category   VARCHAR(30)  NOT NULL,            -- Individual / Non-Individual
    jurisdiction_code   VARCHAR(10),                      -- tax office / region code, e.g. Q
    job_run_id          BIGINT,
    loaded_at           TIMESTAMP
);

-- ---------------------------------------------------------------------
-- dim_tax_period: one row per return month, built from the data (period_key = yyyyMM)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.dim_tax_period (
    period_key          INT          PRIMARY KEY,         -- e.g. 202609
    period_from         DATE         NOT NULL,
    period_to           DATE         NOT NULL,
    period_month        INT          NOT NULL CHECK (period_month BETWEEN 1 AND 12),
    period_year         INT          NOT NULL,
    period_quarter      INT          NOT NULL CHECK (period_quarter BETWEEN 1 AND 4),
    due_date            DATE         NOT NULL,            -- same for every return of the month
    job_run_id          BIGINT,
    loaded_at           TIMESTAMP
);

-- ---------------------------------------------------------------------
-- dim_tax_form (reference, seeded): return form + its tax type
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.dim_tax_form (
    tax_form_key        BIGSERIAL    PRIMARY KEY,
    form_id             VARCHAR(20)  NOT NULL UNIQUE,     -- e.g. 12027
    form_name           VARCHAR(100) NOT NULL,
    tax_type_id         VARCHAR(10)  NOT NULL,            -- e.g. 101
    tax_type_name       VARCHAR(100) NOT NULL
);
INSERT INTO silver.dim_tax_form (form_id, form_name, tax_type_id, tax_type_name) VALUES
    ('12027', 'VAT Standard Rate Return', '101', 'Value Added Tax'),
    ('12028', 'VAT Flat Rate Return',     '101', 'Value Added Tax')
ON CONFLICT (form_id) DO NOTHING;

-- ---------------------------------------------------------------------
-- dim_vat_return_type (reference, seeded)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.dim_vat_return_type (
    return_type_key     BIGSERIAL    PRIMARY KEY,
    return_type_code    VARCHAR(20)  NOT NULL UNIQUE,     -- Original / Amended
    description         VARCHAR(100) NOT NULL
);
INSERT INTO silver.dim_vat_return_type (return_type_code, description) VALUES
    ('Original', 'First return filed for the period'),
    ('Amended',  'Correction of an earlier return of the same period')
ON CONFLICT (return_type_code) DO NOTHING;

-- ---------------------------------------------------------------------
-- dim_vat_return_status (reference, seeded)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.dim_vat_return_status (
    return_status_key   BIGSERIAL    PRIMARY KEY,
    status_code         VARCHAR(10)  NOT NULL UNIQUE,     -- APRV / PEND / REJT
    status_name         VARCHAR(50)  NOT NULL,
    is_final            BOOLEAN      NOT NULL             -- can the status still change?
);
INSERT INTO silver.dim_vat_return_status (status_code, status_name, is_final) VALUES
    ('APRV', 'Approved', TRUE),
    ('PEND', 'Pending',  FALSE),
    ('REJT', 'Rejected', TRUE)
ON CONFLICT (status_code) DO NOTHING;

-- ---------------------------------------------------------------------
-- dim_filing_channel (reference, seeded)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.dim_filing_channel (
    filing_channel_key  BIGSERIAL    PRIMARY KEY,
    channel_code        VARCHAR(10)  NOT NULL UNIQUE,     -- INET / OFFC / MOBL
    channel_name        VARCHAR(50)  NOT NULL
);
INSERT INTO silver.dim_filing_channel (channel_code, channel_name) VALUES
    ('INET', 'Internet portal'),
    ('OFFC', 'Tax office'),
    ('MOBL', 'Mobile app')
ON CONFLICT (channel_code) DO NOTHING;

-- ---------------------------------------------------------------------
-- fact_vat_return: one row per filed return (ack_no). UPSERT on ack_no: a status change updates the row.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.fact_vat_return (
    vat_return_key           BIGSERIAL     PRIMARY KEY,
    ack_no                   VARCHAR(20)   NOT NULL UNIQUE,   -- business key (degenerate dimension)
    taxpayer_key             BIGINT        NOT NULL REFERENCES silver.dim_taxpayer (taxpayer_key),
    period_key               INT           NOT NULL REFERENCES silver.dim_tax_period (period_key),
    tax_form_key             BIGINT        NOT NULL REFERENCES silver.dim_tax_form (tax_form_key),
    return_type_key          BIGINT        NOT NULL REFERENCES silver.dim_vat_return_type (return_type_key),
    return_status_key        BIGINT        NOT NULL REFERENCES silver.dim_vat_return_status (return_status_key),
    filing_channel_key       BIGINT        NOT NULL REFERENCES silver.dim_filing_channel (filing_channel_key),
    filed_date_key           INT           NOT NULL REFERENCES silver.dim_date (date_key),
    due_date_key             INT           NOT NULL REFERENCES silver.dim_date (date_key),
    ack_date_key             INT           REFERENCES silver.dim_date (date_key),
    filed_at                 TIMESTAMP     NOT NULL,          -- rtnRegisterVatDTO.crtDt
    -- flags
    is_nil_return            BOOLEAN       NOT NULL,
    is_late_return           BOOLEAN       NOT NULL,
    is_active                BOOLEAN       NOT NULL,
    delay_days               INT           NOT NULL,          -- filed date - due date (> 0 = late)
    -- taxable outputs (sales)
    taxable_outputs_value    NUMERIC(18,2) NOT NULL,
    vat_levies               NUMERIC(18,2) NOT NULL,
    zero_rated_value         NUMERIC(18,2) NOT NULL,
    relief_value             NUMERIC(18,2) NOT NULL,
    exempt_supplies_value    NUMERIC(18,2) NOT NULL,
    -- purchases / expenses (inputs)
    local_purchases_value    NUMERIC(18,2) NOT NULL,
    imported_goods_value     NUMERIC(18,2) NOT NULL,
    imported_services_value  NUMERIC(18,2) NOT NULL,
    total_input_value        NUMERIC(18,2) NOT NULL,
    input_tax_on_purchases   NUMERIC(18,2) NOT NULL,
    -- VAT computation
    total_output_tax         NUMERIC(18,2) NOT NULL,
    total_input_tax          NUMERIC(18,2) NOT NULL,
    deductible_input_tax     NUMERIC(18,2) NOT NULL,
    net_output_tax           NUMERIC(18,2) NOT NULL,          -- total_output_tax - deductible_input_tax
    withholding_vat_credit   NUMERIC(18,2) NOT NULL,
    credit_last_month        NUMERIC(18,2) NOT NULL,
    net_payable              NUMERIC(18,2) NOT NULL,          -- < 0 = credit for the taxpayer
    credit_brought_forward   NUMERIC(18,2) NOT NULL,
    credit_carried_forward   NUMERIC(18,2) NOT NULL,
    -- penalty / interest / total
    penalty_amount           NUMERIC(18,2) NOT NULL,
    interest_amount          NUMERIC(18,2) NOT NULL,
    net_tax                  NUMERIC(18,2) NOT NULL,          -- net_payable + penalty + interest
    -- stock bought (additionalInputData)
    stock_standard_value     NUMERIC(18,2) NOT NULL,
    stock_standard_levies    NUMERIC(18,2) NOT NULL,
    stock_flat_rate_value    NUMERIC(18,2) NOT NULL,
    stock_flat_rate_levies   NUMERIC(18,2) NOT NULL,
    job_run_id               BIGINT,
    loaded_at                TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_fact_vat_return_taxpayer ON silver.fact_vat_return (taxpayer_key);
CREATE INDEX IF NOT EXISTS idx_fact_vat_return_period   ON silver.fact_vat_return (period_key);
CREATE INDEX IF NOT EXISTS idx_fact_vat_return_filed    ON silver.fact_vat_return (filed_date_key);


-- ---------------------------------------------------------------------
-- SOFT DELETE columns: a document deleted at the source (isDeleted = true) is NOT removed here; the row is
-- marked is_deleted = TRUE, deleted_at = when it was deleted. Reports filter WHERE NOT is_deleted.
-- A document restored at the source (isDeleted = false again) sets is_deleted back to FALSE.
-- ALTER ... ADD COLUMN IF NOT EXISTS also upgrades a database created with an older version (re-run safe).
-- ---------------------------------------------------------------------
ALTER TABLE silver.fact_vat_return ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN NOT NULL DEFAULT FALSE, ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP;


-- =====================================================================
-- PART 3: meta.meta_error_master seed = every data-quality error the vat-return-pipeline can produce
--   (the rules of VatReturnSources.rules(); a rule added later gets its master row automatically on its first reject).
--   The job looks an error up by error_type + error_message. Re-run safe: existing errors are skipped.
--   field_name = the checked field; its bad value is stored per record in meta_rejected_record.field_value.
-- =====================================================================
INSERT INTO meta.meta_error_master (error_type, field_name, error_message)
SELECT v.error_type, v.field_name, v.error_message
  FROM (VALUES
-- vat_return_coll: CORRUPT (wrong data type)
    ('CORRUPT',    'taxableOutputsValue',     'taxableOutputsValue is not a valid number'),
    ('CORRUPT',    'vatLevies',               'vatLevies is not a valid number'),
    ('CORRUPT',    'zeroRatedValue',          'zeroRatedValue is not a valid number'),
    ('CORRUPT',    'reliefValue',             'reliefValue is not a valid number'),
    ('CORRUPT',    'exemptSuppliesValue',     'exemptSuppliesValue is not a valid number'),
    ('CORRUPT',    'localPurchasesValue',     'localPurchasesValue is not a valid number'),
    ('CORRUPT',    'importedGoodsValue',      'importedGoodsValue is not a valid number'),
    ('CORRUPT',    'importedServicesValue',   'importedServicesValue is not a valid number'),
    ('CORRUPT',    'totalInputValue',         'totalInputValue is not a valid number'),
    ('CORRUPT',    'inputTaxOnPurchases',     'inputTaxOnPurchases is not a valid number'),
    ('CORRUPT',    'totalOutputTax',          'totalOutputTax is not a valid number'),
    ('CORRUPT',    'totalInputTax',           'totalInputTax is not a valid number'),
    ('CORRUPT',    'deductibleInputTax',      'deductibleInputTax is not a valid number'),
    ('CORRUPT',    'netOutputTax',            'netOutputTax is not a valid number'),
    ('CORRUPT',    'withholdingVatCredit',    'withholdingVatCredit is not a valid number'),
    ('CORRUPT',    'creditLastMonth',         'creditLastMonth is not a valid number'),
    ('CORRUPT',    'netPayable',              'netPayable is not a valid number'),
    ('CORRUPT',    'creditBroughtForward',    'creditBroughtForward is not a valid number'),
    ('CORRUPT',    'creditCarriedForward',    'creditCarriedForward is not a valid number'),
    ('CORRUPT',    'penaltyAmount',           'penaltyAmount is not a valid number'),
    ('CORRUPT',    'interest',                'interest is not a valid number'),
    ('CORRUPT',    'netTax',                  'netTax is not a valid number'),
    ('CORRUPT',    'stockStandardValue',      'stockStandardValue is not a valid number'),
    ('CORRUPT',    'stockStandardLevies',     'stockStandardLevies is not a valid number'),
    ('CORRUPT',    'stockFlatRateValue',      'stockFlatRateValue is not a valid number'),
    ('CORRUPT',    'stockFlatRateLevies',     'stockFlatRateLevies is not a valid number'),
    ('CORRUPT',    'rtnFromDate',             'rtnFromDate is not a valid date (expected yyyy-MM-dd)'),
    ('CORRUPT',    'rtnToDate',               'rtnToDate is not a valid date (expected yyyy-MM-dd)'),
    ('CORRUPT',    'rtnDueDt',                'rtnDueDt is not a valid date (expected yyyy-MM-dd)'),
    ('CORRUPT',    'month',                   'month is not a whole number'),
    ('CORRUPT',    'year',                    'year is not a whole number'),
    ('CORRUPT',    'delayDays',               'delayDays is not a whole number'),
    ('CORRUPT',    'filedAt',                 'filedAt is not a valid timestamp (expected yyyy-MM-ddTHH:mm:ss)'),
    ('CORRUPT',    'ackDate',                 'ackDate is not a valid timestamp (expected yyyy-MM-ddTHH:mm:ss)'),
-- vat_return_coll: VALIDATION (business rules)
    ('VALIDATION', 'tin',                     'tin is required'),
    ('VALIDATION', 'taxpayerName',            'taxpayerName is required'),
    ('VALIDATION', 'taxpayerCategory',        'taxpayerCategory is required'),
    ('VALIDATION', 'ackNo',                   'ackNo is required'),
    ('VALIDATION', 'rtnFromDate',             'rtnFromDate is required'),
    ('VALIDATION', 'rtnToDate',               'rtnToDate is required'),
    ('VALIDATION', 'month',                   'month is required'),
    ('VALIDATION', 'year',                    'year is required'),
    ('VALIDATION', 'rtnType',                 'rtnType is required'),
    ('VALIDATION', 'formId',                  'formId is required'),
    ('VALIDATION', 'fileFrom',                'fileFrom is required'),
    ('VALIDATION', 'rtnStatus',               'rtnStatus is required'),
    ('VALIDATION', 'filedAt',                 'filedAt is required'),
    ('VALIDATION', 'netTax',                  'netTax is required'),
    ('VALIDATION', 'tin',                     'tin must look like P0000000001'),
    ('VALIDATION', 'ackNo',                   'ackNo must be a 12 digit acknowledgement number'),
    ('VALIDATION', 'taxpayerCategory',        'taxpayerCategory must be one of [Individual, Non-Individual]'),
    ('VALIDATION', 'rtnType',                 'rtnType must be one of [Original, Amended]'),
    ('VALIDATION', 'fileFrom',                'fileFrom must be one of [INET, OFFC, MOBL]'),
    ('VALIDATION', 'rtnStatus',               'rtnStatus must be one of [APRV, PEND, REJT]'),
    ('VALIDATION', 'isNilReturn',             'isNilReturn must be one of [true, false]'),
    ('VALIDATION', 'taxableOutputsValue',     'taxableOutputsValue cannot be negative'),
    ('VALIDATION', 'vatLevies',               'vatLevies cannot be negative'),
    ('VALIDATION', 'totalInputValue',         'totalInputValue cannot be negative'),
    ('VALIDATION', 'inputTaxOnPurchases',     'inputTaxOnPurchases cannot be negative'),
    ('VALIDATION', 'penaltyAmount',           'penaltyAmount cannot be negative'),
    ('VALIDATION', 'interest',                'interest cannot be negative'),
    ('VALIDATION', 'rtnToDate',               'rtnToDate cannot be in the future'),
    ('VALIDATION', NULL,                      'month must be between 1 and 12'),
    ('VALIDATION', NULL,                      'rtnToDate cannot be before rtnFromDate'),
    ('VALIDATION', NULL,                      'month and year must match rtnFromDate'),
    ('VALIDATION', NULL,                      'a nil return cannot have taxable outputs'),
    ('VALIDATION', NULL,                      'netOutputTax must equal totalOutputTax - deductibleInputTax'),
-- vat_return_coll: ORPHAN (reference must exist) + DUPLICATE
    ('ORPHAN',     'formId',                  'formId not found in dim_tax_form'),
    ('DUPLICATE',  'ackNo',                   'another record with the same ackNo was kept (latest updatedAt wins)')
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
