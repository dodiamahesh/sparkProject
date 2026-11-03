-- =====================================================================
-- V1: Schemas + ETL audit / control tables (shared by every pipeline)
-- =====================================================================
CREATE SCHEMA IF NOT EXISTS etl_audit;    -- job history, step history, rejected records
CREATE SCHEMA IF NOT EXISTS etl_staging;  -- temporary tables used for UPSERT
CREATE SCHEMA IF NOT EXISTS silver;       -- clean data in star-schema form (dimensions + facts)

-- ---------------------------------------------------------------------
-- One row per pipeline execution (job run)
-- ---------------------------------------------------------------------
CREATE TABLE etl_audit.etl_job_run (
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
    loaded_count     BIGINT       NOT NULL DEFAULT 0,    -- rows inserted/updated in silver tables
    resolved_count   BIGINT       NOT NULL DEFAULT 0,    -- old rejected records that are valid now
    error_message    TEXT,                               -- short failure reason
    error_detail     TEXT                                -- stack trace (truncated)
);
CREATE INDEX idx_job_run_pipeline ON etl_audit.etl_job_run (pipeline_name, start_time DESC);
CREATE INDEX idx_job_run_status   ON etl_audit.etl_job_run (status);

-- ---------------------------------------------------------------------
-- One row per step inside a job run (extract, validate, load table X ...)
-- Tells you exactly WHERE a job failed and how long each step took.
-- ---------------------------------------------------------------------
CREATE TABLE etl_audit.etl_job_step (
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
CREATE INDEX idx_job_step_run ON etl_audit.etl_job_step (job_run_id, step_order);

-- ---------------------------------------------------------------------
-- Records that could not be loaded (quarantine / error table).
-- One row per source document. When the document becomes valid in a later
-- run, its row is DELETED automatically.
-- ---------------------------------------------------------------------
CREATE TABLE etl_audit.etl_rejected_record (
    rejected_record_id  BIGSERIAL    PRIMARY KEY,
    pipeline_name       VARCHAR(100) NOT NULL,
    source_collection   VARCHAR(100) NOT NULL,
    source_id           VARCHAR(100) NOT NULL,           -- MongoDB _id
    business_key        VARCHAR(200),                    -- e.g. PAY-0007
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
CREATE INDEX idx_rejected_pipeline ON etl_audit.etl_rejected_record (pipeline_name, source_collection, error_type);
