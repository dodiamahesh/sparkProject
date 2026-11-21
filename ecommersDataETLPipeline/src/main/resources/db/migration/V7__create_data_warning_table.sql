-- =====================================================================
-- V7: Data-quality WARNINGS (records that were loaded, but something looked wrong)
--
--   warning_type = RULE          a warning rule failed on a loaded record     (business_key is set)
--                  SCHEMA_DRIFT  a new / missing field in a MongoDB collection (business_key is NULL)
--                  VOLUME        an INCREMENTAL run read too few / too many rows (business_key is NULL)
--
--   Unlike etl_rejected_record (one row per document, updated on retry) this is a HISTORY table:
--   every job run adds its own rows.
-- =====================================================================
CREATE TABLE etl_audit.etl_data_warning (
    data_warning_id    BIGSERIAL    PRIMARY KEY,
    job_run_id         BIGINT       NOT NULL REFERENCES etl_audit.etl_job_run (job_run_id),
    pipeline_name      VARCHAR(100) NOT NULL,
    source_collection  VARCHAR(100) NOT NULL,
    warning_type       VARCHAR(30)  NOT NULL,
    source_id          VARCHAR(100),             -- MongoDB _id (RULE only)
    business_key       VARCHAR(200),             -- e.g. PAY-0007 (RULE only)
    warning_message    TEXT         NOT NULL,
    created_at         TIMESTAMP    NOT NULL DEFAULT now()
);
CREATE INDEX idx_data_warning_run    ON etl_audit.etl_data_warning (job_run_id);
CREATE INDEX idx_data_warning_source ON etl_audit.etl_data_warning (pipeline_name, source_collection, warning_type);

-- Total warnings of a job run, next to the other counters
ALTER TABLE etl_audit.etl_job_run ADD COLUMN warning_count BIGINT NOT NULL DEFAULT 0;
