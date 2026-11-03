-- =====================================================================
-- V2: Date dimension (a "conformed" dimension shared by all star schemas)
-- =====================================================================
CREATE TABLE silver.dim_date (
    date_key      INT         PRIMARY KEY,   -- yyyyMMdd, e.g. 20260315
    full_date     DATE        NOT NULL UNIQUE,
    day_of_month  INT         NOT NULL,
    month         INT         NOT NULL,
    month_name    VARCHAR(20) NOT NULL,
    quarter       INT         NOT NULL,
    year          INT         NOT NULL,
    day_of_week   INT         NOT NULL,      -- 1 = Sunday ... 7 = Saturday
    day_name      VARCHAR(20) NOT NULL,
    is_weekend    BOOLEAN     NOT NULL,
    job_run_id    BIGINT,
    loaded_at     TIMESTAMP
);
