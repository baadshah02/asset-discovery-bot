-- Asset Discovery Bot — initial schema
--
-- Applied by bot/migrations/run_migrations.py on first run. The script
-- walks every .sql file in this directory in lexicographic order, so the
-- `001_` prefix establishes ordering. Every statement uses IF NOT EXISTS
-- semantics so re-runs are no-ops (Req 7.1).
--
-- Requirements traceability:
--   - 7.1  Three tables persist universe, fundamentals cache, and alert
--          history. All creation is idempotent.
--   - 7.2  `INSERT ... ON CONFLICT` upsert semantics are supported by
--          the primary keys declared below.
--   - 7.3  `daily_scans` enforces UNIQUE (ticker, scan_date) via the
--          `uq_scan_per_day` constraint.
--   - 6.7  `daily_scans.config_snapshot` is a NOT NULL JSONB column,
--          storing the exact AppConfig that produced each alert row.

-- Optional extension; reserved for future case-insensitive text columns.
CREATE EXTENSION IF NOT EXISTS citext;

-- ---------------------------------------------------------------------
-- 1. asset_universe — S&P 500 membership, source of truth for scans.
--    `removed_on IS NULL` is the "currently active" predicate; the
--    partial index below keeps active-universe lookups cheap.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS asset_universe (
    ticker          VARCHAR(10)  PRIMARY KEY,
    company_name    VARCHAR(255) NOT NULL,
    sector          VARCHAR(64),
    added_on        DATE         NOT NULL DEFAULT CURRENT_DATE,
    removed_on      DATE,
    last_seen_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_asset_universe_active
    ON asset_universe (ticker)
    WHERE removed_on IS NULL;

-- ---------------------------------------------------------------------
-- 2. fundamentals_cache — gates FMP API calls (Req 3.1, 3.2, 9.5).
--    FK on ticker cascades updates but not deletes; a delisting updates
--    asset_universe.removed_on rather than deleting the row, so cached
--    fundamentals remain queryable for historical reproducibility.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fundamentals_cache (
    ticker          VARCHAR(10)  PRIMARY KEY
        REFERENCES asset_universe (ticker) ON UPDATE CASCADE,
    pe_ratio        NUMERIC(12, 4),
    pe_5y_avg       NUMERIC(12, 4),
    fcf_yield       NUMERIC(10, 6),
    latest_headline VARCHAR(512),
    headline_url    VARCHAR(512),
    fetched_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_fundamentals_cache_fetched
    ON fundamentals_cache (fetched_at);

-- ---------------------------------------------------------------------
-- 3. daily_scans — one row per High_Conviction_Alert emitted.
--    The UNIQUE (ticker, scan_date) constraint enforces alert idempotency
--    under scheduler re-runs (Req 7.3, 7.4, 11.5). `config_snapshot` is
--    JSONB NOT NULL so every alert carries the exact AppConfig that
--    produced it (Req 6.7, 11.11).
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS daily_scans (
    id              BIGSERIAL     PRIMARY KEY,
    ticker          VARCHAR(10)   NOT NULL
        REFERENCES asset_universe (ticker) ON UPDATE CASCADE,
    scan_date       DATE          NOT NULL,
    close           NUMERIC(12, 4) NOT NULL,
    pct_above_low   NUMERIC(6, 4)  NOT NULL,
    rsi_today       NUMERIC(6, 3)  NOT NULL,
    rsi_yesterday   NUMERIC(6, 3)  NOT NULL,
    pe_ratio        NUMERIC(12, 4) NOT NULL,
    pe_5y_avg       NUMERIC(12, 4) NOT NULL,
    fcf_yield       NUMERIC(10, 6) NOT NULL,
    latest_headline VARCHAR(512),
    config_snapshot JSONB          NOT NULL,
    created_at      TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_scan_per_day UNIQUE (ticker, scan_date)
);

CREATE INDEX IF NOT EXISTS ix_daily_scans_date
    ON daily_scans (scan_date DESC);
