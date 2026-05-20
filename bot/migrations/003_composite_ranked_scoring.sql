-- Composite Ranked Scoring — extend fundamentals_cache and daily_scans
--
-- Applied by bot/migrations/run_migrations.py in lexicographic order
-- after 002_universe_expansion.sql. Uses ADD COLUMN IF NOT EXISTS with
-- server defaults so existing rows populate without a full table rewrite.
-- Idempotent: re-running is a no-op (Req 8.4).
--
-- Requirements traceability:
--   8.1 — Extended fundamentals columns on fundamentals_cache.
--   8.2 — Composite scoring columns on daily_scans.
--   8.3 — ADD COLUMN IF NOT EXISTS with server defaults (no rewrite).
--   8.4 — Idempotent (IF NOT EXISTS).
--   8.5 — Existing v1 columns unchanged.

-- ============================================================
-- fundamentals_cache: extended fields for composite scoring
-- ============================================================

ALTER TABLE fundamentals_cache
    ADD COLUMN IF NOT EXISTS total_debt NUMERIC(16, 2);

ALTER TABLE fundamentals_cache
    ADD COLUMN IF NOT EXISTS cash_and_equivalents NUMERIC(16, 2);

ALTER TABLE fundamentals_cache
    ADD COLUMN IF NOT EXISTS ebit NUMERIC(16, 2);

ALTER TABLE fundamentals_cache
    ADD COLUMN IF NOT EXISTS revenue_ttm NUMERIC(16, 2);

ALTER TABLE fundamentals_cache
    ADD COLUMN IF NOT EXISTS book_value_of_equity NUMERIC(16, 2);

ALTER TABLE fundamentals_cache
    ADD COLUMN IF NOT EXISTS dividends_paid_ttm NUMERIC(16, 2);

ALTER TABLE fundamentals_cache
    ADD COLUMN IF NOT EXISTS share_buybacks_ttm NUMERIC(16, 2);

ALTER TABLE fundamentals_cache
    ADD COLUMN IF NOT EXISTS cogs_ttm NUMERIC(16, 2);

ALTER TABLE fundamentals_cache
    ADD COLUMN IF NOT EXISTS total_assets NUMERIC(16, 2);

ALTER TABLE fundamentals_cache
    ADD COLUMN IF NOT EXISTS annual_eps_5y NUMERIC(12, 4)[];

ALTER TABLE fundamentals_cache
    ADD COLUMN IF NOT EXISTS net_income_ttm NUMERIC(16, 2);

ALTER TABLE fundamentals_cache
    ADD COLUMN IF NOT EXISTS operating_cash_flow_ttm NUMERIC(16, 2);

ALTER TABLE fundamentals_cache
    ADD COLUMN IF NOT EXISTS fundamentals_schema_version SMALLINT NOT NULL DEFAULT 1;

-- ============================================================
-- daily_scans: composite scoring output columns
-- ============================================================

ALTER TABLE daily_scans
    ADD COLUMN IF NOT EXISTS value_composite NUMERIC(10, 6);

ALTER TABLE daily_scans
    ADD COLUMN IF NOT EXISTS quality_composite NUMERIC(10, 6);

ALTER TABLE daily_scans
    ADD COLUMN IF NOT EXISTS reversal_signal NUMERIC(10, 6);

ALTER TABLE daily_scans
    ADD COLUMN IF NOT EXISTS composite_score NUMERIC(10, 6);

ALTER TABLE daily_scans
    ADD COLUMN IF NOT EXISTS composite_rank INTEGER;

ALTER TABLE daily_scans
    ADD COLUMN IF NOT EXISTS sector_at_rank VARCHAR(64);

ALTER TABLE daily_scans
    ADD COLUMN IF NOT EXISTS pipeline_mode VARCHAR(32);
