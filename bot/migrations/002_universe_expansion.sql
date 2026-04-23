-- Universe Expansion — add index_sources column to asset_universe
--
-- Applied by bot/migrations/run_migrations.py in lexicographic order
-- after 001_init.sql. Uses ADD COLUMN IF NOT EXISTS so re-runs are
-- idempotent no-ops (Req 4.4).
--
-- The server default '{}'::TEXT[] populates existing rows without a
-- full table rewrite (Req 4.5). No downtime required.
--
-- Requirements traceability:
--   4.1 — index_sources TEXT[] column tracks contributing source names.
--   4.4 — Idempotent: ADD COLUMN IF NOT EXISTS is a no-op on re-run.
--   4.5 — No downtime or data loss; server default avoids full rewrite.
--   8.4 — Existing rows get empty array default, preserving v1 validity.

ALTER TABLE asset_universe
    ADD COLUMN IF NOT EXISTS index_sources TEXT[] NOT NULL DEFAULT '{}'::TEXT[];
