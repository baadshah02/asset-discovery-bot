# Implementation Plan: Composite Ranked Scoring

## Overview

Transform the Asset Discovery Bot from a binary pass/fail 4-layer tollbooth into a ranked composite scorer. The implementation proceeds incrementally: database migration first, then config models, then extended fundamentals retrieval, then the scoring module, then the ranker module, then dual-pipeline orchestration, and finally the extended Discord embed format. Each step builds on the previous and ends with wiring into the existing system.

## Tasks

- [x] 1. Database migration for composite ranked scoring
  - [x] 1.1 Create `bot/migrations/003_composite_ranked_scoring.sql`
    - Add Extended_Fundamentals columns to `fundamentals_cache`: `total_debt`, `cash_and_equivalents`, `ebit`, `revenue_ttm`, `book_value_of_equity`, `dividends_paid_ttm`, `share_buybacks_ttm`, `cogs_ttm`, `total_assets`, `annual_eps_5y` (NUMERIC[]), `net_income_ttm`, `operating_cash_flow_ttm`, `fundamentals_schema_version` (SMALLINT NOT NULL DEFAULT 1)
    - Add composite-scoring columns to `daily_scans`: `value_composite`, `quality_composite`, `reversal_signal`, `composite_score`, `composite_rank`, `sector_at_rank`, `pipeline_mode`
    - Use `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` with server defaults for idempotency
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5_

  - [x] 1.2 Extend SQLAlchemy table declarations in `bot/repo.py`
    - Add the new columns to the `fundamentals_cache` Table declaration
    - Add the new columns to the `daily_scans` Table declaration
    - _Requirements: 8.1, 8.2_

  - [x] 1.3 Extend `Repository` with composite-aware insert and load methods
    - Update `insert_scan` to accept and persist composite-scoring columns when present
    - Add `load_extended_fundamentals` method that returns the full Extended_Fundamentals record
    - Add `upsert_extended_fundamentals` method that writes all extended fields including `fundamentals_schema_version`
    - Preserve backward compatibility: v1 `insert_scan` calls with no composite columns still work
    - _Requirements: 8.6, 8.7, 8.8, 6.3_

  - [ ]* 1.4 Write unit tests for migration idempotency and repository extensions
    - Test that running migration twice is a no-op
    - Test that `insert_scan` with composite columns persists correctly
    - Test that `insert_scan` without composite columns leaves them null
    - Test that `load_extended_fundamentals` returns schema_version
    - _Requirements: 8.4, 8.5, 8.7_

- [x] 2. Extended configuration models
  - [x] 2.1 Add `PipelineMode`, `PipelineConfig`, `ScoringConfig`, `RankingConfig`, `ReversalConfig` to `bot/config.py`
    - Implement `PipelineMode` enum with `hard_threshold` and `composite_rank` values
    - Implement `PipelineConfig` with `mode` field defaulting to `hard_threshold`
    - Implement `ScoringConfig` with `value_weight`, `quality_weight`, `reversal_weight` (defaults 1.0, 1.0, 0.5) each in [0.0, 10.0], with validator rejecting all-zero weights
    - Implement `RankingConfig` with `sector_neutral` (bool, default true), `top_n` (int, default 20, [1, 500]), `top_n_per_sector` (int, default 2, [1, 50]), `min_sector_size` (int, default 5, [1, 50])
    - Implement `ReversalConfig` with `enabled` (bool, default true), `lookback_days` (int, default 21, [5, 252])
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5_

  - [x] 2.2 Extend `AppConfig` with new sections and update `diff_from_defaults`
    - Add `pipeline`, `scoring`, `ranking`, `reversal` fields to `AppConfig`
    - Ensure `diff_from_defaults()` includes the new sections
    - Ensure `model_dump(mode="json")` serializes the new sections for `config_snapshot`
    - Verify backward compatibility: a config with only v1 layer sections loads without error
    - _Requirements: 9.6, 9.7, 9.8, 9.9_

  - [ ]* 2.3 Write unit tests for config validation
    - Test that all-zero weights are rejected with ValidationError
    - Test that `pipeline.mode = "invalid"` is rejected
    - Test that `top_n = 0` is rejected
    - Test that a v1-only config loads successfully with composite defaults
    - Test env-var override for new fields (e.g., `ADB_SCORING__VALUE_WEIGHT=2.0`)
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5, 9.7_

- [x] 3. Checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 4. Extended fundamentals retrieval from EDGAR XBRL
  - [x] 4.1 Define `ExtendedFundamentals` dataclass in `bot/fundamentals.py`
    - Create frozen dataclass with all v1 fields plus: `total_debt`, `cash_and_equivalents`, `ebit`, `revenue_ttm`, `book_value_of_equity`, `dividends_paid_ttm`, `share_buybacks_ttm`, `cogs_ttm`, `total_assets`, `annual_eps_5y`, `net_income_ttm`, `operating_cash_flow_ttm`, `market_cap`, `schema_version`
    - _Requirements: 6.1_

  - [x] 4.2 Add XBRL concept mappings and extraction helpers for new fields
    - Add concept candidate tuples for each new field (total_debt, cash, ebit, revenue, book_value, dividends, buybacks, cogs, total_assets, net_income)
    - Implement fallback order as documented in the design (e.g., EBIT from OperatingIncomeLoss → computed from NetIncome + Interest + Tax)
    - Reuse existing `_facts_for_concept`, `_ttm_sum`, `_latest_quarterly` helpers
    - _Requirements: 6.2, 6.6_

  - [x] 4.3 Extend `FundamentalsClient.fetch()` to populate extended fields
    - Add extraction logic for each new field using the concept mappings
    - Return extended dict with all new keys (any may be None)
    - Preserve existing v1 field extraction unchanged
    - _Requirements: 6.1, 6.2, 6.5, 6.6, 6.7_

  - [x] 4.4 Implement `get_extended_fundamentals` function
    - Cache-gated fetch that returns `ExtendedFundamentals`
    - When `pipeline_mode = composite_rank` and cached row has `schema_version < 2`, treat as stale and refetch
    - When `pipeline_mode = hard_threshold`, serve v1 `Fundamentals` as before (no extended fetch)
    - Upsert with `fundamentals_schema_version = 2` on fresh fetch
    - Preserve EDGAR rate limit via existing `EdgarRateLimiter`
    - _Requirements: 6.3, 6.4, 6.7, 6.8, 8.6, 10.2_

  - [ ]* 4.5 Write unit tests for extended fundamentals
    - Test XBRL concept fallback order (primary missing, fallback used)
    - Test that null individual concept produces null field without failing entire record
    - Test schema_version gating: v1 row treated as stale in composite_rank mode
    - Test that v1 `get_fundamentals` path is unchanged when pipeline_mode = hard_threshold
    - _Requirements: 6.2, 6.5, 6.6, 8.6, 10.2_

- [x] 5. Composite Scorer module (`bot/scoring.py`)
  - [x] 5.1 Create `bot/scoring.py` with `CompositeResult` dataclass and `CompositeScorer` class skeleton
    - Define `CompositeResult` frozen dataclass with all fields from design
    - Define `CompositeScorer.__init__` accepting `ScoringConfig` and `RankingConfig`
    - _Requirements: 1.1, 2.1, 3.1_

  - [x] 5.2 Implement `compute_value_metrics` method
    - Compute EV = market_cap + total_debt - cash_and_equivalents
    - Compute EV/EBIT (None if ebit <= 0 or EV is None)
    - Compute EV/Sales (None if revenue_ttm <= 0 or EV is None)
    - Compute B/M = book_value_of_equity / market_cap
    - Compute Shareholder_Yield = (|dividends| + |buybacks|) / market_cap
    - Return dict with keys; any value may be None
    - _Requirements: 1.1, 1.3, 1.4, 1.5_

  - [x] 5.3 Implement `compute_quality_metrics` method
    - Compute GP/A = (revenue_ttm - cogs_ttm) / total_assets
    - Compute Earnings_Stability = -stddev(annual_eps_5y) with at least 3 values
    - Compute Low_Accruals = -(net_income_ttm - operating_cash_flow_ttm) / total_assets
    - Return dict with keys; any value may be None
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.8_

  - [x] 5.4 Implement `compute_reversal_signal` method
    - Compute raw trailing return: (close_today - close_N_days_ago) / close_N_days_ago
    - Return None if fewer than lookback_days + 1 observations
    - _Requirements: 3.1, 3.2, 3.6_

  - [x] 5.5 Implement cross-sectional z-score computation (sector-neutral and fallback)
    - Implement `_compute_z_scores` helper accepting values, sector_neutral flag, sectors dict, min_sector_size
    - Partition by sector when sector_neutral=True
    - Fall back to cross-sectional for undersized sectors (< min_sector_size), log WARN
    - Handle zero stddev by setting sigma=1.0
    - _Requirements: 1.2, 1.6, 1.7, 2.5, 2.7, 3.3, 3.4, 4.2_

  - [x] 5.6 Implement `compute_composite_scores` method
    - Compute raw metrics for all tickers
    - Filter to eligible tickers per composite (value, quality, reversal)
    - Log exclusions at DEBUG level
    - Z-score each metric (invert EV/EBIT and EV/Sales)
    - Average z-scores into Value_Composite and Quality_Composite
    - Compute Reversal_Signal z-score (negate raw return before z-scoring)
    - When reversal disabled, set reversal_signal = 0.0 for all
    - Compute Composite_Score = value_weight * VC + quality_weight * QC + reversal_weight * RS
    - Return list of CompositeResult for tickers in intersection of all three eligibility sets
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 2.1, 2.5, 2.6, 2.7, 2.8, 3.1, 3.3, 3.4, 3.5, 3.7, 4.2_

  - [ ]* 5.7 Write property test: Composite Score Formula (Property 1)
    - **Property 1: Composite Score Formula**
    - **Validates: Requirements 11.2, 1.4, 2.6**

  - [ ]* 5.8 Write property test: Sector-Neutrality Closure (Property 2)
    - **Property 2: Sector-Neutrality Closure**
    - **Validates: Requirements 1.6, 2.7, 3.4, 4.2, 4.3, 11.4**

  - [ ]* 5.9 Write property test: Null Metric Exclusion (Property 7)
    - **Property 7: Null Metric Exclusion**
    - **Validates: Requirements 1.5, 2.8**

  - [ ]* 5.10 Write property test: Min Sector Size Fallback (Property 8)
    - **Property 8: Min Sector Size Fallback**
    - **Validates: Requirements 1.7**

  - [ ]* 5.11 Write property test: Z-Score Statistical Properties (Property 10)
    - **Property 10: Z-Score Statistical Properties**
    - **Validates: Requirements 1.2, 2.5, 3.3**

  - [ ]* 5.12 Write property test: Metric Computation Correctness (Property 11)
    - **Property 11: Metric Computation Correctness**
    - **Validates: Requirements 2.2, 2.3, 2.4, 3.2, 1.3**

  - [ ]* 5.13 Write property test: Scoring Determinism (Property 6)
    - **Property 6: Scoring Determinism**
    - **Validates: Requirements 11.3**

- [x] 6. Ranker module (`bot/ranker.py`)
  - [x] 6.1 Create `bot/ranker.py` with `RankedCandidate` dataclass and `Ranker` class
    - Define `RankedCandidate` frozen dataclass with all fields from design
    - Define `Ranker.__init__` accepting `RankingConfig`
    - _Requirements: 4.1, 5.1_

  - [x] 6.2 Implement `Ranker.rank` method
    - When `sector_neutral=True`: partition by sector, sort each by (-composite_score, ticker_asc), assign contiguous ranks {1..k}
    - When `sector_neutral=False`: sort all by (-composite_score, ticker_asc), assign contiguous ranks {1..k}
    - Deterministic tie-breaking: lexicographic ascending on ticker
    - _Requirements: 4.1, 4.2, 4.3, 4.5, 4.6, 4.7_

  - [x] 6.3 Implement `Ranker.select_top_n` method
    - When `sector_neutral=True`: select top `top_n_per_sector` from each sector
    - When `sector_neutral=False`: select top `top_n` overall
    - Order final output by composite_score descending, ticker ascending for ties
    - Handle case where fewer than N tickers are scored (emit all, no placeholders)
    - _Requirements: 5.1, 5.2, 5.3, 5.6_

  - [ ]* 6.4 Write property test: Rank Bijection Within Sector (Property 3)
    - **Property 3: Rank Bijection Within Sector**
    - **Validates: Requirements 4.7, 11.5**

  - [ ]* 6.5 Write property test: Deterministic Tie-Breaking (Property 4)
    - **Property 4: Deterministic Tie-Breaking**
    - **Validates: Requirements 4.6, 11.6**

  - [ ]* 6.6 Write property test: Top-N Output Bound (Property 5)
    - **Property 5: Top-N Output Bound**
    - **Validates: Requirements 5.1, 5.2, 11.7**

  - [ ]* 6.7 Write property test: Scored Tickers Subset of Universe (Property 9)
    - **Property 9: Scored Tickers Subset of Universe**
    - **Validates: Requirements 11.1**

- [x] 7. Checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 8. Dual-pipeline dispatch in `bot/run.py`
  - [x] 8.1 Refactor `main()` to dispatch based on `cfg.pipeline.mode`
    - Extract existing v1 pipeline logic into `_run_hard_threshold_pipeline` helper
    - Add `_run_composite_rank_pipeline` function skeleton
    - Dispatch after universe sync based on `cfg.pipeline.mode`
    - Log INFO line with active pipeline mode, weights, and ranking options at startup
    - _Requirements: 10.1, 10.3, 10.4, 9.8_

  - [x] 8.2 Implement `_run_composite_rank_pipeline`
    - Phase 1: Download price history (reuse existing)
    - Phase 2: Compute reversal signals from price frames using `CompositeScorer.compute_reversal_signal`
    - Phase 3: Fetch Extended_Fundamentals for all tickers with reversal data (cache-gated)
    - Phase 4: Load sector assignments from `asset_universe` table
    - Phase 5: Call `CompositeScorer.compute_composite_scores`
    - Phase 6: Call `Ranker.rank` then `Ranker.select_top_n`
    - Phase 7: Persist + alert loop (insert-before-alert contract preserved)
    - Populate legacy NOT NULL columns with fallback values (0 for rsi_today/rsi_yesterday/pct_above_low)
    - Populate new composite columns on each `daily_scans` row
    - _Requirements: 10.4, 5.1, 5.2, 5.3, 5.4, 5.5, 8.7, 8.8_

  - [ ]* 8.3 Write integration test for dual-pipeline dispatch
    - Test that `hard_threshold` mode executes v1 pipeline unchanged
    - Test that `composite_rank` mode executes new pipeline
    - Test that switching modes mid-day respects unique constraint
    - _Requirements: 10.1, 10.3, 10.5, 10.6_

- [x] 9. Extended Discord embed for ranked candidates
  - [x] 9.1 Add `_build_ranked_candidate_embed` to `bot/notify.py`
    - Render: Rank (#N in Sector), Composite Score, Value/Quality/Reversal breakdown, Close, Market Cap, Latest Catalyst
    - Footer: "Composite rank — for human review"
    - Colour: green (same as v1 high-conviction)
    - No trading language (buy, sell, hold, allocate, position, size)
    - _Requirements: 5.4, 5.8_

  - [x] 9.2 Add `send_ranked_candidate` public function to `bot/notify.py`
    - Accept `RankedCandidate`, webhook_url, and `NotificationConfig`
    - Reuse existing `_post_with_retry` for retry semantics
    - _Requirements: 5.4, 5.8_

  - [x] 9.3 Add per-pipeline webhook URL support
    - Add `composite_webhook_secret` field to `NotificationConfig` (default: empty string, meaning use the same webhook as v1)
    - Add `discord_composite_webhook_url` to `Secrets` model as an optional field (default empty string)
    - Add `_SECRET_FILENAMES` entry: `"discord_composite_webhook_url": "discord_composite_webhook_url"`
    - In `_run_composite_rank_pipeline`, resolve the webhook URL: if `secrets.discord_composite_webhook_url` is non-empty, use it; otherwise fall back to `secrets.discord_webhook_url`
    - Add `secrets/discord_composite_webhook_url.txt` to `docker-compose.yml` secrets section
    - Document in config.example.yaml that composite-rank alerts can go to a separate Discord channel
    - _Requirements: 5.4_

  - [ ]* 9.4 Write property test: No Trading Language in Embeds (Property 12)
    - **Property 12: No Trading Language in Embeds**
    - **Validates: Requirements 5.8, 11.10**

  - [ ]* 9.5 Write unit tests for ranked embed rendering
    - Test that all required fields are present in embed
    - Test that embed respects Discord field value limits (1024 chars)
    - Test that footer contains "for human review"
    - _Requirements: 5.4, 5.8_

- [x] 10. Final checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties from the design document
- Unit tests validate specific examples and edge cases
- The implementation language is Python 3.11, matching the existing codebase
- All new modules (`bot/scoring.py`, `bot/ranker.py`) follow the project's existing patterns: frozen dataclasses, pure functions, SQLAlchemy Core 2.0, Pydantic frozen models
