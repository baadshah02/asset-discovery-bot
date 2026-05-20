# Implementation Plan: Universe Expansion

## Overview

Expand the Asset Discovery Bot from a single S&P 500 Wikipedia source to a multi-source universe supporting Russell 1000 and Russell 3000 indices via iShares ETF holdings CSV files. Implementation is ordered by dependency: config and migration first, then universe module changes, then downstream module updates, then tests. All code is Python 3.11, using the existing project conventions (Pydantic frozen models, SQLAlchemy Core 2.0, dataclasses, pytest + hypothesis).

## Tasks

- [x] 1. Update config models for multi-source universe
  - [x] 1.1 Add `UniverseSourceConfig` model and update `UniverseConfig` in `bot/config.py`
    - Add `UniverseSourceConfig` Pydantic model with fields: `name`, `kind` (Literal), `url`, `enabled`, `min_count`, `max_count`
    - Update `UniverseConfig` to add `sources`, `min_composite_count`, `max_composite_count`, `max_scan_minutes` fields
    - Add `_unique_source_names` field validator to reject duplicate source names
    - Add `effective_sources()` method that falls back to a single Wikipedia source when `sources` is None
    - Preserve existing `source_url`, `min_constituent_count`, `max_constituent_count` fields for backward compatibility
    - Update `AppConfig` and `__all__` exports
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 8.1, 8.4, 8.5_
  - [ ]* 1.2 Write property tests for config validation (Properties 1 and 2)
    - **Property 1: Duplicate source names rejected**
    - **Validates: Requirements 1.3**
    - **Property 2: Unrecognized source kind rejected**
    - **Validates: Requirements 1.4**
  - [ ]* 1.3 Write unit tests for `UniverseConfig.effective_sources()` backward-compatible fallback
    - Test that a v1 config with only `source_url` produces a single `UniverseSourceConfig` with `kind="wikipedia_table"`
    - Test that `sources` field takes precedence over `source_url`
    - Test that disabled sources are excluded from `effective_sources()`
    - _Requirements: 1.2, 8.1, 8.5_

- [x] 2. Add database migration for `index_sources` column
  - [x] 2.1 Create `bot/migrations/002_universe_expansion.sql`
    - Add `index_sources TEXT[] NOT NULL DEFAULT '{}'::TEXT[]'` column to `asset_universe` using `ADD COLUMN IF NOT EXISTS`
    - Ensure idempotency so re-runs are no-ops
    - _Requirements: 4.1, 4.4, 4.5, 8.4_
  - [x] 2.2 Update `asset_universe` Table declaration in `bot/repo.py`
    - Add `Column("index_sources", ARRAY(String), nullable=False, server_default=text("'{}'::TEXT[]"))` to the `asset_universe` Table
    - Import `ARRAY` from `sqlalchemy.dialects.postgresql` and `text` from `sqlalchemy`
    - _Requirements: 4.1, 4.2_
  - [x] 2.3 Update `upsert_universe` in `bot/repo.py` to accept and write `source_attribution`
    - Add optional `source_attribution: dict[str, list[str]] | None = None` parameter
    - When `source_attribution` is provided, set each ticker's `index_sources` to its sorted source list during upsert
    - When `source_attribution` is None (v1 compat), do not modify `index_sources`
    - _Requirements: 4.2, 3.7, 8.2_

- [x] 3. Checkpoint — Ensure config and migration changes are consistent
  - Ensure all tests pass, ask the user if questions arise.

- [x] 4. Implement ETF Holdings CSV parser in `bot/universe.py`
  - [x] 4.1 Implement `parse_etf_holdings_csv()` function
    - Locate header row by searching for a row containing both "Ticker"/"Symbol" and "Name" columns
    - Map column indices for ticker, name, sector, and asset_class
    - Extract `(ticker, company_name, sector)` triples from data rows
    - Apply exclusion rules: empty ticker, dash-only, "CASH" prefix, "_USD" suffix, non-Equity asset class
    - Normalize tickers: strip, upper-case, space-to-dot (e.g., `BRK B` → `BRK.B`)
    - Return sorted, deduplicated list of triples
    - Raise `ParseError` on missing header row or zero equity tickers
    - _Requirements: 2.2, 2.3, 2.4, 2.5, 2.6, 2.7_
  - [x] 4.2 Implement `fetch_etf_holdings()` function
    - Download CSV from configured URL via HTTP GET with browser-like User-Agent and configurable timeout (default 15s)
    - Call `parse_etf_holdings_csv()` on the response text
    - _Requirements: 2.1_
  - [ ]* 4.3 Write property tests for ETF CSV parser (Properties 3, 4, and 5)
    - **Property 3: ETF CSV parser produces correctly normalized, sorted, deduplicated triples**
    - **Validates: Requirements 2.2, 2.3, 2.5**
    - **Property 4: ETF CSV parser excludes non-equity rows**
    - **Validates: Requirements 2.4**
    - **Property 5: ETF CSV parser round-trip stability**
    - **Validates: Requirements 2.7, 10.7**
  - [ ]* 4.4 Write unit tests for `parse_etf_holdings_csv()` edge cases
    - Test with representative iShares CSV fixture (preamble rows + data)
    - Test empty CSV, missing columns, all-cash holdings, malformed rows
    - _Requirements: 2.2, 2.4, 2.6_

- [x] 5. Implement multi-source universe sync in `bot/universe.py`
  - [x] 5.1 Add `SourceResult` and extended `UniverseDiff` dataclasses
    - Add `SourceResult` frozen dataclass with `name`, `success`, `tickers`, `error` fields
    - Extend `UniverseDiff` with `source_failures`, `source_attribution`, `composite_size`, `sources_enabled`, `sources_succeeded` fields
    - Preserve backward compatibility of existing `UniverseDiff` (added, removed, as_of)
    - _Requirements: 3.1, 3.4, 3.8, 9.1, 9.2, 9.3_
  - [x] 5.2 Implement `fetch_source()` dispatcher
    - Dispatch by `source.kind`: `wikipedia_table` → existing `fetch_current_constituents`, `etf_holdings_csv` → `fetch_etf_holdings`
    - Return `SourceResult` (never raises); catch all exceptions and record as failure
    - _Requirements: 3.1, 3.4_
  - [x] 5.3 Refactor `sync_universe()` for multi-source orchestration
    - Accept `UniverseConfig` (which now has `effective_sources()`)
    - Iterate enabled sources, fetch each independently via `fetch_source()`
    - Validate per-source count against `[min_count, max_count]`; mark as failed if outside bounds
    - Raise `UniverseSyncError` if all sources fail (no DB mutation)
    - Compute Composite_Universe as set-union of successful sources
    - Validate composite size against `[min_composite_count, max_composite_count]`; raise before DB mutation if outside bounds
    - Compute per-ticker source attribution
    - Diff against previous active universe
    - Call `repo.upsert_universe()` with `source_attribution`
    - Return extended `UniverseDiff` with source failures, attribution, and counts
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8, 10.1, 10.2, 10.4, 10.5, 10.8, 10.9_
  - [ ]* 5.4 Write property tests for multi-source sync (Properties 6, 7, 8, and 9)
    - **Property 6: Composite universe equals set-union of successful sources**
    - **Validates: Requirements 3.2, 10.1, 3.4, 10.8**
    - **Property 7: Source attribution completeness and freshness**
    - **Validates: Requirements 3.3, 3.7, 4.2, 10.2, 10.9**
    - **Property 8: Composite bounds enforcement**
    - **Validates: Requirements 3.6, 10.5**
    - **Property 9: Diff correctness**
    - **Validates: Requirements 3.8**
  - [ ]* 5.5 Write unit tests for `sync_universe()` scenarios
    - Test partial failure (one source fails, others succeed)
    - Test total failure (all sources fail → `UniverseSyncError`, no DB mutation)
    - Test composite bounds violation
    - Test v1 single-source fallback path
    - _Requirements: 3.4, 3.5, 3.6, 8.2_

- [x] 6. Checkpoint — Ensure universe module changes work end-to-end
  - Ensure all tests pass, ask the user if questions arise.

- [x] 7. Implement EDGAR rate limiter in `bot/fundamentals.py`
  - [x] 7.1 Add `EdgarRateLimiter` class
    - Implement token-bucket rate limiter with configurable `max_per_second` (default 10.0)
    - Use `threading.Lock` for thread safety and `time.monotonic()` for timing
    - Track `consecutive_waits` for monitoring
    - _Requirements: 7.5, 7.6_
  - [x] 7.2 Integrate rate limiter into `FundamentalsClient.fetch()`
    - Instantiate `EdgarRateLimiter` on `FundamentalsClient.__init__`
    - Call `rate_limiter.acquire()` before each EDGAR HTTP call (`_edgar_company_facts`, `_load_ticker_cik_map`)
    - Log WARN if rate limiter is engaged for more than 30 consecutive seconds
    - _Requirements: 7.5, 7.6_

- [x] 8. Add progress logging to Price_Service for large universes
  - [x] 8.1 Update `download_price_history()` in `bot/prices.py` to log batch progress
    - When universe size exceeds 1000 tickers, log INFO every `batch_size` tickers processed with count completed and count remaining
    - _Requirements: 5.2_

- [x] 9. Update Notifier for multi-source watchdog alerts
  - [x] 9.1 Update `_build_watchdog_embed()` in `bot/notify.py` for extended `UniverseDiff`
    - Add summary field showing composite size, sources succeeded/enabled
    - Group added tickers by primary source in the embed
    - Add "Source Failures" section when any source failed
    - Truncate all field values to stay within Discord's 1024-char field limit
    - Ensure total embed description does not exceed 4096 characters
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5_
  - [x] 9.2 Update `send_watchdog()` to handle extended `UniverseDiff`
    - Emit watchdog alert when diff is non-empty OR source failures exist
    - _Requirements: 9.4, 3.8_
  - [ ]* 9.3 Write property test for watchdog embed character limits (Property 11)
    - **Property 11: Watchdog embed respects Discord character limits**
    - **Validates: Requirements 9.5**
  - [ ]* 9.4 Write unit tests for watchdog embed construction
    - Test with large diffs (many added/removed tickers)
    - Test with source failures
    - Test truncation behavior
    - _Requirements: 9.1, 9.2, 9.5_

- [x] 10. Update Orchestrator (`bot/run.py`) for multi-source universe
  - [x] 10.1 Update `main()` to pass `cfg.universe` to `sync_universe()` and handle extended `UniverseDiff`
    - Update watchdog alert condition to also trigger on `diff.source_failures`
    - _Requirements: 3.8, 9.4_
  - [x] 10.2 Add graceful degradation logging after enrichment phase
    - Count tickers excluded from L3/L4 due to missing fundamentals (NULL pe_ratio, pe_5y_avg, or fcf_yield)
    - Log INFO summary: "Graceful degradation: N/M L2 survivors excluded (missing fundamentals)"
    - _Requirements: 6.4_
  - [x] 10.3 Add scan-time monitoring
    - Record scan start time at beginning of `main()`
    - After each major phase, check elapsed time against `cfg.universe.max_scan_minutes`
    - Log WARN if threshold exceeded, including elapsed time and active phase
    - _Requirements: 7.7_

- [x] 11. Checkpoint — Ensure all module changes integrate correctly
  - Ensure all tests pass, ask the user if questions arise.

- [x] 12. Update config example and write remaining tests
  - [x] 12.1 Update `config/config.example.yaml` with multi-source configuration
    - Add `universe.sources` list with sp500_wikipedia, russell1000_iwb, and russell2000_iwm (disabled) examples
    - Add `universe.min_composite_count`, `universe.max_composite_count`, `universe.max_scan_minutes`
    - Keep existing fields for backward compatibility reference
    - _Requirements: 1.1, 1.6_
  - [ ]* 12.2 Write property test for sequential filter monotonicity (Property 10)
    - **Property 10: Sequential filter monotonicity (extended)**
    - **Validates: Requirements 10.3**
  - [ ]* 12.3 Write property test for backward compatibility equivalence (Property 12)
    - **Property 12: Backward compatibility equivalence**
    - **Validates: Requirements 8.2, 10.10**
  - [ ]* 12.4 Write integration tests with testcontainers
    - Spin up `postgres:15-alpine` via `testcontainers-python`
    - Apply both migrations (`001_init.sql`, `002_universe_expansion.sql`)
    - Exercise `upsert_universe` with `source_attribution`, verify `index_sources` column values
    - Test idempotent migration re-run
    - _Requirements: 4.1, 4.2, 4.4, 4.5_

- [x] 13. Final checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation after each major component
- Property tests validate universal correctness properties from the design document
- Unit tests validate specific examples and edge cases
- The implementation language is Python 3.11, matching the existing codebase
- PBT library: `hypothesis` (integrates with pytest)
