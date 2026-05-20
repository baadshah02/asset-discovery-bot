# Requirements Document

## Introduction

The Asset Discovery Bot v1 scans only the S&P 500 universe, sourced from a single Wikipedia table. Academic evidence (Fama-French 2012, Asness et al. 2019) shows that value and quality premia are strongest in small- and mid-cap equities — precisely the names absent from the S&P 500. This feature expands the Bot's investable universe to include the Russell 1000 and Russell 3000 indices, sourced from iShares ETF holdings CSV files, while preserving backward compatibility with the existing S&P 500 configuration.

The expansion is phased:

- **Phase 1** — Russell 1000 (adds ~500 mid-caps via iShares IWB holdings CSV).
- **Phase 2** — Russell 3000 (adds ~2000 small-caps via iShares IWM holdings CSV, union with IWB).
- **Phase 3 (out of scope)** — TSX / Toronto Stock Exchange. Requires SEDAR+, IFRS concept mapping, CAD/USD currency handling, and cross-exchange sector neutralization. Phase 3 is scoped separately and is **not** part of these requirements.

All Russell constituents are US-listed, USD-denominated equities purchasable on Wealthsimple Trade with USD conversion. No currency handling is needed for Phases 1–2. The Bot continues to run on a Synology DS220+ NAS with a 2 GB RAM host budget and PostgreSQL capped at 512 MB.

## Glossary

- **Bot**: The `asset-discovery-bot` Python container that executes a single daily scan and then exits.
- **Scan_Run**: One end-to-end invocation of the orchestrator (`bot.run`), initiated by the Synology Task Scheduler via `docker exec`.
- **Universe_Service** (`bot.universe`): The component that fetches constituent lists from one or more configured sources, diffs them against the local `asset_universe`, upserts the canonical membership, and emits universe-change signals.
- **Universe_Source**: A single configured data source that provides a list of index constituents. Each source has a `name`, a `kind` (e.g., `wikipedia_table` or `etf_holdings_csv`), and a `url`.
- **ETF_Holdings_Parser**: The sub-component of the Universe_Service responsible for downloading and parsing iShares ETF holdings CSV files into `(ticker, company_name, sector)` triples.
- **Composite_Universe**: The set-union of all tickers returned by all enabled Universe_Sources in a single Scan_Run, after deduplication.
- **Price_Service** (`bot.prices`): The component that batch-downloads OHLC from yfinance and computes per-ticker technical indicators.
- **Fundamentals_Service** (`bot.fundamentals`): The component that fetches fundamentals from SEC EDGAR XBRL and yfinance, gated by a local cache.
- **Filter_Pipeline** (`bot.filters`): The 4-layer sequential "tollbooth" that reduces the universe to high-conviction candidates.
- **Notifier** (`bot.notify`): The component that formats and POSTs rich embed alerts to Discord webhooks.
- **Config_Loader** (`bot.config`): The component that loads `config.yaml`, applies `ADB_*` environment-variable overrides, loads secrets, and validates via Pydantic.
- **Repository** (`bot.repo`): The SQLAlchemy Core 2.0 data access layer.
- **Index_Sources**: A TEXT ARRAY column on `asset_universe` that records which Universe_Sources contributed each ticker (e.g., `['sp500_wikipedia', 'russell1000_iwb']`).
- **EDGAR_Coverage**: Whether SEC EDGAR has CompanyFacts XBRL data for a given ticker's CIK. Excellent for S&P 500 and Russell 1000; incomplete for some Russell 2000 small-caps.
- **Graceful_Degradation_Ticker**: A ticker for which EDGAR returns no CompanyFacts or insufficient XBRL concepts. The ticker is excluded from Layers 3 and 4 (which require fundamentals) but remains in `asset_universe` for future scans when filings may appear.
- **Source_Health**: Per-source metadata (fetch status, ticker count, error message) recorded after each Scan_Run for watchdog alerting.

## Requirements

### Requirement 1: Multi-Index Universe Configuration

**User Story:** As an operator, I want to configure multiple index sources in `config.yaml` as an ordered list, so that I can expand the scan universe from S&P 500 alone to include Russell 1000 and Russell 3000 constituents without code changes.

#### Acceptance Criteria

1. THE Config_Loader SHALL accept a `universe.sources` field in `config.yaml` as an ordered list of Universe_Source objects, where each object contains a `name` (unique string identifier), a `kind` (one of `wikipedia_table` or `etf_holdings_csv`), a `url` (the fetch endpoint), and optional `enabled` (boolean, default `true`).
2. WHERE `universe.sources` is not present in `config.yaml`, THE Config_Loader SHALL fall back to a single default source with `name: "sp500_wikipedia"`, `kind: "wikipedia_table"`, and `url` equal to the current `universe.source_url` default, so that existing v1 configurations produce identical behavior without modification.
3. THE Config_Loader SHALL validate that every `name` within `universe.sources` is unique at startup, and SHALL reject the configuration with a non-zero exit code if duplicates are found.
4. THE Config_Loader SHALL validate that every `kind` value is one of the recognized source kinds (`wikipedia_table`, `etf_holdings_csv`) at startup, and SHALL reject the configuration with a non-zero exit code if an unrecognized kind is found.
5. THE Config_Loader SHALL accept a `universe.min_composite_count` (default 450) and `universe.max_composite_count` (default 3200) that bound the total Composite_Universe size after set-union deduplication.
6. THE Config_Loader SHALL accept per-source `min_count` and `max_count` fields on each Universe_Source to bound the expected ticker count from that individual source, with defaults appropriate to the source kind (e.g., 450–520 for `sp500_wikipedia`, 900–1100 for a Russell 1000 ETF, 1800–2200 for a Russell 2000 ETF).
7. WHEN `universe.sources` is present, THE Config_Loader SHALL ignore the legacy `universe.source_url` field and SHALL log a WARN if both `sources` and `source_url` are specified simultaneously.

### Requirement 2: ETF Holdings CSV Parser

**User Story:** As a researcher, I want the Bot to parse iShares ETF holdings CSV files (IWB for Russell 1000, IWM for Russell 2000) into constituent lists, so that I can use free, regularly-updated index membership data without a paid data subscription.

#### Acceptance Criteria

1. WHEN a Universe_Source of `kind: "etf_holdings_csv"` is fetched, THE ETF_Holdings_Parser SHALL download the CSV from the configured URL via HTTP GET with a browser-like User-Agent header and a configurable timeout (default 15 seconds).
2. THE ETF_Holdings_Parser SHALL skip header metadata rows (iShares CSVs contain non-tabular preamble lines before the column headers) and locate the row containing column headers by searching for a row that includes both a "Ticker" (or "Symbol") column and a "Name" column.
3. THE ETF_Holdings_Parser SHALL extract `(ticker, company_name, sector)` triples from each data row, where `ticker` is stripped, upper-cased, and dot-normalized to match the Wikipedia convention (e.g., `BRK B` becomes `BRK.B`), `company_name` is the Name column value, and `sector` is the Sector column value when present (NULL otherwise).
4. THE ETF_Holdings_Parser SHALL exclude rows where the Ticker column is empty, contains a dash-only placeholder (e.g., `-`), or represents a non-equity holding (e.g., cash, futures, `CASH_USD`, or rows with Asset Class not equal to "Equity").
5. THE ETF_Holdings_Parser SHALL return a sorted, deduplicated list of triples, consistent with the output shape of the existing `fetch_current_constituents` function.
6. IF the downloaded CSV cannot be parsed (malformed, empty, or missing expected columns), THEN THE ETF_Holdings_Parser SHALL raise a source-specific error that the Universe_Service can handle per Requirement 3.
7. THE ETF_Holdings_Parser SHALL produce a round-trip-stable output: parsing a well-formed iShares CSV, formatting the triples back into CSV rows, and re-parsing SHALL yield an identical list of triples.

### Requirement 3: Multi-Source Universe Synchronization

**User Story:** As a researcher, I want the Bot to fetch constituents from all enabled sources, merge them into a single deduplicated universe, and handle partial source failures gracefully, so that a transient outage on one source does not block the entire scan.

#### Acceptance Criteria

1. WHEN a Scan_Run begins, THE Universe_Service SHALL iterate over all enabled Universe_Sources in configuration order and fetch each source's constituent list independently.
2. WHEN all enabled sources have been fetched (or have failed), THE Universe_Service SHALL compute the Composite_Universe as the set-union of all successfully fetched ticker sets, deduplicated by ticker symbol.
3. WHEN a ticker appears in multiple sources, THE Universe_Service SHALL record all contributing source names in that ticker's Index_Sources metadata.
4. IF one or more sources fail (network error, parse error, or per-source count outside `[min_count, max_count]`), AND at least one source succeeds, THEN THE Universe_Service SHALL continue the Scan_Run using the Composite_Universe from the successful sources, SHALL log a WARN identifying each failed source and the reason, and SHALL include the failure in the Watchdog_Alert.
5. IF every enabled source fails, THEN THE Bot SHALL abort the Scan_Run with a non-zero exit code before any price or fundamentals data is fetched, and THE Repository SHALL NOT be mutated in that run.
6. IF the Composite_Universe size falls outside `[min_composite_count, max_composite_count]`, THEN THE Bot SHALL abort the Scan_Run with a non-zero exit code before any downstream I/O, and THE Repository SHALL NOT be mutated in that run.
7. WHEN the universe sync completes successfully, THE Repository SHALL upsert the Composite_Universe into `asset_universe`, updating each ticker's Index_Sources to reflect the current Scan_Run's source attribution.
8. WHEN the universe sync completes, THE Universe_Service SHALL produce a diff (added, removed, source_failures) relative to the previously active universe, and THE Notifier SHALL emit a Watchdog_Alert if the diff is non-empty or any source failed.

### Requirement 4: Database Schema Evolution

**User Story:** As a developer, I want the `asset_universe` table to track which index sources contributed each ticker, so that watchdog alerts, debugging, and future per-index analytics can attribute tickers to their originating indices.

#### Acceptance Criteria

1. THE Repository SHALL add an `index_sources` column of type `TEXT[]` (PostgreSQL array) to the `asset_universe` table via a new migration file (`002_universe_expansion.sql`), with a default value of `'{}'::TEXT[]` for existing rows.
2. WHEN `upsert_universe` is called, THE Repository SHALL set each ticker's `index_sources` to the array of source names that contributed that ticker in the current Scan_Run.
3. THE `load_universe` method SHALL continue to return only tickers with `removed_on IS NULL`, and SHALL additionally expose the `index_sources` array when requested by callers that need source attribution.
4. THE migration SHALL be idempotent: re-running the migration on a database that already has the `index_sources` column SHALL be a no-op.
5. THE migration SHALL NOT require downtime or data loss; it SHALL use `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` with a server default so that existing rows are populated without a full table rewrite.

### Requirement 5: Scaled Price Download for Larger Universes

**User Story:** As an operator running on a 2 GB RAM NAS, I want the price download phase to handle up to 3000 tickers within bounded memory and time, so that expanding to Russell 3000 does not OOM the container or exceed the scan window.

#### Acceptance Criteria

1. THE Price_Service SHALL download OHLC history for up to 3000 tickers in configurable batches (default `yfinance.batch_size: 100`), processing one batch at a time to bound peak memory.
2. WHILE downloading prices for a universe larger than 1000 tickers, THE Price_Service SHALL log progress at INFO level every `batch_size` tickers processed, reporting the count completed and the count remaining.
3. THE Price_Service SHALL complete the price download and technical snapshot computation for a Russell 1000 universe (~1000 tickers) within 10 minutes under normal conditions (reachable yfinance, warm OS DNS cache).
4. THE Price_Service SHALL complete the price download and technical snapshot computation for a Russell 3000 universe (~3000 tickers) within 20 minutes under normal conditions.
5. THE Bot container SHALL maintain a peak memory footprint of no more than 500 MB during the price download and technical snapshot computation phase for a 3000-ticker universe, measured as the container's RSS.
6. THE Price_Service SHALL release each batch's raw OHLC DataFrames from memory after computing that batch's technical snapshot rows, so that only the final snapshot DataFrame (not the raw history) accumulates across batches.

### Requirement 6: EDGAR Coverage Handling for Small-Cap Tickers

**User Story:** As a researcher scanning Russell 2000 small-caps, I want the Bot to handle tickers with missing or sparse EDGAR XBRL data gracefully, so that a few small-caps without filings do not abort the entire scan or produce misleading alerts.

#### Acceptance Criteria

1. WHEN the Fundamentals_Service cannot find a CIK mapping for a ticker in the SEC ticker-to-CIK table, THE Fundamentals_Service SHALL log a WARN identifying the ticker, SHALL return a Fundamentals record with all numeric fields set to NULL, and SHALL NOT raise an exception.
2. WHEN EDGAR returns a 404 (no CompanyFacts) for a ticker's CIK, THE Fundamentals_Service SHALL log an INFO message, SHALL return a Fundamentals record with all numeric fields set to NULL, and SHALL cache the NULL-valued record so that subsequent Scan_Runs do not re-request the same missing data within the staleness window.
3. WHEN a Fundamentals record has NULL `pe_ratio`, NULL `pe_5y_avg`, or NULL `fcf_yield`, THE Filter_Pipeline SHALL exclude that ticker from Layer 3 and Layer 4 evaluation (existing behavior), effectively treating the ticker as a Graceful_Degradation_Ticker for that Scan_Run.
4. WHEN a Scan_Run completes, THE Bot SHALL log a summary line at INFO level reporting the count of Graceful_Degradation_Tickers (tickers that passed L1+L2 but were excluded from L3/L4 due to missing fundamentals) and the total L2_Survivor count, so the operator can assess EDGAR coverage quality.
5. THE Fundamentals_Service SHALL NOT treat a missing EDGAR filing as a budget-exhaustion event; missing filings SHALL NOT trigger the kill-switch that stops further EDGAR requests for the remainder of the Scan_Run.

### Requirement 7: Run Time and Resource Constraints

**User Story:** As an operator running the Bot on a Synology DS220+ at zero cost, I want explicit time and memory bounds for expanded universes, so that the scan coexists with other NAS services and completes within the daily scheduling window.

#### Acceptance Criteria

1. THE Bot SHALL complete an end-to-end Scan_Run for a Russell 1000 universe (~1000 tickers) in less than 10 minutes under normal conditions (reachable external services, warm fundamentals cache).
2. THE Bot SHALL complete an end-to-end Scan_Run for a Russell 3000 universe (~3000 tickers) in less than 20 minutes under normal conditions.
3. THE Bot container SHALL not exceed 500 MB peak RSS at any point during a Scan_Run for a 3000-ticker universe.
4. THE PostgreSQL container SHALL remain configured with `mem_limit: 512m` in `docker-compose.yml`.
5. THE Bot SHALL incur zero paid-tier charges in steady-state operation; EDGAR (free, no key, 10 req/sec rate limit), yfinance (free, unlimited), Wikipedia (free), and Discord webhooks (free) are the only external services used.
6. THE Bot SHALL respect the EDGAR rate limit of 10 requests per second by throttling outbound EDGAR HTTP calls, and SHALL log a WARN if the rate limiter is engaged for more than 30 consecutive seconds.
7. WHEN the total Scan_Run wall-clock time exceeds a configurable `universe.max_scan_minutes` (default 25), THE Bot SHALL log a WARN with the elapsed time and the phase that was active when the threshold was crossed, but SHALL NOT abort the run (the operator uses this signal to tune batch sizes or reduce the universe).

### Requirement 8: Backward Compatibility

**User Story:** As an existing v1 operator, I want my current `config.yaml` (with a single `universe.source_url`) to produce identical scan behavior after the upgrade, so that the universe expansion is opt-in and I am not forced to reconfigure on day one.

#### Acceptance Criteria

1. WHEN `config.yaml` contains `universe.source_url` but no `universe.sources`, THE Config_Loader SHALL construct a single-source configuration equivalent to `[{name: "sp500_wikipedia", kind: "wikipedia_table", url: <source_url>, enabled: true}]` and SHALL apply the existing `min_constituent_count` and `max_constituent_count` as that source's per-source bounds.
2. WHEN a v1 config is loaded, THE Universe_Service SHALL produce the same `asset_universe` rows, the same Watchdog_Alert content, and the same Composite_Universe ticker set as the v1 code path, except that each ticker's `index_sources` column will be populated with `['sp500_wikipedia']`.
3. WHEN a v1 config is loaded, THE Filter_Pipeline, Price_Service, Fundamentals_Service, Notifier, and Repository SHALL behave identically to v1 for all tickers in the S&P 500 universe.
4. THE new `index_sources` column SHALL have a server default of `'{}'::TEXT[]` so that existing rows inserted by v1 are valid without a backfill migration.
5. THE Config_Loader SHALL continue to accept `ADB_UNIVERSE__SOURCE_URL` as an environment-variable override for the legacy single-source path, and SHALL apply it only when `universe.sources` is not configured.

### Requirement 9: Watchdog Alerts for Multi-Source Universe

**User Story:** As a researcher, I want the Watchdog_Alert to show per-source attribution (which tickers were added or removed from which index), and to flag any source that failed during the sync, so that I can distinguish an index reconstitution from a data-source outage.

#### Acceptance Criteria

1. WHEN the universe diff contains added or removed tickers, THE Notifier SHALL include per-source attribution in the Watchdog_Alert embed, grouping added and removed tickers by the source(s) that contributed them.
2. WHEN one or more sources failed during the sync, THE Notifier SHALL include a "Source Failures" section in the Watchdog_Alert listing each failed source name and a one-line reason (e.g., "HTTP 503", "CSV parse error", "count 0 outside bounds [900, 1100]").
3. THE Watchdog_Alert SHALL include a summary line showing the total Composite_Universe size, the count of enabled sources, and the count of sources that succeeded.
4. WHEN all sources succeed and the diff is empty (no additions, no removals), THE Notifier SHALL NOT emit a Watchdog_Alert.
5. THE Watchdog_Alert embed content SHALL NOT exceed the Discord embed character limit (4096 characters for the description field); IF the diff is too large, THE Notifier SHALL truncate the ticker lists and append a count of omitted tickers.

### Requirement 10: Correctness Invariants for Expanded Universe

**User Story:** As a reviewer of the Bot's output, I want the Bot to preserve a set of universally quantified correctness invariants across all valid inputs and all universe sizes, so that every alert from an expanded universe can be trusted to the same standard as v1 alerts.

#### Acceptance Criteria

1. FOR ALL Scan_Runs, THE Composite_Universe SHALL equal the set-union of all successfully fetched source ticker sets (union correctness).
2. FOR ALL tickers `t` in the Composite_Universe, `t.index_sources` SHALL be a non-empty subset of the names of sources that returned `t` in the current Scan_Run (source attribution completeness).
3. FOR ALL Scan_Runs, THE set of L4_Survivors SHALL be a subset of L3_Survivors, which SHALL be a subset of L2_Survivors, which SHALL be a subset of L1_Survivors, which SHALL be a subset of the Composite_Universe (sequential filter monotonicity, extended from v1).
4. FOR ALL tickers `t` that appear in both source A and source B, THE Composite_Universe SHALL contain exactly one entry for `t` (deduplication correctness).
5. FOR ALL Scan_Runs where at least one source succeeds, THE Composite_Universe size SHALL satisfy `min_composite_count <= |Composite_Universe| <= max_composite_count` (composite bounds invariant).
6. FOR ALL High_Conviction_Candidates `c` emitted in a Scan_Run, ALL v1 correctness invariants (anchor range closure, strict RSI crossover, quality dominates value, config reproducibility, alert idempotency) SHALL continue to hold (v1 invariant preservation).
7. FOR ALL ETF holdings CSV inputs, parsing the CSV into triples, formatting the triples back to CSV rows, and re-parsing SHALL produce an identical triple list (ETF parser round-trip).
8. FOR ALL Scan_Runs, IF source S fails and source S' succeeds, THEN every ticker contributed exclusively by S SHALL be absent from the Composite_Universe, and every ticker contributed exclusively by S' SHALL be present (partial failure isolation).
9. FOR ALL Scan_Runs, THE `index_sources` array for each ticker in `asset_universe` after upsert SHALL exactly equal the set of source names that contributed that ticker in the current Scan_Run (source attribution freshness — no stale source names from previous runs persist).
10. FOR ALL Scan_Runs with a v1-compatible single-source config, THE Composite_Universe SHALL be identical to the set that the v1 `sync_universe` function would have produced (backward compatibility equivalence).
