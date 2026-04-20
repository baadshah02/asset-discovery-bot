# Requirements Document

## Introduction

The Asset Discovery Bot is a containerized research tool that scans the S&P 500 universe on a daily schedule and surfaces equities exhibiting a combined value-and-quality pattern with early signs of price recovery, for **human review only**. It applies a sequential 4-layer filter pipeline (52-week-low anchor, RSI capitulation crossover, trailing P/E vs. 5-year average, free cash flow yield floor), caches expensive fundamentals in PostgreSQL to stay within free API tiers, and posts high-conviction alerts plus universe-change alerts to Discord via webhooks. The bot runs on a Synology DS220+ NAS under a strict 2 GB RAM host budget with Postgres capped at 512 MB.

This document captures the user-facing behavior, acceptance criteria, and non-functional constraints that the approved design at `.kiro/specs/asset-discovery-bot/design.md` satisfies. Requirements are derived from that design and explicitly constrained to the v1 scope — v2 Roadmap items (universe expansion, composite ranking, sector neutralization, backtest harness, portfolio construction, exit rules, paper-trading ledger, additional notification channels, metrics dashboards, weekly digests) are **not** v1 requirements.

**Scope discipline.** v1 is a research and alert tool. It **surfaces candidates for human review**; it does not make investment decisions, execute trades, size positions, manage a portfolio, or recommend capital deployment. Every requirement below must be read under that constraint.

## Glossary

- **Bot**: The `asset-discovery-bot` Python container that executes a single daily scan and then exits.
- **Scan_Run**: One end-to-end invocation of the orchestrator (`bot.run`), initiated by the Synology Task Scheduler via `docker exec`.
- **Universe_Service** (`bot.universe`): The component that scrapes Wikipedia's S&P 500 table, diffs it against the local `asset_universe`, upserts the canonical membership, and emits a universe-change signal.
- **Price_Service** (`bot.prices`): The component that batch-downloads 1-year OHLC from yfinance and computes per-ticker technical indicators (52-week low, percent above low, RSI today and yesterday).
- **Fundamentals_Service** (`bot.fundamentals`): The component that fetches trailing P/E, 5-year average P/E, FCF yield, and the latest press-release headline from Financial Modeling Prep (FMP), gated by a local cache.
- **Filter_Pipeline** (`bot.filters`): The 4-layer sequential "tollbooth" that reduces the universe to high-conviction candidates (Layer 1 anchor → Layer 2 RSI crossover → Layer 3 P/E value → Layer 4 FCF quality).
- **Notifier** (`bot.notify`): The component that formats and POSTs rich embed alerts to Discord webhooks, with retry and backoff.
- **Config_Loader** (`bot.config`): The component that loads `config.yaml`, applies `ADB_*` environment-variable overrides, loads secrets from `/run/secrets/*`, and validates everything via Pydantic.
- **Repository** (`bot.repo`): The SQLAlchemy Core 2.0 data access layer that owns the DB engine and performs upserts against `asset_universe`, `fundamentals_cache`, and `daily_scans`.
- **L1_Survivor / L2_Survivor / L3_Survivor / L4_Survivor**: A ticker that passed the respective filter layer in the current Scan_Run.
- **High_Conviction_Candidate**: An L4_Survivor — a ticker that passed all four filter layers in a single Scan_Run.
- **Watchdog_Alert**: A Discord message emitted when the scraped S&P 500 constituents differ from the previously stored universe.
- **High_Conviction_Alert**: A Discord message emitted for each High_Conviction_Candidate.
- **Fundamentals_Staleness_Days**: The configurable cache TTL (default 7 days) after which a cached fundamentals row is considered stale.
- **Config_Snapshot**: A JSON-serializable dump of the active `AppConfig` at the moment a `daily_scans` row is inserted.
- **FMP_Daily_Soft_Cap**: The configured daily FMP call budget (default 200) used for warning; the FMP free-tier hard limit is 250 calls/day.

## Requirements

### Requirement 1: Universe Synchronization (Watchdog)

**User Story:** As a researcher, I want the Bot to keep its local S&P 500 membership in sync with the live index and notify me of changes, so that scans always run against the correct universe and I am never surprised by an index reconstitution.

#### Acceptance Criteria

1. WHEN a Scan_Run begins, THE Universe_Service SHALL scrape the current S&P 500 constituents from the configured Wikipedia source URL before any price or fundamentals data is fetched.
2. WHEN the scraped constituent list has been obtained, THE Universe_Service SHALL compute the set difference against the currently active rows in `asset_universe` and produce an added set and a removed set.
3. WHEN the added set is non-empty or the removed set is non-empty, THE Notifier SHALL emit exactly one Watchdog_Alert listing the added and removed tickers.
4. WHEN the universe sync completes, THE Repository SHALL reflect the scraped constituents such that every ticker in the scrape has `removed_on IS NULL` and every ticker previously active but absent from the scrape has `removed_on` set to the current date.
5. WHEN the Universe_Service has finished upserting, THE added set and the removed set produced by the sync SHALL be disjoint.
6. IF the Wikipedia scrape raises a network error or returns a constituent count outside the configured `[min_constituent_count, max_constituent_count]` bounds, THEN THE Bot SHALL abort the Scan_Run with a non-zero exit code before any yfinance, FMP, or Discord call is made, and THE Repository SHALL NOT be mutated in that run.

### Requirement 2: Price History and Technical Indicators

**User Story:** As a researcher, I want the Bot to fetch daily OHLC history for the active universe and compute the technical indicators used by Layers 1 and 2, so that the filter pipeline has consistent, auditable inputs.

#### Acceptance Criteria

1. WHEN Scan_Run enters the triage phase, THE Price_Service SHALL batch-download the configured history period (default `1y`) of daily OHLC from yfinance for every ticker currently active in `asset_universe`.
2. THE Price_Service SHALL return a per-ticker technical snapshot containing `close`, `low_52w`, `pct_above_low`, `rsi_today`, and `rsi_yesterday` for every ticker with at least `rsi_period + 1` daily observations.
3. THE Price_Service SHALL compute `pct_above_low` as `(close - low_52w) / low_52w`, expressed as a decimal fraction.
4. THE Price_Service SHALL compute `rsi_today` and `rsi_yesterday` using Wilder's smoothing with the configured `rsi_period` (default 14), producing values in the closed interval `[0, 100]`.
5. WHEN yfinance returns no data for a ticker, THE Price_Service SHALL exclude that ticker from the snapshot and log a warning identifying the ticker.
6. IF a ticker has fewer than `rsi_period + 1` observations, THEN THE Price_Service SHALL exclude that ticker from the snapshot rather than emit NaN indicators.
7. THE Price_Service SHALL retry a failed per-ticker download at most `retries_per_ticker` times (default 3) before giving up on that ticker for the current Scan_Run.

### Requirement 3: Fundamentals Retrieval with Cache Gating

**User Story:** As a researcher operating on a free-tier FMP account, I want expensive fundamentals calls to be gated by a local cache, so that daily FMP usage stays well under the 250 calls/day free-tier limit.

#### Acceptance Criteria

1. WHEN the Fundamentals_Service is asked for a ticker's fundamentals, THE Fundamentals_Service SHALL first query `fundamentals_cache` for an existing row.
2. WHERE a cached row exists and its `fetched_at` timestamp is newer than `now() - fundamentals_staleness_days`, THE Fundamentals_Service SHALL return the cached row without making any FMP call.
3. IF no cached row exists, OR IF the cached row's `fetched_at` is older than or equal to `now() - fundamentals_staleness_days`, THEN THE Fundamentals_Service SHALL call FMP for `/ratios`, `/cash-flow-statement`, `/profile`, and `/press-releases` for that ticker, upsert the enriched row into `fundamentals_cache`, and return the fresh record.
4. THE Fundamentals_Service SHALL derive `fcf_yield` as `trailing_twelve_month_free_cash_flow / market_cap` when `market_cap > 0`, and SHALL set `fcf_yield` to NULL otherwise.
5. THE Fundamentals_Service SHALL derive `pe_5y_avg` as the arithmetic mean of per-year P/E values reported for the most recent five reporting years.
6. THE Fundamentals_Service SHALL be invoked only for tickers that have passed Layer 1 and Layer 2 in the current Scan_Run.
7. IF FMP returns HTTP 429 for any request during a Scan_Run, THEN THE Fundamentals_Service SHALL stop making further FMP calls for the remainder of that Scan_Run, and THE Filter_Pipeline SHALL continue to evaluate only those L2_Survivors whose fundamentals are already available from cache.
8. THE configured `fundamentals_staleness_days` value SHALL default to 7 and SHALL be user-configurable within the bounds `[1, 90]`.

### Requirement 4: 4-Layer Filter Pipeline

**User Story:** As a researcher, I want a deterministic, sequential 4-layer filter to reduce the S&P 500 down to a small set of high-conviction candidates per day, so that I can review a focused list rather than search the full universe by hand.

#### Acceptance Criteria

1. WHEN the triage phase runs, THE Filter_Pipeline SHALL apply Layer 1 to the full technical snapshot such that every L1_Survivor satisfies `pct_above_low_min ≤ pct_above_low ≤ pct_above_low_max` (defaults: 0.05 and 0.15, inclusive).
2. WHEN Layer 1 has produced L1_Survivors, THE Filter_Pipeline SHALL apply Layer 2 such that every L2_Survivor satisfies `rsi_yesterday < rsi_oversold` AND `rsi_today > rsi_recovery` (defaults: 30.0 and 30.0).
3. WHEN Layer 2 has produced L2_Survivors, THE Filter_Pipeline SHALL enrich only those survivors with fundamentals via the Fundamentals_Service, and SHALL NOT request fundamentals for any ticker that failed Layer 1 or Layer 2.
4. WHEN enrichment is complete, THE Filter_Pipeline SHALL apply Layer 3 such that every L3_Survivor has non-null `pe_ratio` and non-null `pe_5y_avg` AND `pe_ratio < pe_5y_avg`.
5. WHERE `layer3.require_positive_earnings` is true (default), THE Filter_Pipeline SHALL additionally require `pe_ratio > 0` AND `pe_5y_avg > 0` for every L3_Survivor.
6. WHEN Layer 3 has produced L3_Survivors, THE Filter_Pipeline SHALL apply Layer 4 such that every L4_Survivor has non-null `fcf_yield` AND `fcf_yield > fcf_yield_min` (default 0.045).
7. THE Filter_Pipeline SHALL preserve input data for each survivor (close, pct_above_low, rsi_today, rsi_yesterday, pe_ratio, pe_5y_avg, fcf_yield, latest_headline, headline_url) in the output so that downstream alerts can justify inclusion.
8. THE Filter_Pipeline SHALL NOT mutate input DataFrames; each layer SHALL return a new frame.

### Requirement 5: Discord Notifications

**User Story:** As a researcher, I want high-conviction candidates and universe-membership changes delivered to a Discord channel as rich embeds, so that I receive scan results on my existing channel without maintaining additional infrastructure.

#### Acceptance Criteria

1. WHEN Scan_Run produces one or more High_Conviction_Candidates, THE Notifier SHALL POST exactly one rich embed to the configured Discord webhook for each candidate, and each embed SHALL include the ticker, close price, percent above 52-week low, RSI yesterday and today, current P/E, 5-year-average P/E, FCF yield, and the latest press-release headline (with URL when available).
2. WHEN Scan_Run produces a non-empty universe diff, THE Notifier SHALL POST exactly one Watchdog_Alert rich embed listing the added and removed tickers.
3. WHEN a Discord POST returns HTTP 429 or a 5xx status, THE Notifier SHALL retry the POST up to `max_retries` times (default 5) using exponential backoff bounded by `backoff_initial_seconds` and `backoff_max_seconds`, and SHALL honor any `Retry-After` header on 429 responses.
4. WHEN the Notifier receives a non-retryable 4xx response (other than 429), THE Notifier SHALL raise a notification error, and THE Bot SHALL exit with a non-zero code for that Scan_Run.
5. THE Bot SHALL insert the `daily_scans` row for a High_Conviction_Candidate before attempting the corresponding Discord POST.
6. THE Notifier SHALL set the `username` field of each Discord payload to the configured `notification.bot_username`.
7. THE Notifier SHALL NOT include any secret values (webhook URL, API keys, database credentials) in the rendered embed content or in any log line.

### Requirement 6: Configuration and Secrets

**User Story:** As an operator, I want every tunable threshold defined in a single validated config file and every secret loaded separately from Docker secrets, so that I can experiment with thresholds safely without touching credentials and without risking secret leakage.

#### Acceptance Criteria

1. WHEN Scan_Run starts, THE Config_Loader SHALL load `AppConfig` from the mounted `config.yaml` file (default path `/app/config/config.yaml`), apply `ADB_*` environment-variable overrides using double-underscore nesting, and validate every field via Pydantic.
2. THE Config_Loader SHALL apply precedence in the order: environment variable `ADB_*` (highest), then `config.yaml` value, then the Pydantic field default.
3. THE Config_Loader SHALL load `Secrets` (database URL, FMP API key, Discord webhook URL) exclusively from files under `/run/secrets/`.
4. IF any `AppConfig` field fails Pydantic validation, OR IF any required secret file under `/run/secrets/` is missing or empty, THEN THE Bot SHALL exit with a non-zero code before any Wikipedia, yfinance, FMP, Discord, or PostgreSQL I/O is performed.
5. THE Config_Loader SHALL expose `AppConfig` as a frozen, immutable object to every other component.
6. WHEN the bot starts a Scan_Run, THE Bot SHALL log a single INFO line listing the active non-default `AppConfig` values, and THE Bot SHALL NOT log any field of `Secrets`.
7. WHEN a `daily_scans` row is inserted, THE Repository SHALL persist the full `AppConfig.model_dump()` into the row's `config_snapshot` JSONB column.
8. THE `Layer1Config` SHALL enforce `pct_above_low_max > pct_above_low_min` at validation time.
9. THE `CacheConfig.fundamentals_staleness_days` SHALL be constrained to the closed interval `[1, 90]` at validation time.

### Requirement 7: Data Persistence

**User Story:** As a researcher, I want every universe change, fundamentals fetch, and high-conviction alert written durably to PostgreSQL with deterministic upsert semantics, so that scans are reproducible, idempotent, and auditable.

#### Acceptance Criteria

1. THE Repository SHALL expose typed methods for reading and upserting `asset_universe`, `fundamentals_cache`, and `daily_scans` using SQLAlchemy Core 2.0 (no ORM).
2. THE Repository SHALL implement upsert semantics via `INSERT ... ON CONFLICT` for `asset_universe` and `fundamentals_cache`.
3. THE `daily_scans` table SHALL enforce a `UNIQUE (ticker, scan_date)` constraint.
4. WHEN the Bot attempts to insert a `daily_scans` row that collides with the `UNIQUE (ticker, scan_date)` constraint, THE Bot SHALL treat the violation as a no-op for that ticker and SHALL NOT emit a duplicate Discord alert for that ticker on that date.
5. THE Repository SHALL provide a transactional context manager so that multi-statement updates commit atomically.
6. THE `load_universe` method SHALL return only tickers whose `removed_on IS NULL`.
7. THE Repository SHALL never open a connection from outside its own context managers.

### Requirement 8: Error Handling and Graceful Degradation

**User Story:** As an operator, I want the Bot to fail fast on unrecoverable errors and degrade gracefully on transient ones, so that partial state is never published to Discord and transient outages do not require manual intervention.

#### Acceptance Criteria

1. IF the Wikipedia scrape fails or returns an implausible constituent count, THEN THE Bot SHALL abort the Scan_Run with a non-zero exit code before any downstream I/O, and SHALL NOT mutate `asset_universe` in that run.
2. WHEN yfinance returns an empty frame for some tickers, THE Bot SHALL exclude those tickers from the current Scan_Run, log a WARN with the count of excluded tickers, and continue processing the remaining universe.
3. IF FMP returns HTTP 429 during a Scan_Run, THEN THE Bot SHALL stop all further FMP calls for that run, SHALL continue Layers 3 and 4 for only those L2_Survivors whose fundamentals came from cache, and SHALL log a WARN identifying the budget event.
4. IF every Discord POST retry for a single alert exhausts `max_retries` without a 2xx response, THEN THE Bot SHALL exit with a non-zero code, and THE corresponding `daily_scans` row SHALL remain persisted so that a future operational backfill can re-attempt delivery without creating a duplicate DB row.
5. IF the Repository raises an operational error (PostgreSQL unreachable, OOM, connection refused), THEN THE Bot SHALL abort the Scan_Run, and no Discord POST SHALL be attempted after the failure point.
6. WHEN the Bot aborts for any of the above reasons, THE Bot SHALL exit with a non-zero code that is observable by the Synology Task Scheduler.

### Requirement 9: Non-Functional Constraints

**User Story:** As an operator running the Bot on a Synology DS220+ at zero cost, I want the Bot to live within a tight resource and cost envelope, so that it coexists with other NAS services and never incurs a paid API bill.

#### Acceptance Criteria

1. THE Bot container SHALL run within a steady-state memory footprint of no more than 300 MB during the Scan_Run's peak Pandas computation phase.
2. THE PostgreSQL container SHALL be configured with `mem_limit: 512m` in `docker-compose.yml`.
3. THE Bot SHALL complete an end-to-end Scan_Run in less than 5 minutes under normal conditions (reachable external services, warm cache).
4. THE Bot SHALL incur zero paid-tier charges in steady-state operation; all external services used by v1 SHALL be free-tier (Wikipedia, yfinance, FMP free tier ≤ 250 calls/day, Discord webhooks).
5. WHEN a Scan_Run completes, THE total number of FMP API calls made during that run SHALL be bounded by `3 × N`, where `N` is the count of L2_Survivors that were cache-miss or cache-stale in that run.
6. THE Bot container SHALL expose no inbound network ports.
7. THE PostgreSQL container SHALL listen only on the internal Docker Compose network and SHALL NOT be bound to a host port.
8. THE Bot SHALL load all secret material (database URL, FMP API key, Discord webhook URL) from files under `/run/secrets/` and SHALL NOT read any secret from a process environment variable, image layer, or committed file.
9. THE `requirements.txt` (or equivalent dependency manifest) SHALL pin every runtime dependency to an exact version.

### Requirement 10: Scope Discipline — Research Tool, Not Capital Deployment

**User Story:** As the author of the bot, I want the system's outputs to be framed unambiguously as research signals for human review, so that no user (including me) mistakes a Discord alert for an investment recommendation or a trading instruction.

#### Acceptance Criteria

1. THE Bot SHALL NOT place trades, submit orders to a broker, size positions, or construct or rebalance a portfolio.
2. THE Bot SHALL NOT compute or recommend position sizes, entry prices, exit prices, stop-loss levels, or take-profit levels.
3. THE Bot SHALL NOT persist any paper-trading ledger, simulated portfolio, or realized P&L computation in v1.
4. THE content of every High_Conviction_Alert SHALL present the candidate as information for human review and SHALL NOT include language that instructs the reader to buy, sell, hold, or allocate capital.
5. THE Bot SHALL NOT implement any Tier-1-through-Tier-4 v2 Roadmap item as part of v1, including universe expansion beyond the S&P 500, multi-metric value composites, gross-profitability quality composites, sector-neutral ranking, top-N composite-rank output in place of hard thresholds, replacement of RSI crossover with a short-term-reversal signal, backtest harnesses, portfolio construction, exit rule modules, paper-trading ledgers, replacement of yfinance with a paid data source, secondary notification channels, Prometheus/Grafana metrics dashboards, or a weekly digest report.

### Requirement 11: Correctness Invariants

**User Story:** As a reviewer of the Bot's output, I want the Bot to preserve a set of universally quantified correctness invariants across all valid inputs, so that every alert can be trusted to reflect the declared filter semantics and so that property-based tests can verify the implementation.

#### Acceptance Criteria

1. FOR ALL Scan_Runs, THE set of L4_Survivors SHALL be a subset of L3_Survivors, which SHALL be a subset of L2_Survivors, which SHALL be a subset of L1_Survivors (sequential filter monotonicity).
2. FOR ALL price series `p` and ALL trading days `i`, THE value of `compute_rsi(p[:i+1])[i]` SHALL equal `compute_rsi(p)[i]` (no RSI look-ahead).
3. FOR ALL tickers `t`, IF `get_fundamentals(t)` returns, THEN the corresponding `fundamentals_cache` row's `fetched_at` SHALL satisfy `now() - fetched_at < fundamentals_staleness_days` (cache semantics).
4. FOR ALL Scan_Runs, THE total FMP calls issued SHALL be less than or equal to `3 × N`, where `N` is the count of L2_Survivors that were cache-miss or cache-stale in that run (FMP budget bound).
5. FOR ALL `(ticker, scan_date)` pairs, AT MOST one row SHALL exist in `daily_scans` with that pair (alert idempotency, enforced by `UNIQUE` constraint).
6. FOR ALL Scan_Runs, after `sync_universe` completes, THE set of tickers in `asset_universe` with `removed_on IS NULL` SHALL equal the set of tickers scraped from Wikipedia in that run (watchdog completeness).
7. FOR ALL High_Conviction_Candidates `c` emitted in a Scan_Run, `c.pct_above_low` SHALL satisfy `layer1.pct_above_low_min ≤ c.pct_above_low ≤ layer1.pct_above_low_max` (anchor range closure).
8. FOR ALL High_Conviction_Candidates `c` emitted in a Scan_Run, `c.rsi_yesterday` SHALL be strictly less than `layer2.rsi_oversold` AND `c.rsi_today` SHALL be strictly greater than `layer2.rsi_recovery` (strict RSI crossover).
9. FOR ALL High_Conviction_Candidates `c` emitted in a Scan_Run, `c.pe_ratio` SHALL be strictly less than `c.pe_5y_avg` AND `c.fcf_yield` SHALL be strictly greater than `layer4.fcf_yield_min` (quality dominates value).
10. FOR ALL successful `daily_scans` inserts in a Scan_Run, THE Bot SHALL attempt at least one Discord POST for the corresponding candidate, with bounded retries on 429/5xx, and SHALL rely on the `UNIQUE (ticker, scan_date)` constraint to prevent duplicate rows on re-runs (at-least-once alert delivery with DB-side de-duplication).
11. FOR ALL rows `r` in `daily_scans`, `r.config_snapshot` SHALL contain every `AppConfig` field that was active at the moment of insert, such that the exact thresholds that produced that alert can be recovered without consulting external state (config reproducibility).
12. FOR ALL Scan_Runs, IF `config.yaml` or any `ADB_*` environment variable fails Pydantic validation, THEN THE Bot SHALL exit non-zero before any network call, database write, or Discord POST is performed (strict config validation).
