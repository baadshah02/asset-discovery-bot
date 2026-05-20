# Requirements Document

## Introduction

The Asset Discovery Bot v1 uses a sequential 4-layer hard-threshold "tollbooth" (52-week anchor → RSI crossover → P/E vs 5-year average → FCF yield floor) that emits a pass/fail verdict per ticker. Academic evidence — Asness, Frazzini, Israel and Moskowitz (2015) *Fact, Fiction and Value Investing*, Novy-Marx (2013) on gross profitability, Jegadeesh (1990) on short-term reversal, and the broader Fama-French factor literature — shows that (a) multi-metric composite signals beat single-ratio thresholds, (b) gross profitability over total assets (GP/A) is a more persistent and less-gameable quality signal than FCF yield, (c) short-term reversal has direct empirical support whereas RSI crossovers do not, (d) sector-neutral ranking prevents loading on whichever sector is currently cheap in absolute terms, and (e) top-N ranking extracts more information from the cross-section than binary pass/fail.

This feature transforms the Bot from a hard-threshold tollbooth into a **ranked composite scorer**. It implements Tier-1 roadmap items B, C, D, E and F from the v1 design document as one coherent upgrade because they are tightly coupled — doing any one alone does not make sense. Together they replace the single P/E filter with a multi-metric value composite, replace the single FCF-yield threshold with a multi-metric quality composite, replace the RSI crossover with a short-term reversal signal, rank each metric cross-sectionally within its GICS sector before combining, and emit the top-N composite-rank candidates per scan rather than an unranked pass/fail set.

**Scope discipline (preserved from v1).** This remains a **research tool for human review**. The Bot continues to surface candidates for human review; it does not recommend trades, size positions, or deploy capital. A composite rank of 1 is not a buy signal — it is an ordered position in a daily research list. Every requirement below must be read under that constraint.

**Relationship to v1.** This is an additive upgrade. The new ranking pipeline runs alongside the v1 hard-threshold pipeline, selectable by a configuration flag. Operators running the existing v1 config receive identical behaviour on day one. Downstream infrastructure (Universe_Service, Price_Service, Fundamentals_Service cache gating, Notifier retry semantics, Repository transaction model, Config_Loader precedence rules) is preserved unchanged except for additive schema and config fields.

## Glossary

- **Bot**: The `asset-discovery-bot` Python container that executes a single daily scan and then exits.
- **Scan_Run**: One end-to-end invocation of the orchestrator (`bot.run`), initiated by the Synology Task Scheduler via `docker exec`.
- **Pipeline_Mode**: The configured selection between the legacy v1 hard-threshold pipeline and the new composite-ranked pipeline. One of `hard_threshold` (v1) or `composite_rank` (new).
- **Value_Composite**: A per-ticker cross-sectional z-score average over the four value metrics: EV/EBIT (inverted so higher = cheaper), EV/Sales (inverted), Book-to-Market (B/M, higher = cheaper), and Shareholder Yield. A higher Value_Composite means "more attractively valued" relative to peers in the current Scan_Run.
- **Quality_Composite**: A per-ticker cross-sectional z-score average over the three quality metrics: Gross Profitability (GP/A), Earnings_Stability, and Low_Accruals. A higher Quality_Composite means "higher quality" relative to peers in the current Scan_Run.
- **Reversal_Signal**: A per-ticker measure of the trailing 1-month (21 trading days) price return, inverted so that lower returns yield higher reversal scores. Used as the short-term reversal input (Jegadeesh 1990). Replaces the v1 RSI crossover.
- **Composite_Score**: The weighted sum of Value_Composite, Quality_Composite, and Reversal_Signal produced by the Composite_Scorer for a single ticker in a single Scan_Run.
- **Composite_Rank**: The integer rank (1 = best) assigned to each scored ticker in descending order of Composite_Score, within its GICS sector when sector-neutral ranking is enabled, or cross-sectionally otherwise.
- **Top_N**: The configured count of candidates emitted per Scan_Run in `composite_rank` mode (default 20).
- **Sector_Neutral_Ranking**: A ranking mode where each individual metric (every z-score and the final Composite_Score) is computed only against other tickers in the same GICS sector for that Scan_Run. Prevents loading on whichever sector happens to be cheap in absolute terms.
- **GICS_Sector**: The Global Industry Classification Standard sector assigned to a ticker (one of 11 sectors: Energy, Materials, Industrials, Consumer Discretionary, Consumer Staples, Health Care, Financials, Information Technology, Communication Services, Utilities, Real Estate). Required for sector-neutral ranking.
- **Sector_Source**: The configured source of GICS sector data for each ticker. Options include Wikipedia (S&P 500 table), iShares ETF CSV holdings (Sector column), and SEC EDGAR SIC-to-GICS mapping.
- **EV**: Enterprise Value, computed as `market_cap + total_debt - cash_and_equivalents`.
- **EBIT**: Earnings Before Interest and Taxes, sourced from EDGAR operating income XBRL concepts.
- **B/M**: Book-to-Market ratio, computed as `book_value_of_equity / market_cap`.
- **Shareholder_Yield**: `(dividends_paid + share_buybacks) / market_cap`, trailing twelve months.
- **GP/A**: Gross Profitability over Total Assets, computed as `(revenue - cost_of_goods_sold) / total_assets` (Novy-Marx 2013).
- **Earnings_Stability**: The negative of the standard deviation of annual EPS over the trailing 5 fiscal years, normalized to z-score. Higher values (less volatility) mean more stable earnings.
- **Low_Accruals**: The negative of `(net_income - operating_cash_flow) / total_assets`, normalized to z-score. Higher values (smaller accruals relative to assets) mean earnings are more cash-backed.
- **Composite_Scorer**: The component (`bot.scoring`) that computes Value_Composite, Quality_Composite, Reversal_Signal, and Composite_Score for every ticker in the active universe.
- **Ranker**: The component (`bot.ranker`) that takes Composite_Scores and produces Composite_Ranks, optionally sector-neutral, with deterministic tie-breaking.
- **Fundamentals_Service** (`bot.fundamentals`): The component that fetches the extended fundamentals (EV components, EBIT, revenue, book value, dividends, buybacks, COGS, total assets, 5-year EPS history, net income, operating cash flow) from SEC EDGAR XBRL, gated by the cache.
- **Extended_Fundamentals**: The expanded record stored in `fundamentals_cache` that carries every input field needed by the Composite_Scorer, in addition to the v1 fields.
- **Fundamentals_Schema_Version**: An integer column on `fundamentals_cache` that identifies the set of fields populated in each row. Used to invalidate pre-upgrade cache rows that lack the new fields.
- **Top_N_Candidate**: A ticker selected for emission in a Scan_Run because its Composite_Rank is in the top `N` for its sector (sector-neutral mode) or cross-sectionally (otherwise). Replaces v1's High_Conviction_Candidate when `Pipeline_Mode = composite_rank`.
- **Config_Loader** (`bot.config`): Preserved from v1. Loads `config.yaml`, applies `ADB_*` environment-variable overrides, loads secrets from `/run/secrets/*`, and validates via Pydantic.
- **Repository** (`bot.repo`): Preserved from v1. The SQLAlchemy Core 2.0 data access layer.
- **Notifier** (`bot.notify`): Preserved from v1. POSTs rich embeds to Discord webhooks.

## Requirements

### Requirement 1: Value Composite Scoring

**User Story:** As a researcher, I want the Bot to rank each ticker on a multi-metric value composite — EV/EBIT, EV/Sales, B/M, and Shareholder Yield combined as a cross-sectional z-score average — so that cheap tickers are identified by a robust multi-ratio signal rather than a single P/E threshold.

#### Acceptance Criteria

1. WHEN `Pipeline_Mode = composite_rank`, THE Composite_Scorer SHALL compute a Value_Composite score for every ticker that has non-null values for all four value metrics (EV/EBIT, EV/Sales, B/M, Shareholder_Yield).
2. THE Composite_Scorer SHALL compute each value metric's z-score against the cross-section of tickers being scored in the current Scan_Run, where a z-score is defined as `(value - mean) / stddev` with `stddev` computed from the same cross-section.
3. THE Composite_Scorer SHALL invert the EV/EBIT and EV/Sales z-scores (multiply by -1) before averaging, so that a smaller ratio contributes a larger positive component to Value_Composite.
4. THE Composite_Scorer SHALL compute Value_Composite as the arithmetic mean of the four z-scores, after any inversions, such that a higher Value_Composite represents a more attractively valued ticker relative to its peer group.
5. IF a ticker has a null or non-finite value for any of the four value metrics, THEN THE Composite_Scorer SHALL exclude that ticker from Value_Composite scoring for the current Scan_Run and SHALL log the exclusion at DEBUG level with the ticker and the missing metric names.
6. WHERE Sector_Neutral_Ranking is enabled (default), THE Composite_Scorer SHALL compute each value metric's z-score using only the subset of tickers in the same GICS sector, not the full cross-section.
7. IF a GICS sector in the current Scan_Run contains fewer than the configured `min_sector_size` tickers (default 5) with non-null value metrics, THEN THE Composite_Scorer SHALL fall back to cross-sectional z-scoring for that sector's tickers and SHALL log a WARN identifying the sector and its member count.

### Requirement 2: Quality Composite Scoring

**User Story:** As a researcher, I want the Bot to rank each ticker on a multi-metric quality composite — Gross Profitability (GP/A), Earnings Stability, and Low Accruals combined as a cross-sectional z-score average — so that quality is measured by Novy-Marx's academically validated gross-profitability signal plus earnings persistence and accruals quality rather than a single FCF-yield threshold.

#### Acceptance Criteria

1. WHEN `Pipeline_Mode = composite_rank`, THE Composite_Scorer SHALL compute a Quality_Composite score for every ticker that has non-null values for all three quality metrics (GP/A, Earnings_Stability, Low_Accruals).
2. THE Composite_Scorer SHALL compute GP/A as `(revenue - cost_of_goods_sold) / total_assets` using the trailing twelve months of revenue and COGS and the most recently reported total_assets.
3. THE Composite_Scorer SHALL compute Earnings_Stability as the negative of the standard deviation of the trailing five fiscal years of diluted EPS, using at least three non-null annual values, and SHALL set Earnings_Stability to null when fewer than three annual EPS values are available.
4. THE Composite_Scorer SHALL compute Low_Accruals as the negative of `(net_income - operating_cash_flow) / total_assets`, using trailing twelve months of net income and operating cash flow and the most recently reported total_assets.
5. THE Composite_Scorer SHALL compute each quality metric's z-score against the cross-section of tickers being scored in the current Scan_Run, so that a higher value on every metric contributes a larger positive component to Quality_Composite.
6. THE Composite_Scorer SHALL compute Quality_Composite as the arithmetic mean of the three z-scores, such that a higher Quality_Composite represents a higher-quality ticker relative to its peer group.
7. WHERE Sector_Neutral_Ranking is enabled (default), THE Composite_Scorer SHALL compute each quality metric's z-score using only the subset of tickers in the same GICS sector.
8. IF a ticker has a null or non-finite value for any of the three quality metrics, THEN THE Composite_Scorer SHALL exclude that ticker from Quality_Composite scoring for the current Scan_Run and SHALL log the exclusion at DEBUG level.

### Requirement 3: Short-Term Reversal Signal

**User Story:** As a researcher, I want the Bot to replace the v1 RSI crossover with a short-term reversal signal based on the trailing 1-month prior return, so that the timing component of the scan rests on Jegadeesh (1990)'s academically supported reversal effect rather than on RSI folklore.

#### Acceptance Criteria

1. WHEN `Pipeline_Mode = composite_rank`, THE Composite_Scorer SHALL compute a Reversal_Signal for every ticker with at least `reversal_lookback_days + 1` daily close observations (default 21 trading days).
2. THE Composite_Scorer SHALL compute each ticker's raw reversal input as the trailing 21-trading-day price return: `(close_today - close_21_days_ago) / close_21_days_ago`.
3. THE Composite_Scorer SHALL compute Reversal_Signal as the cross-sectional z-score of the negated raw reversal input, so that a lower trailing 1-month return contributes a larger positive Reversal_Signal.
4. WHERE Sector_Neutral_Ranking is enabled (default), THE Composite_Scorer SHALL compute the Reversal_Signal z-score using only the subset of tickers in the same GICS sector.
5. WHERE the configuration field `reversal.enabled` is false, THE Composite_Scorer SHALL set Reversal_Signal to zero for every ticker and SHALL omit the reversal term from Composite_Score without otherwise altering the pipeline.
6. IF a ticker has fewer than `reversal_lookback_days + 1` daily close observations, THEN THE Composite_Scorer SHALL exclude that ticker from Reversal_Signal computation for the current Scan_Run and SHALL log a WARN identifying the ticker and its observation count.
7. THE Composite_Scorer SHALL NOT reuse the v1 RSI today / RSI yesterday fields for any ranking computation in `composite_rank` mode.

### Requirement 4: Sector-Neutral Ranking

**User Story:** As a researcher, I want each metric to be z-scored within the ticker's GICS sector before combining into the Composite_Score, so that the final ranking does not simply load on whichever sector is currently cheap in absolute terms.

#### Acceptance Criteria

1. THE Ranker SHALL accept a configuration flag `ranking.sector_neutral` (default true) that selects between sector-neutral ranking (z-score within sector, then rank within sector) and cross-sectional ranking (z-score across all tickers, then rank across all tickers).
2. WHERE `ranking.sector_neutral = true`, THE Composite_Scorer SHALL partition the scored universe by the `sector` column on `asset_universe` and compute every z-score (value, quality, reversal) against only the tickers in that partition.
3. WHERE `ranking.sector_neutral = true`, THE Ranker SHALL assign Composite_Rank independently within each sector, so that a ticker's rank reflects its position among same-sector peers and not among the full universe.
4. IF a ticker's `asset_universe.sector` value is null, THEN THE Ranker SHALL treat the ticker as belonging to a synthetic sector named `unknown` for the current Scan_Run, SHALL apply the same `min_sector_size` fallback rule as any other sector, and SHALL log a WARN reporting the count of tickers in the `unknown` sector.
5. WHERE `ranking.sector_neutral = false`, THE Ranker SHALL compute one cross-sectional ranking across all scored tickers, treating the full universe as a single peer group.
6. THE Ranker SHALL produce a deterministic ranking such that ties in Composite_Score are broken first by ticker symbol in ascending lexicographic order.
7. FOR ALL Scan_Runs where Sector_Neutral_Ranking is enabled, THE set of ranks assigned within each sector SHALL be `{1, 2, ..., k}` where `k` is the count of scored tickers in that sector (no gaps, no duplicates).

### Requirement 5: Top-N Composite-Rank Output

**User Story:** As a researcher, I want the Bot to emit the top N composite-ranked candidates per Scan_Run (configurable, default 20) in ranked order, rather than an unranked pass/fail set, so that I can review candidates in priority order and so that the output extracts more information from the cross-section than binary pass/fail.

#### Acceptance Criteria

1. WHEN `Pipeline_Mode = composite_rank` and the Composite_Scorer and Ranker have produced ranks, THE orchestrator SHALL select the top `N` candidates where `N = ranking.top_n` (default 20).
2. WHERE `ranking.sector_neutral = true`, THE orchestrator SHALL interpret `top_n` as `top_n_per_sector` (default 2), so that the emitted set contains the best `top_n_per_sector` candidates from each GICS sector rather than the overall top `N`.
3. THE orchestrator SHALL order the emitted candidates by Composite_Score descending (best first), with ties broken by ticker ascending (Requirement 4.6).
4. THE orchestrator SHALL emit one Discord embed per Top_N_Candidate, in ranked order, with each embed including the ticker, its GICS sector, its Composite_Rank, its Composite_Score, the Value_Composite, the Quality_Composite, the Reversal_Signal, and the headline fields preserved from v1 (close, market_cap, latest_headline, headline_url).
5. THE orchestrator SHALL persist one `daily_scans` row per Top_N_Candidate before posting the corresponding Discord embed (at-least-once delivery with DB-side de-duplication, preserved from v1 Requirement 5.5).
6. WHEN fewer than `N` tickers have a non-null Composite_Score in the current Scan_Run, THE orchestrator SHALL emit all scored tickers in ranked order and SHALL NOT fabricate placeholder entries.
7. IF `top_n` or `top_n_per_sector` is configured to zero or a negative value, THEN THE Config_Loader SHALL reject the configuration with a validation error before any I/O.
8. THE emitted Discord embed SHALL NOT contain language that instructs the reader to buy, sell, hold, or allocate capital (scope discipline preserved from v1 Requirement 10.4).

### Requirement 6: Extended Fundamentals Retrieval

**User Story:** As a researcher, I want the Fundamentals_Service to fetch the extended fundamentals required by the composite scorer (EV components, EBIT, revenue, book value, dividends, buybacks, COGS, total assets, 5-year EPS history, net income, operating cash flow) from SEC EDGAR XBRL, with the same cache gating semantics as v1, so that free-tier cost and external call volume are unchanged.

#### Acceptance Criteria

1. WHEN the Fundamentals_Service is asked for a ticker's fundamentals in `composite_rank` mode, THE Fundamentals_Service SHALL return an Extended_Fundamentals record populated from the EDGAR CompanyFacts XBRL taxonomy with the following fields in addition to v1's: `total_debt`, `cash_and_equivalents`, `ebit`, `revenue_ttm`, `book_value_of_equity`, `dividends_paid_ttm`, `share_buybacks_ttm`, `cogs_ttm`, `total_assets`, `annual_eps_5y` (list of up to 5 annual EPS values), `net_income_ttm`, and `operating_cash_flow_ttm`.
2. THE Fundamentals_Service SHALL map each new field to one or more EDGAR XBRL concepts in documented fallback order (for example, EBIT from `OperatingIncomeLoss` falling back to `IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest` and then computed from `NetIncomeLoss + InterestExpense + IncomeTaxExpenseBenefit` when the primary concepts are missing).
3. THE Fundamentals_Service SHALL preserve the v1 cache gating semantics: a row whose `fetched_at` is within `cache.fundamentals_staleness_days` of `now()` SHALL be served from cache without any EDGAR call (Requirement 3.2 from v1).
4. THE Fundamentals_Service SHALL NOT call EDGAR for any ticker that is not in the active universe for the current Scan_Run (preserved from v1).
5. IF EDGAR returns no CompanyFacts data for a ticker's CIK, THEN THE Fundamentals_Service SHALL return an Extended_Fundamentals record with every numeric field set to null, SHALL cache the null-valued record for the staleness window, and SHALL NOT raise (preserved graceful-degradation behaviour from the universe-expansion spec).
6. IF any individual XBRL concept lookup fails (missing concept, malformed value, fewer than the required quarters of history for TTM computation), THEN THE Fundamentals_Service SHALL set that specific field to null on the returned record, preserve the remaining fields, and SHALL NOT fail the entire record.
7. THE Fundamentals_Service SHALL respect the existing EDGAR rate limit of 10 requests per second via the existing `EdgarRateLimiter`, with no change to the rate-limit contract introduced in the universe-expansion spec.
8. WHEN the Fundamentals_Service writes an Extended_Fundamentals record to `fundamentals_cache`, THE record SHALL include a `fundamentals_schema_version` integer field identifying it as a v2 row.

### Requirement 7: GICS Sector Source

**User Story:** As a researcher using sector-neutral ranking, I want a consistent source of GICS sector for every ticker in the universe, so that sector-neutral z-scores are computed against the correct peer group and so that tickers with unknown sectors are surfaced rather than silently mis-classified.

#### Acceptance Criteria

1. THE Bot SHALL populate the existing `asset_universe.sector` column with a GICS sector string for every ticker produced by a successful Universe_Source fetch.
2. WHERE the Universe_Source is `kind: wikipedia_table`, THE sector value SHALL come from the "GICS Sector" column on the Wikipedia S&P 500 constituents table (preserved from v1).
3. WHERE the Universe_Source is `kind: etf_holdings_csv`, THE sector value SHALL come from the "Sector" column on the iShares holdings CSV (preserved from universe-expansion Requirement 2.3).
4. WHERE a ticker appears in multiple Universe_Sources with conflicting sector values, THE Universe_Service SHALL select the sector value in the configuration order of the sources (first source wins) and SHALL log an INFO line identifying the ticker, the conflicting values, and the chosen value.
5. IF a ticker has a null or empty sector value after all Universe_Source fetches, THEN THE Ranker SHALL treat the ticker's sector as `unknown` (Requirement 4.4) rather than aborting the scan.
6. THE set of sector values recognised for sector-neutral ranking SHALL be exactly the 11 GICS sectors (Energy, Materials, Industrials, Consumer Discretionary, Consumer Staples, Health Care, Financials, Information Technology, Communication Services, Utilities, Real Estate) plus the synthetic `unknown` sector; any other sector string SHALL be logged at WARN with a count of affected tickers and grouped under `unknown` for ranking purposes.
7. THE Bot SHALL NOT introduce a new external call solely for sector classification in this spec; the GICS sector value SHALL be sourced entirely from data already being fetched by the Universe_Service (Wikipedia table or iShares CSV).

### Requirement 8: Database Schema Evolution

**User Story:** As a developer, I want the `fundamentals_cache` and `daily_scans` tables to carry the new fields needed by the composite scorer with a backward-compatible additive migration, so that the upgrade is idempotent, does not require a full table rewrite, and preserves v1 rows as valid.

#### Acceptance Criteria

1. THE Repository SHALL add the Extended_Fundamentals columns (`total_debt`, `cash_and_equivalents`, `ebit`, `revenue_ttm`, `book_value_of_equity`, `dividends_paid_ttm`, `share_buybacks_ttm`, `cogs_ttm`, `total_assets`, `annual_eps_5y` as `NUMERIC[]`, `net_income_ttm`, `operating_cash_flow_ttm`, `fundamentals_schema_version` as `SMALLINT NOT NULL DEFAULT 1`) to the `fundamentals_cache` table via a new migration file (`003_composite_ranked_scoring.sql`).
2. THE Repository SHALL add composite-scoring columns (`value_composite`, `quality_composite`, `reversal_signal`, `composite_score`, `composite_rank`, `sector_at_rank`, `pipeline_mode`) to the `daily_scans` table in the same migration, where numeric fields are nullable and `pipeline_mode` is a `VARCHAR(32)` nullable field defaulting to NULL for pre-upgrade rows.
3. THE migration SHALL use `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` with server defaults so that existing rows populate without a full table rewrite (preserved from universe-expansion Requirement 4.5).
4. THE migration SHALL be idempotent: re-running it on a database that already has the new columns SHALL be a no-op (preserved from universe-expansion Requirement 4.4).
5. THE existing v1 columns on `fundamentals_cache` and `daily_scans` SHALL remain unchanged in type and nullability so that v1 code paths continue to write and read the same shape.
6. WHEN the Bot loads a cached `fundamentals_cache` row with `fundamentals_schema_version < 2`, THE Fundamentals_Service SHALL treat the row as cache-stale for `composite_rank` mode and refetch the full Extended_Fundamentals record from EDGAR within the normal staleness gating.
7. WHEN the Bot writes a `daily_scans` row in `hard_threshold` mode, THE composite-scoring columns SHALL be left null.
8. WHEN the Bot writes a `daily_scans` row in `composite_rank` mode, THE `close`, `pct_above_low`, `rsi_today`, `rsi_yesterday`, `pe_ratio`, `pe_5y_avg`, and `fcf_yield` columns (all `NOT NULL` in v1) SHALL be populated with values obtained from the current Scan_Run; WHERE the composite pipeline does not produce a native value for one of these legacy columns, THE orchestrator SHALL populate it with a documented fallback value (for example, `0` for `rsi_today` and `rsi_yesterday` when the reversal signal has replaced RSI) and SHALL persist the true composite values in the new composite-scoring columns, so the `NOT NULL` contract on legacy columns is preserved without a destructive schema change.

### Requirement 9: Configuration Changes

**User Story:** As an operator, I want a single validated config section for the composite-ranked pipeline (weights, top-N, sector-neutral flag, reversal lookback, Pipeline_Mode switch) with the same Pydantic validation and precedence rules as v1, so that I can experiment with weights and thresholds without touching code and so that the upgrade is opt-in.

#### Acceptance Criteria

1. THE Config_Loader SHALL accept a `pipeline.mode` string field with the closed value set `{hard_threshold, composite_rank}` and a default of `hard_threshold` (backward compatibility — v1 behaviour when the field is absent).
2. THE Config_Loader SHALL accept a `scoring` section containing `value_weight`, `quality_weight`, and `reversal_weight` float fields (defaults 1.0, 1.0, 0.5) each constrained to the closed interval `[0.0, 10.0]`, where the three weights are applied to the corresponding composites when computing Composite_Score.
3. THE Config_Loader SHALL reject a `scoring` section where every weight is zero, because that configuration produces a constant Composite_Score and a degenerate ranking.
4. THE Config_Loader SHALL accept a `ranking` section containing `sector_neutral` (bool, default true), `top_n` (int, default 20, constrained to `[1, 500]`), `top_n_per_sector` (int, default 2, constrained to `[1, 50]`), `min_sector_size` (int, default 5, constrained to `[1, 50]`), and `reversal_enabled` (bool, default true) fields.
5. THE Config_Loader SHALL accept a `reversal` section containing `lookback_days` (int, default 21, constrained to `[5, 252]`) that governs the Reversal_Signal computation window.
6. THE Config_Loader SHALL continue to load the v1 `layer1`, `layer2`, `layer3`, and `layer4` sections and SHALL NOT reject configs that contain both the v1 layer sections and the new composite sections; WHEN `pipeline.mode = hard_threshold`, the composite sections SHALL be ignored at run time; WHEN `pipeline.mode = composite_rank`, the layer sections SHALL be ignored at run time.
7. THE Config_Loader SHALL continue to apply the precedence rule `ADB_*` environment variable > `config.yaml` > Pydantic default (preserved from v1 Requirement 6.2) for every new field.
8. WHEN the Bot starts a Scan_Run, THE Bot SHALL log a single INFO line listing the active `pipeline.mode`, the active composite weights, and the active ranking options, in addition to the v1 non-default-values log line, so the operator can audit which pipeline is executing.
9. WHEN a `daily_scans` row is inserted in `composite_rank` mode, THE Repository SHALL persist the full `AppConfig.model_dump()` into `config_snapshot` (preserved from v1 Requirement 6.7), which SHALL include the active composite weights and ranking options so that the produced rank is reproducible.

### Requirement 10: Backward Compatibility with v1 Hard-Threshold Pipeline

**User Story:** As an existing v1 operator, I want to keep running the 4-layer hard-threshold pipeline unchanged until I choose to switch to composite ranking, so that the upgrade is opt-in and a single config flag selects between the two pipelines.

#### Acceptance Criteria

1. WHEN `pipeline.mode = hard_threshold`, THE Bot SHALL execute the v1 4-layer pipeline (L1 anchor → L2 RSI crossover → L3 P/E vs 5-year average → L4 FCF yield floor) and SHALL emit High_Conviction_Candidates with the v1 Discord embed shape, unchanged from v1.
2. WHEN `pipeline.mode = hard_threshold`, THE Fundamentals_Service SHALL populate only the v1 fields on the returned `Fundamentals` record and SHALL NOT require the Extended_Fundamentals fields, so v1 operators do not incur additional EDGAR calls.
3. WHEN `pipeline.mode = hard_threshold` is the default and a config file containing only the v1 `layer1..layer4` sections is loaded, THE Bot SHALL produce the same `daily_scans` rows and the same Discord embeds as it did before this feature.
4. WHEN `pipeline.mode = composite_rank`, THE Bot SHALL execute the new composite-rank pipeline (universe sync → technical snapshot for reversal → enrich with Extended_Fundamentals → compute composites → rank within sector → select Top_N_Candidates) and SHALL NOT execute the v1 L1–L4 hard-threshold filters.
5. THE `daily_scans` table SHALL hold rows from both modes over time, distinguishable by the `pipeline_mode` column (Requirement 8.2), so that historical scan output from v1 is not lost when an operator switches modes.
6. THE unique constraint `UNIQUE (ticker, scan_date)` on `daily_scans` SHALL continue to apply in both modes, so a Scan_Run cannot emit two rows for the same ticker on the same date regardless of pipeline (preserved from v1 Requirement 7.3).
7. THE v1 correctness invariants (Requirements 11.1–11.12 in v1) SHALL continue to hold for any Scan_Run where `pipeline.mode = hard_threshold`, unchanged by this feature.

### Requirement 11: Correctness Invariants

**User Story:** As a reviewer of the Bot's output, I want the composite-ranked pipeline to preserve a set of universally quantified correctness invariants across all valid inputs, so that every ranked candidate can be trusted to reflect the declared composite semantics and so that property-based tests can verify the implementation.

#### Acceptance Criteria

1. FOR ALL Scan_Runs in `composite_rank` mode, THE set of scored tickers SHALL be a subset of the active universe for that run (ranking monotonicity relative to the universe).
2. FOR ALL Scan_Runs in `composite_rank` mode, AND FOR ALL tickers `t` with a non-null Composite_Score, `t.composite_score` SHALL equal `value_weight * t.value_composite + quality_weight * t.quality_composite + reversal_weight * t.reversal_signal` (composite score formula), computed from the active `AppConfig.scoring` weights.
3. FOR ALL Scan_Runs in `composite_rank` mode, given the same input universe, the same fundamentals values, the same price history, the same sector assignments, and the same `AppConfig`, THE Composite_Scorer and Ranker SHALL produce identical Composite_Scores and Composite_Ranks (determinism / scoring reproducibility).
4. FOR ALL Scan_Runs in `composite_rank` mode where Sector_Neutral_Ranking is enabled, AND FOR ALL sectors `s` with at least `min_sector_size` scored tickers, every z-score contributing to Composite_Score for a ticker in `s` SHALL have been computed using only tickers in `s` (sector-neutrality closure).
5. FOR ALL Scan_Runs in `composite_rank` mode where Sector_Neutral_Ranking is enabled, THE set of Composite_Ranks assigned within each sector `s` SHALL be the contiguous integer set `{1, 2, ..., k_s}` where `k_s = |scored_tickers_in_s|`, with no gaps and no duplicates (rank bijection within sector).
6. FOR ALL Scan_Runs in `composite_rank` mode, IF two tickers `t1` and `t2` tie on Composite_Score, THEN the ticker with the lexicographically smaller symbol SHALL receive the better (numerically smaller) rank (stable tie-breaking).
7. FOR ALL Scan_Runs in `composite_rank` mode, AT MOST `top_n_per_sector` rows SHALL be emitted per GICS sector when Sector_Neutral_Ranking is enabled, and AT MOST `top_n` rows SHALL be emitted total when Sector_Neutral_Ranking is disabled (top-N bound).
8. FOR ALL `(ticker, scan_date)` pairs, AT MOST one row SHALL exist in `daily_scans` with that pair, regardless of `pipeline_mode` (preserved alert idempotency from v1 Requirement 11.5).
9. FOR ALL Scan_Runs in `composite_rank` mode, every Top_N_Candidate emitted to Discord SHALL correspond to a `daily_scans` row inserted in the same Scan_Run, with the `composite_rank`, `composite_score`, `value_composite`, `quality_composite`, `reversal_signal`, and `sector_at_rank` columns populated (at-least-once delivery with reproducibility).
10. FOR ALL Scan_Runs, THE Bot SHALL NOT instruct a reader to buy, sell, hold, or allocate capital in any Discord embed, any log line, or any `daily_scans` text field (research-tool framing preserved from v1 Requirement 10.4).
11. FOR ALL Scan_Runs, IF `AppConfig.model_validate` fails or any required secret file under `/run/secrets/` is missing or empty, THEN THE Bot SHALL exit non-zero before any Wikipedia, yfinance, EDGAR, Discord, or PostgreSQL I/O is performed (strict config validation preserved from v1 Requirement 11.12, extended to the new composite sections).
12. FOR ALL Scan_Runs in `composite_rank` mode, THE v1 Tier-0 scope-discipline invariants SHALL continue to hold: the Bot SHALL NOT place trades, submit orders, size positions, compute entry or exit prices, compute stop-loss or take-profit levels, persist a paper-trading ledger, or construct or rebalance a portfolio (preserved from v1 Requirement 10.1–10.4).
