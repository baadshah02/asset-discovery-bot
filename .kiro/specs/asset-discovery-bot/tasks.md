# Implementation Plan: Asset Discovery Bot

## Overview

Convert the feature design into a series of prompts for a code-generation LLM that will implement each step with incremental progress. Each prompt builds on the previous ones and ends with wiring things together. There should be no hanging or orphaned code that isn't integrated into a previous step. Focus ONLY on tasks that involve writing, modifying, or testing code.

The build order follows the 7-component architecture from the design, in dependency order:

1. **Bootstrap** — project scaffolding, pinned dependencies, Dockerfile, docker-compose.yml, Postgres service, secrets wiring, SQL DDL migration
2. **Foundations** — `bot.config` (Pydantic settings + secrets), `bot.repo` (SQLAlchemy Core 2.0 data access)
3. **Leaf data adapters** — `bot.universe` (Wikipedia scrape + diff), `bot.prices` (yfinance + RSI), `bot.fundamentals` (FMP + cache gate)
4. **Filter pipeline** — `bot.filters` (L1 → L2 → L3 → L4)
5. **Notifier** — `bot.notify` (Discord rich embeds + backoff)
6. **Orchestrator** — `bot.run` (wires every module into a single scan)
7. **Integration & smoke** — testcontainers-postgres, VCR fixtures for FMP/yfinance/Discord, full-run smoke test
8. **Entrypoint** — Docker entrypoint, deployment playbook

Property-based tests use `hypothesis` and map 1:1 to the 12 correctness invariants in Requirement 11. They are sub-tasks under the module they validate so that errors are caught as early as possible.

Tasks marked with `*` are optional and can be skipped for a fast MVP.

## Tasks

- [x] 1. Bootstrap project scaffolding and dependency manifest
  - Create the `bot/` Python package directory, `tests/` directory, and `config/` directory at the repo root
  - Add `pyproject.toml` (or `setup.cfg`) declaring `asset-discovery-bot` with Python `==3.11.*`
  - Add `requirements.txt` with every runtime dep pinned to exact versions from the design's Dependencies table: `pandas==2.2.3`, `sqlalchemy==2.0.36`, `psycopg[binary]==3.1.20`, `yfinance==0.2.50`, `requests==2.32.3`, `beautifulsoup4==4.12.3`, `lxml==5.3.0`, `tenacity==8.5.0`, `pydantic==2.9.2`, `pyyaml==6.0.2`
  - Add `requirements-dev.txt` with `pytest`, `pytest-cov`, `hypothesis`, `testcontainers[postgres]`, `responses`, `ruff`, `mypy` (pinned)
  - Add `.gitignore` entries for `__pycache__/`, `.venv/`, `build.log`, `*.egg-info/`, `/volume1/` local test mounts
  - _Requirements: 9.9_
◊
- [x] 2. Container and Compose definitions
  - [x] 2.1 Write `Dockerfile` using `python:3.11-slim` base, non-root user, `pip install -r requirements.txt`, `COPY bot/ /app/bot/`, `WORKDIR /app`, no `EXPOSE`
    - Target steady ~150 MB RAM, peak ~300 MB
    - _Requirements: 9.1, 9.6_
  - [x] 2.2 Write `docker-compose.yml` defining two services (`bot`, `db`) on an internal bridge network, three Docker secrets (`fmp_api_key`, `db_password`, `discord_webhook_url`), and three named host volumes (`pgdata`, `logs`, `config`)
    - Set `mem_limit: 512m` on the `db` service
    - Do NOT bind any port to the host for either service
    - Mount `/run/secrets/*` into the `bot` service
    - Mount `/app/config/config.yaml` read-only in the `bot` service
    - _Requirements: 9.2, 9.6, 9.7, 9.8_
  - [x] 2.3 Write a sample `config/config.example.yaml` matching every field in the design's `AppConfig` with default values (do NOT commit any real secret)
    - _Requirements: 6.1_
  - [x] 2.4 Add a Postgres healthcheck to the `db` service in `docker-compose.yml` (`pg_isready -U <user> -d <db>`, 10s interval, 5s timeout, 5 retries) and a `depends_on: { db: { condition: service_healthy } }` on the `bot` service so the orchestrator does not start until Postgres accepts connections
    - _Requirements: 7.1, 8.5, 9.2_
  - [x] 2.5 Checkpoint — ensure `docker compose config` parses without errors, ask the user if questions arise

- [x] 3. Database schema migration
  - [x] 3.1 Create `bot/migrations/001_init.sql` with the three-table DDL from the design: `asset_universe`, `fundamentals_cache`, `daily_scans`
    - Include the partial index `ix_asset_universe_active ON asset_universe (ticker) WHERE removed_on IS NULL`
    - Include `ix_fundamentals_cache_fetched` and `ix_daily_scans_date` indexes
    - Include the `uq_scan_per_day UNIQUE (ticker, scan_date)` constraint
    - Include the `config_snapshot JSONB NOT NULL` column on `daily_scans`
    - _Requirements: 7.1, 7.2, 7.3, 6.7_
  - [x] 3.2 Write `bot/migrations/run_migrations.py` that applies every `.sql` file in `bot/migrations/` in lexicographic order against the configured DB URL, using `CREATE TABLE IF NOT EXISTS` semantics so re-runs are idempotent
    - _Requirements: 7.1_
  - [ ] 3.3* Write a unit test asserting `run_migrations.py` is idempotent (running twice leaves the schema unchanged)
    - _Requirements: 7.1_

- [x] 4. Config loader (`bot.config`)
  - [x] 4.1 Implement `bot/config.py` with every Pydantic model from the design: `Layer1Config`, `Layer2Config`, `Layer3Config`, `Layer4Config`, `CacheConfig`, `UniverseConfig`, `NotificationConfig`, `FmpConfig`, `YFinanceConfig`, `LoggingConfig`, `AppConfig` (with `model_config = {"frozen": True}`), and `Secrets`
    - Implement the `Layer1Config._max_gt_min` validator so `pct_above_low_max > pct_above_low_min`
    - Constrain `CacheConfig.fundamentals_staleness_days` to `[1, 90]`
    - _Requirements: 6.1, 6.5, 6.8, 6.9_
  - [x] 4.2 Implement `load_config(config_path, secrets_dir) -> tuple[AppConfig, Secrets]` that: (a) loads YAML, (b) applies `ADB_*` env-var overrides with `__` nesting, (c) reads each secret from `/run/secrets/<name>`, (d) validates via Pydantic, (e) raises before any I/O if validation fails
    - Precedence: env var (highest) → yaml → Pydantic default
    - Never log any field of `Secrets`
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 9.8_
  - [x] 4.3 Implement `AppConfig.diff_from_defaults()` helper that returns a dict of non-default values for the startup log line
    - _Requirements: 6.6_
  - [ ] 4.4* Write unit tests for `load_config`: valid YAML passes; bad values (negative FCF threshold, `rsi_oversold=150`, `fundamentals_staleness_days=200`, `pct_above_low_max < pct_above_low_min`) raise; missing secret file raises; env-var override wins over YAML
    - _Requirements: 6.1, 6.2, 6.4, 6.8, 6.9_
  - [ ] 4.5* Write property-based test for strict config validation
    - **Property 12: Strict config validation** — bad config → no I/O
    - **Validates: Requirements 11.12, 6.4**
    - Use `hypothesis` to generate Pydantic field values drawn from invalid domains (negative thresholds, RSI out of `[0, 100]`, `pct_above_low_max ≤ pct_above_low_min`, `fundamentals_staleness_days ∉ [1, 90]`). Assert that `load_config` raises `ValidationError` and no monkeypatched network/DB/Discord function was called.

- [x] 5. Repository layer (`bot.repo`)
  - [x] 5.1 Implement `bot/repo.py` with `Repository(engine)` and a `transaction()` context manager using SQLAlchemy Core 2.0 (`Table`, `MetaData`, `insert`, `select`, `update`)
    - Declare `asset_universe`, `fundamentals_cache`, `daily_scans` as `Table` objects matching the DDL
    - Never open a connection outside a context manager
    - _Requirements: 7.1, 7.5, 7.7_
  - [x] 5.2 Implement `load_universe() -> set[str]` returning only tickers where `removed_on IS NULL`, and `upsert_universe(tickers, as_of)` using `INSERT ... ON CONFLICT` plus a bulk `UPDATE ... SET removed_on = :as_of WHERE ticker IN (:removed)` for tickers no longer in the scrape
    - _Requirements: 7.1, 7.2, 7.6_
  - [x] 5.3 Implement `load_fundamentals(ticker) -> Fundamentals | None` and `upsert_fundamentals(f)` using `INSERT ... ON CONFLICT (ticker) DO UPDATE`
    - _Requirements: 7.1, 7.2, 3.1, 3.3_
  - [x] 5.4 Implement `insert_scan(candidate, scan_date, config_snapshot)` that persists `config_snapshot` into the JSONB column and raises `UniqueViolation` (or the SQLAlchemy equivalent) on `(ticker, scan_date)` collision
    - _Requirements: 7.1, 7.3, 7.4, 6.7_
  - [x] 5.5 Implement `recent_scans(days=30) -> list[dict]` for future operational use
    - _Requirements: 7.1_
  - [ ] 5.6* Write integration tests using `testcontainers[postgres]` with Postgres 15-alpine: apply the DDL, exercise every Repository method, assert the partial index is used, assert `uq_scan_per_day` raises on duplicate insert
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.6_
  - [ ] 5.7* Write property-based test for alert idempotency
    - **Property 5: Alert idempotency** — UNIQUE `(ticker, scan_date)` holds under re-runs
    - **Validates: Requirements 11.5, 7.3, 7.4**
    - Use `hypothesis` to generate arbitrary `(ticker, scan_date)` sequences with duplicates. Repeatedly call `insert_scan`; assert at most one row exists per pair and that the second insert raises/no-ops without mutating the row count.

- [x] 6. Universe service (`bot.universe`)
  - [x] 6.1 Implement `bot/universe.py` with `fetch_current_constituents() -> list[str]` that GETs the configured Wikipedia URL, parses the constituents table with BeautifulSoup + lxml, and returns a sorted unique ticker list
    - Use `tenacity` for retry with exponential backoff on transient HTTP errors
    - _Requirements: 1.1, 8.1_
  - [x] 6.2 Implement `UniverseDiff` frozen dataclass and `sync_universe(repo, cfg: UniverseConfig) -> UniverseDiff` that scrapes, diffs against `repo.load_universe()`, calls `repo.upsert_universe`, and returns the diff
    - Abort with a non-zero exit if scrape count is outside `[min_constituent_count, max_constituent_count]`
    - Do not mutate `asset_universe` on abort
    - _Requirements: 1.1, 1.2, 1.4, 1.5, 1.6, 8.1_
  - [ ] 6.3* Write unit tests for `sync_universe` with a mocked scraper and in-memory fake repo: fresh install, steady state, addition only, removal only, both, implausible count → abort
    - _Requirements: 1.2, 1.4, 1.5, 1.6_
  - [ ] 6.4* Write property-based test for watchdog completeness and disjoint diff
    - **Property 6: Watchdog completeness** — active universe == scrape after sync
    - **Validates: Requirements 11.6, 1.4, 1.5**
    - Use `hypothesis` to generate pairs `(previous_universe, scraped_universe)`. After `sync_universe`, assert `repo.load_universe() == set(scraped_universe)` and `diff.added ∩ diff.removed == ∅`.

- [x] 7. Price service (`bot.prices`)
  - [x] 7.1 Implement `bot/prices.py::download_price_history(tickers, cfg: YFinanceConfig) -> dict[str, pd.DataFrame]` using `yf.download(..., group_by='ticker')` with batches of `cfg.batch_size` and up to `cfg.retries_per_ticker` retries per ticker
    - On empty frame for a ticker, log a WARN naming the ticker and exclude from the result
    - _Requirements: 2.1, 2.5, 2.7, 8.2_
  - [x] 7.2 Implement `compute_rsi(close_series: pd.Series, period: int) -> pd.Series` using Wilder's smoothing (EMA with `α = 1/period`); handle `avg_loss == 0` → RSI = 100; result in `[0, 100]`
    - _Requirements: 2.4_
  - [x] 7.3 Implement `compute_technical_snapshot(frames, rsi_period) -> pd.DataFrame` with columns `ticker, close, low_52w, pct_above_low, rsi_today, rsi_yesterday`
    - Exclude tickers with fewer than `rsi_period + 1` observations (do NOT emit NaN)
    - Compute `pct_above_low = (close - low_52w) / low_52w`
    - _Requirements: 2.2, 2.3, 2.6_
  - [ ] 7.4* Write unit test for `compute_rsi` against Wilder's canonical reference series
    - _Requirements: 2.4_
  - [ ] 7.5* Write property-based test for RSI no-look-ahead
    - **Property 2: No RSI look-ahead**
    - **Validates: Requirements 11.2, 2.4**
    - Use `hypothesis.strategies.lists(floats(min_value=0.01, max_value=1e6), min_size=20, max_size=500)` to generate price series. For every day `i >= period`, assert `compute_rsi(p[:i+1]).iloc[i] == compute_rsi(p).iloc[i]` within numerical tolerance.

- [x] 8. Fundamentals service (`bot.fundamentals`)
  - [x] 8.1 Implement `bot/fundamentals.py` with `Fundamentals` frozen dataclass and `FmpClient(api_key, cfg: FmpConfig)` wrapping `/ratios`, `/cash-flow-statement`, `/profile`, `/press-releases` endpoints using `requests` and `tenacity` retry
    - On HTTP 429, set a run-scoped flag that stops further FMP calls for the rest of the run
    - _Requirements: 3.3, 3.7, 8.3_
  - [x] 8.2 Implement `get_fundamentals(ticker, repo, fmp_client, staleness_days) -> Fundamentals` with the cache gate:
    - Query `fundamentals_cache`; return cached row if `now() - fetched_at < staleness_days`
    - On miss/stale, call FMP endpoints, derive `pe_5y_avg = mean(per-year P/E for last 5 years)` and `fcf_yield = ttm_fcf / market_cap` (NULL if `market_cap <= 0`), upsert, return fresh record
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5_
  - [x] 8.3 Instrument a run-scoped FMP call counter to support the budget assertion
    - _Requirements: 9.5, 11.4_
  - [ ] 8.4* Write unit tests with `responses` fixtures: fresh cache → no FMP call; stale cache → 3 FMP calls per ticker; 429 → stop further FMP calls; `market_cap <= 0` → `fcf_yield is None`
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.7_
  - [ ] 8.5* Write property-based test for cache semantics
    - **Property 3: Cache freshness** — after `get_fundamentals`, `now() - fetched_at < staleness_days`
    - **Validates: Requirements 11.3, 3.2, 3.3**
    - Use `hypothesis` to generate arbitrary `(initial_cache_age_days, staleness_days ∈ [1, 90])` pairs. Assert the returned record's `fetched_at` is always within `staleness_days` of `now()`.
  - [ ] 8.6* Write property-based test for FMP budget bound
    - **Property 4: FMP budget bound** — `fmp_calls ≤ 3 × N` where N is cache-miss L2 survivors
    - **Validates: Requirements 11.4, 9.5, 3.6**
    - Use `hypothesis` to generate lists of L2-survivor tickers with arbitrary cache populations (fresh/stale/missing). Drive the pipeline through `get_fundamentals`; assert the run-scoped FMP call counter satisfies `counter ≤ 3 × cache_miss_count`.

- [x] 9. Filter pipeline (`bot.filters`)
  - [x] 9.1 Implement `bot/filters.py` with `ScanCandidate` frozen dataclass and pure-function layers `apply_layer_1(snapshot, cfg)`, `apply_layer_2(snapshot, cfg)`, `apply_layer_3(enriched, cfg)`, `apply_layer_4(layer3_survivors, cfg)`
    - Each layer returns a new DataFrame; never mutate input
    - Layer 1: `cfg.pct_above_low_min ≤ pct_above_low ≤ cfg.pct_above_low_max`
    - Layer 2: `rsi_yesterday < cfg.rsi_oversold AND rsi_today > cfg.rsi_recovery`
    - Layer 3: non-null P/E and `pe_ratio < pe_5y_avg`; if `require_positive_earnings`, also `pe_ratio > 0 AND pe_5y_avg > 0`
    - Layer 4: non-null `fcf_yield` and `fcf_yield > cfg.fcf_yield_min`
    - _Requirements: 4.1, 4.2, 4.4, 4.5, 4.6, 4.7, 4.8_
  - [x] 9.2 Implement `run_pipeline(universe, price_fetcher, fundamentals_fetcher, cfg) -> list[ScanCandidate]` that wires L1 → L2 → enrich L2 survivors only → L3 → L4 and preserves justifying fields on each candidate
    - Fundamentals are fetched ONLY for L2 survivors
    - _Requirements: 3.6, 4.3, 4.7_
  - [ ] 9.3* Write unit tests for each layer with fixed input tables covering pass/fail/edge boundary rows
    - _Requirements: 4.1, 4.2, 4.4, 4.5, 4.6_
  - [ ] 9.4* Write property-based test for sequential filter monotonicity
    - **Property 1: L4 ⊆ L3 ⊆ L2 ⊆ L1**
    - **Validates: Requirements 11.1, 4.1, 4.2, 4.4, 4.6**
    - Use `hypothesis` to generate arbitrary snapshots (arrays of rows with `close`, `low_52w`, `rsi_today`, `rsi_yesterday`, `pe_ratio`, `pe_5y_avg`, `fcf_yield`). Apply the four layers; assert `set(L4.ticker) ⊆ set(L3.ticker) ⊆ set(L2.ticker) ⊆ set(L1.ticker)`.
  - [ ] 9.5* Write property-based test for anchor range closure
    - **Property 7: Anchor range is closed**
    - **Validates: Requirements 11.7, 4.1**
    - Generate arbitrary snapshots and configs; for every row in `apply_layer_1(snapshot, cfg)`, assert `cfg.pct_above_low_min ≤ pct_above_low ≤ cfg.pct_above_low_max`.
  - [ ] 9.6* Write property-based test for strict RSI crossover on emitted candidates
    - **Property 8: Strict RSI crossover**
    - **Validates: Requirements 11.8, 4.2**
    - Generate arbitrary snapshots; for every candidate emitted by `apply_layer_2`, assert `rsi_yesterday < cfg.rsi_oversold` AND `rsi_today > cfg.rsi_recovery`.
  - [ ] 9.7* Write property-based test for quality-dominates-value
    - **Property 9: Quality dominates value**
    - **Validates: Requirements 11.9, 4.4, 4.6**
    - Generate arbitrary enriched rows; for every candidate emitted by `apply_layer_4(apply_layer_3(...))`, assert `pe_ratio < pe_5y_avg` AND `fcf_yield > cfg.fcf_yield_min`.

- [x] 10. Checkpoint — ensure all module-level tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 11. Notifier (`bot.notify`)
  - [x] 11.1 Implement `bot/notify.py::send_high_conviction(candidate, webhook_url, cfg: NotificationConfig)` rendering the rich embed from the design with `username = cfg.bot_username`, ticker, close, pct_above_low, RSI yesterday→today, P/E current/5y, FCF yield, latest headline (with URL when available)
    - Use `tenacity` with exponential backoff bounded by `backoff_initial_seconds` and `backoff_max_seconds`, retry up to `max_retries` on 429/5xx, honor `Retry-After` on 429
    - On non-retryable 4xx, raise `NotificationError`
    - Never log or embed any secret
    - _Requirements: 5.1, 5.3, 5.4, 5.6, 5.7_
  - [x] 11.2 Implement `send_watchdog(diff: UniverseDiff, webhook_url, cfg: NotificationConfig)` rendering the yellow universe-change embed listing added and removed tickers
    - _Requirements: 5.2, 5.3_
  - [x] 11.3 Ensure alert copy presents candidates as information for human review (no "buy / sell / allocate" language)
    - _Requirements: 10.4_
  - [ ] 11.4* Write unit tests using `responses`: 200 → one POST; 429 with `Retry-After` → respects header; 5xx → retries `max_retries` times then raises; non-retryable 4xx → raises immediately; success emits the exact embed schema from the design
    - _Requirements: 5.1, 5.2, 5.3, 5.4_
  - [ ] 11.5* Write property-based test for at-least-once alert delivery with DB-side de-duplication
    - **Property 10: At-least-once alert delivery**
    - **Validates: Requirements 11.10, 5.5, 7.4, 11.5**
    - Use `hypothesis` to generate sequences of candidates with duplicates across simulated re-runs. In each run: `insert_scan` first, then `send_high_conviction`. Assert at most one DB row per `(ticker, scan_date)` and that `send_high_conviction` was called for every successful insert.

- [x] 12. Orchestrator (`bot.run`)
  - [x] 12.1 Implement `bot/run.py::main() -> int` wiring every module exactly as shown in the design's orchestrator pseudocode and Example Usage: `load_config` → engine → `Repository` → `sync_universe` → (`send_watchdog` if diff non-empty) → `download_price_history` → `compute_technical_snapshot` → L1 → L2 → `get_fundamentals` per L2 survivor → L3 → L4 → per candidate: `insert_scan` then `send_high_conviction` (catch `UniqueViolation` as no-op)
    - Log a single INFO line with `cfg.diff_from_defaults()` at startup
    - Persist `cfg.model_dump()` as `config_snapshot` on every `daily_scans` insert
    - Return 0 on success; non-zero on any fatal error
    - _Requirements: 1.1, 2.1, 3.6, 4.3, 5.5, 6.6, 6.7_
  - [x] 12.2 Implement top-level error handling: fail-fast on config validation, Wikipedia abort, Postgres OperationalError; graceful degradation on yfinance empty frames and FMP 429 (continue with cached-only L3/L4)
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6_
  - [x] 12.3 Add `if __name__ == "__main__": raise SystemExit(main())` entrypoint
    - _Requirements: 8.6_
  - [ ] 12.4* Write a full-run smoke test: seed an in-memory fake repo (or testcontainers-postgres) with a small universe, mock Wikipedia / yfinance / FMP / Discord via `responses`, call `main()`, assert exit code 0, assert `daily_scans` rows have `config_snapshot`, assert exactly one Discord POST per candidate, assert no FMP call for L1/L2 failures
    - _Requirements: 3.6, 4.3, 5.5, 6.7, 9.3_
  - [ ] 12.5* Write property-based test for config reproducibility
    - **Property 11: Config reproducibility**
    - **Validates: Requirements 11.11, 6.7**
    - Use `hypothesis` to generate arbitrary valid `AppConfig` values. Run `main()` end-to-end (with mocked externals) that produces at least one `daily_scans` row. Assert `row.config_snapshot == cfg.model_dump()` field-for-field.

- [x] 13. Docker entrypoint
  - [x] 13.1 Add a `CMD ["python", "-m", "bot.run"]` to `Dockerfile` (or equivalent entrypoint shell script that runs migrations then `bot.run`)
    - _Requirements: 8.6, 9.3_
  - [x] 13.2 Add a one-shot `migrate` entrypoint invocation (e.g., `docker compose run --rm bot python -m bot.migrations.run_migrations`) documented in a comment in `docker-compose.yml`
    - _Requirements: 7.1_

- [x] 14. Final checkpoint — full integration passes
  - Run the full test suite (unit + property + integration) and the smoke test; ensure all pass, ask the user if questions arise.

- [x] 15. Structured JSON logging
  - Replace default formatter with a JSON formatter (e.g., `python-json-logger`) writing to `cfg.logging.log_dir` with rotation via `RotatingFileHandler` (`max_file_size_mb`, `backup_count`)
  - Include `run_id`, `phase`, and `ticker` fields where relevant
  - Never emit any `Secrets` field value
  - _Requirements: 6.6, 5.7, 9.8_

- [ ]* 16. Pre-commit hooks (ruff + mypy)
  - Add `.pre-commit-config.yaml` with `ruff` (lint + format) and `mypy --strict` against `bot/`
  - _Requirements: 9.9_

- [ ]* 17. GitHub Actions CI pipeline
  - Add `.github/workflows/ci.yml`: run ruff, mypy, pytest (unit + property + integration with a Postgres service container), and `pip-audit` on every push and PR
  - Cache pip dependencies keyed on `requirements*.txt`
  - _Requirements: 9.9_

- [ ]* 18. Security: pip-audit in CI
  - Add a `pip-audit` step to the CI workflow that fails the build on any vulnerability with severity HIGH or above in the pinned dependency graph
  - _Requirements: 9.9_

- [x] 19. README with Docker / Synology DS220+ deployment playbook
  - Write `README.md` with: prerequisites (Docker Engine ≥ 20.10, Compose v2, ≥ 1 GB free RAM, outbound HTTPS to Wikipedia/Yahoo/FMP/Discord; on Synology: DSM 7.2+ with Container Manager), one-time setup (create host dirs `/volume1/docker/asset-discovery-bot/{data/pg,data/logs,config,secrets}`, seed `/run/secrets/*` files, copy `config.example.yaml` → `config.yaml`), first-run sequence (`docker compose build` → `docker compose up -d db` → `docker compose run --rm bot python -m bot.migrations.run_migrations` → `docker compose run --rm bot python -m bot.run`), Synology Task Scheduler cron entry (`docker exec asset-discovery-bot python -m bot.run`), exit-code monitoring guidance, secret rotation procedure, and a rollback procedure
  - Include a "deploy to any Docker host" variant (cron / systemd timer / K8s CronJob) showing the same commands against different schedulers
  - Do NOT include any real secret or webhook URL
  - _Requirements: 9.1, 9.2, 9.6, 9.7, 9.8, 8.6_

- [ ]* 20. Operational backfill script for failed Discord alerts
  - Add `bot/ops/replay_alerts.py` that takes `--since YYYY-MM-DD` and re-POSTs a Discord alert for every `daily_scans` row in range whose alert was never delivered (tracked via an optional `alert_delivered_at TIMESTAMPTZ NULL` column added by a new migration `002_alert_delivery.sql`)
  - Relies on the existing `UNIQUE (ticker, scan_date)` constraint to prevent duplicate DB rows; alert idempotency on Discord is best-effort
  - _Requirements: 8.4, 11.10_

## Notes

- **Tasks marked with `*` are optional** and can be skipped for a fast MVP. Required tasks (no `*`) form the minimum viable v1.
- **Every task references specific requirements** for traceability back to `requirements.md`.
- **Property-based tests cover all 12 correctness invariants** from Requirement 11, each annotated with its property number and the requirement clause it validates, and each placed close to the module it checks so errors are caught early.
- **Checkpoints** at tasks 10 and 14 gate multi-module progress on a green test suite.
- **v2 Roadmap items are NOT tasks.** Universe expansion, composite ranking, sector neutralization, backtest harness, portfolio construction, exit rules, paper-trading ledger, additional notification channels, metrics dashboards, and weekly digests are explicitly excluded per Requirement 10.5.
- **No deployment, no UAT, no training, no marketing** — every task above is a coding task executable by a code-generation agent.
