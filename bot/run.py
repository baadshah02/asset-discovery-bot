"""Scan_Run orchestrator for the Asset Discovery Bot.

This module fulfils Component 6 of the design. It is the single entry
point invoked once per day by the Synology Task Scheduler (via
``docker exec``) and wires every other module into a single end-to-end
scan.

Dual-pipeline dispatch (v2):
    The orchestrator dispatches to one of two pipelines based on
    ``cfg.pipeline.mode``:
    - ``hard_threshold`` (v1): 4-layer tollbooth (unchanged)
    - ``composite_rank`` (v2): composite scoring + ranking

At-least-once alert delivery contract
-------------------------------------
For every candidate the orchestrator persists the ``daily_scans`` row
**before** the Discord POST (Requirement 5.5). The
``UNIQUE (ticker, scan_date)`` constraint turns a duplicate insert on
re-run into a no-op.

Config reproducibility
----------------------
The full ``cfg.model_dump(mode="json")`` is persisted into
``daily_scans.config_snapshot`` for every inserted row.
"""

from __future__ import annotations

import logging
import time
from datetime import date
from typing import Any

import pandas as pd
from pydantic import ValidationError
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

from bot.config import PipelineMode, load_config
from bot.filters import (
    ScanCandidate,
    apply_layer_1,
    apply_layer_2,
    apply_layer_3,
    apply_layer_4,
    row_to_candidate,
)
from bot.fundamentals import (
    FmpBudgetExhausted,
    FmpClient,
    get_extended_fundamentals,
    get_fundamentals,
)
from bot.log_setup import configure_logging, current_run_id
from bot.notify import (
    NotificationError,
    send_high_conviction,
    send_ranked_candidate,
    send_watchdog,
)
from bot.prices import compute_technical_snapshot, download_price_history
from bot.ranker import Ranker, RankedCandidate
from bot.repo import DuplicateScanError, Repository
from bot.scoring import CompositeScorer
from bot.universe import UniverseSyncError, sync_universe

__all__ = [
    "EXIT_OK",
    "EXIT_UNEXPECTED",
    "EXIT_CONFIG_ERROR",
    "EXIT_UNIVERSE_ERROR",
    "EXIT_DB_ERROR",
    "EXIT_NOTIFY_ERROR",
    "main",
]


# ---------------------------------------------------------------------------
# Exit codes
# ---------------------------------------------------------------------------

EXIT_OK: int = 0
EXIT_UNEXPECTED: int = 1
EXIT_CONFIG_ERROR: int = 2
EXIT_UNIVERSE_ERROR: int = 3
EXIT_DB_ERROR: int = 4
EXIT_NOTIFY_ERROR: int = 5


logger = logging.getLogger(__name__)


def _check_scan_time(
    scan_start: float,
    max_scan_minutes: int,
    phase: str,
) -> None:
    """Log WARN if elapsed scan time exceeds the configured threshold."""
    elapsed_seconds = time.monotonic() - scan_start
    elapsed_minutes = elapsed_seconds / 60.0
    if elapsed_minutes > max_scan_minutes:
        logger.warning(
            "Scan time %.1f min exceeds max_scan_minutes=%d; "
            "active phase: %s",
            elapsed_minutes,
            max_scan_minutes,
            phase,
        )


# ---------------------------------------------------------------------------
# V1 Hard-Threshold Pipeline (extracted from original main())
# ---------------------------------------------------------------------------


def _run_hard_threshold_pipeline(
    cfg: Any,
    secrets: Any,
    repo: Repository,
    universe: list[str],
    scan_start: float,
) -> int:
    """Execute the v1 4-layer hard-threshold pipeline unchanged."""
    notify_failure_count = 0

    # Prices + snapshot + L1 / L2
    frames = download_price_history(universe, cfg.yfinance)
    snapshot = compute_technical_snapshot(frames, cfg.layer2.rsi_period)

    after_l1 = apply_layer_1(snapshot, cfg.layer1)
    after_l2 = apply_layer_2(after_l1, cfg.layer2)

    logger.info(
        "Triage: universe=%d -> L1=%d -> L2=%d",
        len(universe),
        len(after_l1),
        len(after_l2),
    )

    _check_scan_time(scan_start, cfg.universe.max_scan_minutes, "price_download")

    # FMP enrichment (L2 survivors only)
    fmp_client = FmpClient(api_key=secrets.fmp_api_key, cfg=cfg.fmp)
    enriched_rows: list[dict[str, object]] = []
    graceful_degradation_count = 0

    for row in after_l2.to_dict(orient="records"):
        ticker = row["ticker"]
        try:
            f = get_fundamentals(
                ticker,
                repo,
                fmp_client,
                cfg.cache.fundamentals_staleness_days,
            )
        except FmpBudgetExhausted:
            logger.info(
                "FMP budget exhausted; skipping ticker=%s", ticker
            )
            continue
        except OperationalError as exc:
            logger.error(
                "Database unreachable during fundamentals fetch: %s",
                exc,
            )
            return EXIT_DB_ERROR

        if (
            f.pe_ratio is None
            or f.pe_5y_avg is None
            or f.fcf_yield is None
        ):
            graceful_degradation_count += 1
            continue

        enriched_rows.append(
            {
                **row,
                "pe_ratio": f.pe_ratio,
                "pe_5y_avg": f.pe_5y_avg,
                "fcf_yield": f.fcf_yield,
                "latest_headline": f.latest_headline,
                "headline_url": f.headline_url,
            }
        )

    logger.info(
        "Graceful degradation: %d/%d L2 survivors excluded "
        "(missing fundamentals)",
        graceful_degradation_count,
        len(after_l2),
    )

    _check_scan_time(scan_start, cfg.universe.max_scan_minutes, "enrichment")

    # L3 + L4
    if not enriched_rows:
        logger.info(
            "No enriched rows after Phase 6; no candidates to alert"
        )
        logger.info("FMP calls this run: %d", fmp_client.call_count)
        return EXIT_OK

    enriched = pd.DataFrame(enriched_rows)
    after_l3 = apply_layer_3(enriched, cfg.layer3)
    after_l4 = apply_layer_4(after_l3, cfg.layer4)

    logger.info(
        "Deep: enriched=%d -> L3=%d -> L4=%d",
        len(enriched),
        len(after_l3),
        len(after_l4),
    )
    logger.info("FMP calls this run: %d", fmp_client.call_count)

    _check_scan_time(scan_start, cfg.universe.max_scan_minutes, "filtering")

    # Alert + persist
    today = date.today()
    config_snapshot = cfg.model_dump(mode="json")

    for row in after_l4.to_dict(orient="records"):
        candidate: ScanCandidate = row_to_candidate(row)

        try:
            repo.insert_scan(candidate, today, config_snapshot)
        except DuplicateScanError:
            logger.info(
                "Duplicate scan for ticker=%s on %s; skipping alert",
                candidate.ticker,
                today.isoformat(),
            )
            continue
        except OperationalError as exc:
            logger.error(
                "Database unreachable while inserting scan for "
                "ticker=%s: %s",
                candidate.ticker,
                exc,
            )
            return EXIT_DB_ERROR

        try:
            send_high_conviction(
                candidate,
                secrets.discord_webhook_url,
                cfg.notification,
            )
        except NotificationError as exc:
            logger.error(
                "Failed to deliver high-conviction alert for "
                "ticker=%s: %s",
                candidate.ticker,
                exc,
            )
            notify_failure_count += 1

    if notify_failure_count > 0:
        logger.error(
            "Run completed with %d undelivered high-conviction alert(s)",
            notify_failure_count,
        )
        return EXIT_NOTIFY_ERROR

    return EXIT_OK


# ---------------------------------------------------------------------------
# V2 Composite-Rank Pipeline
# ---------------------------------------------------------------------------


def _run_composite_rank_pipeline(
    cfg: Any,
    secrets: Any,
    repo: Repository,
    universe: list[str],
    scan_start: float,
) -> int:
    """Execute the v2 composite-rank pipeline.

    Phases:
        1. Download price history (reuse existing)
        2. Compute reversal signals from price frames
        3. Fetch Extended_Fundamentals for all tickers with reversal data
        4. Load sector assignments from asset_universe
        5. Compute composite scores
        6. Rank + select top-N
        7. Persist + alert (insert-before-alert contract preserved)
    """
    notify_failure_count = 0

    # Phase 1: Download price history
    frames = download_price_history(universe, cfg.yfinance)

    _check_scan_time(scan_start, cfg.universe.max_scan_minutes, "price_download")

    # Phase 2: Compute reversal signals
    scorer = CompositeScorer(cfg.scoring, cfg.ranking, cfg.reversal)
    reversal_returns: dict[str, float] = {}

    for ticker, frame in frames.items():
        if frame is None or frame.empty or "Close" not in frame.columns:
            continue
        if cfg.reversal.enabled:
            ret = scorer.compute_reversal_signal(
                frame["Close"], cfg.reversal.lookback_days
            )
            if ret is not None:
                reversal_returns[ticker] = ret
            else:
                logger.warning(
                    "Ticker %s has insufficient price history for reversal signal",
                    ticker,
                )
        else:
            # When reversal is disabled, all tickers get 0.0
            reversal_returns[ticker] = 0.0

    logger.info(
        "Reversal signals: %d/%d tickers have valid signals",
        len(reversal_returns),
        len(frames),
    )

    if not reversal_returns:
        logger.info("No tickers with valid reversal signals; nothing to score")
        return EXIT_OK

    _check_scan_time(scan_start, cfg.universe.max_scan_minutes, "reversal_signals")

    # Phase 3: Fetch Extended_Fundamentals for all tickers with reversal data
    fmp_client = FmpClient(api_key=secrets.fmp_api_key, cfg=cfg.fmp)
    from bot.fundamentals import ExtendedFundamentals

    fundamentals: dict[str, ExtendedFundamentals] = {}
    for ticker in reversal_returns:
        try:
            f = get_extended_fundamentals(
                ticker,
                repo,
                fmp_client,
                cfg.cache.fundamentals_staleness_days,
                pipeline_mode="composite_rank",
            )
            fundamentals[ticker] = f
        except FmpBudgetExhausted:
            logger.info(
                "Fundamentals budget exhausted; skipping ticker=%s", ticker
            )
            continue
        except OperationalError as exc:
            logger.error(
                "Database unreachable during fundamentals fetch: %s", exc
            )
            return EXIT_DB_ERROR

    logger.info(
        "Extended fundamentals: %d/%d tickers fetched",
        len(fundamentals),
        len(reversal_returns),
    )

    _check_scan_time(scan_start, cfg.universe.max_scan_minutes, "enrichment")

    # Phase 4: Load sector assignments
    sectors_raw = repo.load_sectors(list(fundamentals.keys()))
    sectors: dict[str, str] = {}
    unknown_count = 0
    for ticker in fundamentals:
        sector = sectors_raw.get(ticker)
        if sector is None or sector.strip() == "":
            sectors[ticker] = "unknown"
            unknown_count += 1
        else:
            sectors[ticker] = sector
    if unknown_count > 0:
        logger.warning(
            "%d tickers have null sector; assigned to 'unknown'",
            unknown_count,
        )

    # Phase 5: Score
    results = scorer.compute_composite_scores(
        fundamentals, reversal_returns, sectors
    )
    logger.info(
        "Scoring: %d tickers scored out of %d universe",
        len(results),
        len(universe),
    )

    if not results:
        logger.info("No tickers scored; nothing to emit")
        return EXIT_OK

    _check_scan_time(scan_start, cfg.universe.max_scan_minutes, "scoring")

    # Phase 6: Rank + select top-N
    ranker = Ranker(cfg.ranking)
    all_ranked = ranker.rank(results)

    # Enrich ranked candidates with close price and fundamentals data
    enriched_ranked: list[RankedCandidate] = []
    for candidate in all_ranked:
        ticker = candidate.ticker
        close_price = 0.0
        if ticker in frames and frames[ticker] is not None and not frames[ticker].empty:
            if "Close" in frames[ticker].columns:
                close_price = float(frames[ticker]["Close"].iloc[-1])

        fund = fundamentals.get(ticker)
        enriched_ranked.append(RankedCandidate(
            ticker=candidate.ticker,
            sector=candidate.sector,
            composite_rank=candidate.composite_rank,
            composite_score=candidate.composite_score,
            value_composite=candidate.value_composite,
            quality_composite=candidate.quality_composite,
            reversal_signal=candidate.reversal_signal,
            close=close_price,
            market_cap=fund.market_cap if fund else None,
            latest_headline=fund.latest_headline if fund else None,
            headline_url=fund.headline_url if fund else None,
        ))

    top_n = ranker.select_top_n(enriched_ranked)
    logger.info(
        "Ranking: %d candidates selected",
        len(top_n),
    )

    _check_scan_time(scan_start, cfg.universe.max_scan_minutes, "ranking")

    # Phase 7: Persist + alert (insert-before-alert contract preserved)
    today = date.today()
    config_snapshot = cfg.model_dump(mode="json")

    # Resolve webhook URL for composite pipeline
    webhook_url = secrets.discord_webhook_url
    if (
        hasattr(secrets, "discord_composite_webhook_url")
        and secrets.discord_composite_webhook_url
    ):
        webhook_url = secrets.discord_composite_webhook_url

    for candidate in top_n:
        ticker = candidate.ticker
        fund = fundamentals.get(ticker)

        # Build scan row with legacy NOT NULL columns populated with fallbacks
        scan_row: dict[str, Any] = {
            "ticker": ticker,
            "close": candidate.close,
            "pct_above_low": 0,        # not computed in composite mode
            "rsi_today": 0,            # replaced by reversal signal
            "rsi_yesterday": 0,        # replaced by reversal signal
            "pe_ratio": fund.pe_ratio if fund and fund.pe_ratio else 0,
            "pe_5y_avg": fund.pe_5y_avg if fund and fund.pe_5y_avg else 0,
            "fcf_yield": fund.fcf_yield if fund and fund.fcf_yield else 0,
            "latest_headline": fund.latest_headline if fund else None,
            # Composite columns
            "value_composite": candidate.value_composite,
            "quality_composite": candidate.quality_composite,
            "reversal_signal": candidate.reversal_signal,
            "composite_score": candidate.composite_score,
            "composite_rank": candidate.composite_rank,
            "sector_at_rank": candidate.sector,
            "pipeline_mode": "composite_rank",
        }

        try:
            repo.insert_scan(scan_row, today, config_snapshot)
        except DuplicateScanError:
            logger.info(
                "Duplicate scan for ticker=%s on %s; skipping alert",
                ticker,
                today.isoformat(),
            )
            continue
        except OperationalError as exc:
            logger.error(
                "Database unreachable while inserting scan for "
                "ticker=%s: %s",
                ticker,
                exc,
            )
            return EXIT_DB_ERROR

        try:
            send_ranked_candidate(
                candidate,
                webhook_url,
                cfg.notification,
            )
        except NotificationError as exc:
            logger.error(
                "Failed to deliver ranked alert for ticker=%s: %s",
                ticker,
                exc,
            )
            notify_failure_count += 1

    if notify_failure_count > 0:
        logger.error(
            "Run completed with %d undelivered ranked alert(s)",
            notify_failure_count,
        )
        return EXIT_NOTIFY_ERROR

    return EXIT_OK


# ---------------------------------------------------------------------------
# main() — the orchestrated Scan_Run
# ---------------------------------------------------------------------------


def main() -> int:
    """Execute one Scan_Run end-to-end; return a process exit code."""
    logging.basicConfig(
        level="INFO",
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    # ---- Phase 1: config + secrets ----------------------------------------
    try:
        cfg, secrets = load_config()
    except (ValidationError, FileNotFoundError, ValueError) as exc:
        logger.error("Config load failed: %s", exc)
        return EXIT_CONFIG_ERROR
    except Exception as exc:  # noqa: BLE001
        logger.exception("Unexpected error loading config: %s", exc)
        return EXIT_UNEXPECTED

    configure_logging(cfg.logging)
    logger.info(
        "Scan_Run started",
        extra={"run_id": current_run_id(), "phase": "startup"},
    )

    logger.info(
        "Config: active non-default values = %s",
        cfg.diff_from_defaults(),
    )

    # ---- Phase 2: engine + repository -------------------------------------
    scan_start = time.monotonic()
    engine = create_engine(secrets.db_url, future=True)
    repo = Repository(engine)

    try:
        # ---- Phase 3: universe sync (watchdog) ----------------------------
        try:
            diff = sync_universe(repo, cfg.universe)
        except UniverseSyncError as exc:
            logger.error("Universe sync failed: %s", exc)
            return EXIT_UNIVERSE_ERROR
        except OperationalError as exc:
            logger.error(
                "Database unreachable during universe sync: %s", exc
            )
            return EXIT_DB_ERROR

        if diff.added or diff.removed or diff.source_failures:
            try:
                send_watchdog(
                    diff,
                    secrets.discord_webhook_url,
                    cfg.notification,
                )
            except NotificationError as exc:
                logger.error("Watchdog alert failed: %s", exc)

        # ---- Phase 4: load active universe --------------------------------
        try:
            universe = sorted(repo.load_universe())
        except OperationalError as exc:
            logger.error(
                "Database unreachable while loading universe: %s", exc
            )
            return EXIT_DB_ERROR

        _check_scan_time(scan_start, cfg.universe.max_scan_minutes, "universe_sync")

        if not universe:
            logger.error(
                "asset_universe is empty after sync; aborting run"
            )
            return EXIT_UNIVERSE_ERROR

        # ---- Phase 5: dispatch based on pipeline mode ---------------------
        logger.info(
            "Pipeline mode: %s",
            cfg.pipeline.mode.value,
        )

        if cfg.pipeline.mode == PipelineMode.COMPOSITE_RANK:
            logger.info(
                "Composite-rank pipeline: weights v=%.1f q=%.1f r=%.1f, "
                "sector_neutral=%s, top_n_per_sector=%d, top_n=%d",
                cfg.scoring.value_weight,
                cfg.scoring.quality_weight,
                cfg.scoring.reversal_weight,
                cfg.ranking.sector_neutral,
                cfg.ranking.top_n_per_sector,
                cfg.ranking.top_n,
            )
            return _run_composite_rank_pipeline(
                cfg, secrets, repo, universe, scan_start,
            )
        else:
            return _run_hard_threshold_pipeline(
                cfg, secrets, repo, universe, scan_start,
            )

    except OperationalError as exc:
        logger.error("Database operational error: %s", exc)
        return EXIT_DB_ERROR
    except Exception as exc:  # noqa: BLE001
        logger.exception("Unexpected error during Scan_Run: %s", exc)
        return EXIT_UNEXPECTED
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
