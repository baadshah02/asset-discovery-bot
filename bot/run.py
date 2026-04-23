"""Scan_Run orchestrator for the Asset Discovery Bot.

This module fulfils Component 6 of the design
(:doc:`.kiro/specs/asset-discovery-bot/design.md`). It is the single entry
point invoked once per day by the Synology Task Scheduler (via
``docker exec``) and wires every other module into a single end-to-end
scan:

    load_config                         (bot.config)
        -> create_engine + Repository   (bot.repo)
        -> sync_universe                (bot.universe)
        -> send_watchdog if diff        (bot.notify)
        -> download_price_history       (bot.prices)
        -> compute_technical_snapshot   (bot.prices)
        -> apply_layer_1                (bot.filters)
        -> apply_layer_2                (bot.filters)
        -> get_fundamentals per L2      (bot.fundamentals)
        -> apply_layer_3                (bot.filters)
        -> apply_layer_4                (bot.filters)
        -> per candidate:
             insert_scan                (bot.repo)
             send_high_conviction       (bot.notify)

At-least-once alert delivery contract
-------------------------------------
For every High_Conviction_Candidate the orchestrator persists the
``daily_scans`` row **before** the Discord POST (Requirement 5.5,
Correctness Property 10). The ``UNIQUE (ticker, scan_date)`` constraint
turns a duplicate insert on re-run into a no-op — we catch
:class:`bot.repo.DuplicateScanError` and skip re-alerting so Discord is
never spammed when the scheduler accidentally fires twice. Combined, the
contract is: every candidate is alerted at least once (on the first
successful insert) and at most once per scan date (enforced by the DB).

Config reproducibility
----------------------
The full ``cfg.model_dump(mode="json")`` is persisted into
``daily_scans.config_snapshot`` for every inserted row (Requirements 6.7,
11.11). The ``mode="json"`` flag coerces ``Path`` and other non-JSON
primitives (e.g., ``LoggingConfig.log_dir``) into strings so the dict is
guaranteed JSON-serializable before hitting the JSONB column.

Top-level error handling (Requirements 8.1–8.6)
-----------------------------------------------
The orchestrator fails fast on unrecoverable conditions and degrades
gracefully on transient ones:

* Config load failure -> ``EXIT_CONFIG_ERROR``.
* Wikipedia scrape / sanity-bound abort -> ``EXIT_UNIVERSE_ERROR``.
* Postgres ``OperationalError`` at any phase -> ``EXIT_DB_ERROR``.
* Discord retry budget exhausted for a high-conviction alert ->
  ``EXIT_NOTIFY_ERROR`` (but only after every candidate has been given
  the chance to run, so one flaky alert does not starve the others).
* yfinance empty frames -> excluded by :mod:`bot.prices` already, run
  continues.
* FMP HTTP 429 -> kill-switch engaged by :mod:`bot.fundamentals`; the
  run continues with cache-only L3/L4.
* Any unhandled exception -> ``EXIT_UNEXPECTED``.

The module does not configure structured JSON logging — that is Task 15.
A basic stream handler is sufficient for the orchestrator to surface
phase markers and error context during v1 bring-up.
"""

from __future__ import annotations

import logging
import time
from datetime import date

import pandas as pd
from pydantic import ValidationError
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

from bot.config import load_config
from bot.filters import (
    ScanCandidate,
    apply_layer_1,
    apply_layer_2,
    apply_layer_3,
    apply_layer_4,
    row_to_candidate,
)
from bot.fundamentals import FmpBudgetExhausted, FmpClient, get_fundamentals
from bot.log_setup import configure_logging, current_run_id
from bot.notify import NotificationError, send_high_conviction, send_watchdog
from bot.prices import compute_technical_snapshot, download_price_history
from bot.repo import DuplicateScanError, Repository
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
#
# Observable by the Synology Task Scheduler (Requirement 8.6). The numbers
# are chosen so a zero exit still means success and every distinct fatal
# failure mode has its own non-zero code for operational triage.

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
# main() — the orchestrated Scan_Run
# ---------------------------------------------------------------------------


def main() -> int:
    """Execute one Scan_Run end-to-end; return a process exit code.

    The control flow mirrors the design's orchestrator pseudocode exactly.
    Every phase is bracketed by a ``try`` that converts the module-specific
    exception to a numeric exit code, and every phase from "engine
    created" onwards runs inside a ``try/finally`` that calls
    ``engine.dispose()`` so the Postgres connection pool is released even
    when we exit non-zero.
    """
    # Bring-up bootstrap: a minimal basicConfig so any logging emitted
    # while loading config (before we know `cfg.logging`) still lands
    # somewhere visible. Task 15 replaces this with structured JSON
    # logging as soon as config is successfully loaded.
    logging.basicConfig(
        level="INFO",
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    # ---- Phase 1: config + secrets ----------------------------------------
    # Requirement 6.4 / 11.12: strict validation happens BEFORE any network,
    # database, Wikipedia, yfinance, FMP, or Discord I/O is performed.
    try:
        cfg, secrets = load_config()
    except (ValidationError, FileNotFoundError, ValueError) as exc:
        logger.error("Config load failed: %s", exc)
        return EXIT_CONFIG_ERROR
    except Exception as exc:  # noqa: BLE001 — last-resort boundary
        logger.exception("Unexpected error loading config: %s", exc)
        return EXIT_UNEXPECTED

    # Task 15: now that config is loaded, swap the bring-up basicConfig
    # for the structured JSON formatter + rotating file handler. Every
    # subsequent log record carries the run_id set up by
    # :func:`current_run_id`.
    configure_logging(cfg.logging)
    logger.info(
        "Scan_Run started",
        extra={"run_id": current_run_id(), "phase": "startup"},
    )

    # Requirement 6.6: one INFO line listing non-default values. Secrets
    # are never logged — ``diff_from_defaults`` operates on ``AppConfig``
    # only, not on :class:`Secrets`.
    logger.info(
        "Config: active non-default values = %s",
        cfg.diff_from_defaults(),
    )

    # ---- Phase 2: engine + repository -------------------------------------
    # ``future=True`` is the SQLAlchemy 2.0 idiom; it is the default for
    # SA 2.0.x but we set it explicitly so the behaviour is documented at
    # the orchestrator boundary.
    scan_start = time.monotonic()
    engine = create_engine(secrets.db_url, future=True)
    repo = Repository(engine)

    # Everything from here on goes in a try/finally so the connection pool
    # is always disposed of, even on a crash. The return value of each
    # inner ``return`` statement is honoured by ``finally``'s implicit
    # re-return (Python pass-through semantics).
    notify_failure_count = 0
    try:
        # ---- Phase 3: universe sync (watchdog) ----------------------------
        try:
            diff = sync_universe(repo, cfg.universe)
        except UniverseSyncError as exc:
            # Wikipedia scrape aborted or count outside sanity bounds
            # (Requirements 1.6, 8.1). :mod:`bot.universe` guarantees
            # ``asset_universe`` was NOT mutated in this case.
            logger.error("Universe sync failed: %s", exc)
            return EXIT_UNIVERSE_ERROR
        except OperationalError as exc:
            logger.error(
                "Database unreachable during universe sync: %s", exc
            )
            return EXIT_DB_ERROR

        # Watchdog alert on any non-empty diff or source failures
        # (Requirement 1.3, 9.4). Treat delivery failure as non-fatal:
        # missing a watchdog alert is less bad than missing a
        # high-conviction alert later in the same run, so we log ERROR
        # and keep going.
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

        # ---- Phase 5: prices + snapshot + L1 / L2 -------------------------
        # :func:`download_price_history` logs WARN and excludes tickers
        # that come back empty after retries (Requirement 8.2); no error
        # handling is needed here.
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

        # ---- Phase 6: FMP enrichment (L2 survivors only) ------------------
        # Fundamentals are fetched ONLY for L2 survivors (Requirements 3.6,
        # 4.3). The :class:`FmpClient` is instantiated per run so the
        # kill-switch flag and call counter reset naturally.
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
                # Kill-switch fired and no cached row was available for
                # this ticker — skip and continue; other L2 survivors may
                # still be served from cache (Requirement 8.3).
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

            # Graceful degradation: exclude tickers with missing
            # fundamentals from L3/L4 (Req 6.4).
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

        # ---- Phase 7: L3 + L4 ---------------------------------------------
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

        # ---- Phase 8: alert + persist -------------------------------------
        # ``mode="json"`` coerces ``Path`` (and any other non-primitive
        # field types we add in the future) to JSON-serializable strings
        # so the dict is safe to hand to the JSONB column without any
        # further normalisation (Requirements 6.7, 11.11).
        today = date.today()
        config_snapshot = cfg.model_dump(mode="json")

        for row in after_l4.to_dict(orient="records"):
            candidate: ScanCandidate = row_to_candidate(row)

            # Insert FIRST, alert SECOND (Requirement 5.5 / Property 10).
            # A duplicate insert (same ticker-on-same-date re-run) is a
            # legitimate no-op: we skip the alert to avoid spamming
            # Discord but keep processing the remaining candidates.
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
                # Requirement 8.4: the ``daily_scans`` row stays persisted
                # so a future operational backfill can re-attempt delivery
                # without creating a duplicate DB row. Track the failure
                # so we can surface a non-zero exit at end-of-run without
                # interrupting the remaining candidates.
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

    except OperationalError as exc:
        # Catch-all for any Postgres operational error we didn't already
        # handle phase-locally (Requirement 8.5).
        logger.error("Database operational error: %s", exc)
        return EXIT_DB_ERROR
    except Exception as exc:  # noqa: BLE001 — last-resort boundary
        logger.exception("Unexpected error during Scan_Run: %s", exc)
        return EXIT_UNEXPECTED
    finally:
        # Always release the connection pool, even on fatal errors.
        engine.dispose()


if __name__ == "__main__":
    # Requirement 8.6: the Synology Task Scheduler observes the process
    # exit code, so every failure mode from :func:`main` must propagate as
    # a non-zero ``SystemExit``.
    raise SystemExit(main())
