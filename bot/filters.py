"""Filter_Pipeline: the 4-layer "tollbooth" for the Asset Discovery Bot.

This module fulfils Component 4 of the design
(:doc:`.kiro/specs/asset-discovery-bot/design.md`). It reduces the full
S&P 500 technical snapshot down to the final set of High_Conviction_
Candidates through four sequential, pure-function layers. The design calls
this arrangement a "tollbooth": every layer narrows the survivor set and
the next layer only ever sees the previous layer's output. Nothing ever
flows backwards, nothing is re-evaluated, and no layer mutates its input.

Semantics (Requirement 4):

* **Layer 1 — 52-week anchor (George & Hwang, 2004).** Keep rows where
  ``cfg.pct_above_low_min <= pct_above_low <= cfg.pct_above_low_max``.
* **Layer 2 — RSI capitulation crossover.** Keep rows where
  ``rsi_yesterday < cfg.rsi_oversold`` AND ``rsi_today > cfg.rsi_recovery``.
* **Layer 3 — Fama-French Value (HML proxy).** Keep rows with non-null
  ``pe_ratio`` and ``pe_5y_avg`` where ``pe_ratio < pe_5y_avg``. When
  ``cfg.require_positive_earnings`` is true (default), additionally require
  ``pe_ratio > 0`` AND ``pe_5y_avg > 0``.
* **Layer 4 — Fama-French Quality / QMJ (FCF yield).** Keep rows with
  non-null ``fcf_yield > cfg.fcf_yield_min``.

Each ``apply_layer_*`` function is a **pure function** of
``(DataFrame, cfg)``: it returns a new DataFrame and never mutates the
input (Requirement 4.8). This is guaranteed by using boolean-mask indexing
followed by an explicit ``.copy()`` so downstream mutation cannot leak
back into upstream state.

Enrichment boundary (Requirement 3.6, 4.3)
------------------------------------------
Fundamentals are expensive: every cache-miss L2 survivor costs 3 FMP calls
against a 250/day free-tier budget. :func:`run_pipeline` therefore fetches
fundamentals **only** for L2 survivors — the L1 and L2 predicates are
evaluated entirely from the technical snapshot before any FMP call is
made. The ``fundamentals_fetcher`` callable encapsulates the FMP + cache
lookup so ``run_pipeline`` stays testable without needing a live
:class:`bot.repo.Repository` or :class:`bot.fundamentals.FmpClient`.

Justifying fields (Requirement 4.7)
-----------------------------------
Every field that justified a candidate's inclusion — ``close``,
``pct_above_low``, ``rsi_today``, ``rsi_yesterday``, ``pe_ratio``,
``pe_5y_avg``, ``fcf_yield``, ``latest_headline``, ``headline_url`` — is
preserved through the pipeline and populated on the returned
:class:`ScanCandidate` so the Notifier can render a rich embed that
explains *why* each alert fired, and so the :class:`bot.repo.Repository`
can persist it into ``daily_scans.config_snapshot``-adjacent columns.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable

import pandas as pd

from bot.config import (
    AppConfig,
    Layer1Config,
    Layer2Config,
    Layer3Config,
    Layer4Config,
    YFinanceConfig,
)
from bot.fundamentals import FmpBudgetExhausted, Fundamentals
from bot.prices import compute_technical_snapshot

__all__ = [
    "ScanCandidate",
    "apply_layer_1",
    "apply_layer_2",
    "apply_layer_3",
    "apply_layer_4",
    "row_to_candidate",
    "run_pipeline",
]


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# ScanCandidate — the final output row shape
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ScanCandidate:
    """One High_Conviction_Candidate emitted by the pipeline.

    Carries every field required to (a) justify the alert in a Discord
    embed and (b) persist a reproducible row into ``daily_scans``.

    All numeric fields are ``float`` (not ``float | None``) because a
    candidate only reaches this dataclass after passing all four layers —
    Layer 3 guarantees non-null P/E fields and Layer 4 guarantees non-null
    FCF yield (Requirements 4.4, 4.6). Headline fields remain optional
    because FMP occasionally omits them for smaller constituents.
    """

    ticker: str
    close: float
    pct_above_low: float
    rsi_today: float
    rsi_yesterday: float
    pe_ratio: float
    pe_5y_avg: float
    fcf_yield: float
    latest_headline: str | None
    headline_url: str | None


# ---------------------------------------------------------------------------
# Layer 1 — 52-week anchor check
# ---------------------------------------------------------------------------


def apply_layer_1(snapshot: pd.DataFrame, cfg: Layer1Config) -> pd.DataFrame:
    """Keep rows whose ``pct_above_low`` is inside the configured band.

    Implements Requirement 4.1. Pure function: returns a new DataFrame and
    never mutates ``snapshot`` (Requirement 4.8).

    Empty-input safety: returning ``snapshot.copy()`` on an empty frame
    preserves the column schema for the next layer so downstream code can
    always rely on ``DataFrame.columns``.
    """
    if snapshot.empty:
        return snapshot.copy()

    mask = (
        (snapshot["pct_above_low"] >= cfg.pct_above_low_min)
        & (snapshot["pct_above_low"] <= cfg.pct_above_low_max)
    )
    return snapshot.loc[mask].copy()


# ---------------------------------------------------------------------------
# Layer 2 — RSI capitulation crossover
# ---------------------------------------------------------------------------


def apply_layer_2(snapshot: pd.DataFrame, cfg: Layer2Config) -> pd.DataFrame:
    """Keep rows where RSI crossed up through the oversold threshold.

    Implements Requirement 4.2: ``rsi_yesterday`` must be strictly below
    ``cfg.rsi_oversold`` AND ``rsi_today`` strictly above
    ``cfg.rsi_recovery``. The strict inequalities are intentional — the
    pattern is "was capitulating, is now recovering," and a flat equal at
    the threshold on either side is ambiguous.

    Pure function (Requirement 4.8).
    """
    if snapshot.empty:
        return snapshot.copy()

    mask = (snapshot["rsi_yesterday"] < cfg.rsi_oversold) & (
        snapshot["rsi_today"] > cfg.rsi_recovery
    )
    return snapshot.loc[mask].copy()


# ---------------------------------------------------------------------------
# Layer 3 — Fama-French Value (HML proxy)
# ---------------------------------------------------------------------------


def apply_layer_3(enriched: pd.DataFrame, cfg: Layer3Config) -> pd.DataFrame:
    """Keep rows where current P/E is below the 5-year average P/E.

    Implements Requirements 4.4, 4.5:

    * ``pe_ratio`` and ``pe_5y_avg`` must both be non-null (NaN P/E is a
      valid signal — negative or undefined earnings — and means the row
      is excluded, not emitted as NaN).
    * ``pe_ratio < pe_5y_avg``.
    * When ``cfg.require_positive_earnings`` is true (default), additionally
      require ``pe_ratio > 0`` AND ``pe_5y_avg > 0`` so that an apparent
      "discount" driven by a swing from positive to negative P/E does not
      pass.

    Pure function (Requirement 4.8).
    """
    if enriched.empty:
        return enriched.copy()

    pe_ratio = enriched["pe_ratio"]
    pe_5y_avg = enriched["pe_5y_avg"]

    mask = pe_ratio.notna() & pe_5y_avg.notna() & (pe_ratio < pe_5y_avg)
    if cfg.require_positive_earnings:
        mask = mask & (pe_ratio > 0) & (pe_5y_avg > 0)

    return enriched.loc[mask].copy()


# ---------------------------------------------------------------------------
# Layer 4 — Fama-French Quality / QMJ (FCF yield)
# ---------------------------------------------------------------------------


def apply_layer_4(
    layer3_survivors: pd.DataFrame, cfg: Layer4Config
) -> pd.DataFrame:
    """Keep rows with a non-null ``fcf_yield`` strictly above the floor.

    Implements Requirement 4.6. The strict ``>`` is the same pattern as
    Layer 2's crossover check: a row that only ties the threshold does not
    meet the "quality dominates value" bar and is dropped.

    Pure function (Requirement 4.8).
    """
    if layer3_survivors.empty:
        return layer3_survivors.copy()

    fcf_yield = layer3_survivors["fcf_yield"]
    mask = fcf_yield.notna() & (fcf_yield > cfg.fcf_yield_min)
    return layer3_survivors.loc[mask].copy()


# ---------------------------------------------------------------------------
# Enrichment + candidate construction helpers
# ---------------------------------------------------------------------------


# Columns that the technical snapshot contributes to the enriched frame.
# Declared once so ``run_pipeline`` and any future debugging introspection
# agree on the exact shape.
_TECHNICAL_COLUMNS: tuple[str, ...] = (
    "ticker",
    "close",
    "low_52w",
    "pct_above_low",
    "rsi_today",
    "rsi_yesterday",
)

# Columns the Fundamentals_Service contributes.
_FUNDAMENTAL_COLUMNS: tuple[str, ...] = (
    "pe_ratio",
    "pe_5y_avg",
    "fcf_yield",
    "latest_headline",
    "headline_url",
)


def _merge_fundamentals(
    technical_row: dict[str, Any], fundamentals: Fundamentals
) -> dict[str, Any]:
    """Combine a technical-snapshot row with a :class:`Fundamentals` record.

    Returns a flat dict with every field L3/L4 will read plus every field
    the final :class:`ScanCandidate` will carry. The technical row's
    ``ticker`` is kept as canonical and the fundamentals' ``ticker`` is
    dropped to avoid a column duplication if pandas ever gets the two out
    of order.
    """
    merged: dict[str, Any] = dict(technical_row)
    merged["pe_ratio"] = fundamentals.pe_ratio
    merged["pe_5y_avg"] = fundamentals.pe_5y_avg
    merged["fcf_yield"] = fundamentals.fcf_yield
    merged["latest_headline"] = fundamentals.latest_headline
    merged["headline_url"] = fundamentals.headline_url
    return merged


def row_to_candidate(row: Any) -> ScanCandidate:
    """Convert one Layer-4-survivor DataFrame row into a :class:`ScanCandidate`.

    Accepts either a :class:`dict` or a pandas ``Series`` / namedtuple
    (anything supporting ``__getitem__`` or attribute access via
    :func:`dict`). Normalises to ``dict`` first so numeric fields can be
    cast to ``float`` unconditionally — pandas often returns ``numpy``
    scalars which are not ``float`` subclasses under strict ``isinstance``
    checks.

    Exposed publicly so :mod:`bot.run` can reuse the same conversion when
    materialising candidates from the Layer-4 DataFrame ahead of
    :meth:`bot.repo.Repository.insert_scan` and
    :func:`bot.notify.send_high_conviction`.
    """
    if isinstance(row, pd.Series):
        data = row.to_dict()
    elif isinstance(row, dict):
        data = row
    else:
        # Namedtuple or other attribute-bearing object.
        data = {name: getattr(row, name) for name in _TECHNICAL_COLUMNS + _FUNDAMENTAL_COLUMNS if hasattr(row, name)}

    def _opt_str(value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, float) and pd.isna(value):
            return None
        return str(value)

    return ScanCandidate(
        ticker=str(data["ticker"]),
        close=float(data["close"]),
        pct_above_low=float(data["pct_above_low"]),
        rsi_today=float(data["rsi_today"]),
        rsi_yesterday=float(data["rsi_yesterday"]),
        pe_ratio=float(data["pe_ratio"]),
        pe_5y_avg=float(data["pe_5y_avg"]),
        fcf_yield=float(data["fcf_yield"]),
        latest_headline=_opt_str(data.get("latest_headline")),
        headline_url=_opt_str(data.get("headline_url")),
    )


# ---------------------------------------------------------------------------
# run_pipeline — orchestrated L1 -> L2 -> enrich -> L3 -> L4
# ---------------------------------------------------------------------------


# Callable signatures kept explicit so test doubles are easy to construct.
PriceFetcher = Callable[[list[str], YFinanceConfig], dict[str, pd.DataFrame]]
FundamentalsFetcher = Callable[[str], Fundamentals]


def run_pipeline(
    universe: list[str],
    price_fetcher: PriceFetcher,
    fundamentals_fetcher: FundamentalsFetcher,
    cfg: AppConfig,
) -> list[ScanCandidate]:
    """End-to-end filter pipeline — prices -> L1 -> L2 -> enrich -> L3 -> L4.

    Fundamentals are fetched **only** for L2 survivors (Requirement 3.6,
    4.3); L1 and L2 both run against the pure-technical snapshot with no
    FMP calls involved.

    Args:
        universe: Active S&P 500 tickers (Wikipedia form — dots preserved;
            :mod:`bot.prices` handles the Yahoo conversion).
        price_fetcher: Callable ``(tickers, cfg.yfinance) -> {ticker: frame}``.
            Typically :func:`bot.prices.download_price_history`; tests pass
            a fake for deterministic OHLC.
        fundamentals_fetcher: Callable ``ticker -> Fundamentals`` that
            encapsulates the cache-gated FMP lookup. In production this is
            built by the orchestrator as
            ``partial(get_fundamentals, repo=repo, fmp_client=fmp,
            staleness_days=cfg.cache.fundamentals_staleness_days)``.
            Raising :class:`FmpBudgetExhausted` for a ticker causes that
            ticker to be skipped (logged at INFO) without aborting the run
            — the kill-switch / cached-only fallback is policy owned here.
        cfg: The frozen :class:`AppConfig` for this run.

    Returns:
        A list of :class:`ScanCandidate`, one per Layer-4 survivor. Order
        matches the row order of the Layer-4 DataFrame (which in turn
        preserves Layer-3's order, etc.); the caller is free to sort.
    """
    # ---- Phase 1: prices + technical snapshot -----------------------------
    frames = price_fetcher(universe, cfg.yfinance)
    snapshot = compute_technical_snapshot(frames, cfg.layer2.rsi_period)

    # ---- Phase 2: pure-technical layers (no FMP calls) --------------------
    l1 = apply_layer_1(snapshot, cfg.layer1)
    l2 = apply_layer_2(l1, cfg.layer2)

    logger.info(
        "Filter_Pipeline: universe=%d -> L1=%d -> L2=%d",
        len(universe),
        len(l1),
        len(l2),
    )

    if l2.empty:
        logger.info("Filter_Pipeline: no L2 survivors; skipping enrichment")
        return []

    # ---- Phase 3: enrich L2 survivors only (Req 3.6 / 4.3) ----------------
    enriched_rows: list[dict[str, Any]] = []
    for row in l2.to_dict(orient="records"):
        ticker = row["ticker"]
        try:
            fundamentals = fundamentals_fetcher(ticker)
        except FmpBudgetExhausted:
            # Budget exhausted *and* no cached row to fall back on for this
            # ticker — :func:`bot.fundamentals.get_fundamentals` already
            # served stale cache when it could, so reaching here means we
            # genuinely have nothing. Skip the ticker; the run continues
            # for any ticker whose fundamentals are cached.
            logger.info(
                "Filter_Pipeline: skipping %s — FMP budget exhausted and no "
                "cached fundamentals available",
                ticker,
            )
            continue

        enriched_rows.append(_merge_fundamentals(row, fundamentals))

    if not enriched_rows:
        logger.info(
            "Filter_Pipeline: enrichment produced no rows (every L2 survivor "
            "was skipped); nothing to send to L3/L4"
        )
        return []

    enriched = pd.DataFrame(enriched_rows)

    # ---- Phase 4: value + quality layers ----------------------------------
    l3 = apply_layer_3(enriched, cfg.layer3)
    l4 = apply_layer_4(l3, cfg.layer4)

    logger.info(
        "Filter_Pipeline: enriched=%d -> L3=%d -> L4=%d",
        len(enriched),
        len(l3),
        len(l4),
    )

    if l4.empty:
        return []

    # ---- Phase 5: materialise the survivors as ScanCandidates -------------
    candidates: list[ScanCandidate] = [
        row_to_candidate(row) for row in l4.to_dict(orient="records")
    ]
    return candidates
