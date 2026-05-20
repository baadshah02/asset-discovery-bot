"""Composite Scorer for the Asset Discovery Bot.

This module computes Value_Composite, Quality_Composite, Reversal_Signal,
and the final Composite_Score for every ticker in the scored universe.

Responsibilities:
- Compute individual metrics from ExtendedFundamentals
- Compute cross-sectional z-scores (optionally sector-neutral)
- Apply inversions (EV/EBIT and EV/Sales get negated z-scores)
- Average z-scores into Value_Composite and Quality_Composite
- Compute Reversal_Signal from negated trailing 21-day returns
- Combine into Composite_Score using configured weights
- Exclude tickers with incomplete data (log at DEBUG)
- Fall back to cross-sectional z-scoring for undersized sectors (log at WARN)

Requirements traceability:
    1.1–1.7 — Value Composite Scoring
    2.1–2.8 — Quality Composite Scoring
    3.1–3.7 — Short-Term Reversal Signal
    4.2     — Sector-neutral z-scoring
    11.2    — Composite Score Formula
    11.3    — Scoring Determinism
    11.4    — Sector-Neutrality Closure
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from statistics import mean as _mean
from statistics import pstdev
from typing import Any

import pandas as pd

from bot.config import RankingConfig, ReversalConfig, ScoringConfig
from bot.fundamentals import ExtendedFundamentals

__all__ = [
    "CompositeResult",
    "CompositeScorer",
]

_log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CompositeResult:
    """Scoring output for one ticker."""

    ticker: str
    sector: str
    value_composite: float
    quality_composite: float
    reversal_signal: float
    composite_score: float
    # Individual metrics preserved for audit
    ev_ebit: float | None
    ev_sales: float | None
    book_to_market: float | None
    shareholder_yield: float | None
    gp_a: float | None
    earnings_stability: float | None
    low_accruals: float | None
    raw_reversal_return: float | None


# ---------------------------------------------------------------------------
# CompositeScorer
# ---------------------------------------------------------------------------


class CompositeScorer:
    """Stateless scorer — all state flows through method arguments."""

    def __init__(
        self,
        cfg: ScoringConfig,
        ranking_cfg: RankingConfig,
        reversal_cfg: ReversalConfig | None = None,
    ) -> None:
        self._cfg = cfg
        self._ranking_cfg = ranking_cfg
        self._reversal_cfg = reversal_cfg

    # ------------------------------------------------------------------
    # Individual metric computation
    # ------------------------------------------------------------------

    def compute_value_metrics(
        self, f: ExtendedFundamentals
    ) -> dict[str, float | None]:
        """Derive EV/EBIT, EV/Sales, B/M, Shareholder_Yield from fundamentals.

        Returns dict with keys 'ev_ebit', 'ev_sales', 'book_to_market',
        'shareholder_yield'. Any key may be None if inputs are insufficient.
        """
        # Enterprise Value = market_cap + total_debt - cash
        ev: float | None = None
        if (
            f.market_cap is not None
            and f.total_debt is not None
            and f.cash_and_equivalents is not None
        ):
            ev = f.market_cap + f.total_debt - f.cash_and_equivalents

        # EV/EBIT (lower = cheaper; will be inverted during z-scoring)
        ev_ebit: float | None = None
        if ev is not None and f.ebit is not None and f.ebit > 0:
            ev_ebit = ev / f.ebit

        # EV/Sales (lower = cheaper; will be inverted during z-scoring)
        ev_sales: float | None = None
        if ev is not None and f.revenue_ttm is not None and f.revenue_ttm > 0:
            ev_sales = ev / f.revenue_ttm

        # Book-to-Market (higher = cheaper; no inversion needed)
        book_to_market: float | None = None
        if (
            f.book_value_of_equity is not None
            and f.market_cap is not None
            and f.market_cap > 0
        ):
            book_to_market = f.book_value_of_equity / f.market_cap

        # Shareholder Yield = (|dividends| + |buybacks|) / market_cap
        shareholder_yield: float | None = None
        if f.market_cap is not None and f.market_cap > 0:
            divs = f.dividends_paid_ttm
            buybacks = f.share_buybacks_ttm
            if divs is None and buybacks is None:
                shareholder_yield = None
            else:
                d = abs(divs) if divs is not None else 0.0
                b = abs(buybacks) if buybacks is not None else 0.0
                shareholder_yield = (d + b) / f.market_cap

        return {
            "ev_ebit": ev_ebit,
            "ev_sales": ev_sales,
            "book_to_market": book_to_market,
            "shareholder_yield": shareholder_yield,
        }

    def compute_quality_metrics(
        self, f: ExtendedFundamentals
    ) -> dict[str, float | None]:
        """Derive GP/A, Earnings_Stability, Low_Accruals from fundamentals.

        Returns dict with keys 'gp_a', 'earnings_stability', 'low_accruals'.
        """
        # GP/A = (revenue - COGS) / total_assets (Novy-Marx 2013)
        gp_a: float | None = None
        if (
            f.revenue_ttm is not None
            and f.cogs_ttm is not None
            and f.total_assets is not None
            and f.total_assets > 0
        ):
            gp_a = (f.revenue_ttm - f.cogs_ttm) / f.total_assets

        # Earnings_Stability = -stddev(annual_eps_5y)
        earnings_stability: float | None = None
        if f.annual_eps_5y is not None:
            valid_eps = [v for v in f.annual_eps_5y if v is not None]
            if len(valid_eps) >= 3:
                earnings_stability = -pstdev(valid_eps)

        # Low_Accruals = -(net_income - operating_cash_flow) / total_assets
        low_accruals: float | None = None
        if (
            f.net_income_ttm is not None
            and f.operating_cash_flow_ttm is not None
            and f.total_assets is not None
            and f.total_assets > 0
        ):
            accruals = (f.net_income_ttm - f.operating_cash_flow_ttm) / f.total_assets
            low_accruals = -accruals

        return {
            "gp_a": gp_a,
            "earnings_stability": earnings_stability,
            "low_accruals": low_accruals,
        }

    def compute_reversal_signal(
        self,
        close_series: pd.Series,
        lookback_days: int,
    ) -> float | None:
        """Compute raw trailing return for one ticker. None if insufficient data.

        Raw return = (close_today - close_N_days_ago) / close_N_days_ago
        """
        if close_series is None or len(close_series) < lookback_days + 1:
            return None
        close_today = close_series.iloc[-1]
        close_n_ago = close_series.iloc[-(lookback_days + 1)]
        if close_n_ago is None or close_n_ago == 0:
            return None
        if math.isnan(close_today) or math.isnan(close_n_ago):
            return None
        return (close_today - close_n_ago) / close_n_ago

    # ------------------------------------------------------------------
    # Cross-sectional z-score computation
    # ------------------------------------------------------------------

    def _compute_z_scores(
        self,
        values: list[tuple[str, float]],
        sector_neutral: bool,
        sectors: dict[str, str],
        min_sector_size: int,
    ) -> dict[str, float]:
        """Compute z-scores, optionally sector-neutral with fallback.

        Args:
            values: list of (ticker, metric_value) pairs
            sector_neutral: whether to z-score within sector
            sectors: ticker -> GICS sector mapping
            min_sector_size: minimum tickers for sector-neutral scoring

        Returns:
            dict of ticker -> z-score
        """
        if not values:
            return {}

        if not sector_neutral:
            return self._z_score_group(values)

        # Partition by sector
        by_sector: dict[str, list[tuple[str, float]]] = {}
        for ticker, val in values:
            sector = sectors.get(ticker, "unknown")
            by_sector.setdefault(sector, []).append((ticker, val))

        result: dict[str, float] = {}
        undersized_tickers: list[tuple[str, float]] = []

        for sector, sector_values in by_sector.items():
            if len(sector_values) < min_sector_size:
                _log.warning(
                    "Sector %s has %d tickers (< min_sector_size=%d); "
                    "falling back to cross-sectional",
                    sector,
                    len(sector_values),
                    min_sector_size,
                )
                undersized_tickers.extend(sector_values)
            else:
                sector_z = self._z_score_group(sector_values)
                result.update(sector_z)

        # Fallback: z-score undersized sectors against the full cross-section
        if undersized_tickers:
            all_values = [v for _, v in values]
            mu = _mean(all_values)
            sigma = pstdev(all_values)
            if sigma == 0:
                sigma = 1.0
            for ticker, val in undersized_tickers:
                result[ticker] = (val - mu) / sigma

        return result

    @staticmethod
    def _z_score_group(values: list[tuple[str, float]]) -> dict[str, float]:
        """Compute z-scores for a single group of (ticker, value) pairs."""
        if not values:
            return {}
        raw = [v for _, v in values]
        mu = _mean(raw)
        sigma = pstdev(raw)
        if sigma == 0:
            sigma = 1.0
        return {ticker: (val - mu) / sigma for ticker, val in values}

    # ------------------------------------------------------------------
    # Full composite scoring
    # ------------------------------------------------------------------

    def compute_composite_scores(
        self,
        fundamentals: dict[str, ExtendedFundamentals],
        reversal_returns: dict[str, float],
        sectors: dict[str, str],
    ) -> list[CompositeResult]:
        """Score all tickers. Handles sector-neutral z-scoring and fallback.

        Returns list of CompositeResult for tickers in the intersection
        of all three eligibility sets (value, quality, reversal).
        """
        cfg = self._cfg
        ranking_cfg = self._ranking_cfg
        reversal_cfg = self._reversal_cfg

        # Phase 1: Compute raw metrics for all tickers
        value_metrics: dict[str, dict[str, float | None]] = {}
        quality_metrics: dict[str, dict[str, float | None]] = {}

        for ticker, f in fundamentals.items():
            value_metrics[ticker] = self.compute_value_metrics(f)
            quality_metrics[ticker] = self.compute_quality_metrics(f)

        # Phase 2: Filter to tickers with complete data for each composite
        value_eligible: dict[str, dict[str, float]] = {}
        for ticker, m in value_metrics.items():
            if all(
                m[k] is not None and math.isfinite(m[k])  # type: ignore[arg-type]
                for k in ("ev_ebit", "ev_sales", "book_to_market", "shareholder_yield")
            ):
                value_eligible[ticker] = {k: m[k] for k in m}  # type: ignore[misc]
            else:
                missing = [
                    k for k in ("ev_ebit", "ev_sales", "book_to_market", "shareholder_yield")
                    if m[k] is None or (m[k] is not None and not math.isfinite(m[k]))  # type: ignore[arg-type]
                ]
                _log.debug(
                    "Excluded %s from value scoring: missing %s",
                    ticker,
                    ", ".join(missing),
                )

        quality_eligible: dict[str, dict[str, float]] = {}
        for ticker, m in quality_metrics.items():
            if all(
                m[k] is not None and math.isfinite(m[k])  # type: ignore[arg-type]
                for k in ("gp_a", "earnings_stability", "low_accruals")
            ):
                quality_eligible[ticker] = {k: m[k] for k in m}  # type: ignore[misc]
            else:
                missing = [
                    k for k in ("gp_a", "earnings_stability", "low_accruals")
                    if m[k] is None or (m[k] is not None and not math.isfinite(m[k]))  # type: ignore[arg-type]
                ]
                _log.debug(
                    "Excluded %s from quality scoring: missing %s",
                    ticker,
                    ", ".join(missing),
                )

        reversal_eligible: dict[str, float] = {}
        for ticker, ret in reversal_returns.items():
            if ret is not None and math.isfinite(ret):
                reversal_eligible[ticker] = ret
            else:
                _log.debug(
                    "Excluded %s from reversal scoring: null or non-finite return",
                    ticker,
                )

        # Phase 3: Z-score each metric
        sector_neutral = ranking_cfg.sector_neutral
        min_sector_size = ranking_cfg.min_sector_size

        # Value z-scores (EV/EBIT and EV/Sales are INVERTED)
        ev_ebit_z = self._compute_z_scores(
            [(t, m["ev_ebit"]) for t, m in value_eligible.items()],
            sector_neutral, sectors, min_sector_size,
        )
        ev_ebit_z = {t: -z for t, z in ev_ebit_z.items()}  # INVERT

        ev_sales_z = self._compute_z_scores(
            [(t, m["ev_sales"]) for t, m in value_eligible.items()],
            sector_neutral, sectors, min_sector_size,
        )
        ev_sales_z = {t: -z for t, z in ev_sales_z.items()}  # INVERT

        bm_z = self._compute_z_scores(
            [(t, m["book_to_market"]) for t, m in value_eligible.items()],
            sector_neutral, sectors, min_sector_size,
        )

        sy_z = self._compute_z_scores(
            [(t, m["shareholder_yield"]) for t, m in value_eligible.items()],
            sector_neutral, sectors, min_sector_size,
        )

        # Quality z-scores (all higher = better, no inversion)
        gpa_z = self._compute_z_scores(
            [(t, m["gp_a"]) for t, m in quality_eligible.items()],
            sector_neutral, sectors, min_sector_size,
        )

        es_z = self._compute_z_scores(
            [(t, m["earnings_stability"]) for t, m in quality_eligible.items()],
            sector_neutral, sectors, min_sector_size,
        )

        la_z = self._compute_z_scores(
            [(t, m["low_accruals"]) for t, m in quality_eligible.items()],
            sector_neutral, sectors, min_sector_size,
        )

        # Reversal z-score (negate raw return: lower return = higher signal)
        reversal_enabled = (
            reversal_cfg.enabled if reversal_cfg is not None else True
        )
        if reversal_enabled:
            rev_z = self._compute_z_scores(
                [(t, -r) for t, r in reversal_eligible.items()],
                sector_neutral, sectors, min_sector_size,
            )
        else:
            rev_z = {t: 0.0 for t in reversal_eligible}

        # Phase 4: Compute composites
        scoreable = (
            set(value_eligible.keys())
            & set(quality_eligible.keys())
            & set(reversal_eligible.keys())
        )

        results: list[CompositeResult] = []
        for ticker in sorted(scoreable):  # sorted for determinism
            # Value composite = mean of 4 z-scores
            vc = _mean([
                ev_ebit_z[ticker],
                ev_sales_z[ticker],
                bm_z[ticker],
                sy_z[ticker],
            ])

            # Quality composite = mean of 3 z-scores
            qc = _mean([
                gpa_z[ticker],
                es_z[ticker],
                la_z[ticker],
            ])

            # Reversal signal
            rs = rev_z.get(ticker, 0.0)

            # Composite score = weighted sum
            composite_score = (
                cfg.value_weight * vc
                + cfg.quality_weight * qc
                + cfg.reversal_weight * rs
            )

            results.append(CompositeResult(
                ticker=ticker,
                sector=sectors.get(ticker, "unknown"),
                value_composite=vc,
                quality_composite=qc,
                reversal_signal=rs,
                composite_score=composite_score,
                ev_ebit=value_metrics[ticker].get("ev_ebit"),
                ev_sales=value_metrics[ticker].get("ev_sales"),
                book_to_market=value_metrics[ticker].get("book_to_market"),
                shareholder_yield=value_metrics[ticker].get("shareholder_yield"),
                gp_a=quality_metrics[ticker].get("gp_a"),
                earnings_stability=quality_metrics[ticker].get("earnings_stability"),
                low_accruals=quality_metrics[ticker].get("low_accruals"),
                raw_reversal_return=reversal_returns.get(ticker),
            ))

        return results
