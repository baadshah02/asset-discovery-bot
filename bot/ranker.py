"""Ranker + Top-N Selection for the Asset Discovery Bot.

This module takes Composite_Scores and produces Composite_Ranks with
deterministic tie-breaking, then selects the top-N candidates for emission.

Requirements traceability:
    4.1–4.7 — Sector-Neutral Ranking
    5.1–5.6 — Top-N Composite-Rank Output
    11.5    — Rank Bijection Within Sector
    11.6    — Deterministic Tie-Breaking
    11.7    — Top-N Output Bound
"""

from __future__ import annotations

from dataclasses import dataclass

from bot.config import RankingConfig
from bot.scoring import CompositeResult

__all__ = [
    "RankedCandidate",
    "Ranker",
]


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RankedCandidate:
    """A scored ticker with its assigned rank."""

    ticker: str
    sector: str
    composite_rank: int
    composite_score: float
    value_composite: float
    quality_composite: float
    reversal_signal: float
    # Preserved for embed rendering
    close: float
    market_cap: float | None
    latest_headline: str | None
    headline_url: str | None


# ---------------------------------------------------------------------------
# Ranker
# ---------------------------------------------------------------------------


class Ranker:
    """Stateless ranker — all state flows through method arguments."""

    def __init__(self, cfg: RankingConfig) -> None:
        self._cfg = cfg

    def rank(self, scores: list[CompositeResult]) -> list[RankedCandidate]:
        """Assign ranks. Sector-neutral or cross-sectional per config.

        Tie-breaking: lexicographic ascending on ticker symbol.
        Ranks within each sector (or globally) form {1, 2, ..., k}.
        """
        if not scores:
            return []

        if self._cfg.sector_neutral:
            return self._rank_sector_neutral(scores)
        else:
            return self._rank_cross_sectional(scores)

    def _rank_sector_neutral(
        self, scores: list[CompositeResult]
    ) -> list[RankedCandidate]:
        """Partition by sector, sort each, assign contiguous ranks."""
        by_sector: dict[str, list[CompositeResult]] = {}
        for score in scores:
            by_sector.setdefault(score.sector, []).append(score)

        all_ranked: list[RankedCandidate] = []
        for sector, sector_scores in by_sector.items():
            # Sort: composite_score DESC, then ticker ASC (tie-break)
            sorted_scores = sorted(
                sector_scores,
                key=lambda s: (-s.composite_score, s.ticker),
            )
            for i, score in enumerate(sorted_scores, start=1):
                all_ranked.append(RankedCandidate(
                    ticker=score.ticker,
                    sector=sector,
                    composite_rank=i,
                    composite_score=score.composite_score,
                    value_composite=score.value_composite,
                    quality_composite=score.quality_composite,
                    reversal_signal=score.reversal_signal,
                    close=0.0,  # populated by orchestrator
                    market_cap=None,
                    latest_headline=None,
                    headline_url=None,
                ))

        return all_ranked

    def _rank_cross_sectional(
        self, scores: list[CompositeResult]
    ) -> list[RankedCandidate]:
        """Sort all by composite_score DESC, assign contiguous ranks."""
        sorted_scores = sorted(
            scores,
            key=lambda s: (-s.composite_score, s.ticker),
        )

        ranked: list[RankedCandidate] = []
        for i, score in enumerate(sorted_scores, start=1):
            ranked.append(RankedCandidate(
                ticker=score.ticker,
                sector=score.sector,
                composite_rank=i,
                composite_score=score.composite_score,
                value_composite=score.value_composite,
                quality_composite=score.quality_composite,
                reversal_signal=score.reversal_signal,
                close=0.0,  # populated by orchestrator
                market_cap=None,
                latest_headline=None,
                headline_url=None,
            ))

        return ranked

    def select_top_n(
        self, ranked: list[RankedCandidate]
    ) -> list[RankedCandidate]:
        """Select top_n_per_sector (sector-neutral) or top_n (cross-sectional).

        Returns candidates ordered by composite_score descending,
        ties broken by ticker ascending.
        """
        if not ranked:
            return []

        if self._cfg.sector_neutral:
            # Select top_n_per_sector from each sector
            selected: list[RankedCandidate] = []
            by_sector: dict[str, list[RankedCandidate]] = {}
            for candidate in ranked:
                by_sector.setdefault(candidate.sector, []).append(candidate)

            for sector, sector_ranked in by_sector.items():
                top_in_sector = [
                    c for c in sector_ranked
                    if c.composite_rank <= self._cfg.top_n_per_sector
                ]
                selected.extend(top_in_sector)
        else:
            # Select top_n overall
            selected = [
                c for c in ranked
                if c.composite_rank <= self._cfg.top_n
            ]

        # Final ordering: composite_score DESC, ticker ASC (tie-break)
        selected.sort(key=lambda c: (-c.composite_score, c.ticker))

        return selected
