"""Fundamentals_Service: cache-gated FMP adapter for the Asset Discovery Bot.

This module implements Component 3 of the design (`bot.fundamentals`). It is
the **only** code path that talks to Financial Modeling Prep, and every call
is gated by a local ``fundamentals_cache`` lookup so a warm cache keeps the
bot well inside the FMP free-tier budget (250 calls/day).

High-level contract
-------------------

* :class:`FmpClient` wraps the four FMP endpoints consumed by the bot:
  ``/ratios/{ticker}``, ``/cash-flow-statement-ttm/{ticker}``,
  ``/profile/{ticker}``, and ``/press-releases/{ticker}``. Transport errors
  and 5xx responses are retried with ``tenacity`` exponential backoff; other
  4xx responses propagate unchanged.
* :func:`get_fundamentals` first consults :meth:`Repository.load_fundamentals`.
  A row whose ``fetched_at`` is within ``staleness_days`` is returned
  verbatim (Requirement 3.2). Only on a cache miss or stale row does the
  client actually call FMP, upsert the fresh row, and return it
  (Requirement 3.3).
* ``pe_5y_avg`` is derived as the arithmetic mean of ``priceEarningsRatio``
  across the most recent five annual ``/ratios`` entries (Requirement 3.5).
  ``fcf_yield = ttm_fcf / market_cap`` is computed only when ``market_cap``
  is strictly positive; otherwise it is persisted as ``None``
  (Requirement 3.4).

FMP 429 kill-switch semantics
-----------------------------

Each :class:`FmpClient` instance owns a run-scoped ``budget_exhausted``
flag. The first HTTP 429 response flips that flag to ``True`` and raises
:class:`FmpBudgetExhausted`. Every subsequent call on the same client
short-circuits to ``FmpBudgetExhausted`` **without** touching the network
(Requirements 3.7, 8.3). The orchestrator is expected to treat this as
"continue Layers 3/4 using only L2 survivors whose fundamentals are already
cached" and skip the rest — the translation from kill-switch to that
policy lives in :func:`get_fundamentals` and in the caller.

Call counter (Task 8.3)
-----------------------

The client also owns a run-scoped ``call_count`` that increments on every
outbound HTTP request (including the one that triggers the kill-switch).
This exposes the invariant in Requirement 11.4 / 9.5 — total FMP calls in
a run must be bounded by ``3 × N`` where ``N`` is the count of L2 survivors
that were cache-miss or cache-stale. The orchestrator logs the final value,
and the property-based test in task 8.6 reads it directly from the client.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable

import requests
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from bot.config import FmpConfig

# Re-exported for caller convenience. :class:`Fundamentals` is defined in
# :mod:`bot.repo` so the repository module has no upward imports from this
# module; importing it here and listing it in ``__all__`` preserves the
# historical call site ``from bot.fundamentals import Fundamentals``.
from bot.repo import Fundamentals, Repository  # noqa: F401 — re-exported

__all__ = [
    "Fundamentals",
    "FmpBudgetExhausted",
    "FmpClient",
    "get_fundamentals",
]


_log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class FmpBudgetExhausted(Exception):
    """Raised when the FMP 429 kill-switch has fired for this run.

    The first HTTP 429 response sets :attr:`FmpClient.budget_exhausted` to
    ``True`` and raises this exception. Every subsequent :class:`FmpClient`
    method call short-circuits to the same exception without issuing a
    network request. :func:`get_fundamentals` catches it and falls back to
    stale cache when available (Requirement 8.3).
    """


# ---------------------------------------------------------------------------
# Tenacity retry predicate
# ---------------------------------------------------------------------------


# tenacity predicates should be cheap; we keep this as a module-level
# callable so the retry decorator can be reused across methods.
def _retry_on_transient(exc: BaseException) -> bool:
    """Retry predicate: retry transport errors and 5xx; never retry 429/4xx.

    ``FmpBudgetExhausted`` is explicitly NOT retried because it represents
    a terminal run-scoped condition. HTTP 4xx responses other than 429
    (handled separately inside the methods) also propagate immediately —
    retrying a 404 or 401 only wastes budget.
    """
    if isinstance(exc, FmpBudgetExhausted):
        return False
    if isinstance(exc, (requests.ConnectionError, requests.Timeout)):
        return True
    if isinstance(exc, requests.HTTPError):
        response = exc.response
        if response is None:
            return True
        return 500 <= response.status_code < 600
    return False


# ---------------------------------------------------------------------------
# Helpers — numeric coercion and pe_5y_avg derivation
# ---------------------------------------------------------------------------


def _parse_float(value: Any) -> float | None:
    """Coerce ``value`` to ``float`` or return ``None`` on failure/NaN.

    FMP payloads occasionally contain ``None``, the literal string
    ``"None"``, empty strings, or quoted numerics. A single tolerant
    coercion keeps the call sites readable.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        # ``bool`` is a subtype of ``int`` in Python; guard against silently
        # coercing ``True`` to 1.0.
        return None
    if isinstance(value, (int, float)):
        as_float = float(value)
        if math.isnan(as_float) or math.isinf(as_float):
            return None
        return as_float
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped or stripped.lower() == "none":
            return None
        try:
            as_float = float(stripped)
        except ValueError:
            return None
        if math.isnan(as_float) or math.isinf(as_float):
            return None
        return as_float
    return None


def _first_not_none(payload: dict[str, Any], keys: Iterable[str]) -> Any:
    """Return the first non-None value found under any of ``keys``.

    FMP field names drift between endpoint versions (for example the P/E
    field appears as ``priceEarningsRatio`` on the annual ``/ratios``
    endpoint but as ``peRatio`` on some historical slices). Callers pass
    the preferred key first.
    """
    for key in keys:
        if key in payload and payload[key] is not None:
            return payload[key]
    return None


def _pe_5y_avg(ratios_list: list[dict[str, Any]]) -> float | None:
    """Arithmetic mean of per-year P/E across the most recent five entries.

    Implements Requirement 3.5. The ``/ratios/{ticker}`` endpoint returns
    annual rows ordered newest-first; we slice the first five, coerce each
    ``priceEarningsRatio`` (falling back to ``peRatio``) to ``float``, drop
    ``None`` values, and return the mean. If fewer than one non-null P/E
    survives, we return ``None`` rather than inventing a number.
    """
    pe_values: list[float] = []
    for entry in ratios_list[:5]:
        pe = _parse_float(_first_not_none(entry, ("priceEarningsRatio", "peRatio")))
        if pe is not None:
            pe_values.append(pe)
    if not pe_values:
        return None
    return sum(pe_values) / len(pe_values)


# ---------------------------------------------------------------------------
# FmpClient (Task 8.1 + 8.3)
# ---------------------------------------------------------------------------


class FmpClient:
    """Thin ``requests`` wrapper around the four FMP endpoints we consume.

    The client is intentionally stateless with respect to tickers — all
    per-ticker caching lives in :func:`get_fundamentals` against
    :class:`Repository`. The only state held here is run-scoped:

    * :attr:`call_count` — monotonically increasing counter of outbound
      HTTP attempts (Task 8.3 / Requirement 11.4).
    * :attr:`budget_exhausted` — becomes ``True`` the first time FMP
      returns HTTP 429, at which point every further method call raises
      :class:`FmpBudgetExhausted` without touching the network
      (Requirement 3.7 / 8.3).

    A fresh :class:`FmpClient` is instantiated per Scan_Run by the
    orchestrator; the counter and flag therefore reset naturally between
    runs without any manual teardown.
    """

    def __init__(self, api_key: str, cfg: FmpConfig) -> None:
        self._api_key = api_key
        self._cfg = cfg
        # Public, mutable-looking but read-only by convention. Exposed so
        # the orchestrator can log the total at end-of-run and tests can
        # assert against the budget bound.
        self.call_count: int = 0
        self._budget_exhausted: bool = False

    # ------------------------------------------------------------------
    # Public read-only state
    # ------------------------------------------------------------------

    @property
    def budget_exhausted(self) -> bool:
        """``True`` once FMP has returned HTTP 429 for this run."""
        return self._budget_exhausted

    # ------------------------------------------------------------------
    # Public endpoint methods
    # ------------------------------------------------------------------

    def get_ratios(self, ticker: str) -> list[dict[str, Any]]:
        """GET ``/ratios/{ticker}`` — annual ratios, newest first.

        Returns the raw list exactly as FMP delivered it. Empty list on
        unknown tickers.
        """
        payload = self._request(f"ratios/{ticker}")
        if isinstance(payload, list):
            return payload
        return []

    def get_cash_flow_ttm(self, ticker: str) -> dict[str, Any] | None:
        """GET ``/cash-flow-statement-ttm/{ticker}`` — single TTM row.

        FMP returns a list with (at most) one element containing
        ``freeCashFlow`` alongside the other TTM cash-flow line items. We
        choose the TTM endpoint over ``/cash-flow-statement`` so we don't
        have to reconstruct TTM from the most recent four quarters
        ourselves; the caller only needs a single scalar FCF.
        """
        payload = self._request(f"cash-flow-statement-ttm/{ticker}")
        if isinstance(payload, list) and payload:
            first = payload[0]
            if isinstance(first, dict):
                return first
        return None

    def get_profile(self, ticker: str) -> dict[str, Any] | None:
        """GET ``/profile/{ticker}`` — single-row company profile.

        The caller reads ``mktCap`` off the returned dict to compute
        ``fcf_yield``.
        """
        payload = self._request(f"profile/{ticker}")
        if isinstance(payload, list) and payload:
            first = payload[0]
            if isinstance(first, dict):
                return first
        return None

    def get_latest_press_release(self, ticker: str) -> dict[str, Any] | None:
        """GET ``/press-releases/{ticker}?limit=1`` — most recent headline.

        Returns a dict with at least ``title`` and optionally a URL under
        one of several historical field names (``url``, ``link``). When no
        explicit URL is present the caller may construct a fallback; this
        method only forwards what FMP returned.
        """
        payload = self._request(
            f"press-releases/{ticker}", extra_params={"limit": "1"}
        )
        if isinstance(payload, list) and payload:
            first = payload[0]
            if isinstance(first, dict):
                return first
        return None

    # ------------------------------------------------------------------
    # Internals — request dispatch with retry, counter, and kill-switch
    # ------------------------------------------------------------------

    def _request(
        self,
        path: str,
        *,
        extra_params: dict[str, str] | None = None,
    ) -> Any:
        """Dispatch one GET with retry, counter, and 429 kill-switch.

        The API key is passed as a query parameter rather than embedded
        in the URL so that error messages and log records built from
        ``response.url`` or the raw ``requests`` exception never echo
        the key (Requirement 5.7, 9.8 spirit).

        Tenacity is applied inside the method so the ``call_count``
        increment happens on every attempt, not only on the final
        outcome — the counter measures real outbound calls, not logical
        "get fundamentals for ticker X" operations.
        """
        # Kill-switch short-circuit: no network, no counter increment for
        # the short-circuit. The counter reflects actual HTTP attempts.
        if self._budget_exhausted:
            raise FmpBudgetExhausted(
                f"FMP budget exhausted for this run; skipping GET /{path}"
            )

        url = f"{self._cfg.base_url.rstrip('/')}/{path}"
        params: dict[str, str] = {"apikey": self._api_key}
        if extra_params:
            params.update(extra_params)

        # Wrap a single HTTP attempt so tenacity can retry transport
        # errors and 5xx without us re-running the kill-switch gate.
        def _one_attempt() -> Any:
            self.call_count += 1
            try:
                response = requests.get(
                    url,
                    params=params,
                    timeout=self._cfg.timeout_seconds,
                )
            except (requests.ConnectionError, requests.Timeout):
                # Re-raise so tenacity can see the transport exception.
                raise

            status = response.status_code
            if status == 429:
                # Kill-switch: set the flag BEFORE raising so any handler
                # up the stack that calls another method sees the flipped
                # state immediately.
                self._budget_exhausted = True
                _log.warning(
                    "FMP returned HTTP 429; kill-switch engaged for the rest "
                    "of this run (path=%s)",
                    path,
                )
                raise FmpBudgetExhausted(
                    f"FMP returned HTTP 429 for /{path}"
                )

            # Any other 4xx or 5xx raises HTTPError. Tenacity's predicate
            # decides whether to retry (5xx only) or propagate (4xx).
            response.raise_for_status()
            return response.json()

        # Build the retry wrapper lazily per call so tenacity's state is
        # scoped to this request rather than the client instance.
        wrapped: Callable[[], Any] = retry(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=1, min=1, max=5),
            retry=retry_if_exception(_retry_on_transient),
            reraise=True,
        )(_one_attempt)

        return wrapped()


# ---------------------------------------------------------------------------
# Public entry point — cache-gated fundamentals fetch (Task 8.2)
# ---------------------------------------------------------------------------


def get_fundamentals(
    ticker: str,
    repo: Repository,
    fmp_client: FmpClient,
    staleness_days: int,
) -> Fundamentals:
    """Cache-gated fundamentals fetch for a single ticker.

    Implements Requirements 3.1–3.5 plus the graceful-degradation
    contract from Requirement 8.3:

    1. Call :meth:`Repository.load_fundamentals`. If a row exists and
       ``now() - fetched_at < timedelta(days=staleness_days)``, return it
       unchanged. FMP is not consulted (Requirements 3.1, 3.2).
    2. On cache miss or stale row, and provided the client's 429
       kill-switch has not already fired, call the four FMP endpoints,
       derive ``pe_5y_avg`` and ``fcf_yield``, build a fresh
       :class:`Fundamentals`, upsert it, and return it (Requirement 3.3).
    3. If the kill-switch has fired (either before we entered the
       function or during the FMP calls), fall back to the stale cached
       row when one exists; otherwise re-raise :class:`FmpBudgetExhausted`
       so the caller can skip this ticker (Requirement 8.3).

    The caller (orchestrator / Filter_Pipeline) is responsible for
    restricting invocation to L2_Survivors (Requirement 3.6).
    """
    cached = repo.load_fundamentals(ticker)
    now = datetime.now(timezone.utc)
    freshness_window = timedelta(days=staleness_days)

    if cached is not None:
        # ``fetched_at`` is TIMESTAMPTZ coming back from psycopg; compare
        # in UTC to avoid accidental naive-vs-aware TypeErrors. If the
        # repository ever hands us a naive datetime we assume UTC.
        fetched_at = cached.fetched_at
        if fetched_at.tzinfo is None:
            fetched_at = fetched_at.replace(tzinfo=timezone.utc)
        if now - fetched_at < freshness_window:
            return cached

    # Cache missing or stale. If the kill-switch already fired we can't
    # make any more FMP calls this run — serve the stale cached row
    # (better than nothing) or give up on this ticker.
    if fmp_client.budget_exhausted:
        if cached is not None:
            _log.info(
                "FMP budget exhausted; serving stale cache for %s "
                "(fetched_at=%s)",
                ticker,
                cached.fetched_at.isoformat(),
            )
            return cached
        raise FmpBudgetExhausted(
            f"No cached fundamentals for {ticker!r} and FMP budget "
            f"exhausted for this run"
        )

    try:
        ratios = fmp_client.get_ratios(ticker)
        cash_flow = fmp_client.get_cash_flow_ttm(ticker)
        profile = fmp_client.get_profile(ticker)
        press = fmp_client.get_latest_press_release(ticker)
    except FmpBudgetExhausted:
        # Budget tipped over mid-fetch. Degrade to stale cache if we have
        # one; otherwise let the caller handle it.
        if cached is not None:
            _log.info(
                "FMP 429 during fetch for %s; falling back to stale cache "
                "(fetched_at=%s)",
                ticker,
                cached.fetched_at.isoformat(),
            )
            return cached
        raise

    # Derive the three numeric fields.
    pe_ratio = (
        _parse_float(
            _first_not_none(ratios[0], ("priceEarningsRatio", "peRatio"))
        )
        if ratios
        else None
    )
    pe_5y_avg = _pe_5y_avg(ratios)

    ttm_fcf = (
        _parse_float(cash_flow.get("freeCashFlow")) if cash_flow else None
    )
    market_cap = (
        _parse_float(profile.get("mktCap")) if profile else None
    )
    if ttm_fcf is not None and market_cap is not None and market_cap > 0:
        fcf_yield: float | None = ttm_fcf / market_cap
    else:
        # Requirement 3.4: NULL when market_cap is non-positive or missing.
        fcf_yield = None

    # Headline + optional URL. Respect whichever URL field FMP returned;
    # we don't synthesise a URL here because a dead link is worse than no
    # link in a Discord embed.
    if press is not None:
        latest_headline = press.get("title") or None
        headline_url = _first_not_none(press, ("url", "link", "site")) or None
    else:
        latest_headline = None
        headline_url = None

    fresh = Fundamentals(
        ticker=ticker,
        pe_ratio=pe_ratio,
        pe_5y_avg=pe_5y_avg,
        fcf_yield=fcf_yield,
        latest_headline=latest_headline,
        headline_url=headline_url,
        fetched_at=now,
    )
    repo.upsert_fundamentals(fresh)
    return fresh
