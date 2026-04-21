"""Fundamentals_Service: cache-gated EDGAR + yfinance adapter.

Replaces the original FMP adapter. Sources now:

* **SEC EDGAR CompanyFacts API** — authoritative US GAAP XBRL. Free,
  no key, 10 req/sec rate limit.
* **yfinance** — current price, 6y historical closes for 5y P/E
  derivation, latest news headline.

Derived fields:

* ``pe_ratio``        = current_price / TTM diluted EPS
* ``pe_5y_avg``       = mean of (close_at_fy_end / fy_eps) over 5 years
* ``fcf_yield``       = TTM (OpCashFlow - |CapEx|) / market_cap
* ``market_cap``      = shares_outstanding * current_price
* ``latest_headline`` = first ``yfinance.Ticker(t).news`` entry

Cache semantics and the budget-exhausted stale-cache fallback from the
FMP era are preserved. ``FmpClient``/``FmpBudgetExhausted`` remain as
aliases so the orchestrator works unchanged.
"""

from __future__ import annotations

import logging
import math
from datetime import date, datetime, timedelta, timezone
from statistics import mean
from typing import Any, Iterable

import requests
import yfinance as yf

from bot.config import FmpConfig
from bot.repo import Fundamentals, Repository  # noqa: F401

__all__ = [
    "Fundamentals",
    "FundamentalsBudgetExhausted",
    "FundamentalsClient",
    "FmpBudgetExhausted",
    "FmpClient",
    "get_fundamentals",
]

_log = logging.getLogger(__name__)

# The SEC asks every CompanyFacts consumer to identify themselves in the
# User-Agent. Requests without a recognisable UA return 403.
_EDGAR_UA = "asset-discovery-bot bot@asset-discovery-bot.local"


class FundamentalsBudgetExhausted(Exception):
    """Unrecoverable fundamentals failure for the remainder of this run."""


# Legacy alias — orchestrator imports ``FmpBudgetExhausted``.
FmpBudgetExhausted = FundamentalsBudgetExhausted


def _parse_float(value: Any) -> float | None:
    """Coerce to a finite float, else None. Tolerant of FMP/XBRL quirks."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        out = float(value)
        return None if (math.isnan(out) or math.isinf(out)) else out
    if isinstance(value, str):
        s = value.strip()
        if not s or s.lower() in {"none", "nan"}:
            return None
        try:
            out = float(s)
        except ValueError:
            return None
        return None if (math.isnan(out) or math.isinf(out)) else out
    return None


# ---------------------------------------------------------------------------
# SEC EDGAR — ticker -> CIK lookup (process-wide cache) and CompanyFacts
# ---------------------------------------------------------------------------


_TICKER_CIK_MAP: dict[str, str] | None = None


def _load_ticker_cik_map() -> dict[str, str]:
    """Fetch the SEC ticker -> CIK table once per process."""
    global _TICKER_CIK_MAP
    if _TICKER_CIK_MAP is not None:
        return _TICKER_CIK_MAP
    resp = requests.get(
        "https://www.sec.gov/files/company_tickers.json",
        timeout=10.0,
        headers={"User-Agent": _EDGAR_UA},
    )
    resp.raise_for_status()
    raw = resp.json()
    result: dict[str, str] = {}
    for entry in raw.values():
        ticker = str(entry.get("ticker", "")).upper()
        cik = entry.get("cik_str")
        if ticker and cik is not None:
            result[ticker] = str(cik).zfill(10)
    _TICKER_CIK_MAP = result
    _log.info("Loaded SEC ticker->CIK map: %d entries", len(result))
    return result


def _cik_from_ticker(ticker: str) -> str | None:
    """Map Wikipedia-style ``BRK.B`` to the SEC's 10-digit CIK."""
    mapping = _load_ticker_cik_map()
    upper = ticker.upper()
    for candidate in (upper, upper.replace(".", "-"), upper.replace(".", "")):
        cik = mapping.get(candidate)
        if cik:
            return cik
    return None


def _edgar_company_facts(cik: str) -> dict[str, Any] | None:
    """Fetch CompanyFacts JSON for one CIK. Returns None on any failure."""
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    try:
        resp = requests.get(
            url,
            timeout=15.0,
            headers={"User-Agent": _EDGAR_UA, "Accept": "application/json"},
        )
    except (requests.ConnectionError, requests.Timeout) as exc:
        _log.warning("EDGAR transport error for CIK %s: %s", cik, exc)
        return None
    if resp.status_code == 404:
        _log.info("EDGAR has no CompanyFacts for CIK %s", cik)
        return None
    if not resp.ok:
        _log.warning("EDGAR returned HTTP %d for CIK %s", resp.status_code, cik)
        return None
    try:
        return resp.json()
    except ValueError as exc:
        _log.warning("EDGAR returned unparseable JSON for CIK %s: %s", cik, exc)
        return None


# ---------------------------------------------------------------------------
# XBRL concept extraction
# ---------------------------------------------------------------------------


# Some filers use historical-alias concept names; we try candidates in
# order and use the first that returns non-empty data.
_CONCEPT_EPS_DILUTED = ("EarningsPerShareDiluted",)
_CONCEPT_OPERATING_CASH_FLOW = (
    "NetCashProvidedByUsedInOperatingActivities",
    "NetCashProvidedByOperatingActivities",
)
_CONCEPT_CAPEX = (
    "PaymentsToAcquirePropertyPlantAndEquipment",
    "PaymentsToAcquireProductiveAssets",
)
_CONCEPT_SHARES_OUTSTANDING = (
    "CommonStockSharesOutstanding",
    "dei:EntityCommonStockSharesOutstanding",
)


def _facts_for_concept(
    company_facts: dict[str, Any],
    concept_candidates: Iterable[str],
) -> list[dict[str, Any]]:
    """Return entries for the first concept that resolves (us-gaap, then dei).

    CompanyFacts layout::

        { "facts": { "us-gaap": { "<Concept>": { "units": { "USD": [...] } } } } }

    ``dei:Foo`` in the candidate list forces that taxonomy. All unit
    variants are flattened; the caller already knows which concept they
    asked for and hence which unit to expect.
    """
    facts_root = company_facts.get("facts") or {}
    taxonomies = ("us-gaap", "dei")
    for concept in concept_candidates:
        if ":" in concept:
            taxonomy, stripped = concept.split(":", 1)
            scopes = ((taxonomy, stripped),)
        else:
            scopes = tuple((tax, concept) for tax in taxonomies)
        for taxonomy, concept_name in scopes:
            block = (facts_root.get(taxonomy) or {}).get(concept_name) or {}
            units = block.get("units") or {}
            for entries in units.values():
                if entries:
                    return list(entries)
    return []


def _latest_quarterly(entries: list[dict[str, Any]], n: int) -> list[dict[str, Any]]:
    """Newest-first ``n`` entries deduped on ``end`` (amendments preserved)."""
    filtered = [e for e in entries if e.get("fp") in {"Q1", "Q2", "Q3", "FY"}]
    filtered.sort(
        key=lambda e: (str(e.get("end", "")), str(e.get("filed", ""))),
        reverse=True,
    )
    seen: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for entry in filtered:
        end = str(entry.get("end", ""))
        if end in seen:
            continue
        seen.add(end)
        deduped.append(entry)
    return deduped[:n]


def _ttm_sum(entries: list[dict[str, Any]]) -> float | None:
    """Sum the ``val`` across 4 newest quarters; None if <4 quarters."""
    quarters = _latest_quarterly(entries, n=4)
    if len(quarters) < 4:
        return None
    values = [_parse_float(q.get("val")) for q in quarters]
    if any(v is None for v in values):
        return None
    return float(sum(v for v in values if v is not None))


def _ttm_eps(eps_entries: list[dict[str, Any]]) -> float | None:
    """TTM diluted EPS: prefer newest FY, else sum of 4 newest quarters."""
    if not eps_entries:
        return None
    quarters = _latest_quarterly(eps_entries, n=4)
    if not quarters:
        return None
    if quarters[0].get("fp") == "FY":
        return _parse_float(quarters[0].get("val"))
    if len(quarters) < 4:
        return None
    values = [_parse_float(q.get("val")) for q in quarters]
    if any(v is None for v in values):
        return None
    return float(sum(v for v in values if v is not None))


def _annual_eps_by_fy(eps_entries: list[dict[str, Any]]) -> dict[int, float]:
    """Map fiscal year -> FY diluted EPS. Last restatement wins."""
    result: dict[int, float] = {}
    for entry in eps_entries:
        if entry.get("fp") != "FY":
            continue
        fy = entry.get("fy")
        val = _parse_float(entry.get("val"))
        if fy is None or val is None:
            continue
        result[int(fy)] = val
    return result


# ---------------------------------------------------------------------------
# SEC EDGAR — ticker -> CIK lookup (process cache) and CompanyFacts fetch
# ---------------------------------------------------------------------------


_TICKER_CIK_MAP: dict[str, str] | None = None


def _load_ticker_cik_map() -> dict[str, str]:
    """Fetch the SEC ticker -> CIK table once per process."""
    global _TICKER_CIK_MAP
    if _TICKER_CIK_MAP is not None:
        return _TICKER_CIK_MAP
    resp = requests.get(
        "https://www.sec.gov/files/company_tickers.json",
        timeout=10.0,
        headers={"User-Agent": _EDGAR_UA},
    )
    resp.raise_for_status()
    raw = resp.json()
    result: dict[str, str] = {}
    for entry in raw.values():
        ticker = str(entry.get("ticker", "")).upper()
        cik = entry.get("cik_str")
        if ticker and cik is not None:
            result[ticker] = str(cik).zfill(10)
    _TICKER_CIK_MAP = result
    _log.info("Loaded SEC ticker->CIK map: %d entries", len(result))
    return result


def _cik_from_ticker(ticker: str) -> str | None:
    """Map Wikipedia-style ``BRK.B`` to SEC's 10-digit CIK."""
    mapping = _load_ticker_cik_map()
    upper = ticker.upper()
    for candidate in (upper, upper.replace(".", "-"), upper.replace(".", "")):
        cik = mapping.get(candidate)
        if cik:
            return cik
    return None


def _edgar_company_facts(cik: str) -> dict[str, Any] | None:
    """Fetch CompanyFacts JSON for one CIK. Returns None on any failure."""
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    try:
        resp = requests.get(
            url,
            timeout=15.0,
            headers={"User-Agent": _EDGAR_UA, "Accept": "application/json"},
        )
    except (requests.ConnectionError, requests.Timeout) as exc:
        _log.warning("EDGAR transport error for CIK %s: %s", cik, exc)
        return None
    if resp.status_code == 404:
        _log.info("EDGAR has no CompanyFacts for CIK %s", cik)
        return None
    if not resp.ok:
        _log.warning("EDGAR returned HTTP %d for CIK %s", resp.status_code, cik)
        return None
    try:
        return resp.json()
    except ValueError as exc:
        _log.warning("EDGAR returned unparseable JSON for CIK %s: %s", cik, exc)
        return None


# ---------------------------------------------------------------------------
# yfinance-backed helpers
# ---------------------------------------------------------------------------


def _yf_ticker(symbol: str) -> Any:
    """Return yfinance.Ticker with Wikipedia -> Yahoo symbol normalisation."""
    return yf.Ticker(symbol.replace(".", "-"))


def _current_price(symbol: str) -> float | None:
    """Latest close price, or None on any yfinance failure."""
    try:
        hist = _yf_ticker(symbol).history(period="5d")
    except Exception as exc:  # noqa: BLE001 — yfinance raises varied types
        _log.warning("yfinance history failed for %s: %s", symbol, exc)
        return None
    if hist is None or len(hist) == 0 or "Close" not in hist.columns:
        return None
    return _parse_float(hist["Close"].iloc[-1])


def _historical_closes(symbol: str):
    """~6 years of daily closes as a pandas Series, or None."""
    try:
        hist = _yf_ticker(symbol).history(period="6y")
    except Exception as exc:  # noqa: BLE001
        _log.warning("yfinance 6y history failed for %s: %s", symbol, exc)
        return None
    if hist is None or len(hist) == 0 or "Close" not in hist.columns:
        return None
    return hist["Close"]


def _latest_headline(symbol: str) -> tuple[str | None, str | None]:
    """Most recent (title, url) from yfinance, or (None, None).

    yfinance's news payload shape drifts across releases. We accept
    both the newer wrapped ``{"content": {...}}`` form and the older
    flat ``{"title": ..., "link": ...}`` form.
    """
    try:
        items = _yf_ticker(symbol).news or []
    except Exception as exc:  # noqa: BLE001
        _log.debug("yfinance news failed for %s: %s", symbol, exc)
        return None, None
    for item in items:
        if not isinstance(item, dict):
            continue
        content = item.get("content")
        if isinstance(content, dict):
            title = content.get("title")
            url = None
            cu = content.get("canonicalUrl")
            if isinstance(cu, dict):
                url = cu.get("url")
            if title:
                return str(title), (str(url) if url else None)
        title = item.get("title")
        if title:
            link = item.get("link") or item.get("url")
            return str(title), (str(link) if link else None)
    return None, None


# ---------------------------------------------------------------------------
# 5-year average P/E from annual EPS + historical closes
# ---------------------------------------------------------------------------


def _pe_5y_avg_from_eps_and_price(
    annual_eps: dict[int, float],
    historical_close: Any,
) -> float | None:
    """Mean of (close_at_fy_end / fy_eps) across 5 most recent fiscal years.

    Skips years with EPS <= 0 (negative earnings produce nonsense P/Es).
    Returns ``None`` if fewer than 3 valid data points resolve — a
    2-point "average" is too easily distorted by one outlier year to
    trust Layer 3 with.
    """
    if historical_close is None or len(historical_close) == 0:
        return None
    years = sorted(annual_eps.keys(), reverse=True)[:5]
    if not years:
        return None

    pe_values: list[float] = []
    for fy in years:
        eps = annual_eps[fy]
        if eps is None or eps <= 0:
            continue
        target = date(fy, 12, 31)
        try:
            idx_dates = historical_close.index.date
        except AttributeError:
            continue
        try:
            slice_ = historical_close[idx_dates <= target]
        except Exception:  # noqa: BLE001
            continue
        if len(slice_) == 0:
            continue
        close = _parse_float(slice_.iloc[-1])
        if close is None or close <= 0:
            continue
        pe_values.append(close / eps)

    if len(pe_values) < 3:
        return None
    return mean(pe_values)


# ---------------------------------------------------------------------------
# FundamentalsClient — run-scoped counter + kill-switch + per-ticker fetch
# ---------------------------------------------------------------------------


class FundamentalsClient:
    """EDGAR + yfinance adapter with the old FmpClient's public interface.

    Exposes:

    * :attr:`call_count` — monotonic tally of outbound HTTP attempts.
    * :attr:`budget_exhausted` — becomes ``True`` when a persistent
      failure makes further fetches pointless. In practice EDGAR's
      rate limit (10 req/sec) is never reachable at our scale, so this
      mostly stays ``False`` for the life of a run.

    The ``api_key`` and ``cfg`` arguments are accepted for signature
    compatibility with the old :class:`FmpClient` — the orchestrator
    instantiates us as ``FmpClient(api_key=..., cfg=cfg.fmp)`` and we
    preserve that call shape so nothing needs editing.
    """

    def __init__(
        self,
        api_key: str | None = None,
        cfg: FmpConfig | None = None,
    ) -> None:
        self._api_key = api_key  # unused; EDGAR needs no key
        self._cfg = cfg or FmpConfig()
        self.call_count: int = 0
        self._budget_exhausted: bool = False

    @property
    def budget_exhausted(self) -> bool:
        return self._budget_exhausted

    def fetch(self, ticker: str) -> dict[str, Any]:
        """Fetch every derived field for one ticker.

        Returns a dict with keys ``pe_ratio``, ``pe_5y_avg``,
        ``fcf_yield``, ``market_cap``, ``latest_headline``,
        ``headline_url``. Any key may be ``None``; callers aggregate
        into a :class:`Fundamentals` record.
        """
        result: dict[str, Any] = {
            "pe_ratio": None,
            "pe_5y_avg": None,
            "fcf_yield": None,
            "market_cap": None,
            "latest_headline": None,
            "headline_url": None,
        }

        # CIK lookup (first call may populate the module cache)
        self.call_count += 1
        cik = _cik_from_ticker(ticker)
        if cik is None:
            _log.warning("No SEC CIK for ticker=%s; skipping", ticker)
            return result

        # CompanyFacts
        self.call_count += 1
        facts = _edgar_company_facts(cik)
        if facts is None:
            return result

        # TTM EPS + annual EPS history
        eps_entries = _facts_for_concept(facts, _CONCEPT_EPS_DILUTED)
        ttm_eps = _ttm_eps(eps_entries)
        annual_eps = _annual_eps_by_fy(eps_entries)

        # TTM free cash flow
        ocf_entries = _facts_for_concept(facts, _CONCEPT_OPERATING_CASH_FLOW)
        capex_entries = _facts_for_concept(facts, _CONCEPT_CAPEX)
        ttm_ocf = _ttm_sum(ocf_entries)
        ttm_capex = _ttm_sum(capex_entries)
        ttm_fcf: float | None
        if ttm_ocf is not None and ttm_capex is not None:
            # CapEx is reported as a positive magnitude; subtract abs to
            # get FCF.
            ttm_fcf = ttm_ocf - abs(ttm_capex)
        else:
            ttm_fcf = None

        # Shares outstanding
        shares_entries = _facts_for_concept(facts, _CONCEPT_SHARES_OUTSTANDING)
        shares_latest = _latest_quarterly(shares_entries, n=1)
        shares_out = (
            _parse_float(shares_latest[0].get("val")) if shares_latest else None
        )

        # Current price (yfinance)
        self.call_count += 1
        current_price = _current_price(ticker)

        # Derivations
        if (
            shares_out is not None and shares_out > 0
            and current_price is not None and current_price > 0
        ):
            result["market_cap"] = shares_out * current_price

        if (
            ttm_eps is not None and ttm_eps > 0
            and current_price is not None and current_price > 0
        ):
            result["pe_ratio"] = current_price / ttm_eps

        if (
            ttm_fcf is not None
            and result["market_cap"] is not None
            and result["market_cap"] > 0
        ):
            result["fcf_yield"] = ttm_fcf / result["market_cap"]

        # 5y average P/E (needs historical prices + annual EPS)
        if annual_eps:
            self.call_count += 1
            historical = _historical_closes(ticker)
            result["pe_5y_avg"] = _pe_5y_avg_from_eps_and_price(
                annual_eps, historical
            )

        # Latest headline
        self.call_count += 1
        title, url = _latest_headline(ticker)
        result["latest_headline"] = title
        result["headline_url"] = url

        return result


# Legacy alias so ``from bot.fundamentals import FmpClient`` keeps working.
FmpClient = FundamentalsClient


# ---------------------------------------------------------------------------
# Public entry point — cache-gated fundamentals fetch
# ---------------------------------------------------------------------------


def get_fundamentals(
    ticker: str,
    repo: Repository,
    fmp_client: FundamentalsClient,
    staleness_days: int,
) -> Fundamentals:
    """Cache-gated fundamentals fetch for one ticker.

    1. If ``repo.load_fundamentals(ticker)`` has a row whose
       ``fetched_at`` is within ``staleness_days`` of ``now``, return
       it (Requirement 3.2).
    2. Otherwise, if the client's budget is already exhausted, return
       the stale cached row when available, else raise
       :class:`FundamentalsBudgetExhausted`.
    3. Otherwise, call :meth:`FundamentalsClient.fetch`, upsert, and
       return the fresh :class:`Fundamentals`.

    The parameter name ``fmp_client`` is preserved from the old signature
    so :mod:`bot.run` doesn't need editing; it now accepts a
    :class:`FundamentalsClient`.
    """
    cached = repo.load_fundamentals(ticker)
    now = datetime.now(timezone.utc)
    freshness = timedelta(days=staleness_days)

    if cached is not None:
        fetched_at = cached.fetched_at
        if fetched_at.tzinfo is None:
            fetched_at = fetched_at.replace(tzinfo=timezone.utc)
        if now - fetched_at < freshness:
            return cached

    if fmp_client.budget_exhausted:
        if cached is not None:
            _log.info(
                "Fundamentals budget exhausted; serving stale cache for %s",
                ticker,
            )
            return cached
        raise FundamentalsBudgetExhausted(
            f"No cached fundamentals for {ticker!r} and budget exhausted"
        )

    try:
        fields = fmp_client.fetch(ticker)
    except FundamentalsBudgetExhausted:
        if cached is not None:
            _log.info(
                "Budget exhausted mid-fetch for %s; serving stale cache",
                ticker,
            )
            return cached
        raise

    fresh = Fundamentals(
        ticker=ticker,
        pe_ratio=fields.get("pe_ratio"),
        pe_5y_avg=fields.get("pe_5y_avg"),
        fcf_yield=fields.get("fcf_yield"),
        latest_headline=fields.get("latest_headline"),
        headline_url=fields.get("headline_url"),
        fetched_at=now,
    )
    repo.upsert_fundamentals(fresh)
    return fresh
