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
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from statistics import mean
from typing import Any, Iterable

import requests
import yfinance as yf

from bot.config import FmpConfig
from bot.repo import Fundamentals, Repository  # noqa: F401

__all__ = [
    "EdgarRateLimiter",
    "ExtendedFundamentals",
    "Fundamentals",
    "FundamentalsBudgetExhausted",
    "FundamentalsClient",
    "FmpBudgetExhausted",
    "FmpClient",
    "get_extended_fundamentals",
    "get_fundamentals",
]

_log = logging.getLogger(__name__)

# The SEC asks every CompanyFacts consumer to identify themselves in the
# User-Agent. Requests without a recognisable UA return 403.
_EDGAR_UA = "asset-discovery-bot bot@asset-discovery-bot.local"


# ---------------------------------------------------------------------------
# ExtendedFundamentals — v2 dataclass for composite scoring
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ExtendedFundamentals:
    """Extended record from EDGAR with all fields needed by the composite scorer.

    Carries all v1 fields plus the additional metrics required for
    Value_Composite, Quality_Composite, and Reversal_Signal computation.
    """

    ticker: str
    # v1 fields (preserved)
    pe_ratio: float | None
    pe_5y_avg: float | None
    fcf_yield: float | None
    latest_headline: str | None
    headline_url: str | None
    fetched_at: datetime
    # v2 extended fields
    total_debt: float | None
    cash_and_equivalents: float | None
    ebit: float | None
    revenue_ttm: float | None
    book_value_of_equity: float | None
    dividends_paid_ttm: float | None
    share_buybacks_ttm: float | None
    cogs_ttm: float | None
    total_assets: float | None
    annual_eps_5y: list[float] | None  # up to 5 annual EPS values
    net_income_ttm: float | None
    operating_cash_flow_ttm: float | None
    market_cap: float | None
    schema_version: int = 2


class EdgarRateLimiter:
    """Token-bucket rate limiter for EDGAR API calls (10 req/sec).

    Uses a simple interval-based approach: each :meth:`acquire` call
    blocks until at least ``1 / max_per_second`` seconds have elapsed
    since the previous call. Thread-safe via :class:`threading.Lock`.

    The :attr:`consecutive_waits` counter tracks how many back-to-back
    calls had to sleep, which the caller uses to detect sustained
    throttling (Req 7.6).
    """

    def __init__(self, max_per_second: float = 10.0) -> None:
        self._interval = 1.0 / max_per_second
        self._last_call: float = 0.0
        self._lock = threading.Lock()
        self._consecutive_waits: int = 0

    def acquire(self) -> None:
        """Block until a request slot is available."""
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_call
            if elapsed < self._interval:
                wait = self._interval - elapsed
                time.sleep(wait)
                self._consecutive_waits += 1
            else:
                self._consecutive_waits = 0
            self._last_call = time.monotonic()

    @property
    def consecutive_waits(self) -> int:
        """Number of consecutive :meth:`acquire` calls that had to sleep."""
        return self._consecutive_waits


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

# v2 extended concept mappings for composite scoring
_CONCEPT_TOTAL_DEBT = (
    "LongTermDebt",
    "LongTermDebtNoncurrent",
    "DebtCurrent",
)
_CONCEPT_SHORT_TERM_DEBT = (
    "ShortTermBorrowings",
    "DebtCurrent",
)
_CONCEPT_CASH = (
    "CashAndCashEquivalentsAtCarryingValue",
    "Cash",
)
_CONCEPT_EBIT = (
    "OperatingIncomeLoss",
    "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
)
_CONCEPT_REVENUE = (
    "Revenues",
    "RevenueFromContractWithCustomerExcludingAssessedTax",
)
_CONCEPT_BOOK_VALUE = (
    "StockholdersEquity",
    "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
)
_CONCEPT_DIVIDENDS = (
    "PaymentsOfDividends",
    "PaymentsOfDividendsCommonStock",
)
_CONCEPT_BUYBACKS = (
    "PaymentsForRepurchaseOfCommonStock",
    "PaymentsForRepurchaseOfEquity",
)
_CONCEPT_COGS = (
    "CostOfGoodsAndServicesSold",
    "CostOfRevenue",
)
_CONCEPT_TOTAL_ASSETS = (
    "Assets",
)
_CONCEPT_NET_INCOME = (
    "NetIncomeLoss",
    "ProfitLoss",
)
_CONCEPT_INTEREST_EXPENSE = (
    "InterestExpense",
    "InterestExpenseDebt",
)
_CONCEPT_TAX_EXPENSE = (
    "IncomeTaxExpenseBenefit",
)


def _facts_for_concept(
    company_facts: dict[str, Any],
    concept_candidates: Iterable[str],
    preferred_unit: str | None = None,
) -> list[dict[str, Any]]:
    """Return entries for the first concept that resolves (us-gaap, then dei).

    CompanyFacts layout::

        { "facts": { "us-gaap": { "<Concept>": { "units": { "USD": [...] } } } } }

    ``dei:Foo`` in the candidate list forces that taxonomy.

    When *preferred_unit* is given (e.g. ``"USD"`` or ``"shares"``),
    only that unit key is considered. If missing, falls back to the
    first non-empty unit (legacy behaviour).
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
            if preferred_unit:
                entries = units.get(preferred_unit)
                if entries:
                    return list(entries)
            else:
                for entries in units.values():
                    if entries:
                        return list(entries)
    return []


def _latest_quarterly(
    entries: list[dict[str, Any]], n: int, *, include_fy: bool = False
) -> list[dict[str, Any]]:
    """Newest-first ``n`` entries deduped on ``end``.

    When *include_fy* is False (default), FY entries are excluded to
    prevent double-counting when summing quarters for TTM.
    """
    allowed_fps = {"Q1", "Q2", "Q3", "Q4"}
    if include_fy:
        allowed_fps.add("FY")
    filtered = [e for e in entries if e.get("fp") in allowed_fps]
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
    """Compute trailing twelve months value.

    Strategy:
      1. If a recent FY (full-year) entry exists, use it directly.
      2. Otherwise sum the 4 newest quarterly entries.

    This avoids double-counting where FY already includes Q1–Q4.
    """
    # Try FY first — most accurate single value for TTM
    fy_entries = [e for e in entries if e.get("fp") == "FY"]
    if fy_entries:
        fy_entries.sort(
            key=lambda e: (str(e.get("end", "")), str(e.get("filed", ""))),
            reverse=True,
        )
        val = _parse_float(fy_entries[0].get("val"))
        if val is not None:
            return val

    # Fall back to summing 4 quarters (excluding FY to avoid double-count)
    quarters = _latest_quarterly(entries, n=4, include_fy=False)
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
    # Prefer FY entry (already a full-year figure)
    fy_entries = [e for e in eps_entries if e.get("fp") == "FY"]
    if fy_entries:
        fy_entries.sort(
            key=lambda e: (str(e.get("end", "")), str(e.get("filed", ""))),
            reverse=True,
        )
        val = _parse_float(fy_entries[0].get("val"))
        if val is not None:
            return val
    # Fall back to summing 4 quarters
    quarters = _latest_quarterly(eps_entries, n=4, include_fy=False)
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


def _yf_market_cap(symbol: str) -> float | None:
    """Market cap directly from yfinance (handles dual-class correctly)."""
    try:
        t = _yf_ticker(symbol)
        mc = t.fast_info.get("marketCap") or t.fast_info.get("market_cap")
        if mc is not None and mc > 0:
            return float(mc)
    except Exception as exc:  # noqa: BLE001
        _log.debug("yfinance market_cap failed for %s: %s", symbol, exc)
    return None


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
        self._rate_limiter = EdgarRateLimiter(max_per_second=10.0)
        self._consecutive_wait_start: float | None = None

    @property
    def budget_exhausted(self) -> bool:
        return self._budget_exhausted

    def _acquire_edgar_slot(self) -> None:
        """Acquire a rate-limiter slot before an EDGAR HTTP call.

        Logs a WARN if the rate limiter has been engaged for more than
        30 consecutive seconds (Req 7.6).
        """
        self._rate_limiter.acquire()
        if self._rate_limiter.consecutive_waits > 0:
            if self._consecutive_wait_start is None:
                self._consecutive_wait_start = time.monotonic()
            elapsed = time.monotonic() - self._consecutive_wait_start
            if elapsed > 30.0:
                _log.warning(
                    "EDGAR rate limiter engaged for %.1fs consecutive; "
                    "consider increasing cache.fundamentals_staleness_days "
                    "or reducing universe size",
                    elapsed,
                )
        else:
            self._consecutive_wait_start = None

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
        self._acquire_edgar_slot()
        cik = _cik_from_ticker(ticker)
        if cik is None:
            _log.warning("No SEC CIK for ticker=%s; skipping", ticker)
            return result

        # CompanyFacts
        self.call_count += 1
        self._acquire_edgar_slot()
        facts = _edgar_company_facts(cik)
        if facts is None:
            return result

        # TTM EPS + annual EPS history
        eps_entries = _facts_for_concept(facts, _CONCEPT_EPS_DILUTED, preferred_unit="USD/shares")
        ttm_eps = _ttm_eps(eps_entries)
        annual_eps = _annual_eps_by_fy(eps_entries)

        # TTM free cash flow
        ocf_entries = _facts_for_concept(facts, _CONCEPT_OPERATING_CASH_FLOW, preferred_unit="USD")
        capex_entries = _facts_for_concept(facts, _CONCEPT_CAPEX, preferred_unit="USD")
        ttm_ocf = _ttm_sum(ocf_entries)
        ttm_capex = _ttm_sum(capex_entries)
        ttm_fcf: float | None
        if ttm_ocf is not None and ttm_capex is not None:
            ttm_fcf = ttm_ocf - abs(ttm_capex)
        else:
            ttm_fcf = None

        # Market cap from yfinance (handles dual-class correctly)
        self.call_count += 1
        current_price = _current_price(ticker)
        self.call_count += 1
        yf_mcap = _yf_market_cap(ticker)
        if yf_mcap is not None:
            result["market_cap"] = yf_mcap
        else:
            # Fallback: EDGAR shares * price (may be wrong for dual-class)
            shares_entries = _facts_for_concept(facts, _CONCEPT_SHARES_OUTSTANDING, preferred_unit="shares")
            shares_latest = _latest_quarterly(shares_entries, n=1, include_fy=True)
            shares_out = (
                _parse_float(shares_latest[0].get("val")) if shares_latest else None
            )
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
            raw_fcf_yield = ttm_fcf / result["market_cap"]
            if abs(raw_fcf_yield) <= 10.0:
                result["fcf_yield"] = raw_fcf_yield
            else:
                _log.warning(
                    "Ticker %s: fcf_yield=%.4f implausible (unit mismatch?), "
                    "setting to None",
                    ticker, raw_fcf_yield,
                )

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

    def fetch_extended(self, ticker: str) -> dict[str, Any]:
        """Fetch all v1 + v2 extended fields for one ticker.

        Returns a dict with all keys from :meth:`fetch` plus the extended
        fields needed by the composite scorer: ``total_debt``,
        ``cash_and_equivalents``, ``ebit``, ``revenue_ttm``,
        ``book_value_of_equity``, ``dividends_paid_ttm``,
        ``share_buybacks_ttm``, ``cogs_ttm``, ``total_assets``,
        ``annual_eps_5y``, ``net_income_ttm``, ``operating_cash_flow_ttm``.
        Any key may be ``None`` if the XBRL data is unavailable.
        """
        # Start with v1 fields
        result: dict[str, Any] = {
            "pe_ratio": None,
            "pe_5y_avg": None,
            "fcf_yield": None,
            "market_cap": None,
            "latest_headline": None,
            "headline_url": None,
            # v2 extended fields
            "total_debt": None,
            "cash_and_equivalents": None,
            "ebit": None,
            "revenue_ttm": None,
            "book_value_of_equity": None,
            "dividends_paid_ttm": None,
            "share_buybacks_ttm": None,
            "cogs_ttm": None,
            "total_assets": None,
            "annual_eps_5y": None,
            "net_income_ttm": None,
            "operating_cash_flow_ttm": None,
        }

        # CIK lookup
        self.call_count += 1
        self._acquire_edgar_slot()
        cik = _cik_from_ticker(ticker)
        if cik is None:
            _log.warning("No SEC CIK for ticker=%s; skipping", ticker)
            return result

        # CompanyFacts
        self.call_count += 1
        self._acquire_edgar_slot()
        facts = _edgar_company_facts(cik)
        if facts is None:
            return result

        # --- v1 fields ---
        eps_entries = _facts_for_concept(facts, _CONCEPT_EPS_DILUTED, preferred_unit="USD/shares")
        ttm_eps = _ttm_eps(eps_entries)
        annual_eps = _annual_eps_by_fy(eps_entries)

        ocf_entries = _facts_for_concept(facts, _CONCEPT_OPERATING_CASH_FLOW, preferred_unit="USD")
        capex_entries = _facts_for_concept(facts, _CONCEPT_CAPEX, preferred_unit="USD")
        ttm_ocf = _ttm_sum(ocf_entries)
        ttm_capex = _ttm_sum(capex_entries)
        ttm_fcf: float | None
        if ttm_ocf is not None and ttm_capex is not None:
            ttm_fcf = ttm_ocf - abs(ttm_capex)
        else:
            ttm_fcf = None

        # Market cap from yfinance (handles dual-class correctly)
        self.call_count += 1
        current_price = _current_price(ticker)
        self.call_count += 1
        yf_mcap = _yf_market_cap(ticker)
        if yf_mcap is not None:
            result["market_cap"] = yf_mcap
        else:
            # Fallback: EDGAR shares * price (may be wrong for dual-class)
            shares_entries = _facts_for_concept(facts, _CONCEPT_SHARES_OUTSTANDING, preferred_unit="shares")
            shares_latest = _latest_quarterly(shares_entries, n=1, include_fy=True)
            shares_out = (
                _parse_float(shares_latest[0].get("val")) if shares_latest else None
            )
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
            raw_fcf_yield = ttm_fcf / result["market_cap"]
            if abs(raw_fcf_yield) <= 10.0:
                result["fcf_yield"] = raw_fcf_yield
            else:
                _log.warning(
                    "Ticker %s: fcf_yield=%.4f implausible (unit mismatch?), "
                    "setting to None",
                    ticker, raw_fcf_yield,
                )

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

        # --- v2 extended fields ---

        # Total debt: LongTermDebt + ShortTermBorrowings (fallback variants)
        long_term_entries = _facts_for_concept(facts, _CONCEPT_TOTAL_DEBT, preferred_unit="USD")
        short_term_entries = _facts_for_concept(facts, _CONCEPT_SHORT_TERM_DEBT, preferred_unit="USD")
        lt_latest = _latest_quarterly(long_term_entries, n=1, include_fy=True)
        st_latest = _latest_quarterly(short_term_entries, n=1, include_fy=True)
        lt_val = _parse_float(lt_latest[0].get("val")) if lt_latest else None
        st_val = _parse_float(st_latest[0].get("val")) if st_latest else None
        if lt_val is not None or st_val is not None:
            result["total_debt"] = (lt_val or 0.0) + (st_val or 0.0)

        # Cash and equivalents
        cash_entries = _facts_for_concept(facts, _CONCEPT_CASH, preferred_unit="USD")
        cash_latest = _latest_quarterly(cash_entries, n=1, include_fy=True)
        if cash_latest:
            result["cash_and_equivalents"] = _parse_float(
                cash_latest[0].get("val")
            )

        # EBIT (OperatingIncomeLoss, fallback to computed)
        ebit_entries = _facts_for_concept(facts, _CONCEPT_EBIT, preferred_unit="USD")
        ebit_latest = _latest_quarterly(ebit_entries, n=1, include_fy=True)
        ebit_val = _parse_float(ebit_latest[0].get("val")) if ebit_latest else None
        if ebit_val is not None:
            result["ebit"] = ebit_val
        else:
            # Fallback: NetIncome + InterestExpense + TaxExpense
            ni_entries = _facts_for_concept(facts, _CONCEPT_NET_INCOME, preferred_unit="USD")
            int_entries = _facts_for_concept(facts, _CONCEPT_INTEREST_EXPENSE, preferred_unit="USD")
            tax_entries = _facts_for_concept(facts, _CONCEPT_TAX_EXPENSE, preferred_unit="USD")
            ni_latest = _latest_quarterly(ni_entries, n=1, include_fy=True)
            int_latest = _latest_quarterly(int_entries, n=1, include_fy=True)
            tax_latest = _latest_quarterly(tax_entries, n=1, include_fy=True)
            ni_val = _parse_float(ni_latest[0].get("val")) if ni_latest else None
            int_val = _parse_float(int_latest[0].get("val")) if int_latest else None
            tax_val = _parse_float(tax_latest[0].get("val")) if tax_latest else None
            if ni_val is not None and int_val is not None and tax_val is not None:
                result["ebit"] = ni_val + int_val + tax_val

        # Revenue TTM
        rev_entries = _facts_for_concept(facts, _CONCEPT_REVENUE, preferred_unit="USD")
        result["revenue_ttm"] = _ttm_sum(rev_entries)

        # Book value of equity
        bv_entries = _facts_for_concept(facts, _CONCEPT_BOOK_VALUE, preferred_unit="USD")
        bv_latest = _latest_quarterly(bv_entries, n=1, include_fy=True)
        if bv_latest:
            result["book_value_of_equity"] = _parse_float(
                bv_latest[0].get("val")
            )

        # Dividends paid TTM
        div_entries = _facts_for_concept(facts, _CONCEPT_DIVIDENDS, preferred_unit="USD")
        result["dividends_paid_ttm"] = _ttm_sum(div_entries)

        # Share buybacks TTM
        bb_entries = _facts_for_concept(facts, _CONCEPT_BUYBACKS, preferred_unit="USD")
        result["share_buybacks_ttm"] = _ttm_sum(bb_entries)

        # COGS TTM
        cogs_entries = _facts_for_concept(facts, _CONCEPT_COGS, preferred_unit="USD")
        result["cogs_ttm"] = _ttm_sum(cogs_entries)

        # Total assets (latest quarter)
        assets_entries = _facts_for_concept(facts, _CONCEPT_TOTAL_ASSETS, preferred_unit="USD")
        assets_latest = _latest_quarterly(assets_entries, n=1, include_fy=True)
        if assets_latest:
            result["total_assets"] = _parse_float(
                assets_latest[0].get("val")
            )

        # Annual EPS 5y (list of up to 5 annual EPS values, newest first)
        if annual_eps:
            years_sorted = sorted(annual_eps.keys(), reverse=True)[:5]
            result["annual_eps_5y"] = [annual_eps[y] for y in years_sorted]

        # Net income TTM
        ni_entries_ttm = _facts_for_concept(facts, _CONCEPT_NET_INCOME, preferred_unit="USD")
        result["net_income_ttm"] = _ttm_sum(ni_entries_ttm)

        # Operating cash flow TTM (already computed above for FCF)
        result["operating_cash_flow_ttm"] = ttm_ocf

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


# ---------------------------------------------------------------------------
# Extended fundamentals — cache-gated fetch for composite-rank pipeline
# ---------------------------------------------------------------------------


def get_extended_fundamentals(
    ticker: str,
    repo: Repository,
    fmp_client: FundamentalsClient,
    staleness_days: int,
    pipeline_mode: str = "composite_rank",
) -> ExtendedFundamentals:
    """Cache-gated extended fundamentals fetch for one ticker.

    When ``pipeline_mode = composite_rank`` and the cached row has
    ``fundamentals_schema_version < 2``, the row is treated as stale
    and refetched (Requirement 8.6).

    Returns an :class:`ExtendedFundamentals` record with all fields
    populated (any may be None if EDGAR data is unavailable).
    """
    now = datetime.now(timezone.utc)
    freshness = timedelta(days=staleness_days)

    # Check cache
    cached_row = repo.load_extended_fundamentals(ticker)
    if cached_row is not None:
        fetched_at = cached_row.get("fetched_at")
        if fetched_at is not None:
            if hasattr(fetched_at, "tzinfo") and fetched_at.tzinfo is None:
                fetched_at = fetched_at.replace(tzinfo=timezone.utc)
            schema_version = cached_row.get("fundamentals_schema_version", 1)
            is_fresh = (now - fetched_at) < freshness
            # In composite_rank mode, v1 rows (schema_version < 2) are stale
            schema_ok = (
                pipeline_mode != "composite_rank" or schema_version >= 2
            )
            if is_fresh and schema_ok:
                return _row_to_extended_fundamentals(ticker, cached_row)

    if fmp_client.budget_exhausted:
        if cached_row is not None:
            _log.info(
                "Fundamentals budget exhausted; serving stale cache for %s",
                ticker,
            )
            return _row_to_extended_fundamentals(ticker, cached_row)
        raise FundamentalsBudgetExhausted(
            f"No cached fundamentals for {ticker!r} and budget exhausted"
        )

    try:
        fields = fmp_client.fetch_extended(ticker)
    except FundamentalsBudgetExhausted:
        if cached_row is not None:
            _log.info(
                "Budget exhausted mid-fetch for %s; serving stale cache",
                ticker,
            )
            return _row_to_extended_fundamentals(ticker, cached_row)
        raise

    # Upsert with schema_version = 2
    upsert_fields: dict[str, Any] = {
        "pe_ratio": fields.get("pe_ratio"),
        "pe_5y_avg": fields.get("pe_5y_avg"),
        "fcf_yield": fields.get("fcf_yield"),
        "latest_headline": fields.get("latest_headline"),
        "headline_url": fields.get("headline_url"),
        "fetched_at": now,
        "total_debt": fields.get("total_debt"),
        "cash_and_equivalents": fields.get("cash_and_equivalents"),
        "ebit": fields.get("ebit"),
        "revenue_ttm": fields.get("revenue_ttm"),
        "book_value_of_equity": fields.get("book_value_of_equity"),
        "dividends_paid_ttm": fields.get("dividends_paid_ttm"),
        "share_buybacks_ttm": fields.get("share_buybacks_ttm"),
        "cogs_ttm": fields.get("cogs_ttm"),
        "total_assets": fields.get("total_assets"),
        "annual_eps_5y": fields.get("annual_eps_5y"),
        "net_income_ttm": fields.get("net_income_ttm"),
        "operating_cash_flow_ttm": fields.get("operating_cash_flow_ttm"),
        "fundamentals_schema_version": 2,
    }
    repo.upsert_extended_fundamentals(ticker, upsert_fields)

    return ExtendedFundamentals(
        ticker=ticker,
        pe_ratio=fields.get("pe_ratio"),
        pe_5y_avg=fields.get("pe_5y_avg"),
        fcf_yield=fields.get("fcf_yield"),
        latest_headline=fields.get("latest_headline"),
        headline_url=fields.get("headline_url"),
        fetched_at=now,
        total_debt=fields.get("total_debt"),
        cash_and_equivalents=fields.get("cash_and_equivalents"),
        ebit=fields.get("ebit"),
        revenue_ttm=fields.get("revenue_ttm"),
        book_value_of_equity=fields.get("book_value_of_equity"),
        dividends_paid_ttm=fields.get("dividends_paid_ttm"),
        share_buybacks_ttm=fields.get("share_buybacks_ttm"),
        cogs_ttm=fields.get("cogs_ttm"),
        total_assets=fields.get("total_assets"),
        annual_eps_5y=fields.get("annual_eps_5y"),
        net_income_ttm=fields.get("net_income_ttm"),
        operating_cash_flow_ttm=fields.get("operating_cash_flow_ttm"),
        market_cap=fields.get("market_cap"),
        schema_version=2,
    )


def _row_to_extended_fundamentals(
    ticker: str, row: dict[str, Any]
) -> ExtendedFundamentals:
    """Convert a cached DB row dict to an ExtendedFundamentals dataclass."""
    return ExtendedFundamentals(
        ticker=ticker,
        pe_ratio=row.get("pe_ratio"),
        pe_5y_avg=row.get("pe_5y_avg"),
        fcf_yield=row.get("fcf_yield"),
        latest_headline=row.get("latest_headline"),
        headline_url=row.get("headline_url"),
        fetched_at=row.get("fetched_at", datetime.now(timezone.utc)),
        total_debt=row.get("total_debt"),
        cash_and_equivalents=row.get("cash_and_equivalents"),
        ebit=row.get("ebit"),
        revenue_ttm=row.get("revenue_ttm"),
        book_value_of_equity=row.get("book_value_of_equity"),
        dividends_paid_ttm=row.get("dividends_paid_ttm"),
        share_buybacks_ttm=row.get("share_buybacks_ttm"),
        cogs_ttm=row.get("cogs_ttm"),
        total_assets=row.get("total_assets"),
        annual_eps_5y=row.get("annual_eps_5y"),
        net_income_ttm=row.get("net_income_ttm"),
        operating_cash_flow_ttm=row.get("operating_cash_flow_ttm"),
        market_cap=row.get("market_cap"),
        schema_version=row.get("fundamentals_schema_version", 1),
    )
