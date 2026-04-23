"""Multi-source universe orchestrator: scraper, ETF CSV parser, differ, and upserter.

This module fulfils the "Watchdog" role in the design
(:doc:`.kiro/specs/asset-discovery-bot/design.md`, Component 1). It is the
first data source touched by a Scan_Run (Requirement 1.1) and fetches
constituent lists from one or more configured Universe_Sources.

Responsibilities:

* Scrape the current S&P 500 constituents table from Wikipedia, returning
  ``(ticker, company_name, sector)`` triples.
* Parse iShares ETF holdings CSV files (IWB, IWM) into the same triple
  format, handling preamble rows, non-equity exclusion, and ticker
  normalisation.
* Fetch all enabled sources independently, computing the Composite_Universe
  as the set-union of successful sources.
* Diff the composite against the locally stored active universe
  (``asset_universe`` rows with ``removed_on IS NULL``).
* Upsert the composite through :meth:`bot.repo.Repository.upsert_universe`
  with per-ticker source attribution.
* Return an extended :class:`UniverseDiff` for the orchestrator to hand to
  :mod:`bot.notify` when membership changed or sources failed.

Fail-fast contract (Requirements 1.6, 3.5, 3.6): if all sources fail or
the composite size is outside bounds, this module raises
:class:`UniverseSyncError` *before* calling
:meth:`Repository.upsert_universe`. ``asset_universe`` is not mutated on
abort; the orchestrator (:mod:`bot.run`) converts the exception into a
non-zero exit code.

Note on ticker normalisation: Wikipedia renders compound tickers with dots
(e.g., ``BRK.B``) whereas Yahoo Finance requires dashes (``BRK-B``). ETF
CSV tickers use spaces for share classes (``BRK B``) which are normalised
to dots. :mod:`bot.prices` converts to Yahoo's dash form at its own
boundary.

Requirements traceability:
    1.1 — scrape happens before any price / fundamentals fetch.
    1.2 — diff produces ``added`` and ``removed`` sets.
    1.4 — :meth:`Repository.upsert_universe` reflects the scrape.
    1.5 — ``added`` and ``removed`` are disjoint by construction.
    1.6 — implausible count aborts the run without mutating the DB.
    2.1–2.7 — ETF holdings CSV parsing.
    3.1–3.8 — multi-source orchestration.
    8.1 — transient HTTP errors are retried; any remaining failure aborts
          the run before downstream I/O.
"""

from __future__ import annotations

import csv
import io
import logging
from dataclasses import dataclass, field
from datetime import date

import requests
from bs4 import BeautifulSoup
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from bot.config import UniverseConfig, UniverseSourceConfig
from bot.repo import Repository

_log = logging.getLogger(__name__)

__all__ = [
    "ParseError",
    "SourceResult",
    "UniverseDiff",
    "UniverseSyncError",
    "fetch_current_constituents",
    "fetch_etf_holdings",
    "fetch_source",
    "parse_etf_holdings_csv",
    "sync_universe",
]


# ---------------------------------------------------------------------------
# Domain types
# ---------------------------------------------------------------------------


class UniverseSyncError(Exception):
    """Fatal universe-sync failure.

    Raised when the scraped constituent count falls outside
    ``[min_constituent_count, max_constituent_count]`` or when the scrape
    itself cannot be recovered after retries. The orchestrator
    (:mod:`bot.run`) catches this and exits non-zero; by design the caller
    is guaranteed that ``asset_universe`` was not mutated (Req 1.6).
    """


class ParseError(Exception):
    """Raised when an ETF holdings CSV cannot be parsed.

    This covers missing header rows, unrecognised column layouts, and
    CSVs that parse to zero equity tickers. The Universe_Service catches
    this per-source and records it as a source failure (Req 2.6, 3.4).
    """


@dataclass(frozen=True)
class SourceResult:
    """Result of fetching one Universe_Source.

    Returned by :func:`fetch_source`; never raises. A failed fetch sets
    ``success=False`` and records the error message in ``error``.
    """

    name: str
    success: bool
    tickers: list[tuple[str, str, str | None]]
    error: str | None = None


@dataclass(frozen=True)
class UniverseDiff:
    """Extended result of a universe sync.

    ``added`` and ``removed`` are disjoint sorted lists of tickers (Req 1.5).
    ``as_of`` is the date the sync was performed; it matches the
    ``removed_on`` stamp applied by :meth:`Repository.upsert_universe` and
    the ``Date`` embedded in any Watchdog_Alert.

    The additional fields carry per-source attribution and failure info
    for the multi-source orchestrator (Req 3.1–3.8, 9.1–9.3).
    """

    added: list[str]
    removed: list[str]
    as_of: date
    source_failures: list[tuple[str, str]] = field(default_factory=list)
    source_attribution: dict[str, list[str]] = field(default_factory=dict)
    composite_size: int = 0
    sources_enabled: int = 0
    sources_succeeded: int = 0


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


# Conservative User-Agent — Wikipedia blocks requests with the default
# ``python-requests/...`` UA. Keep the project name + a version-ish token so
# ops can identify the traffic in Wikipedia's access logs.
_USER_AGENT = "asset-discovery-bot/0.1 (+https://github.com/)"

# Constituents table on Wikipedia has had the id ``constituents`` for years;
# the fallback to the first ``wikitable`` guards against a markup change.
_TABLE_ID = "constituents"
_TABLE_FALLBACK_CLASS = "wikitable"


def _is_retryable_http_error(exc: BaseException) -> bool:
    """Predicate for tenacity: retry transient network errors but not 4xx.

    Retryable:
        * :class:`requests.ConnectionError` — socket-level failure.
        * :class:`requests.Timeout` — read/connect timeout.
        * :class:`requests.HTTPError` **only when** the underlying response
          has a 5xx status. Wikipedia occasionally returns 502/503 behind
          the CDN; a 404 on the constituents page is an author-visible bug
          and should surface immediately.

    Anything else is non-retryable.
    """
    if isinstance(exc, (requests.ConnectionError, requests.Timeout)):
        return True
    if isinstance(exc, requests.HTTPError):
        response = getattr(exc, "response", None)
        status = getattr(response, "status_code", None)
        return status is not None and status >= 500
    return False


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception(_is_retryable_http_error),
    reraise=True,
)
def _http_get(url: str, timeout: float) -> requests.Response:
    """Perform a single HTTP GET with retry on transient errors.

    Wrapped in :func:`tenacity.retry` at module scope so that only the
    network call retries — the BeautifulSoup parse downstream is
    deterministic and must never be re-executed against a partial frame.

    Requirement 8.1 (fail fast on scrape error) is honoured because after
    ``stop_after_attempt(3)`` the underlying ``requests`` exception is
    re-raised; :func:`sync_universe` wraps it as :class:`UniverseSyncError`.
    """
    response = requests.get(
        url,
        timeout=timeout,
        headers={"User-Agent": _USER_AGENT},
    )
    response.raise_for_status()
    return response


def _cell_text(cell: object) -> str:
    """Extract a cell's text, preferring the first anchor (ticker/company link)."""
    # BeautifulSoup ``Tag`` exposes ``.find`` and ``.get_text``. The bs4 types
    # are imported only at runtime, so use structural typing via ``getattr``
    # to keep this helper cheap to unit-test without a full BeautifulSoup
    # fixture.
    anchor = cell.find("a") if hasattr(cell, "find") else None  # type: ignore[union-attr]
    if anchor is not None and anchor.get_text(strip=True):  # type: ignore[union-attr]
        return anchor.get_text(strip=True)  # type: ignore[union-attr]
    if hasattr(cell, "get_text"):
        return cell.get_text(strip=True)  # type: ignore[union-attr]
    return str(cell).strip()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def fetch_current_constituents(
    source_url: str,
    timeout: float = 10.0,
) -> list[tuple[str, str, str | None]]:
    """Scrape the S&P 500 constituents table from Wikipedia.

    Returns a sorted, deduplicated list of
    ``(ticker, company_name, sector)`` triples. The 3-tuple shape is what
    :meth:`bot.repo.Repository.upsert_universe` expects; the design's
    narrower ``list[str]`` signature is widened here so that the whole
    sync can proceed without re-scraping for ``company_name`` / ``sector``.

    Tickers are stripped and upper-cased but dots are preserved
    (``BRK.B`` stays ``BRK.B``). :mod:`bot.prices` converts to Yahoo's
    dash form at its own boundary; :mod:`bot.universe` is a faithful
    mirror of Wikipedia.

    Args:
        source_url: Fully-qualified URL of the Wikipedia page. Should
            come from ``cfg.universe.source_url``.
        timeout: Per-attempt HTTP timeout in seconds. Defaults to 10s,
            which matches :class:`bot.config.FmpConfig.timeout_seconds`.

    Raises:
        requests.exceptions.RequestException: After all retries are
            exhausted (typically ``ConnectionError``, ``Timeout``, or a
            5xx ``HTTPError``). Callers should translate to
            :class:`UniverseSyncError` if they need fail-fast semantics.
        ValueError: If the expected table cannot be found or no
            constituent rows are parseable.
    """
    response = _http_get(source_url, timeout=timeout)
    soup = BeautifulSoup(response.text, "lxml")

    table = soup.find("table", id=_TABLE_ID)
    if table is None:
        # Fallback for a markup change — grab the first ``wikitable``.
        table = soup.find("table", class_=_TABLE_FALLBACK_CLASS)
    if table is None:
        raise ValueError(
            "Could not locate the S&P 500 constituents table "
            f"(tried id={_TABLE_ID!r} and class={_TABLE_FALLBACK_CLASS!r})"
        )

    # Map header text -> column index so we survive column reordering.
    header_row = table.find("tr")
    if header_row is None:
        raise ValueError("Constituents table has no rows")
    headers = [
        _cell_text(th).strip().lower()
        for th in header_row.find_all(["th", "td"])
    ]

    def _column_index(*candidates: str) -> int | None:
        for candidate in candidates:
            lowered = candidate.lower()
            for idx, header in enumerate(headers):
                if lowered in header:
                    return idx
        return None

    symbol_idx = _column_index("symbol", "ticker")
    name_idx = _column_index("security", "company")
    sector_idx = _column_index("gics sector", "sector")

    if symbol_idx is None or name_idx is None:
        raise ValueError(
            f"Constituents table is missing required columns; "
            f"found headers={headers!r}"
        )

    # Dedup on ticker; last occurrence wins for company_name / sector in the
    # pathological case of a duplicate row (shouldn't happen, but the
    # original index is stable so the tie-break is deterministic).
    records: dict[str, tuple[str, str, str | None]] = {}
    for tr in table.find_all("tr")[1:]:
        cells = tr.find_all(["td", "th"])
        if len(cells) <= max(symbol_idx, name_idx):
            continue
        ticker = _cell_text(cells[symbol_idx]).strip().upper()
        if not ticker:
            continue
        company_name = _cell_text(cells[name_idx]).strip()
        sector: str | None = None
        if sector_idx is not None and sector_idx < len(cells):
            sector_text = _cell_text(cells[sector_idx]).strip()
            sector = sector_text or None
        records[ticker] = (ticker, company_name, sector)

    if not records:
        raise ValueError("Constituents table parsed to zero tickers")

    return sorted(records.values(), key=lambda triple: triple[0])


# ---------------------------------------------------------------------------
# ETF Holdings CSV parser (Req 2.1–2.7)
# ---------------------------------------------------------------------------


def parse_etf_holdings_csv(
    csv_text: str,
) -> list[tuple[str, str, str | None]]:
    """Parse an iShares ETF holdings CSV into (ticker, company_name, sector) triples.

    Handles the iShares preamble (non-tabular metadata rows before the
    actual column headers). Locates the header row by searching for a row
    containing both a Ticker/Symbol column and a Name column.

    Exclusion rules (Req 2.4):
        - Empty ticker or dash-only placeholder (``-``, ``--``)
        - Ticker starting with ``CASH``
        - Ticker containing ``_USD``
        - Rows with Asset Class not equal to ``Equity`` (when column present)

    Normalisation (Req 2.3):
        - Strip whitespace, upper-case
        - Space-to-dot for share classes (``BRK B`` -> ``BRK.B``)

    Returns a sorted, deduplicated list of triples.

    Raises:
        ParseError: If the header row cannot be located or the CSV
            parses to zero equity tickers.
    """
    reader = csv.reader(io.StringIO(csv_text))
    rows = list(reader)

    # Phase 1: Locate the header row
    header_row_idx: int | None = None
    for i, row in enumerate(rows):
        lower_row = [cell.strip().lower() for cell in row]
        has_ticker = "ticker" in lower_row or "symbol" in lower_row
        has_name = "name" in lower_row
        if has_ticker and has_name:
            header_row_idx = i
            break

    if header_row_idx is None:
        raise ParseError(
            "Cannot locate header row with Ticker/Symbol and Name columns"
        )

    # Phase 2: Map column names to indices
    headers = [cell.strip().lower() for cell in rows[header_row_idx]]

    def _find_col(*candidates: str) -> int | None:
        for candidate in candidates:
            try:
                return headers.index(candidate)
            except ValueError:
                continue
        return None

    ticker_idx = _find_col("ticker", "symbol")
    name_idx = _find_col("name")
    sector_idx = _find_col("sector")
    asset_class_idx = _find_col("asset class")

    if ticker_idx is None or name_idx is None:
        raise ParseError(
            f"Header row missing required columns; found headers={headers!r}"
        )

    min_required = max(ticker_idx, name_idx)

    # Phase 3: Extract triples from data rows
    records: dict[str, tuple[str, str, str | None]] = {}
    for i in range(header_row_idx + 1, len(rows)):
        row = rows[i]
        if len(row) <= min_required:
            continue

        raw_ticker = row[ticker_idx].strip()

        # Exclusion rules (Req 2.4)
        if not raw_ticker or raw_ticker in ("-", "--"):
            continue
        if raw_ticker.upper().startswith("CASH"):
            continue
        if "_USD" in raw_ticker.upper():
            continue
        if (
            asset_class_idx is not None
            and asset_class_idx < len(row)
            and row[asset_class_idx].strip()
            and row[asset_class_idx].strip() != "Equity"
        ):
            continue

        # Normalise ticker: upper-case, space-to-dot (Req 2.3)
        ticker = raw_ticker.strip().upper().replace(" ", ".")

        company_name = row[name_idx].strip()
        sector: str | None = None
        if sector_idx is not None and sector_idx < len(row):
            sector_text = row[sector_idx].strip()
            sector = sector_text or None

        records[ticker] = (ticker, company_name, sector)

    if not records:
        raise ParseError("CSV parsed to zero equity tickers")

    return sorted(records.values(), key=lambda triple: triple[0])


def fetch_etf_holdings(
    source: UniverseSourceConfig,
    timeout: float = 15.0,
) -> list[tuple[str, str, str | None]]:
    """Download and parse an ETF holdings CSV from the configured URL.

    Uses a browser-like User-Agent header and retries transient HTTP
    errors up to 3 times with exponential backoff (Req 2.1).

    Args:
        source: The Universe_Source configuration for this ETF.
        timeout: Per-attempt HTTP timeout in seconds. Defaults to 15s.

    Returns:
        Sorted, deduplicated list of ``(ticker, company_name, sector)``
        triples, identical in shape to :func:`fetch_current_constituents`.

    Raises:
        requests.exceptions.RequestException: After all retries exhausted.
        ParseError: If the CSV cannot be parsed.
    """
    response = _http_get(source.url, timeout=timeout)
    return parse_etf_holdings_csv(response.text)


# ---------------------------------------------------------------------------
# Source dispatcher (Req 3.1, 3.4)
# ---------------------------------------------------------------------------


def fetch_source(source: UniverseSourceConfig) -> SourceResult:
    """Fetch one source, dispatching by kind. Returns SourceResult (never raises).

    Dispatches to the appropriate fetcher based on ``source.kind``:
        - ``wikipedia_table`` -> :func:`fetch_current_constituents`
        - ``etf_holdings_csv`` -> :func:`fetch_etf_holdings`

    All exceptions are caught and recorded as a failed :class:`SourceResult`.
    """
    try:
        if source.kind == "wikipedia_table":
            tickers = fetch_current_constituents(source.url, timeout=10.0)
        elif source.kind == "etf_holdings_csv":
            tickers = fetch_etf_holdings(source, timeout=15.0)
        else:
            return SourceResult(
                name=source.name,
                success=False,
                tickers=[],
                error=f"Unrecognised source kind: {source.kind!r}",
            )
        return SourceResult(
            name=source.name,
            success=True,
            tickers=tickers,
        )
    except Exception as exc:
        _log.warning("Source %r failed: %s", source.name, exc)
        return SourceResult(
            name=source.name,
            success=False,
            tickers=[],
            error=str(exc),
        )


def sync_universe(repo: Repository, cfg: UniverseConfig) -> UniverseDiff:
    """Multi-source universe sync.

    This is the single entry point the orchestrator calls to keep
    ``asset_universe`` in sync with all configured Universe_Sources.

    Algorithm:

        1. Resolve enabled sources via ``cfg.effective_sources()``.
        2. Fetch each source independently via :func:`fetch_source`.
        3. Validate per-source count against ``[min_count, max_count]``;
           mark as failed if outside bounds.
        4. If all sources fail, raise :class:`UniverseSyncError` (no DB
           mutation — Req 3.5).
        5. Compute Composite_Universe as set-union of successful sources.
        6. Validate composite size against ``[min_composite_count,
           max_composite_count]``; raise before DB mutation (Req 3.6).
        7. Compute per-ticker source attribution.
        8. Diff against previous active universe.
        9. Upsert with ``source_attribution``.
       10. Return extended :class:`UniverseDiff`.

    Args:
        repo: Data access gateway.
        cfg: ``AppConfig.universe`` — supplies source list and bounds.

    Raises:
        UniverseSyncError: If all sources fail, or the composite size
            is outside bounds. In every failure case
            ``repo.upsert_universe`` is NOT called.
    """
    effective_sources = cfg.effective_sources()
    if not effective_sources:
        raise UniverseSyncError("No enabled universe sources configured")

    # Phase 1: Fetch all sources independently
    results: list[SourceResult] = []
    for source in effective_sources:
        result = fetch_source(source)

        # Per-source count validation
        if result.success:
            count = len(result.tickers)
            if count < source.min_count or count > source.max_count:
                _log.warning(
                    "Source %r returned %d tickers, outside bounds [%d, %d]",
                    source.name,
                    count,
                    source.min_count,
                    source.max_count,
                )
                result = SourceResult(
                    name=source.name,
                    success=False,
                    tickers=[],
                    error=(
                        f"count {count} outside bounds "
                        f"[{source.min_count}, {source.max_count}]"
                    ),
                )
        results.append(result)

    # Phase 2: Check for total failure
    successful = [r for r in results if r.success]
    failed = [r for r in results if not r.success]

    if not successful:
        raise UniverseSyncError("All enabled sources failed")

    # Phase 3: Compute Composite_Universe via set-union
    all_entries: dict[str, tuple[str, str, str | None]] = {}
    source_attribution: dict[str, list[str]] = {}

    for result in successful:
        for ticker, company_name, sector in result.tickers:
            if ticker not in all_entries:
                all_entries[ticker] = (ticker, company_name, sector)
                source_attribution[ticker] = []
            source_attribution[ticker].append(result.name)

    composite = sorted(all_entries.values(), key=lambda t: t[0])

    # Phase 4: Composite bounds check
    composite_size = len(composite)
    if (
        composite_size < cfg.min_composite_count
        or composite_size > cfg.max_composite_count
    ):
        raise UniverseSyncError(
            f"Composite universe size {composite_size} outside "
            f"[{cfg.min_composite_count}, {cfg.max_composite_count}]"
        )

    # Phase 5: Diff against previous
    previous: set[str] = repo.load_universe()
    current_tickers: set[str] = {t for t, _, _ in composite}
    added = sorted(current_tickers - previous)
    removed = sorted(previous - current_tickers)

    # Phase 6: Upsert with source attribution
    as_of = date.today()
    repo.upsert_universe(composite, as_of=as_of, source_attribution=source_attribution)

    # Phase 7: Build extended diff
    source_failures = [(r.name, r.error or "unknown error") for r in failed]

    return UniverseDiff(
        added=added,
        removed=removed,
        as_of=as_of,
        source_failures=source_failures,
        source_attribution=source_attribution,
        composite_size=composite_size,
        sources_enabled=len(effective_sources),
        sources_succeeded=len(successful),
    )
