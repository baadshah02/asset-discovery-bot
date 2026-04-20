"""Watchdog service: S&P 500 universe scraper, differ, and upserter.

This module fulfils the "Watchdog" role in the design
(:doc:`.kiro/specs/asset-discovery-bot/design.md`, Component 1). It is the
first data source touched by a Scan_Run (Requirement 1.1) and the only
module that reads from Wikipedia.

Responsibilities:

* Scrape the current S&P 500 constituents table from the configured
  Wikipedia URL, returning ``(ticker, company_name, sector)`` triples.
* Diff that scrape against the locally stored active universe
  (``asset_universe`` rows with ``removed_on IS NULL``).
* Upsert the scrape through :meth:`bot.repo.Repository.upsert_universe`, so
  newly added tickers appear and dropped tickers get a ``removed_on`` stamp.
* Return a :class:`UniverseDiff` for the orchestrator to hand to
  :mod:`bot.notify` when membership changed.

Fail-fast contract (Requirements 1.6, 8.1): if the scrape raises a network
error or returns a constituent count outside the configured sanity bounds,
this module raises :class:`UniverseSyncError` *before* calling
:meth:`Repository.upsert_universe`. ``asset_universe`` is not mutated on
abort; the orchestrator (:mod:`bot.run`) converts the exception into a
non-zero exit code.

Note on ticker normalisation: Wikipedia renders compound tickers with dots
(e.g., ``BRK.B``) whereas Yahoo Finance requires dashes (``BRK-B``). This
module represents what Wikipedia says verbatim — normalisation is the
responsibility of :mod:`bot.prices` at the Yahoo Finance boundary.

Requirements traceability:
    1.1 — scrape happens before any price / fundamentals fetch.
    1.2 — diff produces ``added`` and ``removed`` sets.
    1.4 — :meth:`Repository.upsert_universe` reflects the scrape.
    1.5 — ``added`` and ``removed`` are disjoint by construction.
    1.6 — implausible count aborts the run without mutating the DB.
    8.1 — transient HTTP errors are retried; any remaining failure aborts
          the run before downstream I/O.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import requests
from bs4 import BeautifulSoup
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from bot.config import UniverseConfig
from bot.repo import Repository

__all__ = [
    "UniverseDiff",
    "UniverseSyncError",
    "fetch_current_constituents",
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


@dataclass(frozen=True)
class UniverseDiff:
    """Result of a universe sync.

    ``added`` and ``removed`` are disjoint sorted lists of tickers (Req 1.5).
    ``as_of`` is the date the sync was performed; it matches the
    ``removed_on`` stamp applied by :meth:`Repository.upsert_universe` and
    the ``Date`` embedded in any Watchdog_Alert.
    """

    added: list[str]
    removed: list[str]
    as_of: date


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


def sync_universe(repo: Repository, cfg: UniverseConfig) -> UniverseDiff:
    """Scrape, sanity-check, diff, and upsert the S&P 500 universe.

    This is the single entry point the orchestrator calls to keep
    ``asset_universe`` in sync with Wikipedia. The ordering of operations
    is load-bearing for Requirement 1.6 (no mutation on abort):

        1. Scrape Wikipedia and coerce the result into a 3-tuple list.
        2. Enforce ``min_constituent_count ≤ N ≤ max_constituent_count``.
           An out-of-range count raises :class:`UniverseSyncError`
           **before** any DB write.
        3. Load the previously active set from the repository.
        4. Compute ``added`` / ``removed`` (disjoint by set arithmetic,
           satisfying Req 1.5).
        5. Call :meth:`Repository.upsert_universe` with the scrape and
           today's date; this is the only DB write.
        6. Return the :class:`UniverseDiff`.

    The HTTP timeout is fixed at 10s because :class:`UniverseConfig` does
    not carry a timeout field (and all other adapters have their own
    configured timeouts). If future experimentation requires a longer
    timeout, add ``timeout_seconds`` to :class:`UniverseConfig` and plumb
    it through.

    Args:
        repo: Data access gateway. Exactly two calls are made:
            :meth:`Repository.load_universe` and
            :meth:`Repository.upsert_universe`.
        cfg: ``AppConfig.universe`` — supplies ``source_url`` and the
            ``[min_constituent_count, max_constituent_count]`` sanity bounds.

    Raises:
        UniverseSyncError: If the scrape fails after retries, the page
            cannot be parsed, or the constituent count is out of bounds.
            In every failure case ``repo.upsert_universe`` is NOT called.
    """
    # Step 1 — scrape. Wrap any RequestException/ValueError as a
    # UniverseSyncError so callers (and the orchestrator) only ever have to
    # handle one exception type from this module.
    try:
        entries = fetch_current_constituents(cfg.source_url, timeout=10.0)
    except (requests.RequestException, ValueError) as exc:
        raise UniverseSyncError(
            f"Failed to scrape S&P 500 constituents from {cfg.source_url!r}: {exc}"
        ) from exc

    # Step 2 — sanity-check count BEFORE any DB interaction (Req 1.6).
    count = len(entries)
    if count < cfg.min_constituent_count or count > cfg.max_constituent_count:
        raise UniverseSyncError(
            f"Scrape returned {count} tickers, outside sanity bounds "
            f"[{cfg.min_constituent_count}, {cfg.max_constituent_count}]; "
            f"aborting run without mutating asset_universe"
        )

    # Step 3 — previously active set.
    previous: set[str] = repo.load_universe()

    # Step 4 — compute diff. Using set arithmetic gives us disjointness
    # for free (Req 1.5): ``added`` contains only tickers absent from
    # ``previous``; ``removed`` contains only tickers absent from the
    # current scrape.
    current: set[str] = {ticker for ticker, _, _ in entries}
    added = sorted(current - previous)
    removed = sorted(previous - current)

    # Step 5 — upsert (the only mutation).
    as_of = date.today()
    repo.upsert_universe(entries, as_of=as_of)

    # Step 6 — return the diff for the orchestrator to forward to notify.
    return UniverseDiff(added=added, removed=removed, as_of=as_of)
