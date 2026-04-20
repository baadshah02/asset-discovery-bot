"""SQLAlchemy Core 2.0 data access layer for the Asset Discovery Bot.

This module is the only code path in the application that opens database
connections. Every public method on :class:`Repository` acquires its own
connection via a context manager (``self.transaction()``), guaranteeing that
no connection ever leaks into the wider runtime (Requirement 7.7).

Three tables are modelled, matching the DDL in ``bot/migrations/001_init.sql``
exactly:

* ``asset_universe``       — S&P 500 membership (watchdog source of truth).
* ``fundamentals_cache``   — cache of FMP fundamentals keyed by ticker.
* ``daily_scans``          — one row per emitted High_Conviction_Alert,
  carrying the full ``AppConfig.model_dump()`` that produced it.

Upsert semantics use Postgres-specific ``INSERT ... ON CONFLICT`` via
:func:`sqlalchemy.dialects.postgresql.insert` (Requirement 7.2). The
``daily_scans`` table enforces ``UNIQUE (ticker, scan_date)``; a collision
raises :class:`DuplicateScanError` so the orchestrator can treat it as a
no-op without emitting a duplicate Discord alert (Requirements 7.3, 7.4).

Requirements traceability:
    7.1 — typed read/upsert methods on every table, using SQLAlchemy Core.
    7.2 — ``INSERT ... ON CONFLICT`` upserts for ``asset_universe`` and
          ``fundamentals_cache``.
    7.3 — ``UNIQUE (ticker, scan_date)`` reflected in the :class:`Table`.
    7.4 — duplicate scan → :class:`DuplicateScanError`, not a second row.
    7.5 — transactional context manager commits atomically.
    7.6 — ``load_universe`` returns only ``removed_on IS NULL``.
    7.7 — no connection opened outside a context manager.
    6.7 — ``config_snapshot`` persisted to JSONB on every scan row.
    3.1 — cache lookup before any FMP call.
    3.3 — cache upsert on fresh fetch.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Iterator, Mapping

from sqlalchemy import (
    BigInteger,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Index,
    MetaData,
    Numeric,
    String,
    Table,
    UniqueConstraint,
    and_,
    func,
    select,
    update,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import IntegrityError

__all__ = [
    "DuplicateScanError",
    "Fundamentals",
    "Repository",
    "metadata",
    "asset_universe",
    "fundamentals_cache",
    "daily_scans",
]


# ---------------------------------------------------------------------------
# Domain types
# ---------------------------------------------------------------------------


class DuplicateScanError(Exception):
    """Raised when an ``insert_scan`` call collides on ``(ticker, scan_date)``.

    The orchestrator catches this to skip the duplicate Discord alert while
    leaving the existing ``daily_scans`` row untouched (Requirement 7.4).
    """


@dataclass(frozen=True)
class Fundamentals:
    """Typed record mirroring one row of ``fundamentals_cache``.

    Defined here rather than in :mod:`bot.fundamentals` to keep the repository
    free of upward imports. :mod:`bot.fundamentals` re-exports this class so
    callers can continue to ``from bot.fundamentals import Fundamentals``.

    All numeric fields and the headline strings are ``Optional`` because FMP
    endpoints frequently return partial data for smaller constituents; the
    filter pipeline (:mod:`bot.filters`) is responsible for rejecting rows
    with missing required fields.
    """

    ticker: str
    pe_ratio: float | None
    pe_5y_avg: float | None
    fcf_yield: float | None
    latest_headline: str | None
    headline_url: str | None
    fetched_at: datetime


# ---------------------------------------------------------------------------
# Table declarations — kept in lock-step with bot/migrations/001_init.sql
# ---------------------------------------------------------------------------

metadata = MetaData()

asset_universe = Table(
    "asset_universe",
    metadata,
    Column("ticker", String(10), primary_key=True),
    Column("company_name", String(255), nullable=False),
    Column("sector", String(64), nullable=True),
    Column("added_on", Date, nullable=False, server_default=func.current_date()),
    Column("removed_on", Date, nullable=True),
    Column(
        "last_seen_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    ),
)

# Partial index mirrors ``ix_asset_universe_active`` from the DDL. Declared
# here so SQLAlchemy's MetaData understands the schema; it is not used to
# create the index (the migration SQL owns that).
Index(
    "ix_asset_universe_active",
    asset_universe.c.ticker,
    postgresql_where=asset_universe.c.removed_on.is_(None),
)

fundamentals_cache = Table(
    "fundamentals_cache",
    metadata,
    Column(
        "ticker",
        String(10),
        ForeignKey("asset_universe.ticker", onupdate="CASCADE"),
        primary_key=True,
    ),
    Column("pe_ratio", Numeric(12, 4), nullable=True),
    Column("pe_5y_avg", Numeric(12, 4), nullable=True),
    Column("fcf_yield", Numeric(10, 6), nullable=True),
    Column("latest_headline", String(512), nullable=True),
    Column("headline_url", String(512), nullable=True),
    Column(
        "fetched_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    ),
)

Index("ix_fundamentals_cache_fetched", fundamentals_cache.c.fetched_at)

daily_scans = Table(
    "daily_scans",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column(
        "ticker",
        String(10),
        ForeignKey("asset_universe.ticker", onupdate="CASCADE"),
        nullable=False,
    ),
    Column("scan_date", Date, nullable=False),
    Column("close", Numeric(12, 4), nullable=False),
    Column("pct_above_low", Numeric(6, 4), nullable=False),
    Column("rsi_today", Numeric(6, 3), nullable=False),
    Column("rsi_yesterday", Numeric(6, 3), nullable=False),
    Column("pe_ratio", Numeric(12, 4), nullable=False),
    Column("pe_5y_avg", Numeric(12, 4), nullable=False),
    Column("fcf_yield", Numeric(10, 6), nullable=False),
    Column("latest_headline", String(512), nullable=True),
    Column("config_snapshot", JSONB, nullable=False),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    ),
    UniqueConstraint("ticker", "scan_date", name="uq_scan_per_day"),
)

Index("ix_daily_scans_date", daily_scans.c.scan_date.desc())


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


# The fields of a :class:`ScanCandidate` (or a namedtuple produced by
# ``DataFrame.itertuples``) that ``insert_scan`` expects to extract.
_SCAN_FIELDS: tuple[str, ...] = (
    "ticker",
    "close",
    "pct_above_low",
    "rsi_today",
    "rsi_yesterday",
    "pe_ratio",
    "pe_5y_avg",
    "fcf_yield",
    "latest_headline",
)


def _extract(candidate: Any, field: str) -> Any:
    """Pull ``field`` off ``candidate`` supporting attr- or dict-style access.

    The orchestrator's Example Usage drives ``insert_scan`` from
    ``DataFrame.itertuples()`` (namedtuple-like attribute access) while
    tests and ad-hoc callers may pass a plain :class:`dict`. Supporting both
    keeps the repo decoupled from :mod:`bot.filters`.
    """
    if isinstance(candidate, Mapping):
        return candidate.get(field)
    return getattr(candidate, field, None)


# ---------------------------------------------------------------------------
# Repository
# ---------------------------------------------------------------------------


class Repository:
    """SQLAlchemy Core 2.0 data access gateway.

    The engine is held as an attribute; every public method acquires its own
    transactional connection via :meth:`transaction`. No connection is ever
    opened outside a context manager (Requirement 7.7).
    """

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    # ------------------------------------------------------------------
    # Transaction management
    # ------------------------------------------------------------------

    @contextmanager
    def transaction(self) -> Iterator[Connection]:
        """Yield a connection inside a transaction (Requirement 7.5).

        ``engine.begin()`` commits when the block exits normally and rolls
        back on any exception, giving multi-statement methods atomic
        semantics without manual ``commit()``/``rollback()`` calls.
        """
        with self._engine.begin() as conn:
            yield conn

    # ------------------------------------------------------------------
    # asset_universe
    # ------------------------------------------------------------------

    def load_universe(self) -> set[str]:
        """Return the currently-active S&P 500 tickers (Requirement 7.6).

        The predicate ``removed_on IS NULL`` is the "currently in the index"
        marker; the partial index ``ix_asset_universe_active`` keeps this
        lookup cheap.
        """
        stmt = select(asset_universe.c.ticker).where(
            asset_universe.c.removed_on.is_(None)
        )
        with self.transaction() as conn:
            return {row[0] for row in conn.execute(stmt).all()}

    def upsert_universe(
        self,
        entries: list[tuple[str, str, str | None]],
        as_of: date,
    ) -> None:
        """Upsert the scraped universe and mark absent tickers as removed.

        ``entries`` is the full scraped list as 3-tuples
        ``(ticker, company_name, sector)``. The DDL requires ``company_name``
        to be NOT NULL, so the universe scraper must supply it alongside the
        ticker; ``sector`` may be ``None``.

        Behaviour, executed in a single transaction so the diff and the
        removal marker stay consistent:

        1. ``INSERT ... ON CONFLICT (ticker) DO UPDATE`` every input row,
           refreshing ``company_name``, ``sector``, ``last_seen_at`` and
           clearing ``removed_on`` if the ticker had previously been marked
           as removed (handles readmissions).
        2. ``UPDATE asset_universe SET removed_on = :as_of`` for every
           currently-active ticker not present in the scrape.

        An empty ``entries`` list is permitted — the whole universe getting
        flagged for removal is unusual but legal; the sanity-bound check in
        :mod:`bot.universe` is responsible for aborting that case before
        reaching the repository.

        Requirements: 7.1, 7.2, 7.6, 1.4.
        """
        scraped_tickers = {ticker for ticker, _, _ in entries}

        with self.transaction() as conn:
            if entries:
                payload = [
                    {
                        "ticker": ticker,
                        "company_name": company_name,
                        "sector": sector,
                    }
                    for ticker, company_name, sector in entries
                ]
                insert_stmt = pg_insert(asset_universe).values(payload)
                upsert_stmt = insert_stmt.on_conflict_do_update(
                    index_elements=[asset_universe.c.ticker],
                    set_={
                        "company_name": insert_stmt.excluded.company_name,
                        "sector": insert_stmt.excluded.sector,
                        "removed_on": None,
                        "last_seen_at": func.now(),
                    },
                )
                conn.execute(upsert_stmt)

            # Mark any currently-active ticker not in the scrape as removed.
            # Re-read inside the same transaction so the computation is
            # consistent with what we just wrote (Req 7.5).
            active_stmt = select(asset_universe.c.ticker).where(
                asset_universe.c.removed_on.is_(None)
            )
            active_now = {row[0] for row in conn.execute(active_stmt).all()}
            removed = active_now - scraped_tickers
            if removed:
                mark_removed = (
                    update(asset_universe)
                    .where(
                        and_(
                            asset_universe.c.ticker.in_(removed),
                            asset_universe.c.removed_on.is_(None),
                        )
                    )
                    .values(removed_on=as_of)
                )
                conn.execute(mark_removed)

    # ------------------------------------------------------------------
    # fundamentals_cache
    # ------------------------------------------------------------------

    def load_fundamentals(self, ticker: str) -> Fundamentals | None:
        """Return the cached :class:`Fundamentals` row or ``None``.

        Used by :func:`bot.fundamentals.get_fundamentals` to gate FMP calls
        (Requirement 3.1).
        """
        stmt = select(
            fundamentals_cache.c.ticker,
            fundamentals_cache.c.pe_ratio,
            fundamentals_cache.c.pe_5y_avg,
            fundamentals_cache.c.fcf_yield,
            fundamentals_cache.c.latest_headline,
            fundamentals_cache.c.headline_url,
            fundamentals_cache.c.fetched_at,
        ).where(fundamentals_cache.c.ticker == ticker)

        with self.transaction() as conn:
            row = conn.execute(stmt).one_or_none()

        if row is None:
            return None
        return Fundamentals(
            ticker=row.ticker,
            pe_ratio=float(row.pe_ratio) if row.pe_ratio is not None else None,
            pe_5y_avg=float(row.pe_5y_avg) if row.pe_5y_avg is not None else None,
            fcf_yield=float(row.fcf_yield) if row.fcf_yield is not None else None,
            latest_headline=row.latest_headline,
            headline_url=row.headline_url,
            fetched_at=row.fetched_at,
        )

    def upsert_fundamentals(self, f: Fundamentals) -> None:
        """Upsert one :class:`Fundamentals` row (Requirements 7.2, 3.3).

        On conflict the existing row is fully overwritten except for the
        primary key; ``fetched_at`` is refreshed so the cache TTL resets.
        """
        insert_stmt = pg_insert(fundamentals_cache).values(
            ticker=f.ticker,
            pe_ratio=f.pe_ratio,
            pe_5y_avg=f.pe_5y_avg,
            fcf_yield=f.fcf_yield,
            latest_headline=f.latest_headline,
            headline_url=f.headline_url,
            fetched_at=f.fetched_at,
        )
        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=[fundamentals_cache.c.ticker],
            set_={
                "pe_ratio": insert_stmt.excluded.pe_ratio,
                "pe_5y_avg": insert_stmt.excluded.pe_5y_avg,
                "fcf_yield": insert_stmt.excluded.fcf_yield,
                "latest_headline": insert_stmt.excluded.latest_headline,
                "headline_url": insert_stmt.excluded.headline_url,
                "fetched_at": insert_stmt.excluded.fetched_at,
            },
        )
        with self.transaction() as conn:
            conn.execute(upsert_stmt)

    # ------------------------------------------------------------------
    # daily_scans
    # ------------------------------------------------------------------

    def insert_scan(
        self,
        candidate: Any,
        scan_date: date,
        config_snapshot: dict[str, Any],
    ) -> None:
        """Persist one High_Conviction_Alert row (Requirements 7.1, 7.3, 7.4, 6.7).

        ``candidate`` may be any object exposing the scan fields as either
        attributes (e.g., a ``ScanCandidate`` dataclass or a namedtuple from
        ``DataFrame.itertuples()``) or mapping-style keys (a ``dict``).

        ``config_snapshot`` is the full ``AppConfig.model_dump()`` at the
        moment of insert; it is stored verbatim in the JSONB column so
        every alert is reproducible (Requirements 6.7, 11.11).

        On a ``(ticker, scan_date)`` collision the underlying
        :class:`sqlalchemy.exc.IntegrityError` is translated to
        :class:`DuplicateScanError`, which the orchestrator catches as a
        no-op (Requirement 7.4). Any other ``IntegrityError`` is re-raised
        unchanged.
        """
        values = {field: _extract(candidate, field) for field in _SCAN_FIELDS}
        values["scan_date"] = scan_date
        values["config_snapshot"] = config_snapshot

        stmt = pg_insert(daily_scans).values(**values)
        try:
            with self.transaction() as conn:
                conn.execute(stmt)
        except IntegrityError as exc:
            # The constraint name is the most reliable way to distinguish
            # a scan-per-day collision from any other integrity failure
            # (FK violation, NOT NULL violation, ...).
            constraint_name = getattr(
                getattr(exc.orig, "diag", None), "constraint_name", None
            )
            message = str(exc.orig) if exc.orig is not None else str(exc)
            if constraint_name == "uq_scan_per_day" or "uq_scan_per_day" in message:
                raise DuplicateScanError(
                    f"Duplicate scan for {values['ticker']!r} on {scan_date.isoformat()}"
                ) from exc
            raise

    def recent_scans(self, days: int = 30) -> list[dict[str, Any]]:
        """Return recent ``daily_scans`` rows for operational review (Req 7.1).

        Rows within the trailing ``days`` window (inclusive of today) are
        returned as dicts, ordered newest-first by ``scan_date`` then ``id``
        so reruns appear stably.
        """
        if days < 0:
            raise ValueError("days must be non-negative")

        cutoff = select(func.current_date() - days).scalar_subquery()
        stmt = (
            select(daily_scans)
            .where(daily_scans.c.scan_date >= cutoff)
            .order_by(daily_scans.c.scan_date.desc(), daily_scans.c.id.desc())
        )
        with self.transaction() as conn:
            return [dict(row._mapping) for row in conn.execute(stmt).all()]
