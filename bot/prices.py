"""Price_Service: yfinance adapter and technical-indicator compute.

This module fulfils the **Price_Service** role in the design
(:doc:`.kiro/specs/asset-discovery-bot/design.md`, Component 2). It is the
only module in the bot that talks to Yahoo Finance, and the only one that
computes RSI / 52-week-low / percent-above-low.

Responsibilities:

* Batch-download 1-year daily OHLC from yfinance for the active S&P 500
  universe (Requirement 2.1).
* Retry per-ticker on transient failures up to
  ``cfg.retries_per_ticker`` times (Requirement 2.7).
* Exclude tickers with empty frames after retries, logging a WARN
  (Requirements 2.5, 8.2).
* Compute Wilder's-smoothed RSI(period) in a pure, no-look-ahead function
  (Requirements 2.4, 11.2).
* Produce a per-ticker technical snapshot with exactly the columns
  ``ticker, close, low_52w, pct_above_low, rsi_today, rsi_yesterday``
  (Requirement 2.2), excluding any ticker with fewer than
  ``rsi_period + 1`` observations (Requirement 2.6).

Ticker normalisation — the Wikipedia → Yahoo mismatch
-----------------------------------------------------
Wikipedia renders compound tickers with dots (``BRK.B``, ``BF.B``).
yfinance / Yahoo Finance expect dashes (``BRK-B``, ``BF-B``). The scrape in
:mod:`bot.universe` preserves the Wikipedia form so that ``asset_universe``
and Discord alerts read naturally to humans. The conversion to the Yahoo
form happens here, at the yfinance boundary, via :func:`_to_yahoo_ticker`.
We also build a reverse mapping so that the ``dict[str, pd.DataFrame]``
returned by :func:`download_price_history` is keyed by the **original
Wikipedia ticker** — downstream modules never need to know about the
Yahoo-specific spelling.
"""

from __future__ import annotations

import logging
import time
from typing import Iterable

import pandas as pd
import yfinance as yf

from bot.config import YFinanceConfig

__all__ = [
    "download_price_history",
    "compute_rsi",
    "compute_technical_snapshot",
]


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Ticker normalisation
# ---------------------------------------------------------------------------


def _to_yahoo_ticker(ticker: str) -> str:
    """Convert a Wikipedia ticker to the Yahoo Finance form.

    Wikipedia uses dots for share classes (e.g., ``BRK.B``, ``BF.B``);
    yfinance expects dashes (``BRK-B``, ``BF-B``). Any other character is
    preserved verbatim.
    """
    return ticker.replace(".", "-")


def _batched(items: list[str], size: int) -> Iterable[list[str]]:
    """Yield consecutive slices of ``items`` of length ``size`` (last may be short)."""
    if size <= 0:
        raise ValueError(f"batch size must be positive, got {size}")
    for start in range(0, len(items), size):
        yield items[start:start + size]


# ---------------------------------------------------------------------------
# yfinance shape normalisation
# ---------------------------------------------------------------------------


# yfinance 0.2.x returns:
#   * a column-MultiIndex DataFrame when called with 2+ tickers:
#       columns = MultiIndex([(ticker, field), ...])  when group_by='ticker'
#   * a flat-columns DataFrame when called with exactly 1 ticker:
#       columns = [Open, High, Low, Close, Adj Close, Volume]
# We normalise both into a ``dict[yahoo_ticker -> frame]``.
def _split_yahoo_frame(
    raw: pd.DataFrame,
    yahoo_tickers: list[str],
) -> dict[str, pd.DataFrame]:
    """Normalise a yfinance download result into per-ticker frames.

    Empty frames and frames that are entirely NaN after ``dropna()`` are
    replaced with an empty DataFrame — the caller treats that as "no data
    returned" and will either retry or log a WARN and exclude the ticker.
    """
    per_ticker: dict[str, pd.DataFrame] = {}

    if raw is None or len(raw) == 0:
        # yfinance returned nothing at all for this batch.
        return {yt: pd.DataFrame() for yt in yahoo_tickers}

    if isinstance(raw.columns, pd.MultiIndex):
        # Multi-ticker batch. The top level of the MultiIndex is the ticker
        # (we passed group_by='ticker').
        top_level = set(raw.columns.get_level_values(0))
        for yt in yahoo_tickers:
            if yt not in top_level:
                per_ticker[yt] = pd.DataFrame()
                continue
            frame = raw[yt].dropna(how="all")
            per_ticker[yt] = frame if not frame.empty else pd.DataFrame()
    else:
        # Single-ticker batch. There must be exactly one Yahoo ticker here.
        frame = raw.dropna(how="all")
        if len(yahoo_tickers) == 1:
            per_ticker[yahoo_tickers[0]] = (
                frame if not frame.empty else pd.DataFrame()
            )
        else:
            # Defensive: shouldn't happen, but if yfinance returned a flat
            # frame for a multi-ticker call we cannot reliably split it.
            # Treat every requested ticker as "no data".
            logger.warning(
                "yfinance returned a flat frame for a %d-ticker batch; "
                "treating all tickers as empty",
                len(yahoo_tickers),
            )
            for yt in yahoo_tickers:
                per_ticker[yt] = pd.DataFrame()

    return per_ticker


def _download_batch(
    yahoo_tickers: list[str],
    period: str,
) -> dict[str, pd.DataFrame]:
    """Single yfinance call for a batch; returns ``{yahoo_ticker: frame}``.

    Catches any exception from yfinance and returns empty frames for every
    ticker in the batch — the caller retries at the per-ticker level, so a
    blown batch is recoverable as long as the retry budget allows.
    """
    try:
        raw = yf.download(
            tickers=yahoo_tickers,
            period=period,
            group_by="ticker",
            auto_adjust=True,
            progress=False,
            threads=False,
            repair=True,
        )
    except Exception as exc:  # noqa: BLE001 — we deliberately swallow and retry
        logger.warning(
            "yfinance batch download failed (%d tickers): %s",
            len(yahoo_tickers),
            exc,
        )
        return {yt: pd.DataFrame() for yt in yahoo_tickers}

    return _split_yahoo_frame(raw, yahoo_tickers)


# ---------------------------------------------------------------------------
# Public API — download_price_history
# ---------------------------------------------------------------------------


def download_price_history(
    tickers: list[str],
    cfg: YFinanceConfig,
) -> dict[str, pd.DataFrame]:
    """Batch-download OHLC history for every ticker, keyed by Wikipedia form.

    Implements Requirements 2.1, 2.5, 2.7, 8.2.

    Flow:
        1. Split ``tickers`` into batches of ``cfg.batch_size``.
        2. Convert each batch to Yahoo form (``.`` -> ``-``) and call
           ``yf.download`` once per batch.
        3. For any ticker that comes back empty, retry it individually up
           to ``cfg.retries_per_ticker`` times with a small backoff.
        4. Key the result dict by the **original Wikipedia ticker**. Any
           ticker that remained empty after all retries is excluded from
           the result and logged at WARN level.

    Args:
        tickers: Wikipedia-form tickers (e.g., ``BRK.B``). Duplicates and
            ordering are preserved in the reverse-mapping but the returned
            dict is keyed by unique ticker.
        cfg: ``AppConfig.yfinance`` — supplies ``history_period``,
            ``batch_size``, and ``retries_per_ticker``.

    Returns:
        A dict ``{wikipedia_ticker: DataFrame}``. Tickers with no usable
        data are simply absent from the dict.
    """
    if not tickers:
        return {}

    # Build a forward + reverse map so we can always go back to the
    # original Wikipedia ticker when keying the result. Preserve original
    # order for logs.
    wiki_to_yahoo: dict[str, str] = {w: _to_yahoo_ticker(w) for w in tickers}
    # Reverse map: Yahoo -> Wikipedia. Two different Wikipedia tickers
    # mapping to the same Yahoo ticker would be pathological; if it ever
    # happens, the second wins, which is still deterministic.
    yahoo_to_wiki: dict[str, str] = {yt: w for w, yt in wiki_to_yahoo.items()}

    unique_wiki: list[str] = list(dict.fromkeys(tickers))
    result: dict[str, pd.DataFrame] = {}
    missing_after_batches: list[str] = []  # Wikipedia tickers

    # ---- Phase 1: one batch call per batch ---------------------------------
    for batch in _batched(unique_wiki, cfg.batch_size):
        yahoo_batch = [wiki_to_yahoo[w] for w in batch]
        per_yahoo = _download_batch(yahoo_batch, cfg.history_period)

        for wiki_ticker in batch:
            yahoo_ticker = wiki_to_yahoo[wiki_ticker]
            frame = per_yahoo.get(yahoo_ticker, pd.DataFrame())
            if frame.empty:
                missing_after_batches.append(wiki_ticker)
            else:
                result[wiki_ticker] = frame

    # ---- Phase 2: per-ticker retry for anything still missing --------------
    # yfinance doesn't expose a per-ticker retry knob, so we emulate it by
    # calling ``yf.download`` with a single ticker. A short fixed sleep
    # between attempts avoids hammering Yahoo after a transient failure.
    retry_budget = max(0, cfg.retries_per_ticker)
    for wiki_ticker in missing_after_batches:
        yahoo_ticker = wiki_to_yahoo[wiki_ticker]
        recovered = False
        for attempt in range(1, retry_budget + 1):
            per_yahoo = _download_batch([yahoo_ticker], cfg.history_period)
            frame = per_yahoo.get(yahoo_ticker, pd.DataFrame())
            if not frame.empty:
                result[wiki_ticker] = frame
                recovered = True
                break
            logger.debug(
                "yfinance retry %d/%d produced empty frame for %s",
                attempt,
                retry_budget,
                wiki_ticker,
            )
            time.sleep(0.5)
        if not recovered:
            logger.warning(
                "yfinance returned no data for ticker=%s after %d retries; "
                "excluding from snapshot",
                wiki_ticker,
                retry_budget,
            )

    # Silence a possible unused-variable lint on the reverse map; it's here
    # so future callers wanting to go Yahoo -> Wikipedia have it available.
    _ = yahoo_to_wiki
    return result


# ---------------------------------------------------------------------------
# Public API — compute_rsi
# ---------------------------------------------------------------------------


def compute_rsi(close_series: pd.Series, period: int) -> pd.Series:
    """Wilder's-smoothed RSI over ``close_series`` with the given ``period``.

    Implements Requirement 2.4. Guarantees Requirement 11.2 (no look-ahead)
    by using ``ewm(adjust=False, min_periods=period)`` — the EMA at
    position ``i`` is a function only of ``close_series[: i + 1]``.

    Algorithm (Wilder, 1978):

        delta    = close.diff()
        gain     = max(delta,  0)
        loss     = max(-delta, 0)
        avg_gain = gain.ewm(alpha = 1 / period, adjust=False).mean()
        avg_loss = loss.ewm(alpha = 1 / period, adjust=False).mean()
        rs       = avg_gain / avg_loss
        rsi      = 100 - 100 / (1 + rs)

    Special cases:
        * ``avg_loss == 0`` and ``avg_gain > 0``  -> RSI = 100
          (no down-days in the window; the series is in an up-only run).
        * ``avg_loss == 0`` and ``avg_gain == 0`` -> RSI = NaN
          (no movement at all in the window; the indicator is undefined).
        * The first ``period`` positions are NaN by construction
          (``min_periods=period``).

    Args:
        close_series: Chronologically ordered daily close prices. Must have
            a numeric dtype; index is not inspected.
        period: Wilder period. Must be >= 2 (matches
            :class:`bot.config.Layer2Config.rsi_period`).

    Returns:
        A ``pd.Series`` of floats the same length and index as the input,
        with values in ``[0, 100]`` or ``NaN``.
    """
    if period < 2:
        raise ValueError(f"rsi period must be >= 2, got {period}")

    delta = close_series.astype("float64").diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)

    alpha = 1.0 / period
    avg_gain = gain.ewm(alpha=alpha, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=alpha, adjust=False, min_periods=period).mean()

    # RSI formula. We compute it the standard way and then overwrite the
    # avg_loss == 0 case explicitly so that divide-by-zero never surfaces
    # as ``inf`` in the result.
    rs = avg_gain / avg_loss
    rsi = 100.0 - (100.0 / (1.0 + rs))

    # avg_loss == 0 and avg_gain > 0 -> RSI = 100 (pure up-run).
    # avg_loss == 0 and avg_gain == 0 -> RSI stays NaN (no movement).
    no_loss = avg_loss == 0.0
    pure_up = no_loss & (avg_gain > 0.0)
    rsi = rsi.mask(pure_up, 100.0)
    # Explicitly NaN the "no movement" zone — division produced NaN already
    # for 0/0, but being explicit protects against floating-point quirks
    # upstream.
    no_move = no_loss & (avg_gain == 0.0)
    rsi = rsi.mask(no_move, float("nan"))

    return rsi


# ---------------------------------------------------------------------------
# Public API — compute_technical_snapshot
# ---------------------------------------------------------------------------


# Columns present in a yfinance frame when auto_adjust=True. ``Close`` is
# the adjusted close; ``Low`` is the (adjusted) intraday low, which is the
# industry-standard input for "52-week low".
_CLOSE_COL = "Close"
_LOW_COL = "Low"

# 252 trading days ~ 52 calendar weeks. yfinance's ``1y`` period typically
# returns ~252 rows, so in practice ``tail(252)`` is equivalent to "use the
# whole frame"; the explicit cap makes the function robust if the caller
# passes a longer history period.
_TRADING_DAYS_PER_YEAR = 252


def compute_technical_snapshot(
    frames: dict[str, pd.DataFrame],
    rsi_period: int,
) -> pd.DataFrame:
    """Build the per-ticker technical snapshot consumed by Layers 1 and 2.

    Implements Requirements 2.2, 2.3, 2.6. Tickers with fewer than
    ``rsi_period + 1`` observations are excluded (never emitted as NaN), as
    are tickers whose final RSI or required OHLC field is missing.

    For each ticker in ``frames``:

        * ``close`` = last adjusted close (``frame['Close'].iloc[-1]``).
        * ``low_52w`` = minimum of the last 252 trading days of the
          (adjusted) intraday low (``frame['Low'].tail(252).min()``).
        * ``pct_above_low`` = ``(close - low_52w) / low_52w``.
        * ``rsi_today``, ``rsi_yesterday`` = last two values of
          :func:`compute_rsi(close_series, rsi_period)`.

    Args:
        frames: ``{ticker: OHLC DataFrame}`` as returned by
            :func:`download_price_history`. Frames must contain ``Close``
            and ``Low`` columns; anything else is ignored.
        rsi_period: ``AppConfig.layer2.rsi_period`` (default 14).

    Returns:
        A ``pd.DataFrame`` with columns, in order:
        ``ticker, close, low_52w, pct_above_low, rsi_today, rsi_yesterday``.
        ``ticker`` is a column (not the index) and dtype ``object``; the
        remaining columns are floats. An empty input (or all tickers
        excluded) produces an empty frame with the same column schema.
    """
    columns = [
        "ticker",
        "close",
        "low_52w",
        "pct_above_low",
        "rsi_today",
        "rsi_yesterday",
    ]

    rows: list[dict[str, object]] = []
    min_obs = rsi_period + 1

    for ticker, frame in frames.items():
        if frame is None or frame.empty:
            # download_price_history already warned; stay quiet here.
            continue
        if _CLOSE_COL not in frame.columns or _LOW_COL not in frame.columns:
            logger.warning(
                "Frame for ticker=%s missing required columns "
                "(have=%s); excluding from snapshot",
                ticker,
                list(frame.columns),
            )
            continue

        close_series = frame[_CLOSE_COL].dropna()
        low_series = frame[_LOW_COL].dropna()

        if len(close_series) < min_obs:
            # Requirement 2.6 — exclude rather than emit NaN.
            logger.warning(
                "Ticker=%s has %d close observations, need >= %d "
                "(rsi_period + 1); excluding from snapshot",
                ticker,
                len(close_series),
                min_obs,
            )
            continue
        if low_series.empty:
            logger.warning(
                "Ticker=%s has no Low observations; excluding from snapshot",
                ticker,
            )
            continue

        close = float(close_series.iloc[-1])
        low_52w = float(low_series.tail(_TRADING_DAYS_PER_YEAR).min())
        if not (low_52w > 0):
            logger.warning(
                "Ticker=%s has non-positive 52-week low (%.6f); "
                "excluding from snapshot",
                ticker,
                low_52w,
            )
            continue

        rsi_series = compute_rsi(close_series, rsi_period)
        rsi_today = float(rsi_series.iloc[-1])
        rsi_yesterday = float(rsi_series.iloc[-2])
        if pd.isna(rsi_today) or pd.isna(rsi_yesterday):
            logger.warning(
                "Ticker=%s has NaN RSI at tail (today=%s, yesterday=%s); "
                "excluding from snapshot",
                ticker,
                rsi_today,
                rsi_yesterday,
            )
            continue

        pct_above_low = (close - low_52w) / low_52w

        rows.append(
            {
                "ticker": ticker,
                "close": close,
                "low_52w": low_52w,
                "pct_above_low": pct_above_low,
                "rsi_today": rsi_today,
                "rsi_yesterday": rsi_yesterday,
            }
        )

    if not rows:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame(rows, columns=columns)
