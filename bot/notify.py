"""Notifier: Discord webhook publisher for the Asset Discovery Bot.

This module fulfils Component 5 of the design
(:doc:`.kiro/specs/asset-discovery-bot/design.md`). It is the only module
in the bot that talks to Discord, and the only place where Discord rich
embeds are assembled.

Two message types are supported:

* :func:`send_high_conviction` — one green embed per High_Conviction_
  Candidate emitted by :mod:`bot.filters`. Renders ticker, close, percent
  above the 52-week low, RSI (yesterday -> today), P/E (current / 5-year
  average), FCF yield, and the latest press-release headline with URL
  when available (Requirement 5.1).
* :func:`send_watchdog` — one yellow embed per non-empty
  :class:`bot.universe.UniverseDiff`, listing the added and removed
  tickers (Requirement 5.2).

Research-alert framing (Requirement 10.4)
-----------------------------------------
Every embed is copy-reviewed to present the candidate as **information
for human review**. The words "buy", "sell", "allocate", "hold",
"position", and "size" never appear in either embed. The footer of the
High_Conviction embed ends with the phrase "for human review" so the
reader of the alert is reminded, on every message, that the bot is not
issuing a trading instruction.

Retry semantics (Requirement 5.3, 5.4)
--------------------------------------
Discord's documented failure modes are HTTP 429 (rate limit) and 5xx
(transient server errors). Both are retried with exponential backoff
bounded by ``cfg.backoff_initial_seconds`` and ``cfg.backoff_max_seconds``,
up to ``cfg.max_retries`` retries (so ``max_retries + 1`` total attempts).
On 429, the ``Retry-After`` header is honoured before the next attempt —
tenacity's ``wait_exponential`` runs afterwards as a safety net.

Any other 4xx (400, 401, 403, 404, ...) is treated as a caller bug —
malformed payload, revoked webhook, deleted channel — and raises
:class:`NotificationError` immediately without retrying. The orchestrator
converts this into a non-zero exit code (Requirement 5.4).

Secret hygiene (Requirements 5.7, 9.8)
--------------------------------------
The Discord webhook URL is a secret: anyone who holds it can post to the
channel. This module therefore:

* **Never** interpolates ``webhook_url`` into a log line, exception
  message, or embed content.
* Catches transport exceptions raised by :mod:`requests` (which can
  include the URL in their default ``repr``) and re-raises a sanitised
  :class:`NotificationError` with a generic "Discord webhook" phrase.
* Refers to the webhook as "Discord webhook" in every log message.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any

import requests
from tenacity import (
    RetryError,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from bot.config import NotificationConfig
from bot.filters import ScanCandidate
from bot.universe import UniverseDiff

__all__ = [
    "NotificationError",
    "send_high_conviction",
    "send_watchdog",
]


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public exception — the single error type callers have to handle.
# ---------------------------------------------------------------------------


class NotificationError(Exception):
    """Raised when a Discord POST fails non-recoverably.

    Three distinct failure paths collapse into this single exception:

    * Non-retryable 4xx response (other than 429) — caller-visible bug
      (malformed payload, revoked webhook, etc.).
    * Retry budget exhausted on 429 / 5xx after ``max_retries`` attempts.
    * Transport-level error (DNS, TLS, connection reset) after the retry
      budget is exhausted.

    The exception message deliberately never contains the webhook URL
    (Requirement 5.7).
    """


# ---------------------------------------------------------------------------
# Private sentinel exception used purely to drive tenacity's retry loop.
# Not part of the public API.
# ---------------------------------------------------------------------------


class _RetryableNotificationError(Exception):
    """Thrown by the inner POST when the response should trigger a retry.

    Covers HTTP 429 and 5xx only. Anything else becomes a
    :class:`NotificationError` immediately.
    """


# ---------------------------------------------------------------------------
# Colour palette — keep the hex values here so both senders agree.
# ---------------------------------------------------------------------------


_COLOR_HIGH_CONVICTION = 0x2ECC71  # green — "passed every layer"
_COLOR_WATCHDOG = 0xF1C40F  # yellow — "something about the universe changed"


# Discord limits, reproduced here so we don't silently send malformed
# payloads. See https://discord.com/developers/docs/resources/channel
# (truncation is a best-effort safeguard — actual copy almost never hits
# these ceilings, but a freakishly long headline or a 500-ticker diff
# could).
_DISCORD_FIELD_VALUE_LIMIT = 1024
_HTTP_TIMEOUT_SECONDS = 10.0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _truncate(text: str, limit: int = _DISCORD_FIELD_VALUE_LIMIT) -> str:
    """Truncate ``text`` with an ellipsis if it would exceed ``limit`` chars."""
    if len(text) <= limit:
        return text
    # Reserve 3 chars for the ellipsis so the final string is <= limit.
    return text[: max(0, limit - 3)] + "..."


def _to_yahoo_symbol(ticker: str) -> str:
    """Convert a Wikipedia-form ticker to the form Yahoo Finance URLs use.

    Wikipedia writes share classes with dots (``BRK.B``) whereas Yahoo
    Finance's quote URLs expect dashes (``BRK-B``). This mirrors the
    normalisation in :mod:`bot.prices` but is reproduced here to keep the
    notify module free of an implementation-detail import from the price
    adapter. The rule is identical: replace ``.`` with ``-``.
    """
    return ticker.replace(".", "-")


def _yahoo_finance_url(ticker: str) -> str:
    """Compose the public Yahoo Finance quote URL for a ticker."""
    return f"https://finance.yahoo.com/quote/{_to_yahoo_symbol(ticker)}"


def _utc_now_iso() -> str:
    """Current UTC time in ISO 8601 — Discord renders this in each viewer's tz."""
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Embed builders
# ---------------------------------------------------------------------------


def _build_high_conviction_embed(candidate: ScanCandidate) -> dict[str, Any]:
    """Assemble the green rich embed for one :class:`ScanCandidate`.

    Every number is formatted to a fixed precision so the alert reads
    cleanly regardless of the underlying float representation. The
    "Latest Catalyst" field is omitted entirely when no headline exists,
    rather than rendering an ambiguous "N/A" — an omitted field is less
    noisy and Discord collapses the layout around it.
    """
    fields: list[dict[str, Any]] = [
        {
            "name": "Close",
            "value": f"${candidate.close:,.2f}",
            "inline": True,
        },
        {
            "name": "% Above 52W Low",
            # ``pct_above_low`` is stored as a decimal fraction (Req 2.3).
            "value": f"{candidate.pct_above_low * 100:.1f}%",
            "inline": True,
        },
        {
            "name": "RSI (yday\u2192today)",
            "value": f"{candidate.rsi_yesterday:.1f} \u2192 {candidate.rsi_today:.1f}",
            "inline": True,
        },
        {
            "name": "P/E (curr / 5y)",
            "value": f"{candidate.pe_ratio:.1f} / {candidate.pe_5y_avg:.1f}",
            "inline": True,
        },
        {
            "name": "FCF Yield",
            "value": f"{candidate.fcf_yield * 100:.1f}%",
            "inline": True,
        },
    ]

    # Latest catalyst — only include if we actually have a headline.
    if candidate.latest_headline:
        if candidate.headline_url:
            catalyst_value = (
                f"[{candidate.latest_headline}]({candidate.headline_url})"
            )
        else:
            catalyst_value = candidate.latest_headline
        fields.append(
            {
                "name": "Latest Catalyst",
                "value": _truncate(catalyst_value),
                "inline": False,
            }
        )

    return {
        "title": f"\U0001F3AF High Conviction: {candidate.ticker}",
        "url": _yahoo_finance_url(candidate.ticker),
        "color": _COLOR_HIGH_CONVICTION,
        "fields": fields,
        "footer": {
            # Req 10.4 — "for human review" is load-bearing framing and must
            # remain on every high-conviction alert.
            "text": (
                "Layers passed: 52W anchor, RSI crossover, P/E value, "
                "FCF quality \u2014 for human review"
            ),
        },
        "timestamp": _utc_now_iso(),
    }


def _build_watchdog_embed(diff: UniverseDiff) -> dict[str, Any]:
    """Assemble the yellow rich embed for a non-empty :class:`UniverseDiff`.

    ``added`` / ``removed`` are comma-joined. When either is empty the
    field shows an em-dash so Discord still renders a balanced two-column
    layout. Both fields are truncated at Discord's 1024-char ceiling so
    the embed never becomes malformed on a freakishly large diff.
    """
    added_text = ", ".join(diff.added) if diff.added else "\u2014"
    removed_text = ", ".join(diff.removed) if diff.removed else "\u2014"

    return {
        "title": "\u26A0\uFE0F S&P 500 Universe Changed",
        "color": _COLOR_WATCHDOG,
        "fields": [
            {
                "name": "Added",
                "value": _truncate(added_text),
                "inline": True,
            },
            {
                "name": "Removed",
                "value": _truncate(removed_text),
                "inline": True,
            },
        ],
        "footer": {"text": "Local asset_universe has been synced."},
        "timestamp": _utc_now_iso(),
    }


# ---------------------------------------------------------------------------
# POST with retry
# ---------------------------------------------------------------------------


def _post_with_retry(
    webhook_url: str,
    payload: dict[str, Any],
    cfg: NotificationConfig,
) -> None:
    """POST ``payload`` to Discord with bounded exponential-backoff retry.

    Implements Requirement 5.3: up to ``cfg.max_retries`` retries on 429
    and 5xx (so up to ``cfg.max_retries + 1`` total attempts), exponential
    backoff bounded by ``cfg.backoff_initial_seconds`` /
    ``cfg.backoff_max_seconds``, and the ``Retry-After`` header honoured
    on 429. Requirement 5.4: non-retryable 4xx raises
    :class:`NotificationError` without retry.

    The webhook URL is never echoed into any log message or exception
    message (Requirement 5.7).
    """
    # Build the retrying callable inside this function so ``cfg`` is
    # captured in the closure rather than baked into module-level state.
    # ``stop_after_attempt(n)`` means "up to n attempts", so ``max_retries``
    # retries = ``max_retries + 1`` attempts.
    total_attempts = max(1, cfg.max_retries + 1)

    @retry(
        stop=stop_after_attempt(total_attempts),
        wait=wait_exponential(
            multiplier=cfg.backoff_initial_seconds,
            max=cfg.backoff_max_seconds,
        ),
        retry=retry_if_exception_type(_RetryableNotificationError),
        reraise=True,
    )
    def _attempt() -> None:
        try:
            response = requests.post(
                webhook_url,
                json=payload,
                timeout=_HTTP_TIMEOUT_SECONDS,
            )
        except requests.RequestException as exc:
            # Transport-level error (DNS, TLS, connection reset, read
            # timeout, ...). These are transient by nature, so treat them
            # as retryable. Use only the exception *class name* in the log
            # — the exception's ``str`` may contain the URL.
            logger.warning(
                "Discord webhook transport error (%s); will retry if budget remains",
                type(exc).__name__,
            )
            raise _RetryableNotificationError(
                f"Discord webhook transport error: {type(exc).__name__}"
            ) from None  # suppress original exception — its repr may contain the URL

        status = response.status_code

        if 200 <= status < 300:
            return

        if status == 429:
            # Honour Retry-After before raising so tenacity's own wait
            # stacks on top as a safety net. A malformed header (non-int)
            # is treated as 0.
            retry_after_raw = response.headers.get("Retry-After", "0")
            try:
                retry_after = float(retry_after_raw)
            except (TypeError, ValueError):
                retry_after = 0.0
            retry_after = max(0.0, min(retry_after, cfg.backoff_max_seconds))
            if retry_after > 0:
                logger.warning(
                    "Discord webhook returned 429; honouring Retry-After=%.2fs",
                    retry_after,
                )
                time.sleep(retry_after)
            else:
                logger.warning(
                    "Discord webhook returned 429; no Retry-After header, falling back to exponential backoff"
                )
            raise _RetryableNotificationError("Discord webhook rate-limited (429)")

        if 500 <= status < 600:
            logger.warning(
                "Discord webhook returned %d; will retry if budget remains",
                status,
            )
            raise _RetryableNotificationError(
                f"Discord webhook server error (status={status})"
            )

        # Anything else in the 4xx range is a caller bug — malformed
        # payload, revoked webhook, deleted channel. Do NOT retry; raise
        # immediately so the orchestrator exits non-zero (Req 5.4).
        logger.error(
            "Discord webhook rejected request with non-retryable status=%d",
            status,
        )
        raise NotificationError(
            f"Discord webhook rejected payload: status={status}"
        )

    try:
        _attempt()
    except _RetryableNotificationError as exc:
        # Retry budget exhausted. Collapse into the public exception type.
        raise NotificationError(
            f"Discord webhook exhausted retry budget: {exc}"
        ) from None
    except RetryError as exc:
        # Defensive: ``reraise=True`` should mean we never see this, but if
        # tenacity is ever configured differently in the future, collapse
        # it to NotificationError rather than leaking the tenacity type.
        raise NotificationError(
            "Discord webhook exhausted retry budget"
        ) from None
    except NotificationError:
        # Non-retryable 4xx — already the right exception type.
        raise


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def send_high_conviction(
    candidate: ScanCandidate,
    webhook_url: str,
    cfg: NotificationConfig,
) -> None:
    """Post a high-conviction rich embed for one :class:`ScanCandidate`.

    Implements Requirements 5.1, 5.3, 5.4, 5.6, 5.7, 10.4.

    Args:
        candidate: The High_Conviction_Candidate to alert on. Must have
            passed every filter layer; all numeric fields are assumed
            non-null.
        webhook_url: Discord webhook URL from ``Secrets.discord_webhook_url``.
            Treated as secret; never logged, never included in exception
            messages.
        cfg: ``AppConfig.notification`` — supplies ``bot_username``,
            ``max_retries``, ``backoff_initial_seconds``,
            ``backoff_max_seconds``.

    Raises:
        NotificationError: On non-retryable 4xx or after the retry budget
            for 429 / 5xx is exhausted. The caller (orchestrator) should
            convert this to a non-zero process exit.
    """
    payload = {
        "username": cfg.bot_username,
        "embeds": [_build_high_conviction_embed(candidate)],
    }

    logger.info(
        "Posting high-conviction alert to Discord webhook for ticker=%s",
        candidate.ticker,
    )
    _post_with_retry(webhook_url, payload, cfg)
    logger.info(
        "High-conviction alert delivered for ticker=%s",
        candidate.ticker,
    )


def send_watchdog(
    diff: UniverseDiff,
    webhook_url: str,
    cfg: NotificationConfig,
) -> None:
    """Post a watchdog rich embed for a non-empty :class:`UniverseDiff`.

    Implements Requirements 5.2, 5.3, 5.6, 5.7.

    The caller is responsible for deciding whether the diff is non-empty;
    this function will happily render ``Added: \u2014 / Removed: \u2014``
    if asked to. In normal orchestrator flow the caller guards with
    ``if diff.added or diff.removed:``.

    Args:
        diff: The :class:`UniverseDiff` to announce.
        webhook_url: Discord webhook URL from ``Secrets.discord_webhook_url``.
        cfg: ``AppConfig.notification`` — supplies ``bot_username`` and
            retry budget.

    Raises:
        NotificationError: Same semantics as :func:`send_high_conviction`.
    """
    payload = {
        "username": cfg.bot_username,
        "embeds": [_build_watchdog_embed(diff)],
    }

    logger.info(
        "Posting watchdog alert to Discord webhook (added=%d, removed=%d)",
        len(diff.added),
        len(diff.removed),
    )
    _post_with_retry(webhook_url, payload, cfg)
    logger.info("Watchdog alert delivered")
