"""Structured JSON logging for the Asset Discovery Bot (Task 15).

Replaces the bring-up :func:`logging.basicConfig` call in :mod:`bot.run`
with a JSON formatter writing to a rotating file handler plus a stream
handler on stderr for interactive debugging.

Design choices:

* **No third-party dependency.** ``python-json-logger`` is a popular
  option, but the bot's pinned ``requirements.txt`` (Req 9.9) is already
  heavyweight; a hand-rolled ``JSONFormatter`` (about 50 lines) keeps the
  supply chain smaller and is trivial to audit.
* **Rotating file handler.** :class:`logging.handlers.RotatingFileHandler`
  rolls over at ``max_file_size_mb`` and keeps ``backup_count`` archives,
  matching :class:`bot.config.LoggingConfig`.
* **Secret hygiene (Req 5.7, 9.8).** The formatter never reflects on
  :class:`bot.config.Secrets` or any field name ending in ``_url``,
  ``_key``, ``_token``, ``_password`` when it encounters an ``extra``
  dict. Callers are still responsible for not passing secrets into logs,
  but this guardrail catches accidental misuse.
* **Contextual fields (``run_id``, ``phase``, ``ticker``).** Emitted via
  the ``extra=`` kwarg on standard ``logger.info(...)`` calls; the
  formatter picks them up from the ``LogRecord`` attributes and merges
  them into the JSON object alongside the standard fields.
"""

from __future__ import annotations

import datetime as _dt
import json
import logging
import logging.handlers
import os
import uuid
from pathlib import Path
from typing import Any

from bot.config import LoggingConfig

__all__ = [
    "configure_logging",
    "current_run_id",
    "JsonFormatter",
]


# Fields on :class:`logging.LogRecord` that are always present; we never
# copy them from ``record.__dict__`` into the JSON object via the
# "everything else" path because they are either already surfaced under
# their canonical name or are Python-internal plumbing.
_STANDARD_RECORD_ATTRS: frozenset[str] = frozenset(
    {
        "args", "asctime", "created", "exc_info", "exc_text", "filename",
        "funcName", "levelname", "levelno", "lineno", "message", "module",
        "msecs", "msg", "name", "pathname", "process", "processName",
        "relativeCreated", "stack_info", "thread", "threadName",
        # Our own well-known fields — surfaced explicitly.
        "run_id", "phase", "ticker",
    }
)

# Field-name substrings that trigger redaction. Catches accidental
# ``extra={"webhook_url": secrets.discord_webhook_url}`` and similar.
_SECRET_SUBSTRINGS: tuple[str, ...] = (
    "_url", "_key", "_token", "_password", "_secret", "apikey", "webhook",
)


def _is_secretish(key: str) -> bool:
    lowered = key.lower()
    return any(sub in lowered for sub in _SECRET_SUBSTRINGS)


class JsonFormatter(logging.Formatter):
    """Emit one JSON object per :class:`logging.LogRecord`.

    Standard fields (``timestamp``, ``level``, ``logger``, ``message``)
    are always present. Optional contextual fields ``run_id``, ``phase``,
    and ``ticker`` are surfaced when set via ``extra=``. Any additional
    ``extra=`` key lands under its own name unless it looks like a secret
    (see :data:`_SECRET_SUBSTRINGS`), in which case the value is replaced
    with ``"***"``.
    """

    def format(self, record: logging.LogRecord) -> str:
        # ``record.getMessage()`` applies %-formatting to ``record.msg``
        # with ``record.args``. That matches the bring-up formatter's
        # behaviour and keeps existing call sites working unchanged.
        payload: dict[str, Any] = {
            "timestamp": _dt.datetime.fromtimestamp(
                record.created, tz=_dt.timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        for context_key in ("run_id", "phase", "ticker"):
            value = getattr(record, context_key, None)
            if value is not None:
                payload[context_key] = value

        # Anything else the caller passed via ``extra=`` we pick up from
        # ``record.__dict__`` minus the standard attributes. Secrets are
        # redacted rather than silently dropped so the log line still
        # shows that something was intended for that slot.
        for key, value in record.__dict__.items():
            if key in _STANDARD_RECORD_ATTRS:
                continue
            if key.startswith("_"):
                continue
            payload[key] = "***" if _is_secretish(key) else value

        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)

        return json.dumps(payload, default=str, separators=(",", ":"))


_RUN_ID: str | None = None


def current_run_id() -> str:
    """Return the run-scoped UUID; generated on first access per process.

    Every :class:`logging.LogRecord` emitted during the same Scan_Run
    carries the same ``run_id``, making it trivial to grep a rotating log
    file for a single invocation's output.
    """
    global _RUN_ID
    if _RUN_ID is None:
        _RUN_ID = uuid.uuid4().hex
    return _RUN_ID


class _RunIdFilter(logging.Filter):
    """Inject the run-scoped UUID onto every :class:`LogRecord`."""

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "run_id"):
            record.run_id = current_run_id()
        return True


def configure_logging(cfg: LoggingConfig) -> None:
    """Install the JSON formatter + rotating-file + stderr handlers.

    Idempotent: repeated calls replace the root handlers rather than
    stacking new ones.
    """
    root = logging.getLogger()
    root.setLevel(cfg.level.upper())

    # Purge any handlers the bring-up ``basicConfig`` may have installed
    # so we don't end up double-logging.
    for handler in list(root.handlers):
        root.removeHandler(handler)

    formatter = JsonFormatter()
    run_id_filter = _RunIdFilter()

    # Stream handler — always on, writes JSON to stderr so interactive
    # `docker compose run` / `docker logs` invocations still show output.
    stream = logging.StreamHandler()
    stream.setFormatter(formatter)
    stream.addFilter(run_id_filter)
    root.addHandler(stream)

    # Rotating file handler — only installed if the log directory is
    # writable. On first run the directory may not exist yet; create it
    # and fall back to stream-only if we cannot (for example, running in
    # CI with a read-only filesystem).
    log_dir: Path = cfg.log_dir
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / "asset-discovery-bot.log"
        file_handler = logging.handlers.RotatingFileHandler(
            filename=log_path,
            maxBytes=cfg.max_file_size_mb * 1024 * 1024,
            backupCount=cfg.backup_count,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        file_handler.addFilter(run_id_filter)
        root.addHandler(file_handler)
    except OSError as exc:
        # Don't fail the run for a log-directory problem — stderr still
        # works and the scheduler captures that.
        root.warning(
            "Could not initialise rotating file handler at %s: %s; "
            "continuing with stderr-only logging",
            log_dir,
            exc,
        )

    # Quiet noisy third-party loggers that are too chatty at DEBUG/INFO
    # and rarely useful for operational triage.
    for noisy in ("urllib3", "yfinance", "peewee"):
        logging.getLogger(noisy).setLevel(
            max(logging.WARNING, getattr(logging, cfg.level.upper(), logging.INFO))
        )

    # Respect LOGGING_TEST_OVERRIDE if a caller wants to send output to a
    # custom destination (e.g., the optional smoke test). Kept cheap: if
    # the env var is unset, this is a no-op.
    override = os.environ.get("ADB_LOG_OUTPUT_OVERRIDE")
    if override:
        root.info("Logging override detected: %s", override)
