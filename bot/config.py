"""Configuration loader for the Asset Discovery Bot.

This module centralises every tunable parameter (filter thresholds, cache
TTLs, retry budgets, log levels) in a single declarative, Pydantic-validated
config. Secrets (database URL, FMP API key, Discord webhook URL) live in
separate files under ``/run/secrets/`` and are loaded into a distinct
``Secrets`` model so tunables can be experimented with freely without
touching credential material.

Precedence (highest wins):

    1. Environment variable ``ADB_<SECTION>__<FIELD>``
    2. ``config.yaml`` value
    3. Pydantic field default

Nested fields flatten with ``ADB_`` prefix and double-underscore delimiters.
Example:

    ADB_LAYER1__PCT_ABOVE_LOW_MIN=0.03
        -> {"layer1": {"pct_above_low_min": "0.03"}}

See ``.kiro/specs/asset-discovery-bot/design.md`` (Component 6) for the
authoritative contract. The loader must raise ``ValidationError`` (or
``FileNotFoundError`` / ``ValueError`` for secrets) BEFORE any network,
database, Wikipedia, yfinance, FMP, or Discord I/O is performed — this
guarantees Requirement 6.4 / 11.12 (strict config validation).
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field, ValidationInfo, field_validator

_log = logging.getLogger(__name__)

__all__ = [
    "Layer1Config",
    "Layer2Config",
    "Layer3Config",
    "Layer4Config",
    "CacheConfig",
    "UniverseSourceConfig",
    "UniverseConfig",
    "NotificationConfig",
    "FmpConfig",
    "YFinanceConfig",
    "LoggingConfig",
    "AppConfig",
    "Secrets",
    "load_config",
]


# ---------------------------------------------------------------------------
# Per-layer configuration
# ---------------------------------------------------------------------------


class Layer1Config(BaseModel):
    """52-Week Low Anchor (George & Hwang, 2004).

    Candidate must satisfy ``pct_above_low_min <= pct_above_low <= pct_above_low_max``.
    """

    pct_above_low_min: float = Field(0.05, ge=0.0, le=1.0)
    pct_above_low_max: float = Field(0.15, ge=0.0, le=1.0)

    @field_validator("pct_above_low_max")
    @classmethod
    def _max_gt_min(cls, v: float, info: ValidationInfo) -> float:
        """Enforce Requirement 6.8: ``pct_above_low_max > pct_above_low_min``."""
        pct_min = info.data.get("pct_above_low_min", 0.0)
        if v <= pct_min:
            raise ValueError(
                "pct_above_low_max must be strictly greater than pct_above_low_min"
            )
        return v


class Layer2Config(BaseModel):
    """RSI capitulation crossover.

    Independent thresholds permit a buffered crossover
    (e.g., 28 -> 32) for stricter signals.
    """

    rsi_period: int = Field(14, ge=2, le=100)
    rsi_oversold: float = Field(30.0, ge=0.0, le=100.0)
    rsi_recovery: float = Field(30.0, ge=0.0, le=100.0)


class Layer3Config(BaseModel):
    """Fama-French Value (HML proxy): current P/E below 5-year average P/E."""

    require_positive_earnings: bool = True


class Layer4Config(BaseModel):
    """Fama-French Quality / QMJ: FCF yield floor."""

    fcf_yield_min: float = Field(0.045, ge=0.0, le=1.0)


# ---------------------------------------------------------------------------
# Infrastructure / operational configuration
# ---------------------------------------------------------------------------


class CacheConfig(BaseModel):
    """Cache TTL for ``fundamentals_cache`` rows (Requirement 6.9)."""

    fundamentals_staleness_days: int = Field(7, ge=1, le=90)


class UniverseSourceConfig(BaseModel):
    """One configured universe data source.

    Each source has a unique ``name``, a ``kind`` that selects the parser,
    a ``url`` to fetch, and optional per-source count bounds for sanity
    checking.
    """

    name: str
    kind: Literal["wikipedia_table", "etf_holdings_csv"]
    url: str
    enabled: bool = True
    min_count: int = Field(0, ge=0)
    max_count: int = Field(10000, ge=0)


class UniverseConfig(BaseModel):
    """Universe configuration with multi-source support.

    Supports both the legacy single-source ``source_url`` field and the
    new ``sources`` list. When ``sources`` is present, ``source_url`` is
    ignored (Req 1.7). When ``sources`` is absent, a single Wikipedia
    source is constructed from the legacy fields for backward
    compatibility (Req 8.1).
    """

    # Legacy fields — preserved for backward compatibility
    min_constituent_count: int = Field(450, ge=0)
    max_constituent_count: int = Field(520, ge=0)
    source_url: str = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

    # New multi-source field
    sources: list[UniverseSourceConfig] | None = None

    # Composite bounds (applied after set-union)
    min_composite_count: int = Field(450, ge=0)
    max_composite_count: int = Field(3200, ge=0)

    # Scan time monitoring
    max_scan_minutes: int = Field(25, ge=1)

    @field_validator("sources")
    @classmethod
    def _unique_source_names(
        cls, v: list[UniverseSourceConfig] | None,
    ) -> list[UniverseSourceConfig] | None:
        """Reject duplicate source names (Req 1.3)."""
        if v is None:
            return v
        names = [s.name for s in v]
        if len(names) != len(set(names)):
            raise ValueError("universe.sources names must be unique")
        return v

    def effective_sources(self) -> list[UniverseSourceConfig]:
        """Return the resolved source list.

        If ``sources`` is configured, return only the enabled entries
        (ignoring ``source_url``). Otherwise, construct a single-source
        fallback from the legacy fields so v1 configs produce identical
        behavior (Req 8.1).

        When both ``sources`` and ``source_url`` are specified, a WARN
        is logged (Req 1.7).
        """
        if self.sources is not None:
            # Req 1.7: warn if legacy source_url is also specified
            default_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
            if self.source_url != default_url:
                _log.warning(
                    "Both 'universe.sources' and 'universe.source_url' are "
                    "specified; 'source_url' will be ignored in favour of "
                    "'sources'"
                )
            return [s for s in self.sources if s.enabled]
        return [
            UniverseSourceConfig(
                name="sp500_wikipedia",
                kind="wikipedia_table",
                url=self.source_url,
                enabled=True,
                min_count=self.min_constituent_count,
                max_count=self.max_constituent_count,
            )
        ]


class NotificationConfig(BaseModel):
    """Discord webhook retry/backoff budget and embed username."""

    max_retries: int = Field(5, ge=0, le=20)
    backoff_initial_seconds: float = Field(1.0, ge=0.0)
    backoff_max_seconds: float = Field(60.0, ge=0.0)
    bot_username: str = "Asset Discovery Bot"


class FmpConfig(BaseModel):
    """Financial Modeling Prep client tuning."""

    base_url: str = "https://financialmodelingprep.com/api/v3"
    timeout_seconds: float = Field(10.0, ge=1.0)
    max_daily_calls_soft_cap: int = Field(200, ge=0)


class YFinanceConfig(BaseModel):
    """yfinance adapter batch tuning."""

    history_period: str = "1y"
    batch_size: int = Field(100, ge=1, le=500)
    retries_per_ticker: int = Field(3, ge=0, le=10)


class LoggingConfig(BaseModel):
    """Rotating-file logger settings."""

    level: str = "INFO"
    log_dir: Path = Path("/var/log/asset-discovery-bot")
    max_file_size_mb: int = Field(10, ge=1)
    backup_count: int = Field(7, ge=0)


# ---------------------------------------------------------------------------
# Root application config (immutable after load)
# ---------------------------------------------------------------------------


class AppConfig(BaseModel):
    """Root configuration. Immutable after load (Requirement 6.5)."""

    model_config = {"frozen": True}

    layer1: Layer1Config = Field(default_factory=Layer1Config)
    layer2: Layer2Config = Field(default_factory=Layer2Config)
    layer3: Layer3Config = Field(default_factory=Layer3Config)
    layer4: Layer4Config = Field(default_factory=Layer4Config)
    cache: CacheConfig = Field(default_factory=CacheConfig)
    universe: UniverseConfig = Field(default_factory=UniverseConfig)
    notification: NotificationConfig = Field(default_factory=NotificationConfig)
    fmp: FmpConfig = Field(default_factory=FmpConfig)
    yfinance: YFinanceConfig = Field(default_factory=YFinanceConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)

    def diff_from_defaults(self) -> dict[str, dict[str, Any]]:
        """Return non-default leaf values for the startup log line (Req 6.6).

        Structure is ``{section_name: {field_name: value, ...}, ...}``, with
        only leaves that differ from the Pydantic default included. Section
        dicts with zero differing leaves are omitted.
        """
        default_dump = AppConfig().model_dump()
        current_dump = self.model_dump()
        diff: dict[str, dict[str, Any]] = {}
        for section, current_section in current_dump.items():
            default_section = default_dump.get(section)
            if not isinstance(current_section, dict) or not isinstance(
                default_section, dict
            ):
                # Top-level scalar (none today, but future-proofing).
                if current_section != default_section:
                    diff[section] = current_section  # type: ignore[assignment]
                continue
            section_diff: dict[str, Any] = {}
            for key, value in current_section.items():
                if default_section.get(key) != value:
                    section_diff[key] = value
            if section_diff:
                diff[section] = section_diff
        return diff


# ---------------------------------------------------------------------------
# Secrets — separate model, loaded from /run/secrets/*
# ---------------------------------------------------------------------------


class Secrets(BaseModel):
    """Loaded from ``/run/secrets/*``. Never logged, never echoed.

    The ``__repr__`` / ``__str__`` overrides guarantee that accidental
    ``print(secrets)``, ``logger.info("cfg=%r", secrets)``, or traceback
    rendering never leaks credential material (Requirements 5.7, 6.6, 9.8).

    ``fmp_api_key`` is optional as of the EDGAR migration — the
    fundamentals adapter no longer needs an FMP key. The field remains
    so existing secret files keep working; a missing or empty
    ``fmp_api_key`` file no longer aborts startup.
    """

    model_config = {"frozen": True}

    db_url: str
    fmp_api_key: str = ""  # optional; unused by the EDGAR adapter
    discord_webhook_url: str

    def __repr__(self) -> str:
        # Never interpolate field values — only field names.
        redacted = ", ".join(f"{name}=***" for name in type(self).model_fields)
        return f"Secrets({redacted})"

    def __str__(self) -> str:
        return self.__repr__()


# Maps ``Secrets`` field name -> file name under ``secrets_dir``.
_SECRET_FILENAMES: dict[str, str] = {
    "db_url": "db_url",
    "fmp_api_key": "fmp_api_key",
    "discord_webhook_url": "discord_webhook_url",
}


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------


_ENV_PREFIX = "ADB_"


def _deep_merge(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge ``overlay`` into ``base``. Overlay wins on leaves."""
    out: dict[str, Any] = dict(base)
    for key, overlay_value in overlay.items():
        base_value = out.get(key)
        if isinstance(base_value, dict) and isinstance(overlay_value, dict):
            out[key] = _deep_merge(base_value, overlay_value)
        else:
            out[key] = overlay_value
    return out


def _env_overrides(env: dict[str, str]) -> dict[str, Any]:
    """Convert ``ADB_*`` env vars to a nested dict using ``__`` as delimiter.

    Example:
        ``ADB_LAYER1__PCT_ABOVE_LOW_MIN=0.03``
            -> ``{"layer1": {"pct_above_low_min": "0.03"}}``

    Values remain strings; Pydantic coerces them to the target field type.
    """
    result: dict[str, Any] = {}
    for name, value in env.items():
        if not name.startswith(_ENV_PREFIX):
            continue
        stripped = name[len(_ENV_PREFIX):]
        if not stripped:
            continue
        parts = [segment.lower() for segment in stripped.split("__") if segment]
        if not parts:
            continue
        cursor = result
        for segment in parts[:-1]:
            existing = cursor.get(segment)
            if not isinstance(existing, dict):
                existing = {}
                cursor[segment] = existing
            cursor = existing
        cursor[parts[-1]] = value
    return result


def _read_secret(
    secrets_dir: Path,
    field_name: str,
    file_name: str,
    required: bool = True,
) -> str:
    """Read one secret file. Empty/missing raises unless ``required`` is False."""
    path = secrets_dir / file_name
    if not path.is_file():
        if not required:
            return ""
        raise FileNotFoundError(
            f"Required secret file is missing: field={field_name} "
            f"path={path}"
        )
    content = path.read_text(encoding="utf-8").strip()
    if not content:
        if not required:
            return ""
        raise ValueError(
            f"Required secret file is empty: field={field_name} path={path}"
        )
    return content


def load_config(
    config_path: Path = Path("/app/config/config.yaml"),
    secrets_dir: Path = Path("/run/secrets"),
) -> tuple[AppConfig, Secrets]:
    """Load ``AppConfig`` and ``Secrets`` with strict validation.

    Behaviour:
        1. Load ``config_path`` as YAML (empty dict if the file is absent).
        2. Apply ``ADB_*`` environment-variable overrides (highest precedence).
        3. Validate via :class:`AppConfig` — ``ValidationError`` propagates.
        4. Read each secret from ``secrets_dir/<name>``; raise without echoing
           the value if a file is missing or empty.
        5. Validate via :class:`Secrets`.
        6. Return ``(app_config, secrets)``.

    The function must raise before performing any network, database,
    Wikipedia, yfinance, FMP, or Discord I/O (Requirement 6.4 / 11.12).
    Only local filesystem reads happen here.
    """
    # Step 1: YAML
    if config_path.is_file():
        with config_path.open("r", encoding="utf-8") as handle:
            loaded = yaml.safe_load(handle) or {}
        if not isinstance(loaded, dict):
            raise ValueError(
                f"Top-level of {config_path} must be a mapping, "
                f"got {type(loaded).__name__}"
            )
        yaml_dict: dict[str, Any] = loaded
    else:
        yaml_dict = {}

    # Step 2: env-var overrides
    merged = _deep_merge(yaml_dict, _env_overrides(dict(os.environ)))

    # Step 3: validate AppConfig (raises ValidationError on bad values)
    app_config = AppConfig.model_validate(merged)

    # Step 4 + 5: read + validate secrets
    # ``fmp_api_key`` is no longer strictly required (EDGAR migration);
    # read it if present so existing deployments keep working but don't
    # abort when the file is missing.
    _OPTIONAL_SECRETS = {"fmp_api_key"}
    secret_values: dict[str, str] = {
        field_name: _read_secret(
            secrets_dir,
            field_name,
            file_name,
            required=field_name not in _OPTIONAL_SECRETS,
        )
        for field_name, file_name in _SECRET_FILENAMES.items()
    }
    secrets = Secrets.model_validate(secret_values)

    return app_config, secrets
