"""Temporary smoke test for bot.config. Not part of the shipped package.

Delete after verification. This script exercises the AppConfig / Secrets /
load_config surface described in design.md Component 6 and exits non-zero
on any regression.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

# Ensure repo root is importable regardless of CWD.
_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from pydantic import ValidationError  # noqa: E402

from bot.config import (  # noqa: E402
    AppConfig,
    CacheConfig,
    Layer1Config,
    Layer4Config,
    Secrets,
    load_config,
)


def run() -> None:
    # T1: defaults
    cfg = AppConfig()
    assert cfg.layer1.pct_above_low_min == 0.05
    assert cfg.layer4.fcf_yield_min == 0.045
    print("T1 PASS: AppConfig() constructs with defaults")

    # T2: Layer1 validator — max <= min
    try:
        Layer1Config(pct_above_low_min=0.2, pct_above_low_max=0.1)
    except ValidationError as exc:
        assert "pct_above_low_max" in str(exc)
        print("T2 PASS: Layer1 rejects max <= min")
    else:
        raise AssertionError("T2 FAIL")

    # T3: CacheConfig bound
    try:
        CacheConfig(fundamentals_staleness_days=200)
    except ValidationError:
        print("T3 PASS: CacheConfig rejects staleness_days=200")
    else:
        raise AssertionError("T3 FAIL")

    # T4: frozen AppConfig
    try:
        cfg.layer1 = Layer1Config()  # type: ignore[misc]
    except ValidationError:
        print("T4 PASS: AppConfig frozen (ValidationError)")
    except Exception as exc:
        print(f"T4 PASS: AppConfig frozen ({type(exc).__name__})")
    else:
        raise AssertionError("T4 FAIL")

    # T5: diff on default
    assert cfg.diff_from_defaults() == {}
    print("T5 PASS: default.diff_from_defaults() == {}")

    # T6: leaf-level diff
    custom = AppConfig(layer4=Layer4Config(fcf_yield_min=0.05))
    diff = custom.diff_from_defaults()
    assert diff == {"layer4": {"fcf_yield_min": 0.05}}, diff
    print("T6 PASS: diff_from_defaults detects leaf change")

    # T7-T11: load_config
    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)
        yaml_path = td_path / "config.yaml"
        yaml_path.write_text("layer1:\n  pct_above_low_min: 0.03\n")
        secrets_dir = td_path / "secrets"
        secrets_dir.mkdir()
        (secrets_dir / "db_url").write_text("postgresql://u:p@localhost/db\n")
        (secrets_dir / "fmp_api_key").write_text("fake-key-123\n")
        (secrets_dir / "discord_webhook_url").write_text(
            "https://discord.test/webhook/xyz\n"
        )

        # Strip any stray ADB_* env
        for k in list(os.environ):
            if k.startswith("ADB_"):
                del os.environ[k]

        cfg2, secrets = load_config(yaml_path, secrets_dir)
        assert cfg2.layer1.pct_above_low_min == 0.03
        assert cfg2.layer1.pct_above_low_max == 0.15
        assert isinstance(secrets, Secrets)
        assert secrets.db_url.startswith("postgresql://")
        assert secrets.fmp_api_key == "fake-key-123"
        assert secrets.discord_webhook_url.endswith("/webhook/xyz")
        print("T7 PASS: YAML override applied")
        print("T8 PASS: secrets loaded from files")

        os.environ["ADB_LAYER1__PCT_ABOVE_LOW_MIN"] = "0.07"
        try:
            cfg3, _ = load_config(yaml_path, secrets_dir)
            assert cfg3.layer1.pct_above_low_min == 0.07, cfg3.layer1
            print("T9 PASS: env var wins over YAML")
        finally:
            del os.environ["ADB_LAYER1__PCT_ABOVE_LOW_MIN"]

        # Missing secret file
        (secrets_dir / "fmp_api_key").unlink()
        try:
            load_config(yaml_path, secrets_dir)
        except FileNotFoundError as exc:
            assert "fmp_api_key" in str(exc)
            print("T10 PASS: missing secret -> FileNotFoundError")
        else:
            raise AssertionError("T10 FAIL")

        # Empty secret file
        (secrets_dir / "fmp_api_key").write_text("   \n")
        try:
            load_config(yaml_path, secrets_dir)
        except ValueError as exc:
            assert "fmp_api_key" in str(exc)
            print("T11 PASS: empty secret -> ValueError")
        else:
            raise AssertionError("T11 FAIL")


if __name__ == "__main__":
    run()
    print("\nAll smoke tests passed.")
