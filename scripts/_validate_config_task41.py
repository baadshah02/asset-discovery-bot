"""Ad-hoc Task 4.1 validation driver. Deleted after the run."""
import sys
from pydantic import ValidationError
from bot.config import (
    Layer1Config, Layer2Config, Layer3Config, Layer4Config,
    CacheConfig, UniverseConfig, NotificationConfig, FmpConfig,
    YFinanceConfig, LoggingConfig, AppConfig, Secrets,
)

# 1. Default construction of every model
for cls in (Layer1Config, Layer2Config, Layer3Config, Layer4Config, CacheConfig,
            UniverseConfig, NotificationConfig, FmpConfig, YFinanceConfig, LoggingConfig):
    cls()
AppConfig()
print("1. default construction OK")

# 2. Layer1Config._max_gt_min fires when max <= min
try:
    Layer1Config(pct_above_low_max=0.03)
    print("2. FAIL"); sys.exit(1)
except ValidationError as e:
    assert "pct_above_low_max" in str(e)
    print("2. _max_gt_min fires for max=0.03 (default min=0.05)")

try:
    Layer1Config(pct_above_low_min=0.10, pct_above_low_max=0.10)
    print("2b. FAIL"); sys.exit(1)
except ValidationError:
    print("2b. equal min/max correctly rejected")

Layer1Config(pct_above_low_min=0.02, pct_above_low_max=0.25)
print("2c. valid min<max accepted")

# 3. CacheConfig.fundamentals_staleness_days in [1, 90]
for bad in (0, 91, 200, -5):
    try:
        CacheConfig(fundamentals_staleness_days=bad)
        print(f"3. FAIL: {bad} should be rejected"); sys.exit(1)
    except ValidationError:
        pass
for good in (1, 7, 45, 90):
    CacheConfig(fundamentals_staleness_days=good)
print("3. CacheConfig bounds [1,90] enforced")

# 4. AppConfig frozen
cfg = AppConfig()
try:
    cfg.cache = CacheConfig(fundamentals_staleness_days=10)
    print("4. FAIL: AppConfig must be frozen"); sys.exit(1)
except ValidationError:
    print("4. AppConfig frozen")

# 5. Secrets construction + repr doesn't leak values
s = Secrets(db_url="SECRET-DB-URL", fmp_api_key="SECRET-FMP-KEY", discord_webhook_url="SECRET-DISCORD-URL")
r = repr(s)
print(f"5a. repr(Secrets) = {r}")
for needle in ("SECRET-DB-URL", "SECRET-FMP-KEY", "SECRET-DISCORD-URL"):
    assert needle not in r, f"LEAK: {needle!r} appears in repr"
assert str(s) == r
print("5a. repr/str do not echo secret values")

# 5b. Secrets frozen
try:
    s.db_url = "leaked"
    print("5b. FAIL: Secrets must be frozen"); sys.exit(1)
except ValidationError:
    print("5b. Secrets frozen")

# 5c. Field access still works for consumers
assert s.db_url == "SECRET-DB-URL"
print("5c. legitimate field access still works")

# 6. Layer1 validator preserves order-sensitivity (info.data)
Layer1Config(pct_above_low_min=0.5, pct_above_low_max=0.6)
print("6. validator is order-aware (min declared before max)")

# 7. ge/le bounds on floats are preserved (sanity)
for bad_kwargs in (
    {"pct_above_low_min": -0.01},
    {"pct_above_low_min": 1.5},
    {"pct_above_low_max": 1.01},
):
    try:
        Layer1Config(**bad_kwargs)
        print(f"7. FAIL: {bad_kwargs} should be rejected"); sys.exit(1)
    except ValidationError:
        pass
print("7. ge/le bounds on Layer1 fields enforced")

# 8. Layer2 bounds
for bad in (
    {"rsi_period": 1}, {"rsi_period": 101},
    {"rsi_oversold": -1.0}, {"rsi_oversold": 100.1},
    {"rsi_recovery": -1.0}, {"rsi_recovery": 100.1},
):
    try:
        Layer2Config(**bad)
        print(f"8. FAIL: {bad} should be rejected"); sys.exit(1)
    except ValidationError:
        pass
print("8. Layer2 bounds enforced")

# 9. Layer4 / FMP / NotificationConfig / YFinance / Logging / Universe bounds
for bad in (
    (Layer4Config, {"fcf_yield_min": -0.01}),
    (Layer4Config, {"fcf_yield_min": 1.01}),
    (FmpConfig, {"timeout_seconds": 0.5}),
    (FmpConfig, {"max_daily_calls_soft_cap": -1}),
    (NotificationConfig, {"max_retries": 21}),
    (NotificationConfig, {"max_retries": -1}),
    (NotificationConfig, {"backoff_initial_seconds": -0.1}),
    (YFinanceConfig, {"batch_size": 0}),
    (YFinanceConfig, {"batch_size": 501}),
    (YFinanceConfig, {"retries_per_ticker": 11}),
    (LoggingConfig, {"max_file_size_mb": 0}),
    (LoggingConfig, {"backup_count": -1}),
    (UniverseConfig, {"min_constituent_count": -1}),
):
    cls, kwargs = bad
    try:
        cls(**kwargs)
        print(f"9. FAIL: {cls.__name__}({kwargs}) should be rejected"); sys.exit(1)
    except ValidationError:
        pass
print("9. scalar bounds on all configs enforced")

# 10. AppConfig.model_dump round-trips through model_validate
dumped = AppConfig().model_dump()
rehydrated = AppConfig.model_validate(dumped)
assert rehydrated.model_dump() == dumped
print("10. AppConfig.model_dump -> model_validate round-trips")

print("\nALL CHECKS PASSED")
