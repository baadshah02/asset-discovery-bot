"""Ad-hoc validation for bot/filters.py purity + monotonicity.

Stubs out the heavy deps (bot.fundamentals, bot.prices, pydantic-based
bot.config) so we can exercise apply_layer_1..4 against a small fixed
DataFrame and check:

    1. Each layer is pure (input DataFrame is unchanged after call).
    2. L4 ⊆ L3 ⊆ L2 ⊆ L1 for a seeded input where we know the answer.
"""
from __future__ import annotations

import sys
import types
from dataclasses import dataclass
from pathlib import Path

# Make ``import bot.filters`` work.
sys.path.insert(0, str(Path.cwd()))

# ---- Stub bot.config -------------------------------------------------------
config_mod = types.ModuleType("bot.config")


@dataclass(frozen=True)
class Layer1Config:
    pct_above_low_min: float = 0.05
    pct_above_low_max: float = 0.15


@dataclass(frozen=True)
class Layer2Config:
    rsi_period: int = 14
    rsi_oversold: float = 30.0
    rsi_recovery: float = 30.0


@dataclass(frozen=True)
class Layer3Config:
    require_positive_earnings: bool = True


@dataclass(frozen=True)
class Layer4Config:
    fcf_yield_min: float = 0.045


@dataclass(frozen=True)
class YFinanceConfig:
    history_period: str = "1y"
    batch_size: int = 100
    retries_per_ticker: int = 3


@dataclass(frozen=True)
class AppConfig:
    layer1: Layer1Config = Layer1Config()
    layer2: Layer2Config = Layer2Config()
    layer3: Layer3Config = Layer3Config()
    layer4: Layer4Config = Layer4Config()
    yfinance: YFinanceConfig = YFinanceConfig()


config_mod.Layer1Config = Layer1Config
config_mod.Layer2Config = Layer2Config
config_mod.Layer3Config = Layer3Config
config_mod.Layer4Config = Layer4Config
config_mod.YFinanceConfig = YFinanceConfig
config_mod.AppConfig = AppConfig
sys.modules["bot.config"] = config_mod

# ---- Stub bot.fundamentals -------------------------------------------------
fundamentals_mod = types.ModuleType("bot.fundamentals")


class FmpBudgetExhausted(Exception):
    pass


@dataclass(frozen=True)
class Fundamentals:
    ticker: str
    pe_ratio: float | None
    pe_5y_avg: float | None
    fcf_yield: float | None
    latest_headline: str | None
    headline_url: str | None
    fetched_at: object = None


fundamentals_mod.FmpBudgetExhausted = FmpBudgetExhausted
fundamentals_mod.Fundamentals = Fundamentals
sys.modules["bot.fundamentals"] = fundamentals_mod

# ---- Stub bot.prices -------------------------------------------------------
prices_mod = types.ModuleType("bot.prices")


def compute_technical_snapshot(frames, rsi_period):
    raise NotImplementedError


prices_mod.compute_technical_snapshot = compute_technical_snapshot
sys.modules["bot.prices"] = prices_mod

# Register the bot package (prevents real bot/__init__.py from pulling in
# its real submodules).
bot_pkg = types.ModuleType("bot")
bot_pkg.__path__ = [str((Path.cwd() / "bot").resolve())]
sys.modules["bot"] = bot_pkg

# ---- Now import the module under test --------------------------------------
import importlib.util

spec = importlib.util.spec_from_file_location("bot.filters", "bot/filters.py")
filters = importlib.util.module_from_spec(spec)
sys.modules["bot.filters"] = filters
spec.loader.exec_module(filters)

# ---- Seed a small DataFrame ------------------------------------------------
import pandas as pd

rows = [
    dict(
        ticker="PASS_ALL",
        close=100.0, low_52w=90.0,
        pct_above_low=0.10,
        rsi_today=32.0, rsi_yesterday=28.0,
        pe_ratio=18.0, pe_5y_avg=22.0, fcf_yield=0.06,
        latest_headline="headline", headline_url="https://example.com",
    ),
    dict(
        ticker="FAIL_L1_TOO_FAR",
        close=150.0, low_52w=90.0, pct_above_low=0.66,
        rsi_today=32.0, rsi_yesterday=28.0,
        pe_ratio=18.0, pe_5y_avg=22.0, fcf_yield=0.06,
        latest_headline=None, headline_url=None,
    ),
    dict(
        ticker="FAIL_L2_NO_CROSS",
        close=100.0, low_52w=90.0, pct_above_low=0.10,
        rsi_today=40.0, rsi_yesterday=45.0,
        pe_ratio=18.0, pe_5y_avg=22.0, fcf_yield=0.06,
        latest_headline=None, headline_url=None,
    ),
    dict(
        ticker="FAIL_L3_EXPENSIVE",
        close=100.0, low_52w=90.0, pct_above_low=0.10,
        rsi_today=32.0, rsi_yesterday=28.0,
        pe_ratio=30.0, pe_5y_avg=22.0, fcf_yield=0.06,
        latest_headline=None, headline_url=None,
    ),
    dict(
        ticker="FAIL_L4_LOW_FCF",
        close=100.0, low_52w=90.0, pct_above_low=0.10,
        rsi_today=32.0, rsi_yesterday=28.0,
        pe_ratio=18.0, pe_5y_avg=22.0, fcf_yield=0.03,
        latest_headline=None, headline_url=None,
    ),
    dict(
        ticker="FAIL_L3_NULL_PE",
        close=100.0, low_52w=90.0, pct_above_low=0.10,
        rsi_today=32.0, rsi_yesterday=28.0,
        pe_ratio=float("nan"), pe_5y_avg=22.0, fcf_yield=0.06,
        latest_headline=None, headline_url=None,
    ),
    dict(
        ticker="FAIL_L3_NEG_EARNINGS",
        close=100.0, low_52w=90.0, pct_above_low=0.10,
        rsi_today=32.0, rsi_yesterday=28.0,
        pe_ratio=-5.0, pe_5y_avg=-10.0,  # pe < pe_5y but both negative
        fcf_yield=0.06,
        latest_headline=None, headline_url=None,
    ),
]
df = pd.DataFrame(rows)
original = df.copy(deep=True)

# ---- 1. Purity check ------------------------------------------------------
l1 = filters.apply_layer_1(df, Layer1Config())
assert df.equals(original), "apply_layer_1 mutated input!"

l1_orig = l1.copy(deep=True)
l2 = filters.apply_layer_2(l1, Layer2Config())
assert l1.equals(l1_orig), "apply_layer_2 mutated its input!"

l2_orig = l2.copy(deep=True)
l3 = filters.apply_layer_3(l2, Layer3Config())
assert l2.equals(l2_orig), "apply_layer_3 mutated its input!"

l3_orig = l3.copy(deep=True)
l4 = filters.apply_layer_4(l3, Layer4Config())
assert l3.equals(l3_orig), "apply_layer_4 mutated its input!"

print("Purity: OK — no layer mutated its input.")

# ---- 2. Monotonicity L4 ⊆ L3 ⊆ L2 ⊆ L1 ------------------------------------
t_l1 = set(l1["ticker"])
t_l2 = set(l2["ticker"])
t_l3 = set(l3["ticker"])
t_l4 = set(l4["ticker"])

assert t_l2 <= t_l1, f"L2 not subset of L1: {t_l2 - t_l1}"
assert t_l3 <= t_l2, f"L3 not subset of L2: {t_l3 - t_l2}"
assert t_l4 <= t_l3, f"L4 not subset of L3: {t_l4 - t_l3}"
print(f"Monotonicity: OK — |L1|={len(t_l1)} |L2|={len(t_l2)} |L3|={len(t_l3)} |L4|={len(t_l4)}")
print(f"  L1: {sorted(t_l1)}")
print(f"  L2: {sorted(t_l2)}")
print(f"  L3: {sorted(t_l3)}")
print(f"  L4: {sorted(t_l4)}")

# ---- 3. Happy-path ---------------------------------------------------------
assert "PASS_ALL" in t_l4, "PASS_ALL should have survived all four layers"
assert "FAIL_L3_NULL_PE" not in t_l3, "null P/E should be excluded from L3"
assert "FAIL_L3_NEG_EARNINGS" not in t_l3, "negative earnings should be excluded when require_positive_earnings=True"
print("Happy path: OK.")

# ---- 4. require_positive_earnings=False allows negative pe < pe_5y --------
l3_permissive = filters.apply_layer_3(l2, Layer3Config(require_positive_earnings=False))
# Not asserting — just confirming the alternate branch runs.
print(f"L3 with require_positive_earnings=False: {sorted(set(l3_permissive['ticker']))}")

# ---- 5. Empty-input safety ------------------------------------------------
empty = pd.DataFrame(columns=df.columns)
e1 = filters.apply_layer_1(empty, Layer1Config())
e2 = filters.apply_layer_2(e1, Layer2Config())
e3 = filters.apply_layer_3(e2, Layer3Config())
e4 = filters.apply_layer_4(e3, Layer4Config())
assert all(frame.empty for frame in (e1, e2, e3, e4))
print("Empty-input safety: OK.")

# ---- 6. ScanCandidate materialisation -------------------------------------
cand = filters.row_to_candidate(l4.iloc[0].to_dict())
assert isinstance(cand, filters.ScanCandidate)
assert cand.ticker == "PASS_ALL"
assert cand.close == 100.0
assert cand.fcf_yield == 0.06
assert cand.latest_headline == "headline"
assert cand.headline_url == "https://example.com"
print(f"ScanCandidate: OK — {cand}")

# ---- 7. run_pipeline end-to-end with fakes --------------------------------
# Fake price_fetcher returns frames with enough data for compute_technical_snapshot,
# but we stubbed compute_technical_snapshot. Monkey-patch it to return our df[l1-style].
def fake_compute_technical_snapshot(frames, rsi_period):
    # Return the technical columns (ticker..rsi_yesterday) for our seed rows.
    cols = ["ticker", "close", "low_52w", "pct_above_low", "rsi_today", "rsi_yesterday"]
    return df[cols].copy()


filters.compute_technical_snapshot = fake_compute_technical_snapshot


def fake_price_fetcher(tickers, ycfg):
    # Shape only matters for the stubbed compute_technical_snapshot; return something.
    return {t: pd.DataFrame() for t in tickers}


fundamentals_lookup = {
    "PASS_ALL": Fundamentals("PASS_ALL", 18.0, 22.0, 0.06, "headline", "https://example.com"),
    "FAIL_L3_EXPENSIVE": Fundamentals("FAIL_L3_EXPENSIVE", 30.0, 22.0, 0.06, None, None),
    "FAIL_L4_LOW_FCF": Fundamentals("FAIL_L4_LOW_FCF", 18.0, 22.0, 0.03, None, None),
    # Populated to complete the L2 survivor set. L3 should reject these.
    "FAIL_L3_NULL_PE": Fundamentals("FAIL_L3_NULL_PE", None, 22.0, 0.06, None, None),
    "FAIL_L3_NEG_EARNINGS": Fundamentals("FAIL_L3_NEG_EARNINGS", -5.0, -10.0, 0.06, None, None),
}


def fake_fundamentals_fetcher(ticker: str) -> Fundamentals:
    return fundamentals_lookup[ticker]


candidates = filters.run_pipeline(
    universe=[r["ticker"] for r in rows],
    price_fetcher=fake_price_fetcher,
    fundamentals_fetcher=fake_fundamentals_fetcher,
    cfg=AppConfig(),
)
print(f"run_pipeline: emitted {len(candidates)} candidate(s): {[c.ticker for c in candidates]}")
assert len(candidates) == 1 and candidates[0].ticker == "PASS_ALL"
print("run_pipeline: OK — only PASS_ALL emerged as a High_Conviction_Candidate.")

# ---- 8. FmpBudgetExhausted on a non-critical ticker is skipped -----------
def budget_blown_for_one(ticker: str) -> Fundamentals:
    if ticker == "PASS_ALL":
        raise FmpBudgetExhausted("budget blown for PASS_ALL")
    return fundamentals_lookup[ticker]


candidates_degraded = filters.run_pipeline(
    universe=[r["ticker"] for r in rows],
    price_fetcher=fake_price_fetcher,
    fundamentals_fetcher=budget_blown_for_one,
    cfg=AppConfig(),
)
assert len(candidates_degraded) == 0, f"expected 0 after skipping PASS_ALL, got {candidates_degraded}"
print("FmpBudgetExhausted path: OK — skipped ticker; run continued; no candidates emitted.")

print("\nAll validation checks passed.")
