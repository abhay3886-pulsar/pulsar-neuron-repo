from __future__ import annotations

"""Session/regime heuristics for deterministic intraday analysis."""

from typing import Dict, Tuple

import pandas as pd

from .levels import CPR


def atr_percent(df_1d: pd.DataFrame, n: int = 14) -> float:
    """Return ATR%% proxy computed as mean true range over ``n`` sessions."""

    if len(df_1d) < n + 1:
        return 0.0
    range_mean = (df_1d["high"] - df_1d["low"]).rolling(n, min_periods=n).mean().iloc[-1]
    close = float(df_1d["close"].iloc[-1])
    if close == 0:
        return 0.0
    return float((range_mean / close) * 100.0)


def initial_balance(df_5m: pd.DataFrame, window: Tuple[str, str] = ("09:15", "10:15")) -> Dict[str, float]:
    """Return Initial Balance stats (high/low/range) for the provided window."""

    if df_5m.empty:
        return {"ib_high": 0.0, "ib_low": 0.0, "ib_range": 0.0}
    hhmm = df_5m["ts"].dt.strftime("%H:%M")
    mask = hhmm.between(*window)
    if not mask.any():
        first = df_5m.iloc[0]
        high = float(first["high"])
        low = float(first["low"])
        return {"ib_high": high, "ib_low": low, "ib_range": high - low}
    win = df_5m.loc[mask]
    high = float(win["high"].max())
    low = float(win["low"].min())
    return {"ib_high": high, "ib_low": low, "ib_range": high - low}


def cpr_width(cpr: CPR, pdc: float, narrow_thresh_pct: float = 0.3) -> Dict[str, float | bool]:
    """Return CPR width percentage and a flag for narrow ranges."""

    if pdc == 0:
        return {"width_pct": 0.0, "narrow": False}
    width_pct = float(abs(cpr.tc - cpr.bc) / pdc * 100.0)
    return {"width_pct": width_pct, "narrow": width_pct <= narrow_thresh_pct}
