from __future__ import annotations

import numpy as np
import pandas as pd


def sma(series: pd.Series, n: int) -> pd.Series:
    return series.rolling(n, min_periods=n).mean()


def sma_slope(series: pd.Series, n: int) -> float:
    s = sma(series, n).dropna()
    if len(s) < 2:
        return 0.0
    return float(s.iloc[-1] - s.iloc[-2])


def vwap_distance(last_price: float, vwap: float) -> float:
    if not vwap:
        return 0.0
    return (last_price - vwap) / vwap * 100.0


def volume_ratio(vol_series: pd.Series, n: int = 20) -> float:
    if len(vol_series) < n + 1:
        return 1.0
    med = float(vol_series.iloc[-(n + 1) : -1].median())
    if med == 0:
        return 1.0
    return float(vol_series.iloc[-1] / med)


def swing_points(df: pd.DataFrame, lookback: int = 20) -> tuple[float | None, float | None]:
    hi = df["high"].iloc[-lookback:].max() if len(df) >= lookback else None
    lo = df["low"].iloc[-lookback:].min() if len(df) >= lookback else None
    return hi, lo
