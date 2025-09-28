from __future__ import annotations

import pandas as pd


def sma(series: pd.Series, n: int) -> pd.Series:
    return series.rolling(n, min_periods=n).mean()


def sma_slope(series: pd.Series, n: int) -> float:
    s = sma(series, n).dropna()
    if len(s) < 2:
        return 0.0
    return float(s.iloc[-1] - s.iloc[-2])


def volume_ratio(vol_series: pd.Series, n: int = 20) -> float:
    """Current vol vs median of previous n bars."""

    if len(vol_series) < n + 1:
        return 1.0
    med = float(vol_series.iloc[-(n + 1) : -1].median())
    return 1.0 if med == 0 else float(vol_series.iloc[-1] / med)


def vwap_from_bars(df: pd.DataFrame) -> float:
    """Compute session VWAP from bars with columns [high,low,close,volume]."""

    if df.empty:
        return 0.0
    typical = (df["high"] + df["low"] + df["close"]) / 3.0
    num = (typical * df["volume"]).sum()
    den = df["volume"].sum()
    return float(num / den) if den else 0.0
