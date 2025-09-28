from __future__ import annotations

import pandas as pd


def sma(series: pd.Series, n: int) -> pd.Series:
    """
    Simple moving average with fixed window n.
    Returns a series aligned to input index.
    """

    return series.rolling(n, min_periods=n).mean()


def sma_slope(series: pd.Series, n: int) -> float:
    """
    One-step slope: SMA(n)[-1] - SMA(n)[-2].
    Returns 0.0 if not enough data.
    """

    s = sma(series, n).dropna()
    if len(s) < 2:
        return 0.0
    return float(s.iloc[-1] - s.iloc[-2])


def volume_ratio(vol_series: pd.Series, n: int = 20) -> float:
    """
    Current bar volume vs median of the previous n bars.
    Fallbacks to 1.0 if not enough history or zero median.
    """

    if len(vol_series) < n + 1:
        return 1.0
    median_prev = float(vol_series.iloc[-(n + 1) : -1].median())
    if median_prev == 0:
        return 1.0
    return float(vol_series.iloc[-1] / median_prev)


def vwap_from_bars(df: pd.DataFrame) -> float:
    """
    Session VWAP from bars with columns: high, low, close, volume.
    Returns 0.0 if volume sum is zero or df empty.
    """

    if df.empty:
        return 0.0
    typical = (df["high"] + df["low"] + df["close"]) / 3.0
    num = (typical * df["volume"]).sum()
    den = df["volume"].sum()
    return float(num / den) if den else 0.0
