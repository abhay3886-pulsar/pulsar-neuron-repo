from __future__ import annotations

"""Deterministic indicator helpers for intraday index analysis."""

import pandas as pd


def sma(series: pd.Series, n: int) -> pd.Series:
    """Return the simple moving average over a fixed window ``n``.

    The calculation requires ``n`` observations; values before that are NaN.
    """

    return series.rolling(n, min_periods=n).mean()


def sma_slope(series: pd.Series, n: int) -> float:
    """Return the discrete slope of the SMA over the most recent step."""

    s = sma(series, n).dropna()
    if len(s) < 2:
        return 0.0
    return float(s.iloc[-1] - s.iloc[-2])


def volume_ratio(vol_series: pd.Series, n: int = 20) -> float:
    """Compare the latest volume against the median of the previous ``n`` bars."""

    if len(vol_series) < n + 1:
        return 1.0
    median_prev = float(vol_series.iloc[-(n + 1) : -1].median())
    if median_prev == 0:
        return 1.0
    return float(vol_series.iloc[-1] / median_prev)


def vwap_from_bars(df: pd.DataFrame) -> float:
    """Compute VWAP from bars with ``high``, ``low``, ``close``, ``volume`` columns."""

    if df.empty:
        return 0.0
    typical_price = (df["high"] + df["low"] + df["close"]) / 3.0
    numerator = (typical_price * df["volume"]).sum()
    denominator = df["volume"].sum()
    return float(numerator / denominator) if denominator else 0.0


def distance_from_vwap_pct(price: float, vwap: float) -> float:
    """Return the percentage distance between ``price`` and ``vwap``.

    If ``vwap`` is zero the distance is reported as ``0.0`` to avoid division
    by zero and keep the indicator bounded.
    """

    if vwap == 0:
        return 0.0
    return ((price - vwap) / vwap) * 100.0

