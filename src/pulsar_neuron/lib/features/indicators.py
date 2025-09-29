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


def ema(series: pd.Series, span: int) -> pd.Series:
    """Return the exponential moving average using a smoothing ``span``.

    The function requires ``span`` strictly greater than zero and propagates
    ``NaN`` values until enough observations are available.
    """

    if span <= 0:
        raise ValueError("span must be a positive integer")
    values = [float(v) for v in series]
    length = len(values)
    ema_values: list[float | None] = [None] * length
    if length < span:
        return pd.Series(ema_values, name=getattr(series, "name", None))

    alpha = 2.0 / (span + 1.0)
    initial = sum(values[:span]) / span
    ema_values[span - 1] = float(initial)
    prev = initial
    for idx in range(span, length):
        prev = (values[idx] - prev) * alpha + prev
        ema_values[idx] = float(prev)

    return pd.Series(ema_values, name=getattr(series, "name", None))


def rsi(series: pd.Series, n: int = 14) -> pd.Series:
    """Return the Relative Strength Index computed with Wilder smoothing."""

    if n <= 0:
        raise ValueError("n must be a positive integer")

    values = [float(v) for v in series]
    length = len(values)
    rsi_values: list[float | None] = [None] * length

    if length <= n:
        return pd.Series(rsi_values, name=getattr(series, "name", None))

    gains = [0.0] * length
    losses = [0.0] * length
    for idx in range(1, length):
        change = values[idx] - values[idx - 1]
        if change > 0:
            gains[idx] = change
        elif change < 0:
            losses[idx] = -change

    initial_gain = sum(gains[1 : n + 1]) / n
    initial_loss = sum(losses[1 : n + 1]) / n
    avg_gain = initial_gain
    avg_loss = initial_loss

    rsi_values[n] = _rsi_from_averages(avg_gain, avg_loss)
    for idx in range(n + 1, length):
        avg_gain = ((avg_gain * (n - 1)) + gains[idx]) / n
        avg_loss = ((avg_loss * (n - 1)) + losses[idx]) / n
        rsi_values[idx] = _rsi_from_averages(avg_gain, avg_loss)

    return pd.Series(rsi_values, name=getattr(series, "name", None))


def _rsi_from_averages(avg_gain: float, avg_loss: float, tol: float = 1e-12) -> float:
    if abs(avg_loss) <= tol and abs(avg_gain) <= tol:
        return 50.0
    if abs(avg_loss) <= tol:
        return 100.0
    if abs(avg_gain) <= tol:
        return 0.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def true_range(df: pd.DataFrame) -> pd.Series:
    """Return the True Range series for a price dataframe."""

    highs = [float(v) for v in df["high"]]
    lows = [float(v) for v in df["low"]]
    closes = [float(v) for v in df["close"]]

    tr_values: list[float] = []
    for idx, (high, low) in enumerate(zip(highs, lows)):
        range_hl = high - low
        if idx == 0:
            tr_values.append(float(range_hl))
            continue
        prev_close = closes[idx - 1]
        range_high = abs(high - prev_close)
        range_low = abs(low - prev_close)
        tr_values.append(float(max(range_hl, range_high, range_low)))

    return pd.Series(tr_values, name="true_range")


def average_true_range(df: pd.DataFrame, n: int) -> pd.Series:
    """Return the rolling Average True Range over a window of ``n`` bars."""

    if n <= 0:
        raise ValueError("n must be a positive integer")
    tr = true_range(df)
    return tr.rolling(window=n, min_periods=n).mean()

