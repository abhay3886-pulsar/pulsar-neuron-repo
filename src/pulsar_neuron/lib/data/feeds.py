from __future__ import annotations

from datetime import datetime, timedelta
import math
from typing import Iterable, Literal

import pandas as pd

Timeframe = Literal["1d", "15m", "5m"]

_FREQ_DELTA: dict[Timeframe, timedelta] = {
    "1d": timedelta(days=1),
    "15m": timedelta(minutes=15),
    "5m": timedelta(minutes=5),
}
_DEFAULT_END: dict[Timeframe, datetime] = {
    "1d": datetime(2025, 1, 31),
    "15m": datetime(2025, 1, 31, 15, 30),
    "5m": datetime(2025, 1, 31, 15, 30),
}


def _symbol_seed(symbol: str, tf: Timeframe) -> int:
    """Create a deterministic integer seed derived from ``symbol`` and ``tf``."""

    payload = f"{symbol}|{tf}".encode("utf-8")
    return sum(payload) % 10_000


def _generate_bars(index: Iterable[datetime], symbol: str, tf: Timeframe) -> pd.DataFrame:
    """Return a deterministic OHLCV dataframe indexed by ``index``."""

    index_list = list(index)
    if not index_list:
        return pd.DataFrame([])

    seed = _symbol_seed(symbol, tf)
    base_price = 50.0 + (seed % 500) / 5.0
    amplitude = 1.0 + (seed % 17) / 10.0
    count = len(index_list)
    close = []
    open_ = []
    high = []
    low = []
    volume = []
    slope = 0.05 + (seed % 7) * 0.01
    wick = 0.35 + amplitude * 0.05
    base_volume = 1_000 + (seed % 250) + amplitude * 25.0
    for i in range(count):
        trend = i * slope
        wave = math.sin(i / 3.0 + seed / 13.0) * amplitude
        close_i = base_price + trend + wave
        open_i = close_i - math.cos(i / 4.0 + seed / 11.0) * 0.2
        high_i = max(open_i, close_i) + wick
        low_i = min(open_i, close_i) - wick
        volume_i = base_volume + (i % 12) * 40.0
        close.append(close_i)
        open_.append(open_i)
        high.append(high_i)
        low.append(low_i)
        volume.append(volume_i)

    rows = []
    for ts, o, h, l, c, v in zip(index_list, open_, high, low, close, volume):
        rows.append({"ts": ts, "open": o, "high": h, "low": l, "close": c, "volume": v})
    return pd.DataFrame(rows)


def get_ohlcv(symbol: str, tf: Timeframe, start: datetime | str, end: datetime | str) -> pd.DataFrame:
    """Return synthetic OHLCV bars for ``symbol`` between ``start`` and ``end``.

    The data is generated deterministically from the inputs so repeated calls with
    identical arguments return the exact same dataframe.  Columns follow the
    standard ``ts/open/high/low/close/volume`` schema used across the project.
    """

    start_ts = start if isinstance(start, datetime) else datetime.fromisoformat(str(start))
    end_ts = end if isinstance(end, datetime) else datetime.fromisoformat(str(end))
    if end_ts < start_ts:
        raise ValueError("end must be greater than or equal to start")
    step = _FREQ_DELTA[tf]
    current = start_ts
    stamps = []
    while current <= end_ts:
        stamps.append(current)
        current += step
    return _generate_bars(stamps, symbol, tf)


def get_last_n(symbol: str, tf: Timeframe, n: int) -> pd.DataFrame:
    """Return the last ``n`` synthetic bars for ``symbol`` at timeframe ``tf``.

    The anchor ``end`` timestamp is deterministic per timeframe to maintain
    repeatability across test runs.
    """

    if n < 0:
        raise ValueError("n must be non-negative")
    if n == 0:
        return pd.DataFrame([])
    end_ts = _DEFAULT_END[tf]
    step = _FREQ_DELTA[tf]
    start_ts = end_ts - step * (n - 1)
    return get_ohlcv(symbol, tf, start_ts, end_ts)


def get_live_quote(symbol: str) -> dict[str, float]:
    """Return a deterministic synthetic live quote snapshot for ``symbol``."""

    latest = get_last_n(symbol, "5m", 1)
    if latest.empty:
        return {"ltp": 0.0, "bid": 0.0, "ask": 0.0, "vwap": 0.0}
    last_row = latest.iloc[-1]
    last_close = float(last_row["close"])
    vwap_like = (last_row["high"] + last_row["low"] + last_row["close"]) / 3.0
    spread = 0.05
    return {
        "ltp": last_close,
        "bid": last_close - spread,
        "ask": last_close + spread,
        "vwap": float(vwap_like),
    }
