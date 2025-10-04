"""Validation helpers for OHLCV bars."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Iterable, Mapping, Sequence, cast

from .timeutils import (
    Timeframe,
    is_bar_boundary,
    is_bar_complete,
    is_intraday,
    is_within_session,
    tf_minutes,
)

logger = logging.getLogger(__name__)

REQUIRED_OHLCV_KEYS = ("symbol", "ts_ist", "tf", "o", "h", "l", "c", "v")


def require_keys(d: Mapping, keys: Sequence[str]) -> None:
    """Ensure that ``d`` contains ``keys``; raise with missing ones otherwise."""

    missing = [key for key in keys if key not in d]
    if missing:
        logger.debug("Missing keys detected: %s", missing)
        raise KeyError(f"Missing keys: {', '.join(missing)}")


def _is_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def validate_ohlcv_row(row: Mapping) -> None:
    """Validate structure, types, and invariants of an OHLCV row."""

    require_keys(row, REQUIRED_OHLCV_KEYS)

    symbol = row["symbol"]
    if not isinstance(symbol, str) or not symbol:
        raise ValueError("'symbol' must be a non-empty string")

    ts = row["ts_ist"]
    if not isinstance(ts, datetime):
        raise ValueError("'ts_ist' must be a datetime instance")
    if ts.tzinfo is None:
        raise ValueError("'ts_ist' must be timezone-aware and in IST")
    if getattr(ts.tzinfo, "key", None) != "Asia/Kolkata":
        raise ValueError("'ts_ist' must be in Asia/Kolkata timezone")

    tf = row["tf"]
    if not isinstance(tf, str):
        raise ValueError("'tf' must be a string")
    try:
        tf_minutes(tf)  # type: ignore[arg-type]
    except ValueError as exc:
        raise ValueError("Unsupported timeframe value") from exc
    timeframe = cast(Timeframe, tf)

    prices = {key: row[key] for key in ("o", "h", "l", "c")}
    for key, value in prices.items():
        if not _is_number(value):
            raise ValueError(f"'{key}' must be a numeric value")
        if value <= 0:
            raise ValueError(f"'{key}' must be positive")

    low = float(prices["l"])
    high = float(prices["h"])
    open_ = float(prices["o"])
    close = float(prices["c"])
    if low > min(open_, close, high):
        raise ValueError("'l' must be less than or equal to min(o, c, h)")
    if high < max(open_, close, low):
        raise ValueError("'h' must be greater than or equal to max(o, c, l)")

    volume = row["v"]
    if not isinstance(volume, int) or volume < 0:
        raise ValueError("'v' must be a non-negative integer")

    if is_intraday(timeframe) and not is_within_session(ts):
        raise ValueError("'ts_ist' must fall within the trading session for intraday bars")

    if not is_bar_boundary(ts, timeframe):
        raise ValueError("'ts_ist' is not aligned to a valid bar boundary")


def enforce_bar_complete(row: Mapping) -> None:
    """Ensure that a row represents a complete bar."""

    ts = row.get("ts_ist")
    tf = row.get("tf")
    if not isinstance(ts, datetime) or not isinstance(tf, str):
        raise ValueError("Row must contain 'ts_ist' datetime and 'tf' string")
    if getattr(ts.tzinfo, "key", None) != "Asia/Kolkata":
        raise ValueError("'ts_ist' must be in Asia/Kolkata timezone")
    if tf not in ("5m", "15m", "1d"):
        raise ValueError("Unsupported timeframe value")
    timeframe: Timeframe = cast(Timeframe, tf)
    if not is_bar_complete(ts, timeframe):
        raise ValueError("Bar is not complete for the given timeframe")


def ensure_sorted_unique(bars: Iterable[Mapping]) -> None:
    """Ensure ``bars`` are strictly increasing in ``ts_ist`` with no duplicates."""

    last_ts: datetime | None = None
    for row in bars:
        ts = row.get("ts_ist")
        if not isinstance(ts, datetime):
            raise ValueError("Each bar must contain a 'ts_ist' datetime")
        if ts.tzinfo is None:
            raise ValueError("'ts_ist' values must be timezone-aware")
        if getattr(ts.tzinfo, "key", None) != "Asia/Kolkata":
            raise ValueError("All 'ts_ist' values must be in Asia/Kolkata timezone")

        if last_ts is not None and ts <= last_ts:
            raise ValueError("Bars must be strictly increasing in 'ts_ist'")
        last_ts = ts

