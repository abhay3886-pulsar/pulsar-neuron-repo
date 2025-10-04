"""Utilities to normalize OHLCV bar payloads."""
from __future__ import annotations

from datetime import date
from typing import Iterable


def _to_float(value: float | int) -> float:
    """Convert numeric inputs to floats with two decimal precision."""
    return round(float(value), 2)


def normalize_ohlcv(raw: Iterable[dict], *, tf: str) -> list[dict]:
    """Normalize raw OHLCV bars into a consistent schema.

    Parameters
    ----------
    raw:
        Iterable of raw bar dictionaries containing ``o``, ``h``, ``l``, ``c``,
        ``v`` and ``ts_ist`` keys.
    tf:
        The timeframe string the bars represent.

    Returns
    -------
    list[dict]
        Normalized bar dictionaries ready for downstream processing.
    """

    normalized = []
    trade_date = date.today().isoformat()
    for bar in raw:
        normalized.append(
            {
                "symbol": bar.get("symbol"),
                "date": trade_date,
                "timeframe": tf,
                "open": _to_float(bar.get("o", 0.0)),
                "high": _to_float(bar.get("h", 0.0)),
                "low": _to_float(bar.get("l", 0.0)),
                "close": _to_float(bar.get("c", 0.0)),
                "volume": int(bar.get("v", 0)),
                "ts_ist": bar.get("ts_ist"),
            }
        )
    return normalized
