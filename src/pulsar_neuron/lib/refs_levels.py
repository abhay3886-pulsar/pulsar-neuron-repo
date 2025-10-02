"""Reference levels such as session highs/lows and pivots."""

from __future__ import annotations

from typing import Any, Iterable


def compute_session_refs(candles_5m: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Compute session reference levels from 5-minute candles.

    Raises:
        NotImplementedError: This function is a stub and must be implemented with
            deterministic logic.
    """

    return {
        "ok": False,
        "status": "NOT_IMPLEMENTED",
        "inputs": list(candles_5m),
        "levels": {},
    }


def compute_pivots(day_candle: dict[str, Any]) -> dict[str, Any]:
    """Compute pivot levels from a daily candle.

    Raises:
        NotImplementedError: This function is a stub and must be implemented with
            deterministic logic.
    """

    return {
        "ok": False,
        "status": "NOT_IMPLEMENTED",
        "inputs": day_candle,
        "levels": {},
    }
