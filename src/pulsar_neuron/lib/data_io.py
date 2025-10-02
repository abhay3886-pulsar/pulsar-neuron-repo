"""Data access adapters with deterministic placeholders."""

from __future__ import annotations

from datetime import datetime
from typing import Any


def get_spot_ohlcv(symbol: str, timeframe: str, lookback: int) -> dict[str, Any]:
    """Return spot OHLCV candles for the requested lookback.

    Raises:
        NotImplementedError: This adapter is a stub and must be replaced with a
            deterministic implementation.
    """

    return {
        "ok": False,
        "status": "NOT_IMPLEMENTED",
        "symbol": symbol,
        "timeframe": timeframe,
        "lookback": lookback,
        "data": [],
    }


def get_futures_oi_snapshot(symbol: str, at: datetime) -> dict[str, Any]:
    """Return a futures open-interest snapshot for a symbol.

    Raises:
        NotImplementedError: This adapter is a stub and must be replaced with a
            deterministic implementation.
    """

    return {
        "ok": False,
        "status": "NOT_IMPLEMENTED",
        "symbol": symbol,
        "timestamp": at.isoformat(),
        "snapshot": {},
    }


def get_option_chain(symbol: str, at: datetime) -> dict[str, Any]:
    """Return the option chain for a symbol at a given timestamp.

    Raises:
        NotImplementedError: This adapter is a stub and must be replaced with a
            deterministic implementation.
    """

    return {
        "ok": False,
        "status": "NOT_IMPLEMENTED",
        "symbol": symbol,
        "timestamp": at.isoformat(),
        "chain": [],
    }


def get_breadth(at: datetime) -> dict[str, Any]:
    """Return market breadth metrics.

    Raises:
        NotImplementedError: This adapter is a stub and must be replaced with a
            deterministic implementation.
    """

    return {
        "ok": False,
        "status": "NOT_IMPLEMENTED",
        "timestamp": at.isoformat(),
        "breadth": {},
    }


def get_vix(at: datetime) -> dict[str, Any]:
    """Return volatility index metrics.

    Raises:
        NotImplementedError: This adapter is a stub and must be replaced with a
            deterministic implementation.
    """

    return {
        "ok": False,
        "status": "NOT_IMPLEMENTED",
        "timestamp": at.isoformat(),
        "value": None,
    }
