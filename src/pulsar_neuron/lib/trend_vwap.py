"""VWAP and trend analysis helpers with placeholder outputs."""

from __future__ import annotations

from typing import Any, Iterable


def vwap_session(candles_5m: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Compute session VWAP from 5-minute candles.

    Raises:
        NotImplementedError: This function is a stub and must be implemented with
            deterministic logic.
    """

    return {
        "ok": False,
        "status": "NOT_IMPLEMENTED",
        "inputs": list(candles_5m),
        "vwap": None,
    }


def price_vs_vwap(last_price: float | None, vwap: float | None) -> dict[str, Any]:
    """Compare the last traded price with VWAP.

    Raises:
        NotImplementedError: This function is a stub and must be implemented with
            deterministic logic.
    """

    return {
        "ok": False,
        "status": "NOT_IMPLEMENTED",
        "price": last_price,
        "vwap": vwap,
        "relationship": None,
    }


def slope_5m(candles: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Compute the slope using 5-minute candles.

    Raises:
        NotImplementedError: This function is a stub and must be implemented with
            deterministic logic.
    """

    return {
        "ok": False,
        "status": "NOT_IMPLEMENTED",
        "inputs": list(candles),
        "slope": None,
    }


def slope_15m(candles: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Compute the slope using 15-minute candles.

    Raises:
        NotImplementedError: This function is a stub and must be implemented with
            deterministic logic.
    """

    return {
        "ok": False,
        "status": "NOT_IMPLEMENTED",
        "inputs": list(candles),
        "slope": None,
    }


def orb_status(candles_5m: Iterable[dict[str, Any]], ib_window: dict[str, str]) -> dict[str, Any]:
    """Compute opening range breakout status for the session.

    Raises:
        NotImplementedError: This function is a stub and must be implemented with
            deterministic logic.
    """

    return {
        "ok": False,
        "status": "NOT_IMPLEMENTED",
        "inputs": list(candles_5m),
        "ib_window": ib_window,
        "status_detail": None,
    }
