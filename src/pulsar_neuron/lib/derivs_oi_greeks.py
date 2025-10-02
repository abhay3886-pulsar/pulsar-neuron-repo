"""Derivatives open-interest and Greeks classifiers with placeholders."""

from __future__ import annotations

from typing import Any


def futures_oi_bias(baseline: dict[str, Any], current: dict[str, Any]) -> dict[str, Any]:
    """Classify futures open-interest bias.

    Raises:
        NotImplementedError: This function is a stub and must be implemented with
            deterministic logic.
    """

    return {
        "ok": False,
        "status": "NOT_IMPLEMENTED",
        "baseline": baseline,
        "current": current,
        "bias": None,
    }


def options_oi_walls(option_chain: list[dict[str, Any]]) -> dict[str, Any]:
    """Detect option open-interest walls for calls and puts.

    Raises:
        NotImplementedError: This function is a stub and must be implemented with
            deterministic logic.
    """

    return {
        "ok": False,
        "status": "NOT_IMPLEMENTED",
        "chain": option_chain,
        "walls": {},
    }


def pcr_trend(option_chain: list[dict[str, Any]]) -> dict[str, Any]:
    """Compute put-call ratio trend.

    Raises:
        NotImplementedError: This function is a stub and must be implemented with
            deterministic logic.
    """

    return {
        "ok": False,
        "status": "NOT_IMPLEMENTED",
        "chain": option_chain,
        "pcr": None,
    }


def greeks_regime(option_chain: list[dict[str, Any]]) -> dict[str, Any]:
    """Classify expected gamma/vega regime.

    Raises:
        NotImplementedError: This function is a stub and must be implemented with
            deterministic logic.
    """

    return {
        "ok": False,
        "status": "NOT_IMPLEMENTED",
        "chain": option_chain,
        "regime": None,
    }


def expected_move(option_chain: list[dict[str, Any]]) -> dict[str, Any]:
    """Estimate the expected move in points.

    Raises:
        NotImplementedError: This function is a stub and must be implemented with
            deterministic logic.
    """

    return {
        "ok": False,
        "status": "NOT_IMPLEMENTED",
        "chain": option_chain,
        "expected_move": None,
    }
