"""Price action structure helpers with placeholder outputs."""

from __future__ import annotations

from typing import Any, Iterable


def structure_hh_hl_lh_ll(candles: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Detect swing structure classifications from candles.

    Raises:
        NotImplementedError: This function is a stub and must be implemented with
            deterministic logic.
    """

    return {
        "ok": False,
        "status": "NOT_IMPLEMENTED",
        "inputs": list(candles),
        "structure": None,
    }


def detect_bos(candles: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Detect break of structure events.

    Raises:
        NotImplementedError: This function is a stub and must be implemented with
            deterministic logic.
    """

    return {
        "ok": False,
        "status": "NOT_IMPLEMENTED",
        "inputs": list(candles),
        "bos": [],
    }


def detect_choch(candles: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Detect change-of-character events.

    Raises:
        NotImplementedError: This function is a stub and must be implemented with
            deterministic logic.
    """

    return {
        "ok": False,
        "status": "NOT_IMPLEMENTED",
        "inputs": list(candles),
        "choch": [],
    }


def detect_sweep(levels: Iterable[float] | None, candles: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Detect liquidity sweeps around important levels.

    Raises:
        NotImplementedError: This function is a stub and must be implemented with
            deterministic logic.
    """

    return {
        "ok": False,
        "status": "NOT_IMPLEMENTED",
        "levels": list(levels or []),
        "inputs": list(candles),
        "sweeps": [],
    }


def detect_box(candles: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Detect consolidation boxes.

    Raises:
        NotImplementedError: This function is a stub and must be implemented with
            deterministic logic.
    """

    return {
        "ok": False,
        "status": "NOT_IMPLEMENTED",
        "inputs": list(candles),
        "boxes": [],
    }
