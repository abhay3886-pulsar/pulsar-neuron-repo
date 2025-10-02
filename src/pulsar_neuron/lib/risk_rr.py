"""Risk and reward guard-rails with placeholder outputs."""

from __future__ import annotations

from typing import Any, Iterable


def rr_preview(entry: float | None, sl: float | None, tp: float | None) -> dict[str, Any]:
    """Preview reward-to-risk metrics for a trade idea.

    Raises:
        NotImplementedError: This function is a stub and must be implemented with
            deterministic logic.
    """

    return {
        "ok": False,
        "status": "NOT_IMPLEMENTED",
        "entry": entry,
        "stop_loss": sl,
        "take_profit": tp,
        "rr": None,
    }


def sl_viability(spread: float | None, atr: float | None) -> dict[str, Any]:
    """Assess stop-loss viability relative to spread and ATR.

    Raises:
        NotImplementedError: This function is a stub and must be implemented with
            deterministic logic.
    """

    return {
        "ok": False,
        "status": "NOT_IMPLEMENTED",
        "spread": spread,
        "atr": atr,
        "viable": None,
    }


def time_guard(now: str, policy: dict[str, str]) -> dict[str, Any]:
    """Check if current time is within policy constraints.

    Raises:
        NotImplementedError: This function is a stub and must be implemented with
            deterministic logic.
    """

    return {
        "ok": False,
        "status": "NOT_IMPLEMENTED",
        "now": now,
        "policy": policy,
    }


def positions_guard(open_positions: Iterable[str], max_positions: int) -> dict[str, Any]:
    """Check if the number of open positions respects limits.

    Raises:
        NotImplementedError: This function is a stub and must be implemented with
            deterministic logic.
    """

    return {
        "ok": False,
        "status": "NOT_IMPLEMENTED",
        "open_positions": list(open_positions),
        "max_positions": max_positions,
    }


def level_proximity_guard(wall_distance_em: float | None, min_required: float) -> dict[str, Any]:
    """Check if the trade is sufficiently far from major OI walls.

    Raises:
        NotImplementedError: This function is a stub and must be implemented with
            deterministic logic.
    """

    return {
        "ok": False,
        "status": "NOT_IMPLEMENTED",
        "distance_em": wall_distance_em,
        "min_required": min_required,
    }
