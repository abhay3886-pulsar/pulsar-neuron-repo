from __future__ import annotations

import os
from typing import Any, Dict

from ..utils.common import Mode


def _mode() -> str:
    return os.getenv("MODE", Mode.DRY_RUN)


def place(intent: Dict[str, Any]) -> Dict[str, Any]:
    """
    Place order intent via broker.
    DRY_RUN/PAPER: return simulated response.
    LIVE: TODO — integrate with broker SDK.
    """

    mode = _mode()
    if mode in (Mode.DRY_RUN, Mode.PAPER):
        return {"order_id": "SIM-ORDER-1", "status": "PLACED", "avg_fill": None, "mode": mode}
    return {"order_id": "LIVE-PLACE-NOOP", "status": "NOOP", "mode": mode}


def modify_sl_tp(order_id: str, sl: float | None = None, tp: float | None = None) -> Dict[str, Any]:
    return {"order_id": order_id, "status": "MODIFIED", "sl": sl, "tp": tp, "mode": _mode()}


def exit_market(symbol: str, qty: int) -> Dict[str, Any]:
    return {"symbol": symbol, "status": "EXIT_SENT", "qty": qty, "mode": _mode()}


def position_snapshot(symbol: str) -> Dict[str, Any]:
    """Return a synthetic snapshot for DRY_RUN/PAPER."""

    return {"symbol": symbol, "side": None, "qty": 0, "avg_fill": None, "mode": _mode()}
