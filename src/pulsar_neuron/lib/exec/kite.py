from __future__ import annotations

from typing import Any, Dict


def place(intent: Dict[str, Any]) -> Dict[str, Any]:
    """Place order intent via broker (stub)."""
    return {"order_id": "SIM-ORDER-1", "status": "PLACED", "avg_fill": None}


def modify_sl_tp(order_id: str, sl: float | None = None, tp: float | None = None) -> Dict[str, Any]:
    return {"order_id": order_id, "status": "MODIFIED", "sl": sl, "tp": tp}


def exit_market(symbol: str, qty: int) -> Dict[str, Any]:
    return {"symbol": symbol, "status": "EXIT_SENT", "qty": qty}
