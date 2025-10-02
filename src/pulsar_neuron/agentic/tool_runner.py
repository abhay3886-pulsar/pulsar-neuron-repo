"""Registry for deterministic tool execution returning evidence items."""

from __future__ import annotations

from typing import Any, Callable, Dict

from pulsar_neuron.lib import data_io, derivs_oi_greeks, price_action, refs_levels, risk_rr, trend_vwap
from pulsar_neuron.lib.schemas import Bias, EvidenceItem

TOOL_REGISTRY: Dict[str, Callable[..., dict[str, Any]]] = {
    "data.get_spot_ohlcv": data_io.get_spot_ohlcv,
    "data.get_futures_oi_snapshot": data_io.get_futures_oi_snapshot,
    "data.get_option_chain": data_io.get_option_chain,
    "data.get_breadth": data_io.get_breadth,
    "data.get_vix": data_io.get_vix,
    "refs.compute_session_refs": refs_levels.compute_session_refs,
    "refs.compute_pivots": refs_levels.compute_pivots,
    "trend.vwap_session": trend_vwap.vwap_session,
    "trend.price_vs_vwap": trend_vwap.price_vs_vwap,
    "trend.slope_5m": trend_vwap.slope_5m,
    "trend.slope_15m": trend_vwap.slope_15m,
    "trend.orb_status": trend_vwap.orb_status,
    "price.structure": price_action.structure_hh_hl_lh_ll,
    "price.detect_bos": price_action.detect_bos,
    "price.detect_choch": price_action.detect_choch,
    "price.detect_sweep": price_action.detect_sweep,
    "price.detect_box": price_action.detect_box,
    "derivs.futures_oi_bias": derivs_oi_greeks.futures_oi_bias,
    "derivs.options_oi_walls": derivs_oi_greeks.options_oi_walls,
    "derivs.pcr_trend": derivs_oi_greeks.pcr_trend,
    "derivs.greeks_regime": derivs_oi_greeks.greeks_regime,
    "derivs.expected_move": derivs_oi_greeks.expected_move,
    "risk.rr_preview": risk_rr.rr_preview,
    "risk.sl_viability": risk_rr.sl_viability,
    "risk.time_guard": risk_rr.time_guard,
    "risk.positions_guard": risk_rr.positions_guard,
    "risk.level_proximity_guard": risk_rr.level_proximity_guard,
}


def run_tool(name: str, **kwargs: Any) -> EvidenceItem:
    """Execute a registered tool and wrap the result into an evidence item."""

    func = TOOL_REGISTRY.get(name)
    if func is None:
        return EvidenceItem(
            tool=name,
            ok=False,
            bias=Bias.na,
            value={"status": "NOT_IMPLEMENTED"},
            explain="Unknown tool",
        )
    result = func(**kwargs)
    payload: dict[str, Any]
    if isinstance(result, dict):
        payload = dict(result)
    else:
        payload = {"result": result}
    payload.setdefault("status", "NOT_IMPLEMENTED")
    ok_value = bool(payload.get("ok", False))
    payload.setdefault("ok", ok_value)
    return EvidenceItem(tool=name, ok=ok_value, bias=Bias.na, value=payload)
