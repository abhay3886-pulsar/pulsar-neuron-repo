from __future__ import annotations
from typing import Dict, List
from pulsar_neuron.db.ohlcv_repo import read_last_n


def _sma(vals: List[float], n: int) -> float:
    if len(vals) < n:
        return float('nan')
    return sum(vals[-n:]) / n


def build_from_db(symbols: List[str]) -> Dict[str, dict]:
    """
    Minimal context pack:
      - last 60 x 5m closes
      - sma20 on 5m
      - simple 5-bar slope on 5m
    Extend later with vwap_rel, ORB state, OI bias, options skew.
    """
    ctx: Dict[str, dict] = {}
    for s in symbols:
        bars5 = read_last_n(s, "5m", 60)
        bars15 = read_last_n(s, "15m", 20)

        closes5 = [float(b["c"]) for b in bars5]
        sma20 = _sma(closes5, 20) if closes5 else float('nan')
        slope5 = (closes5[-1] - closes5[-5]) / 5 if len(closes5) >= 5 else float('nan')

        ctx[s] = {
            "last_5m_ts": bars5[-1]["ts_ist"] if bars5 else None,
            "last_15m_ts": bars15[-1]["ts_ist"] if bars15 else None,
            "sma20_5m": sma20,
            "slope_5m": slope5,
            "closes5": closes5[-10:],  # keep a tail for debugging
        }
    return ctx
