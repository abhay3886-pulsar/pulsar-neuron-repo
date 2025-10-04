"""Helpers to derive higher timeframe bars from 5-minute data."""
from __future__ import annotations

from typing import Dict, List


def derive_15m(bars_5m: List[Dict]) -> List[Dict]:
    """Aggregate sequential 5m bars into a 15m bar."""
    out: List[Dict] = []
    buffer: List[Dict] = []

    for bar in bars_5m:
        buffer.append(bar)
        if len(buffer) == 3:
            first, _, last = buffer
            out.append(
                {
                    "symbol": last["symbol"],
                    "tf": "15m",
                    "ts_ist": last["ts_ist"],
                    "o": first["o"],
                    "h": max(b["h"] for b in buffer),
                    "l": min(b["l"] for b in buffer),
                    "c": last["c"],
                    "v": sum(int(b["v"]) for b in buffer),
                }
            )
            buffer.clear()

    return out
