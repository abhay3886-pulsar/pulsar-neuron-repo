from __future__ import annotations
from typing import List, Dict


def derive_15m(bars_5m: List[Dict]) -> List[Dict]:
    """Aggregate 3×5m → 1×15m. Input must be ascending by ts_ist."""
    out: List[Dict] = []
    buf: List[Dict] = []
    for b in bars_5m:
        buf.append(b)
        if len(buf) == 3:
            first, mid, last = buf[0], buf[1], buf[2]
            out.append(
                {
                    "symbol": last["symbol"],
                    "tf": "15m",
                    "ts_ist": last["ts_ist"],  # end aligns to the third 5m
                    "o": first["o"],
                    "h": max(x["h"] for x in buf),
                    "l": min(x["l"] for x in buf),
                    "c": last["c"],
                    "v": sum(x["v"] for x in buf),
                }
            )
            buf.clear()
    return out
