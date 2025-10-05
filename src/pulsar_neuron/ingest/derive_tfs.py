from __future__ import annotations
from typing import List, Dict, DefaultDict
from collections import defaultdict
from datetime import datetime, timedelta


def derive_15m(bars_5m: List[Dict]) -> List[Dict]:
    """Aggregate 3×5m → 1×15m.
    Assumptions:
      - Input is ascending by ts_ist (global stream).
      - May contain interleaved symbols.
      - Only contiguous 5m bars are stitched; on gaps, buffer resets for that symbol.
      - Trailing partial buffers are ignored (no output).
    """
    out: List[Dict] = []
    buffers: DefaultDict[str, List[Dict]] = defaultdict(list)

    def _is_5m(b: Dict) -> bool:
        tf = b.get("tf")
        return tf == "5m" or tf is None  # tolerate missing tf if upstream omitted

    def _ts(dt) -> datetime:
        # assume already a datetime; tolerate string ISO by not converting silently
        # (raise early to avoid silent corruption)
        if not isinstance(dt, datetime):
            raise TypeError(f"ts_ist must be datetime, got {type(dt).__name__}")
        return dt

    for b in bars_5m:
        if not _is_5m(b):
            continue  # skip non-5m rows defensively

        symbol = b["symbol"]
        ts = _ts(b["ts_ist"])
        buf = buffers[symbol]

        if not buf:
            buf.append(b)
        else:
            prev_ts = _ts(buf[-1]["ts_ist"])
            # contiguous only if exactly +5 minutes
            if ts - prev_ts == timedelta(minutes=5):
                buf.append(b)
            else:
                # gap or overlap -> reset buffer to start a new 15m window
                buf.clear()
                buf.append(b)

        if len(buf) == 3:
            first, mid, last = buf[0], buf[1], buf[2]
            out.append(
                {
                    "symbol": symbol,
                    "tf": "15m",
                    "ts_ist": _ts(last["ts_ist"]),  # end aligns to the third 5m
                    "o": float(first["o"]),
                    "h": float(max(x["h"] for x in buf)),
                    "l": float(min(x["l"] for x in buf)),
                    "c": float(last["c"]),
                    "v": int(sum(int(x.get("v", 0)) for x in buf)),
                }
            )
            buf.clear()

    return out
