from __future__ import annotations
from typing import List, Dict
from datetime import datetime, timedelta
from collections import defaultdict
from pulsar_neuron.db import ohlcv_repo
from pulsar_neuron.normalize import normalize_ohlcv

# ---------------------------- generic rollup ---------------------------------
def _aggregate(bars: List[Dict], group_size: int, tf_label: str) -> List[Dict]:
    """Aggregate contiguous bars (e.g. 3×5m→15m, 12×5m→1h)."""
    out: List[Dict] = []
    buffers: defaultdict[str, List[Dict]] = defaultdict(list)

    for b in bars:
        sym = b["symbol"]
        tf = b.get("tf", "5m")
        if tf != "5m":
            continue
        ts: datetime = b["ts_ist"]
        buf = buffers[sym]

        if buf and (ts - buf[-1]["ts_ist"]) != timedelta(minutes=5):
            buf.clear()  # gap reset
        buf.append(b)

        if len(buf) == group_size:
            first, last = buf[0], buf[-1]
            out.append(
                {
                    "symbol": sym,
                    "tf": tf_label,
                    "ts_ist": last["ts_ist"],
                    "o": float(first["o"]),
                    "h": max(float(x["h"]) for x in buf),
                    "l": min(float(x["l"]) for x in buf),
                    "c": float(last["c"]),
                    "v": sum(int(x.get("v", 0)) for x in buf),
                }
            )
            buf.clear()
    return out


def _aggregate_daily(bars: List[Dict]) -> List[Dict]:
    out: dict[tuple[str, datetime.date], Dict] = {}
    for b in bars:
        sym = b["symbol"]
        ts: datetime = b["ts_ist"]
        key = (sym, ts.date())
        agg = out.setdefault(
            key,
            {"symbol": sym, "tf": "1d", "ts_ist": ts.replace(hour=15, minute=30),
             "o": float(b["o"]), "h": float(b["h"]), "l": float(b["l"]),
             "c": float(b["c"]), "v": 0},
        )
        agg["h"] = max(agg["h"], float(b["h"]))
        agg["l"] = min(agg["l"], float(b["l"]))
        agg["c"] = float(b["c"])
        agg["v"] += int(b.get("v", 0))
    return list(out.values())


# --------------------------- orchestrator ------------------------------------
def postprocess_and_store(bars_5m: List[Dict]) -> None:
    if not bars_5m:
        return

    rollups = [
        (3,  "15m"),
        (12, "1h"),
    ]

    for n, label in rollups:
        agg = _aggregate(bars_5m, n, label)
        if agg:
            ohlcv_repo.upsert_many(normalize_ohlcv(agg))
            print(f"🧭 Stored {len(agg)} bars ({label})")

    daily = _aggregate_daily(bars_5m)
    if daily:
        ohlcv_repo.upsert_many(normalize_ohlcv(daily))
        print(f"📘 Stored {len(daily)} bars (1d)")
