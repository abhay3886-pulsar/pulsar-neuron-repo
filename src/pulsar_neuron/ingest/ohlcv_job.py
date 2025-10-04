"""
Fetch and normalize OHLCV bars (mock mode).
Later will connect to Kite WebSocket or REST.
"""
from datetime import datetime, timezone
import math, random


def make_mock_bars(symbol: str, tf: str = "5m", limit: int = 120):
    bars = []
    base = 20000.0 if "BANK" in symbol else 2200.0
    for i in range(limit):
        o = base + math.sin(i/6)*8 + random.uniform(-2,2)
        h = o + random.uniform(1,5)
        l = o - random.uniform(1,5)
        c = l + (h-l)*random.random()
        v = random.randint(800,2000)
        bars.append({
            "symbol": symbol, "tf": tf,
            "o": o, "h": h, "l": l, "c": c, "v": v,
            "ts_ist": datetime.now(timezone.utc).isoformat()
        })
    return bars


def run(symbols: list[str], tf: str = "5m", mode: str = "mock"):
    if mode != "mock":
        raise NotImplementedError("only mock mode supported")
    return {s: make_mock_bars(s, tf) for s in symbols}
