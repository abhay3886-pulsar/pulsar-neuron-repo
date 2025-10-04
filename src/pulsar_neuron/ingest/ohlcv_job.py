"""
Fetch and normalize OHLCV bars (mock mode).
Later this will read from Kite WebSocket or API.
"""
from pulsar_neuron.normalize.ohlcv_norm import normalize_ohlcv


def make_mock_bars(symbol: str, tf: str = "5m", limit: int = 120):
    import math
    import random

    bars = []
    base = 20000.0 if "BANK" in symbol else 2200.0
    for i in range(limit):
        o = base + math.sin(i / 5) * 10 + random.uniform(-2, 2)
        h = o + random.uniform(1, 5)
        l = o - random.uniform(1, 5)
        c = l + (h - l) * random.random()
        v = random.randint(500, 2000)
        bars.append(
            {
                "symbol": symbol,
                "tf": tf,
                "o": o,
                "h": h,
                "l": l,
                "c": c,
                "v": v,
                "ts_ist": f"{i}",
            }
        )
    return bars


def run(symbols: list[str], tf: str = "5m", mode: str = "mock"):
    if mode != "mock":
        raise NotImplementedError("Only mock mode supported now.")
    all_data = {}
    for s in symbols:
        raw = make_mock_bars(s, tf)
        all_data[s] = normalize_ohlcv(raw, tf=tf)
    return all_data
