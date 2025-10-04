"""
Fetch mock futures OI snapshots.
Later will pull from Kite quote API.
"""
from datetime import datetime, timezone
import random


def run(symbols: list[str], mode: str = "mock"):
    now = datetime.now(timezone.utc)
    out = []
    for s in symbols:
        price = 20000.0 if "BANK" in s else 2200.0
        oi = random.randint(1000000, 2500000)
        out.append({"symbol": s, "ts_ist": now.isoformat(), "price": price, "oi": oi})
    return out
