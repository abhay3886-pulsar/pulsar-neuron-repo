"""
Fetch mock Futures OI snapshots (baseline + intraday).
"""
from datetime import datetime, timezone
import random


def run(symbols: list[str], mode: str = "mock"):
    now = datetime.now(timezone.utc).isoformat()
    out = []
    for s in symbols:
        price = 20000.0 if "BANK" in s else 2200.0
        oi = random.randint(1_000_000,2_500_000)
        out.append({"symbol": s,"ts_ist": now,"price": price,"oi": oi})
    return out
