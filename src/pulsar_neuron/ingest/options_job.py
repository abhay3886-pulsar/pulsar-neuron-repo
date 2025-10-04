"""
Fetch mock option-chain snapshot (ATM ± N strikes).
"""
from datetime import datetime, timezone
import random


def run(symbols: list[str], mode: str = "mock", strikes: int = 3):
    now = datetime.now(timezone.utc).isoformat()
    chain = []
    for s in symbols:
        base = 20000 if "BANK" in s else 2200
        expiry = "2025-10-10"
        for i in range(-strikes,strikes+1):
            strike = base + i*100
            for side in ("CE","PE"):
                chain.append({
                    "symbol": s, "ts_ist": now,
                    "expiry": expiry, "strike": strike, "side": side,
                    "ltp": round(random.uniform(50,350),2),
                    "iv":  round(random.uniform(10,30),2),
                    "oi":  random.randint(10_000,80_000),
                    "volume": random.randint(500,5000)
                })
    return chain
