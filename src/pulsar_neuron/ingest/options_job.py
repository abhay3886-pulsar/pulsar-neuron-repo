"""
Fetch mock option chain (ATM ± N strikes).
Later will use Kite option quote API.
"""
from datetime import datetime, timezone
import random


def run(symbols: list[str], mode: str = "mock", strikes: int = 10):
    now = datetime.now(timezone.utc)
    chain = []
    for s in symbols:
        base = 20000 if "BANK" in s else 2200
        expiry = "2025-10-10"
        for i in range(-strikes, strikes + 1):
            strike = base + i * 100
            for side in ("CE", "PE"):
                ltp = round(random.uniform(50, 350), 2)
                iv = round(random.uniform(10, 30), 2)
                oi = random.randint(10000, 80000)
                vol = random.randint(500, 5000)
                chain.append(
                    {
                        "symbol": s,
                        "ts_ist": now.isoformat(),
                        "expiry": expiry,
                        "strike": strike,
                        "side": side,
                        "ltp": ltp,
                        "iv": iv,
                        "oi": oi,
                        "volume": vol,
                    }
                )
    return chain
