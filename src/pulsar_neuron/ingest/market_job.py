"""
Fetch mock market breadth & India VIX.
Later will read from NSE index API.
"""
from datetime import datetime, timezone
import random


def run(mode: str = "mock"):
    now = datetime.now(timezone.utc)
    adv = random.randint(100, 300)
    dec = random.randint(50, 150)
    unch = random.randint(5, 20)
    vix = round(random.uniform(10, 18), 2)
    return {"ts_ist": now.isoformat(), "adv": adv, "dec": dec, "unch": unch, "vix": vix}
