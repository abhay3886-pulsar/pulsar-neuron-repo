"""
Fetch mock market breadth + India VIX.
"""
from datetime import datetime, timezone
import random


def run(mode: str = "mock"):
    now = datetime.now(timezone.utc).isoformat()
    adv, dec, unch = random.randint(100,300), random.randint(50,150), random.randint(5,20)
    vix = round(random.uniform(10,18),2)
    return {"ts_ist": now,"adv": adv,"dec": dec,"unch": unch,"vix": vix}
