from __future__ import annotations

from datetime import datetime, timezone

from pulsar_neuron.db_legacy.market_repo import upsert_one, read_latest
from pulsar_neuron.db_legacy.ohlcv_repo import read_last_n


def main():
    # Upsert one breadth row (ts now)
    row = {"ts_ist": datetime.now(timezone.utc), "adv": 200, "dec": 120, "unch": 10, "vix": 14.5}
    upsert_one(row)
    latest = read_latest()
    print("market_breadth latest:", latest)

    # Try reading OHLCV (will be empty until your live bars run)
    for s in ["NIFTY 50", "NIFTY BANK"]:
        bars = read_last_n(s, "5m", 3)
        print(f"{s} last 3x5m:", bars)


if __name__ == "__main__":
    main()
