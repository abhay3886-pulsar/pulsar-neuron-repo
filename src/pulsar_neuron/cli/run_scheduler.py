from __future__ import annotations

import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from pulsar_neuron.ingest import fut_oi_job, options_job, market_job
from pulsar_neuron.db.fut_oi_repo import upsert_many as upsert_fut_oi
from pulsar_neuron.db.options_repo import upsert_many as upsert_options

IST = ZoneInfo("Asia/Kolkata")


def now_ist() -> datetime:
    return datetime.now(tz=IST)


def sleep_until(target: datetime) -> None:
    while True:
        now = now_ist()
        dt = (target - now).total_seconds()
        if dt <= 0:
            return
        time.sleep(min(dt, 0.5))


def next_on_second(mod: int, offset_sec: int = 0) -> datetime:
    """Next IST time where epoch % mod == offset_sec."""
    now = now_ist().replace(microsecond=0)
    epoch = int(now.timestamp())
    add = (mod - (epoch % mod) + offset_sec) % mod
    return now + timedelta(seconds=add)


def main():
    # Cadence plan:
    # - OI every 120s @ +10s
    # - Options every 180s @ +20s
    # - Breadth/VIX every 300s @ +30s  (TODO persist later)
    while True:
        t_oi = next_on_second(120, 10)
        t_opt = next_on_second(180, 20)
        t_mkt = next_on_second(300, 30)
        target = min(t_oi, t_opt, t_mkt)
        sleep_until(target)
        now = now_ist()

        if abs((now - t_oi).total_seconds()) < 1.0:
            rows = fut_oi_job.run(["NIFTY", "BANKNIFTY"], mode="live")
            if rows:
                upsert_fut_oi(rows)

        if abs((now - t_opt).total_seconds()) < 1.0:
            rows = options_job.run(["NIFTY", "BANKNIFTY"], mode="live", strikes=5)
            if rows:
                upsert_options(rows)

        if abs((now - t_mkt).total_seconds()) < 1.0:
            _ = market_job.run(mode="live")  # TODO: persist breadth/vix repo when ready


if __name__ == "__main__":
    main()
