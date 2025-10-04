from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from pulsar_neuron.db.ohlcv_repo import read_last_n as read_ohlcv_last
from pulsar_neuron.db.fut_oi_repo import read_last as read_oi_last
from pulsar_neuron.db.options_repo import read_latest_snapshot

IST = ZoneInfo("Asia/Kolkata")


def _fmt_ts(ts) -> str:
    if isinstance(ts, datetime):
        return ts.astimezone(IST).strftime("%H:%M:%S")
    return str(ts)


def main():
    for s in ["NIFTY 50", "NIFTY BANK"]:
        last_5m = read_ohlcv_last(s, "5m", 1)
        last_15m = read_ohlcv_last(s, "15m", 1)
        ts5 = _fmt_ts(last_5m[-1]["ts_ist"]) if last_5m else "-"
        ts15 = _fmt_ts(last_15m[-1]["ts_ist"]) if last_15m else "-"
        print(f"{s:11s}  5m:{ts5}  15m:{ts15}")

    for s in ["NIFTY", "BANKNIFTY"]:
        oi = read_oi_last(s, 1)
        oi_ts = _fmt_ts(oi[0]["ts_ist"]) if oi else "-"
        chain = read_latest_snapshot(s)
        ch_ts = _fmt_ts(chain[0]["ts_ist"]) if chain else "-"
        print(f"{s:10s}  OI:{oi_ts}  OPT:{ch_ts}")


if __name__ == "__main__":
    main()
