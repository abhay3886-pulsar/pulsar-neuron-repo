from __future__ import annotations

import datetime as dt

import pytz

IST = pytz.timezone("Asia/Kolkata")
TRADING_START = dt.time(9, 20)
TRADING_END = dt.time(15, 15)
HARD_EXIT = dt.time(15, 20)


def now_ist() -> dt.datetime:
    return dt.datetime.now(IST)


def is_trading_time(t: dt.datetime) -> bool:
    ti = t.astimezone(IST).time()
    return TRADING_START <= ti <= TRADING_END
