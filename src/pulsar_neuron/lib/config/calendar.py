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


def session_bounds(date: dt.date) -> dict:
    """Return canonical intraday schedule in IST."""

    return {
        "open": dt.datetime.combine(date, dt.time(9, 15, tzinfo=IST)),
        "trade_start": dt.datetime.combine(date, TRADING_START, tzinfo=IST),
        "trade_end": dt.datetime.combine(date, TRADING_END, tzinfo=IST),
        "hard_exit": dt.datetime.combine(date, HARD_EXIT, tzinfo=IST),
    }
