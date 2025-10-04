"""Utilities to aggregate tick data into IST-aligned OHLCV bars."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta
from typing import Dict, List, Literal, Optional
from zoneinfo import ZoneInfo

Timeframe = Literal["5m", "15m", "1d"]

IST = ZoneInfo("Asia/Kolkata")
SESSION_START = time(9, 15)
SESSION_END = time(15, 30)


def _now_ist() -> datetime:
    """Return the current time in IST."""
    return datetime.now(tz=IST)


def _session_bounds(ts: datetime) -> tuple[datetime, datetime]:
    """Return the start/end datetimes of the trading session for ``ts``."""
    base = ts.astimezone(IST)
    start = base.replace(hour=SESSION_START.hour, minute=SESSION_START.minute, second=0, microsecond=0)
    end = base.replace(hour=SESSION_END.hour, minute=SESSION_END.minute, second=0, microsecond=0)
    return start, end


def _next_5m_end(after: datetime) -> datetime:
    """Return the next 5-minute bar end after ``after`` (inclusive)."""
    current = after.astimezone(IST)
    start, end = _session_bounds(current)

    if current < start:
        return start + timedelta(minutes=5)

    if current >= end:
        next_day = (current + timedelta(days=1)).replace(hour=SESSION_START.hour, minute=SESSION_START.minute, second=0, microsecond=0)
        return next_day + timedelta(minutes=5)

    minutes_since_start = int((current - start).total_seconds() // 60)
    next_bucket = (minutes_since_start // 5 + 1) * 5
    return start + timedelta(minutes=next_bucket)


@dataclass
class BarState:
    ts_end: datetime
    o: float
    h: float
    l: float
    c: float
    v: int


class BarBuilder:
    """Maintain per-symbol bar state and emit completed bars on boundaries."""

    def __init__(self, symbols: List[str], tf: Timeframe = "5m"):
        if tf != "5m":
            raise ValueError("BarBuilder currently only supports 5m timeframe")

        self.tf = tf
        self.state: Dict[str, Optional[BarState]] = {symbol: None for symbol in symbols}

    def _ensure_bucket(self, symbol: str, now_ist: datetime, price: float) -> None:
        state = self.state.get(symbol)
        if state is not None:
            return

        ts_end = _next_5m_end(now_ist - timedelta(milliseconds=1))
        self.state[symbol] = BarState(ts_end=ts_end, o=price, h=price, l=price, c=price, v=0)

    def on_tick(
        self,
        symbol: str,
        price: float,
        vol: Optional[int] = None,
        ts: Optional[datetime] = None,
    ) -> None:
        now = ts.astimezone(IST) if ts else _now_ist()
        self._ensure_bucket(symbol, now, price)
        state = self.state[symbol]
        if state is None:
            return

        if price > state.h:
            state.h = price
        if price < state.l:
            state.l = price
        state.c = price

        if vol is not None:
            state.v += int(max(vol, 0))

    def _roll_state(self, state: BarState) -> Optional[BarState]:
        if state.ts_end.astimezone(IST).time() == SESSION_END:
            return None

        next_end = _next_5m_end(state.ts_end)
        return BarState(ts_end=next_end, o=state.c, h=state.c, l=state.c, c=state.c, v=0)

    def maybe_close(self, now: Optional[datetime] = None) -> List[dict]:
        current = now.astimezone(IST) if now else _now_ist()
        completed: List[dict] = []

        for symbol, state in list(self.state.items()):
            if state is None:
                continue

            while current >= state.ts_end:
                completed.append(
                    {
                        "symbol": symbol,
                        "tf": self.tf,
                        "ts_ist": state.ts_end,
                        "o": float(state.o),
                        "h": float(state.h),
                        "l": float(state.l),
                        "c": float(state.c),
                        "v": int(state.v),
                    }
                )

                next_state = self._roll_state(state)
                self.state[symbol] = next_state

                if next_state is None:
                    break

                state = next_state

        return completed
